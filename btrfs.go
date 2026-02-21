package btrfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/dennwc/ioctl"
)

const SuperMagic uint32 = 0x9123683E

func CloneFile(dst, src *os.File) error {
	return iocClone(dst, src)
}

func Open(path string, ro bool) (*FS, error) {
	if ok, err := IsSubVolume(path); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrNotBtrfs{Path: path}
	}
	var (
		dir *os.File
		err error
	)
	if ro {
		dir, err = os.OpenFile(path, os.O_RDONLY|syscall.O_NOATIME, 0644)
		if err != nil {
			// Try without O_NOATIME as it requires ownership of the file
			// or other priviliges
			dir, err = os.OpenFile(path, os.O_RDONLY, 0644)
		}
	} else {
		dir, err = os.Open(path)
	}
	if err != nil {
		return nil, err
	} else if st, err := dir.Stat(); err != nil {
		dir.Close()
		return nil, err
	} else if !st.IsDir() {
		dir.Close()
		return nil, fmt.Errorf("not a directory: %s", path)
	}
	return &FS{f: dir}, nil
}

type FS struct {
	f *os.File
}

func (f *FS) Close() error {
	return f.f.Close()
}

type Info struct {
	MaxID          uint64
	NumDevices     uint64
	FSID           FSID
	NodeSize       uint32
	SectorSize     uint32
	CloneAlignment uint32
}

func (f *FS) SubVolumeID() (uint64, error) {
	id, err := getFileRootID(f.f)
	if err != nil {
		return 0, err
	}
	return uint64(id), nil
}

func (f *FS) Info() (out Info, err error) {
	var arg btrfs_ioctl_fs_info_args
	arg, err = iocFsInfo(f.f)
	if err == nil {
		out = Info{
			MaxID:          arg.max_id,
			NumDevices:     arg.num_devices,
			FSID:           arg.fsid,
			NodeSize:       arg.nodesize,
			SectorSize:     arg.sectorsize,
			CloneAlignment: arg.clone_alignment,
		}
	}
	return
}

type DevInfo struct {
	UUID       UUID
	BytesUsed  uint64
	TotalBytes uint64
	Path       string
}

func (f *FS) GetDevInfo(id uint64) (out DevInfo, err error) {
	var arg btrfs_ioctl_dev_info_args
	arg.devid = id

	if err = ioctl.Do(f.f, _BTRFS_IOC_DEV_INFO, &arg); err != nil {
		return
	}
	out.UUID = arg.uuid
	out.BytesUsed = arg.bytes_used
	out.TotalBytes = arg.total_bytes
	out.Path = stringFromBytes(arg.path[:])

	return
}

type DevStatsFlags = uint64

const (
	DevStatsFlagsReset DevStatsFlags = _BTRFS_DEV_STATS_RESET
)

type DevStats struct {
	WriteErrs uint64
	ReadErrs  uint64
	FlushErrs uint64
	// Checksum error, bytenr error or contents is illegal: this is an
	// indication that the block was damaged during read or write, or written to
	// wrong location or read from wrong location.
	CorruptionErrs uint64
	// An indication that blocks have not been written.
	GenerationErrs uint64
	Unknown        []uint64
}

func (f *FS) GetDevStatsWithFlags(id uint64, flags uint64) (out DevStats, err error) {
	var arg btrfs_ioctl_get_dev_stats
	arg.devid = id
	arg.nr_items = _BTRFS_DEV_STAT_VALUES_MAX
	arg.flags = flags
	if err = ioctl.Do(f.f, _BTRFS_IOC_GET_DEV_STATS, &arg); err != nil {
		return
	}
	i := 0
	out.WriteErrs = arg.values[i]
	i++
	out.ReadErrs = arg.values[i]
	i++
	out.FlushErrs = arg.values[i]
	i++
	out.CorruptionErrs = arg.values[i]
	i++
	out.GenerationErrs = arg.values[i]
	i++
	if int(arg.nr_items) > i {
		out.Unknown = arg.values[i:arg.nr_items]
	}
	return
}
func (f *FS) GetDevStats(id uint64) (out DevStats, err error) {
	return f.GetDevStatsWithFlags(id, 0)
}
func (f *FS) ResetDevStats(id uint64) (err error) {
	var arg btrfs_ioctl_get_dev_stats
	arg.devid = id
	arg.nr_items = _BTRFS_DEV_STAT_VALUES_MAX
	arg.flags = DevStatsFlagsReset
	return ioctl.Do(f.f, _BTRFS_IOC_GET_DEV_STATS, &arg)
}

type ScrubProgress struct {
	DataExtentsScrubbed uint64 // # of data extents scrubbed
	TreeExtentsScrubbed uint64 // # of tree extents scrubbed
	DataBytesScrubbed   uint64 // # of data bytes scrubbed
	TreeBytesScrubbed   uint64 // # of tree bytes scrubbed
	ReadErrors          uint64 // # of read errors encountered (EIO)
	CsumErrors          uint64 // # of failed csum checks
	// # of occurences, where the metadata of a tree block did not match the expected values, like generation or logical
	VerifyErrors uint64
	// # of 4k data block for which no csum is present, probably the result of data written with nodatasum
	NoCsum              uint64
	CsumDiscards        uint64 // # of csum for which no data was found in the extent tree.
	SuperErrors         uint64 // # of bad super blocks encountered
	MallocErrors        uint64 // # of internal kmalloc errors. These will likely cause an incomplete scrub
	UncorrectableErrors uint64 // # of errors where either no intact copy was found or the writeback failed
	CorrectedErrors     uint64 // # of errors corrected
	// last physical address scrubbed. In case a scrub was aborted, this can be used to restart the scrub
	LastPhysical uint64
	// # of occurences where a read for a full (64k) bio failed, but the re-
	// check succeeded for each 4k piece. Intermittent error.
	UnverifiedErrors uint64
}

// Start a scrub on the given device, starting at start blocks and ending on end blocks
// If you want to scan the whole device, set start to 0 and end to the maximal value of uint64( for example via math.MaxUint64)
// Another option is to resume a earlier interrupted scrub, by setting the start to the same value as reported in LastPhysical (can be retrieved via .ScrubStatus)
// WARNING: This method WILL BLOCK until the scrub is done, or the scrub is cancelled
// Scrub operations requiere CAP_SYSADMIN or root
func (f *FS) ScrubStart(dev uint64, start uint64, end uint64) error {
	var arg btrfs_ioctl_scrub_args
	arg.devid = dev
	arg.flags = 0
	arg.start = start
	arg.end = end
	return iocScrub(f.f, &arg)
}

// Cancel a scrub on the given device
// Scrub operations requiere CAP_SYSADMIN or root
func (f *FS) ScrubCancel(dev uint64) error {
	return iocScrubCancel(f.f)
}

// Get the progress of a scrub on the given device
// Scrub operations requiere CAP_SYSADMIN or root
func (f *FS) ScrubStatus(dev uint64) (ScrubProgress, error) {
	var arg btrfs_ioctl_scrub_args
	arg.devid = dev
	arg.flags = 0
	if err := iocScrubProgress(f.f, &arg); err != nil {
		return ScrubProgress{}, err
	}
	return ScrubProgress{
		arg.progress.data_extents_scrubbed,
		arg.progress.tree_extents_scrubbed,
		arg.progress.data_bytes_scrubbed,
		arg.progress.tree_bytes_scrubbed,
		arg.progress.read_errors,
		arg.progress.csum_errors,
		arg.progress.verify_errors,
		arg.progress.no_csum,
		arg.progress.csum_discards,
		arg.progress.super_errors,
		arg.progress.malloc_errors,
		arg.progress.uncorrectable_errors,
		arg.progress.corrected_errors,
		arg.progress.last_physical,
		arg.progress.unverified_errors,
	}, nil
}

type FSFeatureFlags struct {
	Compatible   FeatureFlags
	CompatibleRO FeatureFlags
	Incompatible IncompatFeatures
}

func (f *FS) GetFeatures() (out FSFeatureFlags, err error) {
	var arg btrfs_ioctl_feature_flags
	if err = ioctl.Do(f.f, _BTRFS_IOC_GET_FEATURES, &arg); err != nil {
		return
	}
	out = FSFeatureFlags{
		Compatible:   arg.compat_flags,
		CompatibleRO: arg.compat_ro_flags,
		Incompatible: arg.incompat_flags,
	}
	return
}

func (f *FS) GetSupportedFeatures() (out FSFeatureFlags, err error) {
	var arg [3]btrfs_ioctl_feature_flags
	if err = ioctl.Do(f.f, _BTRFS_IOC_GET_SUPPORTED_FEATURES, &arg); err != nil {
		return
	}
	out = FSFeatureFlags{
		Compatible:   arg[0].compat_flags,
		CompatibleRO: arg[0].compat_ro_flags,
		Incompatible: arg[0].incompat_flags,
	}
	//for i, a := range arg {
	//	out[i] = FSFeatureFlags{
	//		Compatible:   a.compat_flags,
	//		CompatibleRO: a.compat_ro_flags,
	//		Incompatible: a.incompat_flags,
	//	}
	//}
	return
}

func (f *FS) GetFlags() (SubvolFlags, error) {
	return iocSubvolGetflags(f.f)
}

func (f *FS) SetFlags(flags SubvolFlags) error {
	return iocSubvolSetflags(f.f, flags)
}

func (f *FS) Sync() (err error) {
	if err = ioctl.Ioctl(f.f, _BTRFS_IOC_START_SYNC, 0); err != nil {
		return
	}
	return ioctl.Ioctl(f.f, _BTRFS_IOC_WAIT_SYNC, 0)
}

func (f *FS) CreateSubVolume(name string) error {
	return CreateSubVolume(filepath.Join(f.f.Name(), name))
}

func (f *FS) DeleteSubVolume(name string) error {
	return DeleteSubVolume(filepath.Join(f.f.Name(), name))
}

func (f *FS) Snapshot(dst string, ro bool) error {
	return SnapshotSubVolume(f.f.Name(), filepath.Join(f.f.Name(), dst), ro)
}

func (f *FS) SnapshotSubVolume(name string, dst string, ro bool) error {
	return SnapshotSubVolume(filepath.Join(f.f.Name(), name),
		filepath.Join(f.f.Name(), dst), ro)
}

func (f *FS) Send(w io.Writer, parent string, subvols ...string) error {
	if parent != "" {
		parent = filepath.Join(f.f.Name(), parent)
	}
	sub := make([]string, 0, len(subvols))
	for _, s := range subvols {
		sub = append(sub, filepath.Join(f.f.Name(), s))
	}
	return Send(w, parent, sub...)
}

func (f *FS) Receive(r io.Reader) error {
	return Receive(r, f.f.Name())
}

func (f *FS) ReceiveTo(r io.Reader, mount string) error {
	return Receive(r, filepath.Join(f.f.Name(), mount))
}

func (f *FS) ListSubvolumes(filter func(SubvolInfo) bool) ([]SubvolInfo, error) {
	m, err := listSubVolumes(f.f, filter)
	if err != nil {
		return nil, err
	}
	out := make([]SubvolInfo, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out, nil
}

func (f *FS) SubvolumeByUUID(uuid UUID) (*SubvolInfo, error) {
	id, err := lookupUUIDSubvolItem(f.f, uuid)
	if err != nil {
		return nil, err
	}
	return subvolSearchByRootID(f.f, id, "")
}

func (f *FS) SubvolumeByReceivedUUID(uuid UUID) (*SubvolInfo, error) {
	id, err := lookupUUIDReceivedSubvolItem(f.f, uuid)
	if err != nil {
		return nil, err
	}
	return subvolSearchByRootID(f.f, id, "")
}

func (f *FS) SubvolumeByPath(path string) (*SubvolInfo, error) {
	return subvolSearchByPath(f.f, path)
}

func (f *FS) Usage() (UsageInfo, error) { return spaceUsage(f.f) }

func (f *FS) Balance(flags BalanceFlags) (BalanceProgress, error) {
	args := btrfs_ioctl_balance_args{flags: flags}
	err := iocBalanceV2(f.f, &args)
	return args.stat, err
}

func (f *FS) Resize(size int64) error {
	amount := strconv.FormatInt(size, 10)
	args := &btrfs_ioctl_vol_args{}
	args.SetName(amount)
	if err := iocResize(f.f, args); err != nil {
		return fmt.Errorf("resize failed: %v", err)
	}
	return nil
}

func (f *FS) ResizeToMax() error {
	args := &btrfs_ioctl_vol_args{}
	args.SetName("max")
	if err := iocResize(f.f, args); err != nil {
		return fmt.Errorf("resize failed: %v", err)
	}
	return nil
}
