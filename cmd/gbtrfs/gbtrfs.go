package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/dennwc/btrfs"
	"github.com/spf13/cobra"
)

func init() {
	RootCmd.AddCommand(
		SubvolumeCmd,
		SendCmd,
		ReceiveCmd,
		ScrubCmd,
		DeviceCmd,
	)
	ScrubCmd.AddCommand(
		ScrubStartCmd,
		ScrubStatusCmd,
		ScrubCancelCmd,
	)
	SubvolumeCmd.AddCommand(
		SubvolumeCreateCmd,
		SubvolumeDeleteCmd,
		SubvolumeListCmd,
	)
	DeviceCmd.AddCommand(StatsGet, StatsReset)
	StatsGet.Flags().BoolP("reset", "z", false, "reset the stats after reading")
	StatsGet.Flags().BoolP("check", "c", false, "return a non zero code if any stat counter is not zero")
	StatsGet.Flags().BoolP("tabular", "T", false, "return a non zero code if any stat counter is not zero")
	SendCmd.Flags().StringP("parent", "p", "", "Send an incremental stream from <parent> to <subvol>.")
}

var RootCmd = &cobra.Command{
	Use:   "btrfs [--help] [--version] <group> [<group>...] <command> [<args>]",
	Short: "Use --help as an argument for information on a specific group or command.",
}

var DeviceCmd = &cobra.Command{
	Use:     "device <command> <args>",
	Aliases: []string{"statistics"},
}
var SubvolumeCmd = &cobra.Command{
	Use:     "subvolume <command> <args>",
	Aliases: []string{"subvol", "sub", "sv"},
}
var ScrubCmd = &cobra.Command{
	Use: "scrub <command> <args>",
}
var SubvolumeCreateCmd = &cobra.Command{
	Use:   "create [-i <qgroupid>] [<dest>/]<name>",
	Short: "Create a subvolume",
	Long:  `Create a subvolume <name> in <dest>.  If <dest> is not given subvolume <name> will be created in the current directory.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("subvolume not specified")
		} else if len(args) > 1 {
			return fmt.Errorf("only one subvolume name is allowed")
		}
		return btrfs.CreateSubVolume(args[0])
	},
}

var SubvolumeDeleteCmd = &cobra.Command{
	Use:   "delete [options] <subvolume> [<subvolume>...]",
	Short: "Delete subvolume(s)",
	Long: `Delete subvolumes from the filesystem. The corresponding directory
is removed instantly but the data blocks are removed later.
The deletion does not involve full commit by default due to
performance reasons (as a consequence, the subvolume may appear again
after a crash). Use one of the --commit options to wait until the
operation is safely stored on the media.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		for _, arg := range args {
			if err := btrfs.DeleteSubVolume(arg); err != nil {
				return err
			}
		}
		return nil
	},
}

var SubvolumeListCmd = &cobra.Command{
	Use:     "list <mount>",
	Short:   "List subvolumes",
	Aliases: []string{"ls"},
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected one destination argument")
		}
		fs, err := btrfs.Open(args[0], true)
		if err != nil {
			return err
		}
		defer fs.Close()
		list, err := fs.ListSubvolumes(nil)
		if err == nil {
			for _, v := range list {
				fmt.Printf("%+v\n", v)
			}
		}
		return err
	},
}

var SendCmd = &cobra.Command{
	Use:   "send [-v] [-p <parent>] [-c <clone-src>] [-f <outfile>] <subvol> [<subvol>...]",
	Short: "Send the subvolume(s) to stdout.",
	Long: `Sends the subvolume(s) specified by <subvol> to stdout.
<subvol> should be read-only here.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		parent, _ := cmd.Flags().GetString("parent")
		return btrfs.Send(os.Stdout, parent, args...)
	},
}

var ReceiveCmd = &cobra.Command{
	Use:   "receive [-v] [-f <infile>] [--max-errors <N>] <mount>",
	Short: "Receive subvolumes from stdin.",
	Long: `Receives one or more subvolumes that were previously
sent with btrfs send. The received subvolumes are stored
into <mount>.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected one destination argument")
		}
		return btrfs.Receive(os.Stdin, args[0])
	},
}

var ScrubStartCmd = &cobra.Command{
	Use:   "start <mount>",
	Short: "Start scrubs",
	Long: `Start sscrub on all devices that mount the given path e.g.
	on a raid1 configuration it would start the scrub on both devices,
	while on a non raid configuration only on the single device`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("mount not specified")
		} else if len(args) > 1 {
			return fmt.Errorf("only one mount path is allowed")
		}
		fs, err := btrfs.Open(args[0], false)
		if err != nil {
			return err
		}
		info, err := fs.Info()
		if err != nil {
			return err
		}
		for i := uint64(1); i <= info.MaxID; i++ {
			fmt.Println("starting scrub: ", i)
			if err := fs.ScrubStart(i, 0, math.MaxUint64); err != nil {
				fmt.Println("starting scrub: ", i, " failed", err)
				return err
			}
			fmt.Println("starting scrub: ", i, " ok")
		}
		fmt.Println("starting scrub done")
		return nil
	},
}
var ScrubCancelCmd = &cobra.Command{
	Use:   "cancel <mount>",
	Short: "Cancel scrubs",
	Long: `Cancel a scrub on all devices that back the given mount e.g.
	on a raid1 configuration it would start the scrub on both devices,
	while on a non raid configuration only on the single device`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("mount not specified")
		} else if len(args) > 1 {
			return fmt.Errorf("only one mount path is allowed")
		}
		fs, err := btrfs.Open(args[0], false)
		if err != nil {
			return err
		}
		info, err := fs.Info()
		if err != nil {
			return err
		}
		for i := uint64(1); i <= info.MaxID; i++ {
			if err := fs.ScrubCancel(i); err != nil {
				return err
			}
		}
		return nil
	},
}
var ScrubStatusCmd = &cobra.Command{
	Use:   "status <mount>",
	Short: "Print the status of scrubs",
	Long: `Print the status of scrubs on all devices that back the given mount
	e.g. on  raid1 configuration this will display the scrub status on both devices, on a non raid configuratrion only the scrub status of the single device`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("mount not specified")
		} else if len(args) > 1 {
			return fmt.Errorf("only one mount path is allowed")
		}
		fs, err := btrfs.Open(args[0], false)
		if err != nil {
			return err
		}
		info, err := fs.Info()
		if err != nil {
			return err
		}
		for i := uint64(1); i <= info.MaxID; i++ {
			progress, err := fs.ScrubStatus(i)
			if err != nil {
				return err
			}
			fmt.Printf("scrub status on device %d: %+v", i, progress)
			fmt.Println()
		}
		return nil
	},
}
var StatsReset = &cobra.Command{
	Use:   "stats-reset <mount>",
	Short: "Reset device stats",
	Long:  `Reset device stats on the given device`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("mount not specified")
		} else if len(args) > 1 {
			return fmt.Errorf("only one mount path is allowed")
		}
		fs, err := btrfs.Open(args[0], false)
		if err != nil {
			return err
		}
		info, err := fs.Info()
		if err != nil {
			return err
		}
		for i := uint64(1); i <= info.MaxID; i++ {
			if err := fs.ResetDevStats(i); err != nil {
				return err
			}
		}
		return nil
	},
}
var StatsGet = &cobra.Command{
	Use:   "stats <mount>",
	Short: "Get device stats",
	Long:  `Get device stats on the given device`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("mount not specified")
		} else if len(args) > 1 {
			return fmt.Errorf("only one mount path is allowed")
		}
		resetFlag, err := cmd.Flags().GetBool("reset")
		if err != nil {
			return err
		}
		returnErrorOnNonZeroValues, err := cmd.Flags().GetBool("check")
		if err != nil {
			return err
		}
		tabular, err := cmd.Flags().GetBool("tabular")
		if err != nil {
			return err
		}
		flags := uint64(0)
		if resetFlag {
			fmt.Println("Stats will be reset after reading")
			flags = btrfs.DevStatsFlagsReset
		}

		fs, err := btrfs.Open(args[0], false)
		if err != nil {
			return err
		}
		info, err := fs.Info()
		if err != nil {
			return err
		}
		if tabular {

		}
		hadErros := false
		stats := make([]DeviceWithStats, 0)
		longestPath := 0
		for i := uint64(1); i <= info.MaxID; i++ {
			devInfo, err := fs.GetDevInfo(i)
			if err != nil {
				return err
			}

			stat, err := fs.GetDevStatsWithFlags(i, flags)
			if err != nil {
				return err
			}
			if stat.CorruptionErrs > 0 {
				hadErros = true
			}
			if stat.FlushErrs > 0 {
				hadErros = true
			}
			if stat.GenerationErrs > 0 {
				hadErros = true
			}
			if stat.ReadErrs > 0 {
				hadErros = true
			}
			if stat.WriteErrs > 0 {
				hadErros = true
			}
			for _, v := range stat.Unknown {
				if v > 0 {
					hadErros = true
				}
			}
			if len(devInfo.Path) > longestPath {
				longestPath = len(devInfo.Path)
			}
			stats = append(stats, DeviceWithStats{
				Stats: stat,
				Id:    i,
				Path:  devInfo.Path,
			})
		}
		if tabular {
			fmt.Print("Id")
			fmt.Print(" ")
			fmt.Print("Path")
			fmt.Print(strings.Repeat(" ", longestPath-len("Path")))
			fmt.Print(" ")
			fmt.Print("Write errors")
			fmt.Print(" ")
			fmt.Print("Read errors")
			fmt.Print(" ")
			fmt.Print("Flush errors")
			fmt.Print(" ")
			fmt.Print("Corruption errors")
			fmt.Print(" ")
			fmt.Print("Generation errors")
			fmt.Println("")
			fmt.Print(strings.Repeat("-", len("Id")))
			fmt.Print(" ")
			fmt.Print(strings.Repeat("-", longestPath))
			fmt.Print(" ")
			fmt.Print(strings.Repeat("-", len("Write errors")))
			fmt.Print(" ")
			fmt.Print(strings.Repeat("-", len("Read errors")))
			fmt.Print(" ")
			fmt.Print(strings.Repeat("-", len("Flush errors")))
			fmt.Print(" ")
			fmt.Print(strings.Repeat("-", len("Corruption errors")))
			fmt.Print(" ")
			fmt.Print(strings.Repeat("-", len("Generation errors")))
			fmt.Println(" ")
			for _, v := range stats {
				fmt.Printf("%d  %s", v.Id, v.Path)
				fmt.Print(" ")
				WriteSpaced("Write errors", v.Stats.WriteErrs)
				fmt.Print(" ")
				WriteSpaced("Read errors", v.Stats.ReadErrs)
				fmt.Print(" ")
				WriteSpaced("Flush errors", v.Stats.FlushErrs)
				fmt.Print(" ")
				WriteSpaced("Corruption errors", v.Stats.CorruptionErrs)
				fmt.Print(" ")
				WriteSpaced("Generation errors", v.Stats.GenerationErrs)
				fmt.Println("")
			}
		} else {
			for _, v := range stats {
				fmt.Printf("[%s].write_io_errs:   %d", v.Path, v.Stats.WriteErrs)
				fmt.Println()
				fmt.Printf("[%s].read_io_errs:    %d", v.Path, v.Stats.ReadErrs)
				fmt.Println()
				fmt.Printf("[%s].corruption_errs: %d", v.Path, v.Stats.CorruptionErrs)
				fmt.Println()
				fmt.Printf("[%s].generation_errs: %d", v.Path, v.Stats.GenerationErrs)
				fmt.Println()
			}
		}
		if hadErros && returnErrorOnNonZeroValues {
			return errors.New("some stats had non zero values")
		}
		return nil
	},
}

func WriteSpaced(header string, value uint64) {
	valString := fmt.Sprintf("%d", value)
	fmt.Print(strings.Repeat(" ", len(header)-len(valString)))
	fmt.Print(valString)
}

type DeviceWithStats struct {
	Path  string
	Id    uint64
	Stats btrfs.DevStats
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
