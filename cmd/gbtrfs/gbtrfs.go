package main

import (
	"fmt"
	"math"
	"os"

	"github.com/dennwc/btrfs"
	"github.com/spf13/cobra"
)

func init() {
	RootCmd.AddCommand(
		SubvolumeCmd,
		SendCmd,
		ReceiveCmd,
		ScrubCmd,
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

	SendCmd.Flags().StringP("parent", "p", "", "Send an incremental stream from <parent> to <subvol>.")
}

var RootCmd = &cobra.Command{
	Use:   "btrfs [--help] [--version] <group> [<group>...] <command> [<args>]",
	Short: "Use --help as an argument for information on a specific group or command.",
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
	Short: "Start a scrub",
	Long:  `Start a scrub on all devices that mount the given path`,
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
	Short: "Start a scrub",
	Long:  `Start a scrub on all devices that mount the given path`,
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
	Short: "Start a scrub",
	Long:  `Start a scrub on all devices that mount the given path`,
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

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
