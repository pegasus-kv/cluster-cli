package cmd

import (
	"errors"
	"log"
	"os"
	"pegasus-cluster-cli"

	"github.com/spf13/cobra"
)

var (
	all      bool
	cluster  string
	metaList string
	nodes    []string
	shellDir string
	RootCmd  = &cobra.Command{
		Use:   "pegasus-cluster-cli",
		Short: "A command line app to easily add/remove/update nodes in pegasus cluster",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if shellDir == "" {
				return errors.New("pegasus-shell-dir is empty, set flag --shell-dir or env PEGASUS_SHELL_PATH")
			}
			pegasus.SetShellDir(shellDir)
			if Validate != nil {
				return Validate()
			}
			return nil
		},
	}
	addNodeCmd = &cobra.Command{
		Use:   "add-node",
		Short: "add a list of nodes to cluster",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if nodes == nil {
				return errors.New("list of nodes must be provided")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			deploy := pegasus.CreateDeployment(cluster)
			if err := pegasus.AddNodes(cluster, deploy, metaList, nodes); err != nil {
				log.Fatalf("%+v", err)
			}
		},
	}
	removeNodeCmd = &cobra.Command{
		Use:   "remove-node",
		Short: "remove a list of nodes from cluster",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if nodes == nil {
				return errors.New("list of nodes must be provided")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			deploy := pegasus.CreateDeployment(cluster)
			if err := pegasus.RemoveNodes(cluster, deploy, metaList, nodes); err != nil {
				log.Fatalf("%+v", err)
			}
		},
	}
	rollingUpdateCmd = &cobra.Command{
		Use:   "rolling-update",
		Short: "update a list of nodes or update all meta/replica/collector",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if all {
				nodes = nil
			} else if len(nodes) == 0 {
				return errors.New("when --all/-a is not specified, a list of nodes(--node/-n) is required")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			deploy := pegasus.CreateDeployment(cluster)
			if err := pegasus.RollingUpdateNodes(cluster, deploy, metaList, nodes); err != nil {
				log.Fatalf("%+v", err)
			}
		},
	}

	Validate func() error = nil
)

func init() {
	RootCmd.PersistentFlags().StringVarP(&cluster, "cluster", "c", "", "name of the cluster to take action on")
	RootCmd.PersistentFlags().StringVarP(&metaList, "meta-list", "m", "", "a list of meta servers(ip:port), seperated by comma")
	RootCmd.PersistentFlags().StringArrayVarP(&nodes, "node", "n", []string{}, "list of nodes to take action on")
	RootCmd.PersistentFlags().StringVar(&shellDir, "shell-dir", os.Getenv("PEGASUS_SHELL_PATH"), "directory of pegasus binary package. Could be set from env PEGASUS_SHELL_PATH")
	RootCmd.MarkPersistentFlagRequired("cluster")
	RootCmd.MarkPersistentFlagRequired("meta-list")
	RootCmd.MarkPersistentFlagDirname("shell-dir")
	rollingUpdateCmd.Flags().BoolVarP(&all, "all", "a", false, "whether to update all nodes")
	RootCmd.AddCommand(addNodeCmd, removeNodeCmd, rollingUpdateCmd)
}

func Execute() error {
	return RootCmd.Execute()
}
