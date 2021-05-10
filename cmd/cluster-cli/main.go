package main

import "github.com/spf13/cobra"

var (
	all      bool
	cluster  string
	metaList string
	node     string

	rootCmd = &cobra.Command{
		Use:   "pegasus-cluster-cli",
		Short: "A command-line tool to easily add/remove/update nodes in pegasus cluster",
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&cluster, "cluster", "c", "", "Name of the cluster to take action on")
	rootCmd.PersistentFlags().StringVarP(&node, "node", "n", "", "The node to take action on")
	_ = rootCmd.MarkPersistentFlagRequired("cluster")

	rootCmd.AddCommand(addNodeCmd, removeNodeCmd, rollingUpdateCmd)
}
