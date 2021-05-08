package main

import (
	"github.com/pegasus-kv/cluster-cli/deployment"
	"github.com/spf13/cobra"
)

var (
	rollingUpdateCmd = &cobra.Command{
		Use:   "rolling-update <cluster_name> --task {TaskID} --job {replica|meta|collector} --user {user_name}",
		Args:  cobra.ExactArgs(1),
		Short: "Rolling-update a single Pegasus node",
		RunE:  runRollingUpdate,
	}
)

func init() {
	registerOpCmdFlags(rollingUpdateCmd)
}

func runRollingUpdate(cmd *cobra.Command, args []string) error {
	return runNodeOp(cmd, args, func(m deployment.Deployment, node deployment.Node) error {
		return m.RollingUpdate(node)
	})
}
