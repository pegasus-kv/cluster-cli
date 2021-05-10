package main

import (
	"errors"
	"fmt"
	"os"

	pegasus "github.com/pegasus-kv/cluster-cli"
	"github.com/pegasus-kv/cluster-cli/deployment"
	"github.com/spf13/cobra"
)

var (
	rollingUpdateCmd = &cobra.Command{
		Use:   "rolling-update",
		Short: "Upgrade one replica node or upgrade all meta/replica/collector nodes",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if all {
				nodes = nil
			}
			if len(nodes) == 0 {
				return errors.New("when --all/-a is not specified, a list of nodes(--node/-n) is required")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			deploy := deployment.CreateDeployment(cluster)
			if err := pegasus.RollingUpdateNodes(cluster, deploy, metaList, nodes); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
)

func init() {
	rollingUpdateCmd.Flags().BoolVarP(&all, "all", "a", false, "Whether to update all nodes")
}
