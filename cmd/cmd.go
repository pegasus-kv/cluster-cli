/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"errors"
	"fmt"
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
		Short: "A command line tool to easily add/remove/update nodes in pegasus cluster",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if shellDir == "" {
				return errors.New("pegasus-shell-dir is empty, set flag --shell-dir or env PEGASUS_SHELL_PATH")
			}
			pegasus.SetShellDir(shellDir)
			if ValidateEnvsHook != nil {
				return ValidateEnvsHook()
			}
			return nil
		},
	}
	addNodeCmd = &cobra.Command{
		Use:   "add-node",
		Short: "Add a list of nodes to the pegasus cluster",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if len(nodes) == 0 {
				return errors.New("list of nodes must be provided")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			deploy := pegasus.CreateDeployment(cluster)
			if err := pegasus.AddNodes(cluster, deploy, metaList, nodes); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
	removeNodeCmd = &cobra.Command{
		Use:   "remove-node",
		Short: "Remove a list of nodes from cluster",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if len(nodes) == 0 {
				return errors.New("list of nodes must be provided")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			deploy := pegasus.CreateDeployment(cluster)
			if err := pegasus.RemoveNodes(cluster, deploy, metaList, nodes); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
	rollingUpdateCmd = &cobra.Command{
		Use:   "rolling-update",
		Short: "Update a list of replica nodes or update all meta/replica/collector nodes",
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
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}

	// ValidateEnvsHook validates the customized environment variables (whhich can set from flags)
	// before the execution of command.
	ValidateEnvsHook func() error = nil
)

func init() {
	RootCmd.PersistentFlags().StringVarP(&cluster, "cluster", "c", "", "name of the cluster to take action on")
	RootCmd.PersistentFlags().StringVarP(&metaList, "meta-list", "m", "", "a list of meta servers(ip:port), seperated by comma")
	RootCmd.PersistentFlags().StringArrayVarP(&nodes, "node", "n", []string{}, "list of nodes to take action on")
	RootCmd.PersistentFlags().StringVar(&shellDir, "shell-dir", os.Getenv("PEGASUS_SHELL_PATH"), "directory of pegasus binary package. Could be set from env PEGASUS_SHELL_PATH")
	_ = RootCmd.MarkPersistentFlagRequired("cluster")
	_ = RootCmd.MarkPersistentFlagRequired("meta-list")
	_ = RootCmd.MarkPersistentFlagDirname("shell-dir")
	rollingUpdateCmd.Flags().BoolVarP(&all, "all", "a", false, "whether to update all nodes")
	RootCmd.AddCommand(addNodeCmd, removeNodeCmd, rollingUpdateCmd)
}

func Execute() error {
	return RootCmd.Execute()
}
