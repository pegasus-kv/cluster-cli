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

package main

import (
	"fmt"

	"github.com/pegasus-kv/cluster-cli/deployment"
	"github.com/spf13/cobra"
)

var (
	startCmd = &cobra.Command{
		Use:   "start <cluster_name> --task {TaskID} --job {replica|meta|collector} --user {user_name}",
		Args:  cobra.ExactArgs(1),
		Short: "Start a single Pegasus node",
		RunE:  runStart,
	}
	stopCmd = &cobra.Command{
		Use:   "stop <cluster_name> --task {TaskID} --job {replica|meta|collector} --user {user_name}",
		Args:  cobra.ExactArgs(1),
		Short: "Stop a single Pegasus node",
		RunE:  runStop,
	}
)

func init() {
	registerOpCmdFlags(startCmd)
	registerOpCmdFlags(stopCmd)
}

func registerOpCmdFlags(cmd *cobra.Command) {
	cmd.Flags().String("job", "", "The type of node to operate. Options: replica|meta|collector")
	cmd.Flags().Int("task", -1, "The node ID to operate.")
	_ = cmd.MarkFlagRequired("job")
	_ = cmd.MarkFlagRequired("task")
}

type nodeOpFunc func(m deployment.Deployment, node deployment.Node) error

func runNodeOp(cmd *cobra.Command, args []string, op nodeOpFunc) error {
	cluster := args[0]
	taskID, _ := cmd.Flags().GetInt("task")

	var jobType deployment.JobType
	jobArg, _ := cmd.Flags().GetString("job")
	switch jobArg {
	case "replica":
		jobType = deployment.JobReplica
	case "meta":
		jobType = deployment.JobMeta
	case "collector":
		jobType = deployment.JobCollector
	}

	m := deployment.NewMinos(cluster, "")
	err := op(m, deployment.Node{Name: fmt.Sprint(taskID), Job: jobType})
	if err != nil {
		fmt.Println(err.Error())
	}

	fmt.Println("Success")
	err = printAllNodes(m)
	if err != nil {
		fmt.Println(err.Error())
	}

	return nil
}

func runStart(cmd *cobra.Command, args []string) error {
	return runNodeOp(cmd, args, func(m deployment.Deployment, node deployment.Node) error {
		return m.StartNode(node)
	})
}

func runStop(cmd *cobra.Command, args []string) error {
	return runNodeOp(cmd, args, func(m deployment.Deployment, node deployment.Node) error {
		return m.StopNode(node)
	})
}
