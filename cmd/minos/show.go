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
	"os"
	"sort"
	"strconv"

	"github.com/pegasus-kv/admin-cli/tabular"
	"github.com/pegasus-kv/cluster-cli/deployment"
	"github.com/spf13/cobra"
)

var (
	showCmd = &cobra.Command{
		Use:   "show <cluster_name>",
		Args:  cobra.ExactArgs(1),
		Short: "Show the status of Pegasus nodes",
		RunE:  runShow,
	}
)

func runShow(cmd *cobra.Command, args []string) error {
	cluster := args[0]

	// user name is not required for `show`
	m := deployment.NewMinos(cluster, "")
	return printAllNodes(m)
}

func printAllNodes(m deployment.Deployment) error {
	nodes, err := m.ListAllNodes()
	if err != nil {
		return err
	}

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Job == nodes[j].Job {
			idI, _ := strconv.Atoi(nodes[i].Name)
			idJ, _ := strconv.Atoi(nodes[j].Name)
			return idI < idJ
		}
		return nodes[i].Job < nodes[j].Job
	})

	var rows []interface{}
	for _, n := range nodes {
		rows = append(rows, n)
	}
	tabular.Print(os.Stdout, rows)
	return nil
}
