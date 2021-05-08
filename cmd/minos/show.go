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

	"github.com/pegasus-kv/admin-cli/tabular"
	pegasus "github.com/pegasus-kv/cluster-cli"
	"github.com/pegasus-kv/cluster-cli/deployment"
	"github.com/spf13/cobra"
)

func runShow(cmd *cobra.Command, args []string) error {
	cluster := args[0]

	// user name is not required for `show`
	m := deployment.NewMinos(cluster, "")
	nodes, err := m.ListAllNodes()
	if err != nil {
		return err
	}

	type nodeCmdRow struct {
		ID     string
		Job    pegasus.JobType
		IpPort string `json:"ip_port"`
	}
	var rows []interface{}
	for _, n := range nodes {
		rows = append(rows, nodeCmdRow{
			ID:     n.Name(),
			Job:    n.Job(),
			IpPort: n.IPPort(),
		})
	}
	tabular.Print(os.Stdout, rows)
	return nil
}
