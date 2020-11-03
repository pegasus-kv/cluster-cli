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

package pegasus

import (
	"errors"
	"fmt"
)

func AddNodes(cluster string, deploy Deployment, metaList string, nodeNames []string) error {
	if err := initNodes(deploy); err != nil {
		return err
	}
	client, err := NewMetaClient(cluster, metaList)
	if err != nil {
		return err
	}

	if err = client.SetMetaLevel("steady"); err != nil {
		return err
	}

	fmt.Println()
	for _, name := range nodeNames {
		node, ok := findReplicaNode(name)
		if !ok {
			return errors.New("replica node '" + name + "' not found")
		}
		fmt.Println("Starting node " + node.IPPort + " by deployment...")
		if err := deploy.StartNode(node); err != nil {
			return err
		}
		fmt.Println("Starting node by deployment done")
		fmt.Println()
	}
	if err := client.Rebalance(false); err != nil {
		return err
	}

	return nil
}
