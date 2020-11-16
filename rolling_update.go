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
	"pegasus-cluster-cli/client"

	log "github.com/sirupsen/logrus"
)

// RollingUpdateNodes implements the rolling-update command.
// If `nodeNames` are given nil, it means rolling-update on all nodes, including Meta/Collector.
// If not nil, it executes rolling-update on only the Replica nodes specified.
func RollingUpdateNodes(cluster string, deploy Deployment, metaList string, nodeNames []string) error {
	if err := listAndCacheAllNodes(deploy); err != nil {
		return err
	}
	client, err := NewMetaClient(cluster, metaList)
	if err != nil {
		return err
	}

	// preparation: stop automatic rebalance
	if err := client.SetMetaLevel("steady"); err != nil {
		return err
	}

	if nodeNames == nil {
		for _, n := range globalAllNodes {
			if rn, ok := n.(*ReplicaNode); ok {
				if err := rollingUpdateNode(deploy, client, rn); err != nil {
					return err
				}
			}
		}
	} else {
		for _, name := range nodeNames {
			node, ok := findReplicaNode(name)
			if !ok {
				return errors.New("replica node '" + name + "' not found")
			}
			if err := rollingUpdateNode(deploy, client, node); err != nil {
				return err
			}
		}
	}

	if _, err := client.RemoteCommand("meta.lb.add_secondary_max_count_for_one_node", "DEFAULT"); err != nil {
		return err
	}
	if nodeNames == nil {
		log.Print("Rolling update meta servers...")
		for _, node := range globalAllNodes {
			if node.Job() == JobMeta {
				if err := deploy.RollingUpdate(node); err != nil {
					return err
				}
			}
		}
		log.Print("Rolling update meta servers done")
		log.Print("Rolling update collectors...")
		for _, node := range globalAllNodes {
			if node.Job() == JobCollector {
				if err := deploy.RollingUpdate(node); err != nil {
					return err
				}
			}
		}
		log.Print("Rolling update collectors done")

		if err := client.Rebalance(false); err != nil {
			return err
		}
	}

	return nil
}

// rolling-update a single node.
func rollingUpdateNode(deploy Deployment, metaClient MetaClient, node *ReplicaNode) error {
	log.Printf("rolling update replica node \"%s\" [%s]", node.Name(), node.IPPort())

	log.Print("Set lb.add_secondary_max_count_for_one_node to 0...")
	if _, err := metaClient.RemoteCommand("meta.lb.add_secondary_max_count_for_one_node", "0"); err != nil {
		return err
	}

	if err := migratePrimariesOutOfNode(metaClient, node); err != nil {
		return err
	}

	gpids, err := downgradeReplicasOnNode(metaClient, node)
	if err != nil {
		return err
	}

	remoteCmdClient := client.NewRemoteCmdClient(node.IPPort())
	if err := CloseReplicasOnNode(remoteCmdClient, node, gpids); err != nil {
		return err
	}

	if _, err := remoteCmdClient.Call("flush_log", nil); err != nil {
		return err
	}

	log.Print("Set lb.add_secondary_max_count_for_one_node to 100...")
	if _, err := metaClient.RemoteCommand("meta.lb.add_secondary_max_count_for_one_node", "100"); err != nil {
		return err
	}

	log.Print("Rolling update by deployment...")
	if err := deploy.RollingUpdate(node); err != nil {
		return err
	}
	log.Print("Rolling update by deployment done")

	if _, err := waitNodeAlive(node, metaClient); err != nil {
		return err
	}

	if _, err := waitNodeHealthy(node, metaClient); err != nil {
		return err
	}

	return nil
}
