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
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

func RemoveNodes(cluster string, deploy Deployment, metaList string, nodeNames []string) error {
	if err := listAndCacheAllNodes(deploy); err != nil {
		return err
	}
	client, err := NewMetaClient(cluster, metaList)
	if err != nil {
		return err
	}

	nodes := make([]*ReplicaNode, len(nodeNames))
	addrs := make([]string, len(nodeNames))
	for i, name := range nodeNames {
		node, ok := findReplicaNode(name)
		if !ok {
			return errors.New("replica node '" + name + "' not found")
		}
		nodes[i] = node
		addrs[i] = node.IPPort()
	}
	if _, err := client.RemoteCommand("meta.lb.assign_secondary_black_list", strings.Join(addrs, ",")); err != nil {
		return err
	}
	if _, err := client.RemoteCommand("meta.live_percentage", "0"); err != nil {
		return err
	}

	for _, node := range nodes {
		if err := removeNode(deploy, client, node); err != nil {
			return err
		}
	}

	if _, err := client.RemoteCommand("meta.lb.assign_secondary_black_list clear"); err != nil {
		return err
	}
	return nil
}

func removeNode(deploy Deployment, metaClient MetaClient, node *ReplicaNode) error {
	log.Printf("Stopping replica node %s of %s ...", node.Name(), node.IPPort())
	if err := metaClient.SetMetaLevel("steady"); err != nil {
		return err
	}

	if _, err := metaClient.RemoteCommand("meta.lb.assign_delay_ms", "10"); err != nil {
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

	log.Print("Stop node by deployment...")
	if err := deploy.StopNode(node); err != nil {
		return err
	}
	log.Print("Stop node by deployment done")
	time.Sleep(time.Second)

	if _, err := waitNodeHealthy(node, metaClient); err != nil {
		return err
	}

	if _, err := metaClient.RemoteCommand("meta.lb.assign_delay_ms", "DEFAULT"); err != nil {
		return err
	}
	return nil
}
