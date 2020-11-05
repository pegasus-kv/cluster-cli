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
	"pegasus-cluster-cli/client"
	"strconv"
	"strings"
	"time"
)

func RemoveNodes(cluster string, deploy Deployment, metaList string, nodeNames []string) error {
	if err := listAndCacheAllNodes(deploy); err != nil {
		return err
	}
	client, err := NewMetaClient(cluster, metaList)
	if err != nil {
		return err
	}

	nodes := make([]Node, len(nodeNames))
	addrs := make([]string, len(nodeNames))
	for i, name := range nodeNames {
		node, ok := findReplicaNode(name)
		if !ok {
			return errors.New("replica node '" + name + "' not found")
		}
		nodes[i] = node
		addrs[i] = node.IPPort
	}
	if _, err := client.RemoteCommand("meta.lb.assign_secondary_black_list", strings.Join(addrs, ",")); err != nil {
		return err
	}
	if _, err := client.RemoteCommand("meta.live_percentage", "0"); err != nil {
		return err
	}

	fmt.Println()
	for _, node := range nodes {
		if err := removeNode(deploy, client, node); err != nil {
			return err
		}
		fmt.Println()
	}
	return nil
}

func removeNode(deploy Deployment, metaClient MetaClient, node Node) error {
	fmt.Println("Stopping replica node " + node.Name + " of " + node.IPPort + " ...")
	if err := metaClient.SetMetaLevel("steady"); err != nil {
		return err
	}

	if _, err := metaClient.RemoteCommand("meta.lb.assign_delay_ms", "10"); err != nil {
		return err
	}

	// migrate node
	fmt.Println("Migrating primary replicas out of node...")
	if err := metaClient.Migrate(node.IPPort); err != nil {
		return err
	}
	// wait for pri_count == 0
	fmt.Println("Wait " + node.IPPort + " to migrate done...")
	if _, err := waitFor(func() (bool, error) {
		val := 0
		nodes, err := metaClient.ListNodes()
		if err != nil {
			return false, err
		}
		for _, n := range nodes {
			if n.IPPort == node.IPPort {
				val = n.Info.PrimaryCount
				break
			}
		}
		if val == 0 {
			fmt.Println("Migrate done.")
			return true, nil
		}
		fmt.Println("Still " + strconv.Itoa(val) + " primary replicas left on " + node.IPPort)
		return false, nil
	}, time.Second, 0); err != nil {
		return err
	}
	time.Sleep(time.Second)

	// downgrade node and kill partition
	fmt.Println("Downgrading replicas on node...")
	gpids, err := metaClient.Downgrade(node.IPPort)
	if err != nil {
		return err
	}
	// wait for rep_count == 0
	fmt.Println("Wait " + node.IPPort + " to downgrade done...")
	if _, err := waitFor(func() (bool, error) {
		val := 0
		nodes, err := metaClient.ListNodes()
		if err != nil {
			return false, err
		}
		for _, n := range nodes {
			if n.IPPort == node.IPPort {
				val = n.Info.ReplicaCount
				break
			}
		}
		if val == 0 {
			fmt.Println("Downgrade done.")
			return true, nil
		}
		fmt.Println("Still " + strconv.Itoa(val) + " replicas left on " + node.IPPort)
		return false, nil
	}, time.Second, 0); err != nil {
		return err
	}
	time.Sleep(time.Second)

	remoteCmdClient := client.NewRemoteCmdClient(node.IPPort)
	for _, gpid := range gpids {
		if _, err := remoteCmdClient.KillPartition(gpid); err != nil {
			return err
		}
	}

	fmt.Println("Stop node by deployment...")
	if err := deploy.StopNode(node); err != nil {
		return err
	}
	fmt.Println("Stop node by deployment done")
	time.Sleep(time.Second)

	fmt.Println("Wait cluster to become healthy...")
	if _, err := waitFor(func() (bool, error) {
		infos, err := metaClient.ListTableHealthInfos()
		if err != nil {
			return false, err
		}
		count := 0
		for _, info := range infos {
			if info.PartitionCount != info.FullyHealthy {
				count += info.Unhealthy
			}
		}
		if count != 0 {
			fmt.Printf("Cluster not healthy, unhealthy_partition_count = %d\n", count)
			return false, nil
		}
		fmt.Println("Cluster becomes healthy")
		return true, nil
	}, time.Duration(10)*time.Second, 0); err != nil {
		return err
	}

	if _, err := metaClient.RemoteCommand("meta.lb.assign_delay_ms", "DEFAULT"); err != nil {
		return err
	}
	return nil
}
