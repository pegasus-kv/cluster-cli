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
	"time"

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

	// TODO(wutao): add a log
	if _, err := metaClient.RemoteCommand("meta.lb.add_secondary_max_count_for_one_node", "0"); err != nil {
		return err
	}

	if err := migratePrimariesOutOfNode(metaClient, node); err != nil {
		return err
	}

	log.Print("Downgrading replicas on node...")
	c := 0
	var gpids []string
	var err error
	fin, err := waitFor(func() (bool, error) {
		if c%10 == 0 {
			gpids, err = metaClient.Downgrade(node.IPPort())
			if err != nil {
				return false, err
			}
			log.Print("Sent downgrade propose")
		}
		if err := node.updateInfo(metaClient); err != nil {
			return false, err
		}
		log.Printf("Still %d primary replicas left on %s", node.PrimaryCount, node.IPPort())
		c++
		return node.PrimaryCount == 0, nil
	}, time.Second, 28)
	if err != nil {
		return err
	}
	if fin {
		log.Print("Downgrade done")
	} else {
		log.Print("Downgrade timeout")
	}
	time.Sleep(time.Second)

	// TODO: Check replicas closed on node here
	remoteCmdClient := client.NewRemoteCmdClient(node.IPPort())
	c = 0
	log.Print("Checking replicas closed on node...")
	fin, err = waitFor(func() (bool, error) {
		if c%10 == 0 {
			log.Print("Send kill_partition commands to node...")
			for _, gpid := range gpids {
				if _, err := remoteCmdClient.KillPartition(gpid); err != nil {
					return false, err
				}
			}
			log.Printf("Sent to %d partitions.", len(gpids))
		}
		counters, err := remoteCmdClient.GetPerfCounters(".*replica(Count)")
		if err != nil {
			return false, err
		}
		count := 0
		for _, counter := range counters {
			count += int(counter.Value)
		}
		log.Printf("Still %d replicas not closed on %s", count, node.IPPort())
		c++
		return count == 0, nil
	}, time.Second, 28)
	if err != nil {
		return err
	}
	if fin {
		log.Print("Close done.")
	} else {
		log.Print("Close timeout.")
	}

	if _, err := remoteCmdClient.Call("flush_log", nil); err != nil {
		return err
	}

	if _, err := metaClient.RemoteCommand("meta.lb.add_secondary_max_count_for_one_node", "100"); err != nil {
		return err
	}

	log.Print("Rolling update by deployment...")
	if err := deploy.RollingUpdate(node); err != nil {
		return err
	}
	log.Print("Rolling update by deployment done")

	log.Printf("Wait %s to become alive...", node.IPPort())
	if _, err := waitFor(func() (bool, error) {
		if err := node.updateInfo(metaClient); err != nil {
			return false, err
		}
		return node.Status == "ALIVE", nil
	}, time.Second, 0); err != nil {
		return err
	}

	log.Printf("Wait %s to become healthy...", node.IPPort())
	if _, err := waitFor(func() (bool, error) {
		infos, err := metaClient.ListTableHealthInfos()
		if err != nil {
			return false, err
		}
		count := 0
		for _, info := range infos {
			if info.PartitionCount != info.FullyHealthy {
				count++
			}
		}
		if count != 0 {
			log.Printf("Cluster not healthy, unhealthy app count %d", count)
			return false, nil
		}
		log.Print("Cluster becomes healthy")
		return true, nil
	}, time.Duration(10)*time.Second, 0); err != nil {
		return err
	}

	return nil
}

func migratePrimariesOutOfNode(metaClient MetaClient, node *ReplicaNode) error {
	c := 0
	log.Print("migrating primary replicas out of node...")
	fin, err := waitFor(func() (bool, error) {
		if c%10 == 0 {
			if err := metaClient.Migrate(node.IPPort()); err != nil {
				return false, err
			}
			log.Print("proposed primaries migration to MetaServer")
		}
		if err := node.updateInfo(metaClient); err != nil {
			return false, err
		}
		log.Printf("Still %d primary replicas left on %s", node.PrimaryCount, node.IPPort())
		c++
		return node.PrimaryCount == 0, nil
	}, time.Second, 28)

	if err != nil {
		return err
	}
	if fin {
		log.Print("migrate done")
	} else {
		log.Print("migrate timeout")
	}
	time.Sleep(time.Second)

	return nil
}
