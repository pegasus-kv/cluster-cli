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
	"strings"
	"time"

	"github.com/pegasus-kv/cluster-cli/deployment"
	"github.com/pegasus-kv/cluster-cli/meta"
	log "github.com/sirupsen/logrus"
)

func RemoveNode(cluster string, deploy deployment.Deployment, metaList string, node string) error {
	meta, err := newMeta(cluster, deploy)
	if err != nil {
		return err
	}

	node, ok := findReplicaNode(name)
	if !ok {
		return errors.New("replica node '" + name + "' not found")
	}
	if err := client.AssignSecondaryBlackList(strings.Join(addrs, ",")); err != nil {
		return err
	}
	if err := client.SetNodeLivePercentageZero(); err != nil {
		return err
	}

	if err := removeNode(deploy, client, node); err != nil {
		return err
	}
	return nil
}

func removeNode(deploy deployment.Deployment, metaClient meta.Meta, node deployment.Node) error {
	log.Printf("Stopping replica node %s of %s ...", node.Name(), node.IPPort())
	if err := metaClient.SetMetaLevelSteady(); err != nil {
		return err
	}
	if err := metaClient.SetAssignDelayMs(10); err != nil {
		return err
	}

	// migrate node
	log.Print("Migrating primary replicas out of node...")
	if err := metaClient.MigratePrimariesOut(node); err != nil {
		return err
	}

	// downgrade node and kill partition
	log.Print("Downgrading replicas on node...")
	gpids, err := metaClient.Downgrade(node.IPPort())
	if err != nil {
		return err
	}
	// wait for rep_count == 0
	log.Printf("Wait %s to downgrade done...", node.IPPort())
	if _, err := waitFor(func() (bool, error) {
		val := 0
		nodes, err := metaClient.ListNodes()
		if err != nil {
			return false, err
		}
		for _, n := range nodes {
			if n.IPPort() == node.IPPort() {
				val = n.ReplicaCount
				break
			}
		}
		if val == 0 {
			log.Print("Downgrade done.")
			return true, nil
		}
		log.Printf("Still %d replicas left on %s", val, node.IPPort())
		return false, nil
	}, time.Second, 0); err != nil {
		return err
	}
	time.Sleep(time.Second)

	remoteCmdClient := client.NewRemoteCmdClient(node.IPPort())
	for _, gpid := range gpids {
		if _, err := remoteCmdClient.KillPartition(gpid); err != nil {
			return err
		}
	}

	log.Print("Stop node by deployment...")
	if err := deploy.StopNode(node); err != nil {
		return err
	}
	log.Print("Stop node by deployment done")
	time.Sleep(time.Second)

	log.Print("Wait cluster to become healthy...")
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
			log.Printf("Cluster not healthy, unhealthy_partition_count = %d", count)
			return false, nil
		}
		log.Print("Cluster becomes healthy")
		return true, nil
	}, time.Duration(10)*time.Second, 0); err != nil {
		return err
	}

	if err := metaClient.ResetDefaultAssignDelayMs(); err != nil {
		return err
	}
	return nil
}
