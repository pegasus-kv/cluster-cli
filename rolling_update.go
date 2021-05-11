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
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/pegasus-kv/admin-cli/util"
	"github.com/pegasus-kv/cluster-cli/deployment"
	metaApi "github.com/pegasus-kv/cluster-cli/meta"
	log "github.com/sirupsen/logrus"
)

type Updater struct {
	meta   metaApi.Meta
	deploy deployment.Deployment

	down Downgrader
}

func PrepareRollingUpdate(cluster string, deploy deployment.Deployment) (*Updater, error) {
	meta, err := newMeta(cluster, deploy)
	if err != nil {
		return nil, err
	}

	// preparation: stop automatic rebalance
	if err := meta.SetMetaLevelSteady(); err != nil {
		return nil, err
	}

	return &Updater{
		meta:   meta,
		deploy: deploy,
		down:   newDowngrader(meta, deploy),
	}, nil
}

func (u *Updater) FindAndUpdateNode(nodeName string, jobType deployment.JobType) error {
	node, err := findNode(nodeName, jobType)
	if err != nil {
		return err
	}
	return u.UpdateNode(node)
}

// rolling-update a single node.
func (u *Updater) UpdateNode(node *deployment.Node) error {
	switch node.Job {
	case deployment.JobCollector:
		return u.updateStatelessNode(node)
	case deployment.JobMeta:
		return u.updateStatelessNode(node)
	case deployment.JobReplica:
		return u.updateSingleReplicaNode(node)
	default:
		return fmt.Errorf("unknown node type: \"%s\"", node.Job)
	}
}

// Stateless node means any node other than ReplicaServer. Simple rolling is fine.
func (u *Updater) updateStatelessNode(node *deployment.Node) error {
	return u.deploy.RollingUpdate(*node)
}

func (u *Updater) updateSingleReplicaNode(nInfo *deployment.Node) error {
	log.Debug("set meta.lb.add_secondary_max_count_for_one_node to 0")
	if err := u.meta.SetAddSecondaryMaxCountForOneNode(0); err != nil {
		return err
	}

	node := util.NewNodeFromTCPAddr(nInfo.IPPort, session.NodeTypeReplica)
	if err := u.down.Downgrade(node); err != nil {
		return err
	}

	log.Print("Rolling update by deployment...")
	if err := u.deploy.RollingUpdate(*nInfo); err != nil {
		return err
	}
	log.Print("Rolling update by deployment done")

	if err := u.waitNodeAlive(node); err != nil {
		return err
	}

	if err := u.meta.SetAddSecondaryMaxCountForOneNode(100); err != nil {
		return err
	}

	if err := u.waitClusterHealthy(); err != nil {
		return err
	}
	return nil
}

func (u *Updater) Finish() error {
	if err := u.meta.ResetDefaultAddSecondaryMaxCountForOneNode(); err != nil {
		return err
	}
	return u.meta.Rebalance(false)
}

func (u *Updater) waitNodeAlive(n *util.PegasusNode) error {
	for {
		nodes, err := u.meta.ListNodes()
		if err != nil {
			return err
		}
		for _, ninfo := range nodes {
			if ninfo.Address.GetAddress() == n.TCPAddr() {
				if ninfo.Status == admin.NodeStatus_NS_ALIVE {
					return nil
				}
			}
		}
		time.Sleep(time.Second)
	}
}

func (u *Updater) waitClusterHealthy() error {
	for {
		clusterInfo, err := u.meta.GetClusterReplicaInfo()
		if err != nil {
			return err
		}
		unhealthy := int32(0)
		for _, tb := range clusterInfo.Tables {
			unhealthy += tb.Unhealthy
		}
		if unhealthy == int32(0) {
			return nil
		}
		time.Sleep(time.Second)
	}
}
