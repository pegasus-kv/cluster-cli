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

import "fmt"

type ClusterInfo struct {
	Cluster               string
	PrimaryMeta           string
	BalanceOperationCount int
}

// HealthInfo is the health information of a table.
type HealthInfo struct {
	PartitionCount int
	FullyHealthy   int
	Unhealthy      int
	WriteUnhealthy int
	ReadUnhealthy  int
}

type ReplicaNode struct {
	name string
	addr string

	// ALIVE / UNALIVE
	Status         string
	ReplicaCount   int
	PrimaryCount   int
	SecondaryCount int
}

func (*ReplicaNode) Job() JobType {
	return JobReplica
}

func (r *ReplicaNode) Name() string {
	return r.name
}

func (r *ReplicaNode) IPPort() string {
	return r.addr
}

type metaNode struct {
	name string
	addr string
}

func (*metaNode) Job() JobType {
	return JobMeta
}

func (m *metaNode) Name() string {
	return m.name
}

func (m *metaNode) IPPort() string {
	return m.addr
}

type collectorNode struct {
	name string
	addr string
}

func (*collectorNode) Job() JobType {
	return JobCollector
}

func (c *collectorNode) Name() string {
	return c.name
}

func (c *collectorNode) IPPort() string {
	return c.addr
}

// NewNode returns a Node.
func NewNode(name string, ipPort string, job JobType) Node {
	switch job {
	case JobReplica:
		return &ReplicaNode{name: name, addr: ipPort}
	case JobMeta:
		return &metaNode{name: name, addr: ipPort}
	case JobCollector:
		return &collectorNode{name: name, addr: ipPort}
	default:
		panic(fmt.Sprintf("unknown job: %d", int(job)))
	}
}

var globalAllNodes []Node

func listAndCacheAllNodes(deploy Deployment) error {
	res, err := deploy.ListAllNodes()
	if err != nil {
		return err
	}
	globalAllNodes = res
	return nil
}

func findReplicaNode(name string) (*ReplicaNode, bool) {
	for _, node := range globalAllNodes {
		if node.Job() == JobReplica && name == node.Name() {
			return node.(*ReplicaNode), true
		}
	}
	return nil, false
}
