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

// Deployment is the abstraction of a deployment automation system that's capable of
// mananging all the nodes in a Pegasus cluster.
// pegasus-cluster-cli operates the cluster based on Deployment, using graceful strategies
// with higher availability, less performance downgrade than directly killing/starting
// pegasus server.
type Deployment interface {

	// Start a Pegasus node on the specified machine. A possible implementation may
	// login to the machine, download the binary package and config, and launch the
	// process.
	StartNode(Node) error

	// Stop a Pegasus node on the specified machine. A possible implementation may
	// login to the machine, and kill the process (via supervisord).
	StopNode(Node) error

	// Rolling-update a Pegasus node on the specified machine. A possible implementation
	// may login to the machine, redownload the binary package and config, restart the process.
	RollingUpdate(Node) error

	// Retrieves the nodes information in the Pegasus cluster.
	ListAllNodes() ([]Node, error)
}

// CreateDeployment creates a non-nil instance of Deployment that binds to a specific cluster.
var CreateDeployment func(cluster string) Deployment = nil

// Node could be a MetaServer/ReplicaServer/Collector. Provided with a Node, the implementation of
// Deployment must be able to remotely operates the node.
type Node struct {
	Job JobType

	// Node's name should be unique within the cluster.
	// There's no exact rule on the naming of a node.
	// It could be some ID like "1", "2" ..., or a hostname, UUID, TCP address, etc.
	Name string

	IPPort string
	Info   *NodeInfo
}

type JobType int

const (
	// JobMeta represents MetaServer
	JobMeta = 0

	// JobReplica represents ReplicaServer
	JobReplica = 1

	// JobCollector represents Collector
	JobCollector = 2
)

func (j JobType) String() string {
	switch j {
	case JobMeta:
		return "meta"
	case JobReplica:
		return "replica"
	default:
		return "collector"
	}
}
