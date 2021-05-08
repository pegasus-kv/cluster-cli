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

package deployment

import (
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/pegasus-kv/admin-cli/util"
)

// Deployment is the abstraction of a deployment automation system that provides
// the ability to operate all the nodes in a Pegasus cluster.
// pegasus-cluster-cli operates the cluster based on `Deployment`, using graceful strategies
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
	// may login to the machine, re-download the binary package and config and restart the process.
	RollingUpdate(Node) error

	// Retrieves all nodes information in the Pegasus cluster.
	ListAllNodes() ([]Node, error)

	// Name returns a simple name identifies the deployment system.
	Name() string
}

// CreateDeployment creates a non-nil instance of Deployment that binds to a specific cluster.
var CreateDeployment func(cluster string) Deployment = nil

// Node could be a MetaServer/ReplicaServer/Collector. Provided with a Node, the implementation of
// Deployment must be able to remotely operates the node.
type Node struct {
	Job JobType `json:"job"`

	// Node's name should be unique within the cluster.
	// There's no strict rule on the naming of a node.
	// It could be some ID like "1", "2" ..., or a hostname, UUID, TCP address, etc.
	Name string `json:"name"`

	IPPort string `json:"ip_port"`

	Hostname string `json:"hostname"`

	// Attrs contains the additional attributes that are provided by the specific Deployment.
	// This field is optional, can be left empty.
	// A typical use-case could be giving a "Status" that represents the node running status:
	//   Attrs : map[string]interface{"Status" : "Running"}
	// It will be useful to check if the node behaves normal after start/stop/rolling-update.
	Attrs map[string]interface{}
}

// NewNode returns a Node.
func NewNode(name string, tcpAddr string, job JobType) Node {
	// resolve ip
	n := util.NewNodeFromTCPAddr(tcpAddr, session.NodeTypeReplica /*dummy field*/)
	return Node{
		Job:      job,
		Name:     name,
		IPPort:   n.TCPAddr(),
		Hostname: n.Hostname,
		Attrs:    map[string]interface{}{},
	}
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
