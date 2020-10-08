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

type JobType int

const (
	JobMeta      = 0
	JobReplica   = 1
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

type Node struct {
	Job    JobType
	Name   string
	IPPort string
}

var globalAllNodes []Node

func initNodes(deploy Deployment) error {
	res, err := deploy.ListAllNodes()
	if err != nil {
		return err
	}
	globalAllNodes = res
	return nil
}

func findReplicaNode(name string) (Node, bool) {
	for _, node := range globalAllNodes {
		if node.Job == JobReplica && name == node.Name {
			return node, true
		}
	}
	return Node{}, false
}
