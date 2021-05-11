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

	"github.com/pegasus-kv/cluster-cli/deployment"
	"github.com/pegasus-kv/cluster-cli/meta"
)

var globalAllNodes []deployment.Node

func listAndCacheAllNodes(deploy deployment.Deployment) error {
	res, err := deploy.ListAllNodes()
	if err != nil {
		return err
	}
	globalAllNodes = res
	return nil
}

func findNode(name string, jobType deployment.JobType) (*deployment.Node, error) {
	for _, node := range globalAllNodes {
		if node.Job == jobType && name == node.Name {
			return &node, nil
		}
	}
	return nil, fmt.Errorf("%s node '%s' was not found", jobType, name)
}

func findReplicaNode(name string) (*deployment.Node, error) {
	return findNode(name, deployment.JobReplica)
}

// A simple wrapper around NewMetaClient.
func newMeta(cluster string, deploy deployment.Deployment) (meta.Meta, error) {
	if err := listAndCacheAllNodes(deploy); err != nil {
		return nil, err
	}
	var metaList []string
	for _, n := range globalAllNodes {
		if n.Job == deployment.JobMeta {
			metaList = append(metaList, n.IPPort)
		}
	}
	return meta.NewMetaClient(cluster, metaList)
}
