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
	"regexp"
	"strings"
)

// Deployment is the abstraction of a deployment automation system that's capable of
// mananging all the nodes in a Pegasus cluster.
// pegasus-cluster-cli manipulates the cluster based on Deployment, using strategies
// with higher availability, less performance downgrade of the online service.
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

type CommandError struct {
	Msg    string
	Output []byte
}

func (e *CommandError) Error() string {
	return e.Msg + ". Output:\n" + string(e.Output)
}

func NewCommandError(msg string, out []byte) *CommandError {
	return &CommandError{msg, out}
}

var CreateDeployment func(cluster string) Deployment = nil

func ValidateCluster(cluster string, metaList string, nodeNames []string) (string, error) {
	fmt.Println("Validate cluster name and node list...")
	nodeMap := make(map[string]bool)
	for _, name := range nodeNames {
		_, prs := nodeMap[name]
		if prs {
			return "", errors.New("duplicate node '" + name + "' in node list")
		}
		nodeMap[name] = true
	}

	ok1, ok2 := false, false
	r1 := regexp.MustCompile(`/([^/]*)$`)
	r2 := regexp.MustCompile(`([0-9.:]*)\s*$`)
	cmd, err := runShellInput("cluster_info", metaList)
	if err != nil {
		return "", err
	}

	var pmeta string
	out, err := checkOutput(cmd, true, func(line string) bool {
		if strings.Contains(line, "zookeeper_root") {
			rs := r1.FindStringSubmatch(line)
			if len(rs) > 1 && strings.TrimSpace(rs[1]) == cluster {
				ok1 = true
			}
		} else if strings.Contains(line, "primary_meta_server") {
			rs := r2.FindStringSubmatch(line)
			if len(rs) > 1 && len(rs[1]) != 0 {
				ok2 = true
				pmeta = rs[1]
			}
		}
		return ok1 && ok2
	})
	if err != nil {
		return "", err
	}
	if !ok1 {
		return "", NewCommandError("cluster name and meta list not matched", out)
	} else if !ok2 {
		return "", NewCommandError("extract primary_meta_server by shell failed", out)
	} else {
		return pmeta, nil
	}
}
