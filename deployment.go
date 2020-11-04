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
