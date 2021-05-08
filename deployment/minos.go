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
	"encoding/json"
	"fmt"
	"net/http/httputil"
	"os"
	"strconv"

	"github.com/go-resty/resty/v2"
)

type minosDeployment struct {
	client *resty.Client

	cluster string

	// the user who is operating
	userName string

	// the pegasus team's MiCloud orgID
	orgID int
	// the minos api service url
	minosAPIAddress string
	// the pegasus gateway url
	pegasusGatewayURL string
}

// NewMinos returns a deployment of Minos.
func NewMinos(cluster string, userName string) Deployment {
	d := &minosDeployment{
		cluster:  cluster,
		userName: userName,
	}

	d.minosAPIAddress = os.Getenv("MINOS_API_URL")
	if d.minosAPIAddress == "" {
		panic("Please set the environment variable MINOS_API_URL")
	}

	orgIDEnvVal := os.Getenv("PEGASUS_TEAM_ORG_ID")
	if orgIDEnvVal == "" {
		panic("Please set the environment variable PEGASUS_TEAM_ORG_ID")
	}
	var err error
	d.orgID, err = strconv.Atoi(orgIDEnvVal)
	if err != nil {
		panic(fmt.Sprintf("PEGASUS_TEAM_ORG_ID is not a valid integer: \"%s\"", orgIDEnvVal))
	}

	d.pegasusGatewayURL = os.Getenv("PEGASUS_GATEWAY_URL")
	if d.pegasusGatewayURL == "" {
		panic("Please set the environment variable PEGASUS_GATEWAY_URL")
	}

	d.client = resty.New()
	return d
}

type minosOpRetVal struct {
	ErrorMsg  string `json:"error_msg"`
	ErrorCode int    `json:"error_code"`
}

type minosOpResponse struct {
	Retval  minosOpRetVal `json:"retval"`
	Success bool          `json:"success"`
}

// The minos RESTFul API is basically general for start/stop/rolling-update operations.
// So we use a generic function for them all.
func (m *minosDeployment) performGenericMinosOp(opType string, node Node) error {
	taskID, _ := strconv.Atoi(node.Name)

	reqBody := map[string]interface{}{
		"action":    opType,
		"user_name": m.userName,
		"org_ids":   []int{m.orgID},
		"job_list": map[string]interface{}{
			node.Job.String(): []int{taskID},
		},
		"concurrency":    1,
		"step":           0,
		"update_package": 1,
		"update_config":  1,
	}

	var results minosOpResponse
	resp, err := m.client.R().
		SetBody(reqBody).
		Post(m.minosAPIAddress + "/cloud_manager/pegasus-" + m.cluster + "?action")
	if err := handleRestyResult("minos_"+opType, err, resp, &results); err != nil {
		return err
	}
	if !results.Success {
		return fmt.Errorf("code: %d, message: %s", results.Retval.ErrorCode, results.Retval.ErrorMsg)
	}
	return nil
}

func (m *minosDeployment) StartNode(node Node) error {
	return m.performGenericMinosOp("start", node)
}

func (m *minosDeployment) StopNode(node Node) error {
	return m.performGenericMinosOp("stop", node)
}

func (m *minosDeployment) RollingUpdate(node Node) error {
	return m.performGenericMinosOp("rolling_update", node)
}

func (m *minosDeployment) ListAllNodes() ([]Node, error) {
	type nodeDetails struct {
		Job    string `json:"job"`
		TaskID int    `json:"task_id"`
	}
	var results map[string]nodeDetails

	resp, err := m.client.R().Post(m.pegasusGatewayURL + "/endpoints?cluster=" + m.cluster)
	if err := handleRestyResult("GatewayListEndpoints", err, resp, &results); err != nil {
		return nil, err
	}

	var allNodes []Node
	for tcpAddr, n := range results {
		var job JobType
		switch n.Job {
		case "replica":
			job = JobReplica
		case "collector":
			job = JobCollector
		case "meta":
			job = JobMeta
		}
		allNodes = append(allNodes, NewNode(fmt.Sprint(n.TaskID), tcpAddr, job))
	}
	return allNodes, nil
}

func (m *minosDeployment) Name() string {
	return "minos"
}

func handleRestyError(op string, err error, resp *resty.Response) error {
	if err != nil {
		return err
	}
	if !resp.IsSuccess() {
		reqBytes, _ := httputil.DumpRequest(resp.Request.RawRequest, true)
		reqBodyBytes, _ := json.MarshalIndent(resp.Request.Body, "", "  ")
		return fmt.Errorf("%s failed: %s %s%s\n\nResponse: %s", op, resp.Status(), string(reqBytes), reqBodyBytes, resp.Body())
	}
	return nil
}

func handleRestyResult(op string, err error, resp *resty.Response, v interface{}) error {
	if err = handleRestyError(op, err, resp); err != nil {
		return err
	}
	return json.Unmarshal(resp.Body(), v)
}
