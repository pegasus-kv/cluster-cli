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

	"github.com/go-resty/resty/v2"
)

type minosDeployment struct {
	client *resty.Client

	cluster string

	// the user who is operating
	userName string

	// the pegasus team's MiCloud orgID
	orgID string
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

	d.orgID = os.Getenv("PEGASUS_TEAM_ORG_ID")
	if d.orgID == "" {
		panic("Please set the environment variable PEGASUS_TEAM_ORG_ID")
	}

	d.pegasusGatewayURL = os.Getenv("PEGASUS_GATEWAY_URL")
	if d.pegasusGatewayURL == "" {
		panic("Please set the environment variable PEGASUS_GATEWAY_URL")
	}

	d.client = resty.New()
	return d
}

func (m *minosDeployment) StartNode(node Node) error {
	type minosStartServiceBody struct {
		Action        string   `json:"action"`
		UserName      string   `json:"user_name"`
		UpdatePackage int      `json:"update_package"`
		UpdateConfig  int      `json:"update_config"`
		Step          int      `json:"step"`
		Concurrency   int      `json:"concurrency"`
		JobList       []string `json:"job_list"`
		OrgIDs        []string `json:"org_ids"`
	}

	resp, err := m.client.R().Post(m.minosAPIAddress + "/" + m.cluster)
	if err := handleRestyError("MinosStartNode", err, resp); err != nil {
		return err
	}
	return nil
}

func (m *minosDeployment) StopNode(node Node) error {
	return nil
}

func (m *minosDeployment) RollingUpdate(node Node) error {
	return nil
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
		return fmt.Errorf("%s failed: %s %s%s\n%s", op, resp.Status(), string(reqBytes), reqBodyBytes, resp.Body())
	}
	return nil
}

func handleRestyResult(op string, err error, resp *resty.Response, v interface{}) error {
	if err = handleRestyError(op, err, resp); err != nil {
		return err
	}
	return json.Unmarshal(resp.Body(), v)
}
