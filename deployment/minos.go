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
	pegasus "github.com/pegasus-kv/cluster-cli"
)

type minosDeployment struct {
	client *resty.Client

	cluster string

	// the user who is operating
	userName string

	// the pegasus team's MiCloud orgID
	orgID string

	minosAPIAddress string
}

func newMinosDeployment(cluster string, userName string) pegasus.Deployment {
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

	d.client = resty.New()
	return d
}

func (m *minosDeployment) StartNode(node pegasus.Node) error {
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

func (m *minosDeployment) StopNode(node pegasus.Node) error {
	return nil
}

func (m *minosDeployment) RollingUpdate(node pegasus.Node) error {
	return nil
}

func (m *minosDeployment) ListAllNodes() ([]pegasus.Node, error) {
	type minosShowServiceBody struct {
		Action   string                 `json:"action"`
		UserName string                 `json:"user_name"`
		Step     int                    `json:"step"`
		OrgIDs   []string               `json:"org_ids"`
		JobList  map[string]interface{} `json:"job_list"`
	}
	reqBody := &minosShowServiceBody{
		Action:   "show",
		UserName: m.userName,
		Step:     1,
		OrgIDs:   []string{m.orgID},
		JobList:  map[string]interface{}{},
	}

	type nodeDetails struct {
		Host string `json:"host"`
	}
	type nodesByType struct {
		Collector map[string]nodeDetails `json:"collector"`
		Replica   map[string]nodeDetails `json:"replica"`
		Meta      map[string]nodeDetails `json:"meta"`
	}
	type showResults struct { // The body of response
		Retval nodesByType `json:"retval"`
	}
	var results showResults

	resp, err := m.client.R().
		SetBody(reqBody).
		Post(m.minosAPIAddress + "/cloud_manager/pegasus-" + m.cluster + "?action")
	if err := handleRestyResult("MinosShow", err, resp, &results); err != nil {
		return nil, err
	}

	var allNodes []pegasus.Node
	for id, collector := range results.Retval.Collector {
		allNodes = append(allNodes, pegasus.NewNode(id, collector.Host, pegasus.JobCollector))
	}
	for id, replica := range results.Retval.Replica {
		allNodes = append(allNodes, pegasus.NewNode(id, replica.Host, pegasus.JobReplica))
	}
	for id, meta := range results.Retval.Meta {
		allNodes = append(allNodes, pegasus.NewNode(id, meta.Host, pegasus.JobMeta))
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
