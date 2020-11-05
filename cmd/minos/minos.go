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

package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path"
	"pegasus-cluster-cli"
	"pegasus-cluster-cli/cmd"
	"strconv"
)

type minosDeployment struct {
	Cluster   string
	ClientDir string
}

func init() {
	var (
		pegasusConf    string
		minosClientDir string
		minosConf      string
	)
	cmd.RootCmd.PersistentFlags().StringVar(&pegasusConf, "pegasus-conf", os.Getenv("PEGASUS_CONFIG"), "directory where the config files of pegasus are stored, usually deployment-config/xiaomi-config/conf/pegasus. Could be set from env PEGASUS_CONFIG")
	cmd.RootCmd.PersistentFlags().StringVar(&minosClientDir, "minos-client-dir", os.Getenv("MINOS_CLIENT_DIR"), "directory of the executable file of minos(deploy). Could be set from env MINOS_CLIENT_DIR")
	cmd.RootCmd.PersistentFlags().StringVar(&minosConf, "minos-conf", os.Getenv("MINOS_CONFIG_FILE"), "location of minos configuration file. Could be set from env MINOS_CONFIG_FILE")
	_ = cmd.RootCmd.MarkPersistentFlagDirname("pegasus-conf")
	_ = cmd.RootCmd.MarkPersistentFlagDirname("minos-client-dir")
	_ = cmd.RootCmd.MarkPersistentFlagFilename("minos-conf", "cfg")
	cmd.ValidateEnvsHook = func() error {
		if pegasusConf == "" {
			return errors.New("pegasus-config is empty, set flag --pegasus-conf or env PEGASUS_CONFIG")
		}
		if minosClientDir == "" {
			return errors.New("minos-client-dir is empty, set flag --minos-client-dir or env MINOS_CLIENT_DIR")
		}
		if minosConf == "" {
			return errors.New("minos-config is empty, set flag --minos-conf or env MINOS_CONFIG_FILE")
		}
		return nil
	}
	pegasus.CreateDeployment = func(cluster string) pegasus.Deployment {
		d, err := newMinosDeployment(cluster, pegasusConf, minosClientDir)
		if err != nil {
			panic(err)
		}
		return d
	}
}

func newMinosDeployment(cluster string, confPath string, clientDir string) (*minosDeployment, error) {
	clusterCfg := path.Join(confPath, "pegasus-"+cluster+".cfg")
	if !fileExists(clusterCfg) {
		return nil, errors.New("config file for cluster '" + cluster + "' not found")
	}
	return &minosDeployment{cluster, clientDir}, nil
}

func (m *minosDeployment) StartNode(node pegasus.Node) error {
	// ./deploy bootstrap pegasus --job <job> --task <task-id>
	cmd := m.execDeploy("bootstrap", "pegasus", m.Cluster, "--job", node.Job.String(), "--task", node.Name)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		return errors.New("failed to execute minos bootstrap")
	}
	return nil
}

func (m *minosDeployment) StopNode(node pegasus.Node) error {
	// ./deploy stop pegasus --job <job> --task <task-id> --skip_confirm
	cmd := m.execDeploy("stop", "pegasus", m.Cluster, "--job", node.Job.String(), "--task", node.Name, "--skip_confirm")
	if out, err := cmd.CombinedOutput(); err != nil {
		return newCommandError("failed to execute minos stop", out)
	}
	return nil
}

func (m *minosDeployment) RollingUpdate(node pegasus.Node) error {
	cmd := m.execDeploy("rolling_update", "pegasus", m.Cluster, "--job", node.Job.String(),
		"--task", node.Name, "--update_package --update_config --time_interval 20 --skip_confirm")
	if out, err := cmd.CombinedOutput(); err != nil {
		return newCommandError("failed to execute minos rolling_update", out)
	}
	return nil
}

func (m *minosDeployment) ListAllNodes() ([]pegasus.Node, error) {
	type Node struct {
		Job  string `json:"job"`
		Task int    `json:"task_id"`
	}
	var nodeMap map[string]Node // IP:Port as the key
	req, _ := http.NewRequest("GET", "http://pegasus-gateway.hadoop.srv/endpoints", nil)
	q := req.URL.Query()
	q.Add("cluster", m.Cluster)
	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("failed to fetch endpoints of '" + m.Cluster + "' from pegasus-gateway")
	}
	defer resp.Body.Close()

	if err = json.NewDecoder(resp.Body).Decode(&nodeMap); err != nil {
		return nil, err
	}
	var nodes []pegasus.Node
	for k, v := range nodeMap {
		var job pegasus.JobType
		if v.Job == "meta" {
			job = pegasus.JobMeta
		} else if v.Job == "collector" {
			job = pegasus.JobCollector
		} else {
			job = pegasus.JobReplica
		}
		nodes = append(nodes, pegasus.Node{
			Job:    job,
			Name:   strconv.Itoa(v.Task), // use task id as the name
			IPPort: k,
			Info:   nil,
		})
	}
	return nodes, nil
}

func (m *minosDeployment) execDeploy(args ...string) *exec.Cmd {
	cmd := exec.Command("./deploy", args...)
	cmd.Dir = m.ClientDir
	return cmd
}

func fileExists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false
		}
		panic(err)
	}
	return true
}

func newCommandError(msg string, out []byte) error {
	return errors.New(msg + ". Output:\n" + string(out))
}
