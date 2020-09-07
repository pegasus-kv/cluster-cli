package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path"
	"pegasus-cluster-cli"
	"strconv"
)

type Minos struct {
	Cluster   string
	ClientDir string
}

func init() {
	pegasusConf := os.Getenv("PEGASUS_CONFIG")
	if pegasusConf == "" {
		fmt.Println("env PEGASUS_CONFIG not provided")
		os.Exit(1)
	}
	minosClientDir := os.Getenv("MINOS_CLIENT_DIR")
	if minosClientDir == "" {
		fmt.Println("env MINOS_CLIENT_DIR not provided")
		os.Exit(1)
	}
	pegasus.CreateDeployment = func(cluster string) pegasus.Deployment {
		d, err := NewMinosDeployment(cluster, pegasusConf, minosClientDir)
		if err != nil {
			panic(err)
		}
		return d
	}
}

func NewMinosDeployment(cluster string, confPath string, clientDir string) (*Minos, error) {
	clusterCfg := path.Join(confPath, "pegasus-"+cluster+".cfg")
	if !fileExists(clusterCfg) {
		return nil, errors.New("config file for cluster '" + cluster + "' not found")
	}
	return &Minos{cluster, clientDir}, nil
}

func (m *Minos) StartNode(node pegasus.Node) error {
	cmd := m.execDeploy("bootstrap", "pegasus", m.Cluster, "--job", node.Job.String(), "--task", node.Name)
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

func (m *Minos) StopNode(node pegasus.Node) error {
	cmd := m.execDeploy("stop", "pegasus", m.Cluster, "--job", node.Job.String(), "--task", node.Name, "--skip_confirm")
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

func (m *Minos) RollingUpdate(node pegasus.Node) error {
	cmd := m.execDeploy("rolling_update", "pegasus", m.Cluster, "--job", node.Job.String(),
		"--task", node.Name, "--update-package --update-config --time_interval 20 --skip_confirm")
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

func (m *Minos) ListAllNodes() ([]pegasus.Node, error) {
	type Node struct {
		Job  string `json:"job"`
		Task int    `json:"task_id"`
	}
	var nodeMap map[string]Node
	req, _ := http.NewRequest("GET", "http://pegasus-gateway.hadoop.srv/endpoints", nil)
	q := req.URL.Query()
	q.Add("cluster", m.Cluster)
	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("failed to fetch for '" + m.Cluster + "' from pegasus-gateway")
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
		nodes = append(nodes, pegasus.Node{job, k, strconv.Itoa(v.Task)})
	}
	return nodes, nil
}

func (m *Minos) execDeploy(args ...string) *exec.Cmd {
	return exec.Command(path.Join(m.ClientDir, "deploy"), args...)
}

func fileExists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func outputEachLine(cmd *exec.Cmd, forEach func(line string)) error {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	defer stdout.Close()

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		forEach(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}
