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

type Minos struct {
	Cluster   string
	ClientDir string
}

func init() {
	var (
		pegasusConf string
		minosClientDir string
		minosConf string
	)
	cmd.RootCmd.PersistentFlags().StringVar(&pegasusConf, "pegasus-conf", os.Getenv("PEGASUS_CONFIG"), "directory where the config files of pegasus are stored, usually deployment-config/xiaomi-config/conf/pegasus. Could be set from env PEGASUS_CONFIG")
	cmd.RootCmd.PersistentFlags().StringVar(&minosClientDir, "minos-client-dir", os.Getenv("MINOS_CLIENT_DIR"), "directory of the executable file of minos(deploy). Could be set from env MINOS_CLIENT_DIR")
	cmd.RootCmd.PersistentFlags().StringVar(&minosConf, "minos-conf", os.Getenv("MINOS_CONFIG_FILE"), "location of minos configuration file. Could be set from env MINOS_CONFIG_FILE")
	cmd.RootCmd.MarkPersistentFlagDirname("pegasus-conf")
	cmd.RootCmd.MarkPersistentFlagDirname("minos-client-dir")
	cmd.RootCmd.MarkPersistentFlagFilename("minos-conf", "cfg")
	cmd.Validate = func() error {
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
