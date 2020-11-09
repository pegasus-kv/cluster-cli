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
	"os/exec"
	"pegasus-cluster-cli/client"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// MetaClient is a suite of API that connects to the Pegasus MetaServer,
// retrieves the cluster information or controls cluster state.
type MetaClient interface {
	// Lists all tables' health information.
	ListTableHealthInfos() ([]*HealthInfo, error)

	RemoteCommand(string, ...string) (string, error)
	SetMetaLevel(string) error
	Rebalance(bool) error
	Migrate(string) error
	Downgrade(string) ([]string, error)

	// Lists the replica nodes in the Pegasus cluster.
	ListNodes() ([]*ReplicaNode, error)

	GetClusterInfo() (*ClusterInfo, error)
}

// A MetaClient based on Pegasus shell.
type shellMetaClient struct {
	primaryMeta string
	metaList    string
	cmdClient   *client.RemoteCmdClient
}

// NewMetaClient creates an instance of MetaClient. It fails if the
// target cluster doesn't exactly match the name `cluster`.
func NewMetaClient(cluster string, metaList string) (MetaClient, error) {
	c := &shellMetaClient{
		metaList: metaList,
	}

	info, err := c.GetClusterInfo()
	if err != nil {
		return nil, err
	}
	if info.Cluster != cluster {
		return nil, fmt.Errorf("cluster name and meta list not matched, got '%s'", info.Cluster)
	}
	c.primaryMeta = info.PrimaryMeta
	c.cmdClient = client.NewMetaRemoteCmdClient(info.PrimaryMeta)
	return c, nil
}

func (c *shellMetaClient) buildCmd(command string) (*exec.Cmd, error) {
	return runShellInput(command, c.metaList)
}

func (c *shellMetaClient) GetClusterInfo() (*ClusterInfo, error) {
	cmd, err := runShellInput("cluster_info", c.metaList)
	if err != nil {
		return nil, err
	}
	var (
		primaryMeta *string
		clusterName *string
		opCount     *int
	)
	out, err := checkOutputByLine(cmd, true, func(line string) bool {
		if strings.HasPrefix(line, "primary_meta_server") {
			ss := strings.Fields(line)
			if len(ss) > 2 {
				primaryMeta = &ss[2]
			}
		} else if strings.HasPrefix(line, "zookeeper_root") {
			ss := strings.Fields(line)
			if len(ss) > 2 {
				ss1 := strings.Split(ss[2], "/")
				clusterName = &ss1[len(ss1)-1]
			}
		} else if strings.HasPrefix(line, "balance_operation_count") {
			ss := strings.Fields(line)
			if len(ss) > 2 {
				s := ss[2]
				i := strings.LastIndexByte(s, '=')
				if i != -1 {
					n, err := strconv.Atoi(s[i+1:])
					if err == nil {
						opCount = &n
					}
				}
			}
		}
		return primaryMeta != nil && clusterName != nil && opCount != nil
	})
	if err != nil {
		return nil, err
	}
	if primaryMeta == nil || clusterName == nil || opCount == nil {
		return nil, newCommandError("failed to get cluster info", out)
	}
	return &ClusterInfo{
		Cluster:               *clusterName,
		PrimaryMeta:           *primaryMeta,
		BalanceOperationCount: *opCount,
	}, nil
}

func (c *shellMetaClient) ListTableHealthInfos() ([]*HealthInfo, error) {
	cmd, err := c.buildCmd("ls -d")
	if err != nil {
		return nil, err
	}
	flag := false
	var infos []*HealthInfo
	_, err = checkOutputByLine(cmd, false, func(line string) bool {
		if !flag && strings.Contains(line, "  fully_healthy  ") {
			flag = true
			// reach the section of apps health info
			return false
		}
		if !flag { // unrelated line
			return false
		}
		ss := strings.Fields(line)
		if len(ss) < 7 {
			flag = false // reach unrelated section
			return false
		}
		// fields:
		// app_id | app_name | partition_count | fully_healthy | unhealthy | write_unhealthy | read_unhealthy
		partitionCount, err := strconv.Atoi(ss[2])
		if err != nil {
			return false
		}
		fullyHealthy, err := strconv.Atoi(ss[3])
		if err != nil {
			return false
		}
		unhealthy, err := strconv.Atoi(ss[4])
		if err != nil {
			return false
		}
		writeUnhealthy, err := strconv.Atoi(ss[5])
		if err != nil {
			return false
		}
		readUnhealthy, err := strconv.Atoi(ss[6])
		if err != nil {
			return false
		}
		infos = append(infos, &HealthInfo{
			PartitionCount: partitionCount,
			FullyHealthy:   fullyHealthy,
			Unhealthy:      unhealthy,
			WriteUnhealthy: writeUnhealthy,
			ReadUnhealthy:  readUnhealthy,
		})
		return true
	})
	if err != nil {
		return nil, err
	}
	return infos, nil
}

func (c *shellMetaClient) RemoteCommand(command string, args ...string) (string, error) {
	return c.cmdClient.Call(command, args)
}

func (c *shellMetaClient) SetMetaLevel(level string) error {
	fmt.Println("Set meta level to " + level + "...")
	cmd, err := c.buildCmd("set_meta_level " + level)
	if err != nil {
		return err
	}
	ok, out, err := checkOutputContainsOnce(cmd, false, "control meta level ok")
	if err != nil {
		return err
	}
	if !ok {
		return newCommandError("set meta level to "+level+" failed", out)
	}
	return nil
}

func (c *shellMetaClient) Rebalance(primaryOnly bool) error {
	if primaryOnly {
		if _, err := c.RemoteCommand("meta.lb.only_move_primary", "true"); err != nil {
			return err
		}
	}

	if err := c.SetMetaLevel("lively"); err != nil {
		return err
	}

	fmt.Println("Wait for 3 minutes to do load balance...")
	time.Sleep(time.Duration(180) * time.Second)

	remainTimes := 1
	for {
		info, err := c.GetClusterInfo()
		if err != nil {
			return err
		}
		if info.BalanceOperationCount == 0 {
			if remainTimes == 0 {
				break
			} else {
				fmt.Println("cluster may be balanced, try wait 30 seconds...")
				remainTimes--
				time.Sleep(time.Duration(30) * time.Second)
			}
		} else {
			fmt.Printf("still %d balance operations to do...\n", info.BalanceOperationCount)
			time.Sleep(time.Duration(10) * time.Second)
		}
	}

	if err := c.SetMetaLevel("steady"); err != nil {
		return err
	}

	if primaryOnly {
		if _, err := c.RemoteCommand("meta.lb.only_move_primary", "false"); err != nil {
			return err
		}
	}
	return nil
}

func (c *shellMetaClient) Migrate(addr string) error {
	return runSh("migrate_node", "-c", c.metaList, "-n", addr, "-t", "run").Run()
}

func (c *shellMetaClient) Downgrade(addr string) ([]string, error) {
	var gpids []string
	if _, err := checkOutputByLine(runSh("downgrade_node", "-c", c.metaList, "-n", addr, "-t run"), false, func(line string) bool {
		if strings.HasPrefix(line, "propose ") {
			ss := strings.Fields(line)
			if len(ss) > 2 {
				gpids = append(gpids, strings.ReplaceAll(ss[2], ".", " "))
			}
		}
		return false
	}); err != nil {
		return nil, err
	}
	return gpids, nil
}

func (c *shellMetaClient) ListNodes() ([]*ReplicaNode, error) {
	cmd, err := c.buildCmd("nodes -d")
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+:\d+`)
	nodes := []*ReplicaNode{}
	_, err = checkOutputByLine(cmd, false, func(line string) bool {
		if !re.MatchString(line) {
			return false
		}
		ss := strings.Fields(line)
		if len(ss) != 5 {
			return false
		}
		replica, err := strconv.Atoi(ss[2])
		if err != nil {
			return false
		}
		primary, err := strconv.Atoi(ss[3])
		if err != nil {
			return false
		}
		secondary, err := strconv.Atoi(ss[4])
		if err != nil {
			return false
		}
		node := &ReplicaNode{
			addr:           ss[0],
			Status:         ss[1],
			ReplicaCount:   replica,
			PrimaryCount:   primary,
			SecondaryCount: secondary,
		}
		nodes = append(nodes, node)
		return false
	})
	if err != nil {
		return nil, err
	}
	return nodes, nil
}
