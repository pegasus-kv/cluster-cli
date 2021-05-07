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
	"strconv"
	"strings"
	"time"

	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/util"
	log "github.com/sirupsen/logrus"
)

// Meta is a suite of API that connects to the Pegasus MetaServer,
// retrieves the cluster information or controls cluster state.
type Meta interface {
	// Lists all tables' health information.
	ListTableHealthInfos() ([]*client.TableHealthInfo, error)

	SetMetaLevelSteady() error
	SetMetaLevelLively() error

	Rebalance(bool) error

	SetOnlyMovePrimary() error

	ResetAddSecondaryMaxCountForOneNode() error
	SetAddSecondaryMaxCountForOneNode(num int) error

	MigratePrimariesOut(ipPort string) error

	Downgrade(string) ([]string, error)

	// Lists the replica nodes in the Pegasus cluster.
	ListNodes() ([]*ReplicaNode, error)

	GetClusterInfo() (*ClusterInfo, error)
}

// A MetaClient based on RPC.
type metaClient struct {
	meta client.Meta

	primaryMeta *util.PegasusNode
}

// NewMetaClient creates an instance of MetaClient. It fails if the
// target cluster doesn't exactly match the name `cluster`.
func NewMetaClient(cluster string, metaList string) (Meta, error) {
	c := &metaClient{
		meta: client.NewRPCBasedMeta(strings.Split(metaList, ",")),
	}
	info, err := c.GetClusterInfo()
	if err != nil {
		return nil, err
	}
	if info.Cluster != cluster {
		return nil, fmt.Errorf("cluster name and meta list aren't matched, got '%s'", info.Cluster)
	}
	c.primaryMeta = util.NewNodeFromTCPAddr(info.PrimaryMeta, session.NodeTypeMeta)
	return c, nil
}

// TODO(wutao): use mapsturecture to parse map into struct
func (c *metaClient) GetClusterInfo() (*ClusterInfo, error) {
	infoMap, err := c.meta.QueryClusterInfo()
	if err != nil {
		return nil, err
	}

	primaryMeta := infoMap["primary_meta_server"]

	// extract clusterName from zookeeper path
	zookeeperRoot := infoMap["zookeeper_root"]
	zookeeperRootParts := strings.Split(zookeeperRoot, "/")
	clusterName := zookeeperRootParts[len(zookeeperRootParts)-1]

	opCountStr := infoMap["balance_operation_count"]
	opCount, err := strconv.Atoi(opCountStr)
	if err != nil {
		return nil, fmt.Errorf("\"balance_operation_count\" in cluster info is not a valid integer")
	}

	return &ClusterInfo{
		Cluster:               clusterName,
		PrimaryMeta:           primaryMeta,
		BalanceOperationCount: opCount,
	}, nil
}

// TODO(wutao): implement this API in admin-cli
func (c *metaClient) ListTableHealthInfos() ([]*client.TableHealthInfo, error) {
	tbs, err := c.meta.ListAvailableApps()
	if err != nil {
		return nil, err
	}

	var result []*client.TableHealthInfo
	for _, tb := range tbs {
		tbHealthInfo, err := client.GetTableHealthInfo(c.meta, tb.AppName)
		if err != nil {
			return nil, err
		}
		result = append(result, tbHealthInfo)
	}
	return result, nil
}

func (c *metaClient) SetOnlyMovePrimary() error {
	return nil
}

func (c *metaClient) ResetAddSecondaryMaxCountForOneNode() error {
	return nil
}

func (c *metaClient) SetAddSecondaryMaxCountForOneNode(num int) error {
	return nil
}

func (c *metaClient) SetMetaLevelSteady() error {
	return client.SetMetaLevelSteady(c.meta)
}

func (c *metaClient) SetMetaLevelLively() error {
	return client.SetMetaLevelLively(c.meta)
}

func (c *metaClient) Rebalance(primaryOnly bool) error {
	if primaryOnly {
		if _, err := c.RemoteCommand("meta.lb.only_move_primary", "true"); err != nil {
			return err
		}
	}

	if err := c.SetMetaLevelLively(); err != nil {
		return err
	}

	log.Print("Wait for 3 minutes to do load balance...")
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
				log.Print("cluster may be balanced, try wait 30 seconds...")
				remainTimes--
				time.Sleep(time.Duration(30) * time.Second)
			}
		} else {
			log.Printf("still %d balance operations to do...", info.BalanceOperationCount)
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

func (c *metaClient) MigratePrimariesOut(tcpAddr string) error {
	return client.MigratePrimariesOut(c.meta, util.NewNodeFromTCPAddr(tcpAddr, session.NodeTypeReplica))
}

func (c *metaClient) Downgrade(addr string) ([]string, error) {
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

func (c *metaClient) ListNodes() ([]*ReplicaNode, error) {
	return nil, nil
}
