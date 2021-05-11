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

package meta

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/util"
	log "github.com/sirupsen/logrus"
)

type ClusterInfo struct {
	Cluster               string
	PrimaryMeta           string
	BalanceOperationCount int
}

// Meta is a suite of API that connects to the Pegasus MetaServer,
// retrieves the cluster information or controls cluster state.
//
// NOTE: This interface hides the many meta APIs in "admin-cli/client#Meta",
// and exposes only the API which are necessary for this tool.
// This design is to simplify mocking the MetaServer.
//
type Meta interface {
	// Lists all tables' health information.
	ListTableHealthInfos() ([]*client.TableHealthInfo, error)

	SetMetaLevelSteady() error

	SetAddSecondaryMaxCountForOneNode(num int) error
	ResetDefaultAddSecondaryMaxCountForOneNode() error

	SetNodeLivePercentageZero() error

	AssignSecondaryBlackList(blacklist string) error

	SetAssignDelayMs(delayMs int) error
	ResetDefaultAssignDelayMs() error

	// MigratePrimariesOut ensures that the node has no primary after the call finishes.
	MigratePrimariesOut(n *util.PegasusNode) error

	DowngradeNodeWithDetails(n *util.PegasusNode) (downgradedParts []*base.Gpid, err error)

	Rebalance(bool) error

	GetClusterInfo() (*ClusterInfo, error)

	ListNodes() ([]*admin.NodeInfo, error)

	GetClusterReplicaInfo() (*client.ClusterReplicaInfo, error)
}

// A MetaClient based on RPC.
type metaClient struct {
	meta client.Meta

	primaryMeta *util.PegasusNode
}

// NewMetaClient creates an instance of MetaClient. It fails if the
// target cluster doesn't exactly match the name `cluster`.
func NewMetaClient(cluster string, metaList []string) (Meta, error) {
	c := &metaClient{
		meta: client.NewRPCBasedMeta(metaList),
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

// TODO(wutao): use mapstructure to parse map into struct
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

func (c *metaClient) setOnlyMovePrimary() error {
	return nil
}

func (c *metaClient) UnsetOnlyMovePrimary() error {
	return nil
}

func (c *metaClient) SetAddSecondaryMaxCountForOneNode(num int) error {
	numStr := fmt.Sprint(num)
	return client.CallCmd(c.primaryMeta, "meta.lb.add_secondary_max_count_for_one_node", []string{numStr}).Error()
}

func (c *metaClient) ResetDefaultAddSecondaryMaxCountForOneNode() error {
	return client.CallCmd(c.primaryMeta, "meta.lb.add_secondary_max_count_for_one_node", []string{"DEFAULT"}).Error()
}

func (c *metaClient) SetNodeLivePercentageZero() error {
	return client.CallCmd(c.primaryMeta, "meta.live_percentage", []string{"0"}).Error()
}

func (c *metaClient) AssignSecondaryBlackList(blacklist string) error {
	return nil
}

func (c *metaClient) SetAssignDelayMs(delayMs int) error {
	delayStr := fmt.Sprint(delayMs)
	return client.CallCmd(c.primaryMeta, "meta.lb.assign_delay_ms", []string{delayStr}).Error()
}

func (c *metaClient) ResetDefaultAssignDelayMs() error {
	return client.CallCmd(c.primaryMeta, "meta.lb.assign_delay_ms", []string{"DEFAULT"}).Error()
}

func (c *metaClient) SetMetaLevelSteady() error {
	return client.SetMetaLevelSteady(c.meta)
}

func (c *metaClient) setMetaLevelLively() error {
	return client.SetMetaLevelLively(c.meta)
}

func (c *metaClient) Rebalance(primaryOnly bool) error {
	if primaryOnly {
		if err := c.setOnlyMovePrimary(); err != nil {
			return err
		}
	}

	if err := c.setMetaLevelLively(); err != nil {
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

	if err := c.SetMetaLevelSteady(); err != nil {
		return err
	}

	if primaryOnly {
		if err := c.UnsetOnlyMovePrimary(); err != nil {
			return err
		}
	}
	return nil
}

func (c *metaClient) getNodeState(n *util.PegasusNode) (*client.NodeState, error) {
	nodes, err := client.ListNodesReplicaInfo(c.meta)
	if err != nil {
		return nil, err
	}
	for _, rs := range nodes {
		if rs.IPPort == n.TCPAddr() {
			return rs, nil
		}
	}
	panic(fmt.Sprintf("no such node %s", n.TCPAddr()))
}

func (c *metaClient) MigratePrimariesOut(n *util.PegasusNode) error {
	sleptSecs := 0

	// Wait until the node is confirmed to have no primary.
	for {
		if sleptSecs >= 28 {
			return errors.New("MigratePrimariesOut timeout")
		}
		if sleptSecs%10 == 0 {
			err := client.MigratePrimariesOut(c.meta, n)
			if err != nil {
				return err
			}
		}

		nodeState, err := c.getNodeState(n)
		if err == nil {
			if nodeState.PrimariesNum == 0 {
				return nil
			}
			log.Printf("still %d primaries left on %s", nodeState.PrimariesNum, n.CombinedAddr())
		} else {
			log.Error(err)
		}

		time.Sleep(time.Second)
		sleptSecs++
	}
}

func (c *metaClient) DowngradeNodeWithDetails(n *util.PegasusNode) (downgradedParts []*base.Gpid, err error) {
	sleptSecs := 0

	// Wait until the node is confirmed to have no replica.
	for {
		if sleptSecs >= 28 {
			return nil, errors.New("DowngradeNode timeout")
		}
		if sleptSecs%10 == 0 {
			downgradedParts, err = client.DowngradeNodeWithDetails(c.meta, n)
			if err != nil {
				return nil, err
			}
		}

		nodeState, err := c.getNodeState(n)
		if err == nil {
			if nodeState.ReplicaCount == 0 {
				return downgradedParts, nil
			}
			log.Printf("still %d replicas left on %s", nodeState.ReplicaCount, n.CombinedAddr())
		} else {
			log.Error(err)
		}

		time.Sleep(time.Second)
		sleptSecs++
	}
}

func (c *metaClient) ListNodes() ([]*admin.NodeInfo, error) {
	return c.meta.ListNodes()
}

func (c *metaClient) GetClusterReplicaInfo() (*client.ClusterReplicaInfo, error) {
	return client.GetClusterReplicaInfo(c.meta)
}
