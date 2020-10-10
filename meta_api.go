package pegasus

import (
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type MetaAPI interface {
	GetHealthyInfo() (*HealthyInfo, error)
	RemoteCommand(string, string) error
	SetMetaLevel(string) error
	Rebalance(bool) error
	Migrate(string) error
	Downgrade(string) ([]string, error)
	KillPartitions(string, []string) error
	ListNodes() ([]Node, error)
}

type MetaClient struct {
	PrimaryMeta string
	MetaList    string
}

func NewMetaClient(cluster string, metaList string) (*MetaClient, error) {
	info, err := GetClusterInfo(metaList)
	if err != nil {
		return nil, err
	}
	if info.Cluster != cluster {
		return nil, errors.New(fmt.Sprintf("cluster name and meta list not matched, got '%s'", info.Cluster))
	}
	return &MetaClient{
		PrimaryMeta: info.PrimaryMeta,
		MetaList:    metaList,
	}, nil
}

func (c *MetaClient) buildCmd(command string) (*exec.Cmd, error) {
	return runShellInput(command, c.MetaList)
}

func (c *MetaClient) GetHealthyInfo() (*HealthyInfo, error) {
	cmd, err := c.buildCmd("ls -d")
	if err != nil {
		return nil, err
	}
	flag := false
	ok := false
	var (
		partitionCount int
		fullyHealthy int
		unhealthy int
		writeUnhealthy int
		readUnhealthy int
	)
	out, err := checkOutput(cmd, false, func(line string) bool {
		if flag {
			ss := strings.Fields(line)
			if len(ss) < 7 {
				flag = false
			} else {
				partitionCount, err = strconv.Atoi(ss[2])
				if err != nil {
					return false
				}
				fullyHealthy, err = strconv.Atoi(ss[3])
				if err != nil {
					return false
				}
				unhealthy, err = strconv.Atoi(ss[4])
				if err != nil {
					return false
				}
				writeUnhealthy, err = strconv.Atoi(ss[5])
				if err != nil {
					return false
				}
				readUnhealthy, err = strconv.Atoi(ss[6])
				if err != nil {
					return false
				}
				ok = true
				return true
			}
		} else if strings.Contains(line, "  fully_healthy  ") {
			flag = true
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, NewCommandError("failed to get healthy info", out)
	}
	return &HealthyInfo{
		PartitionCount: partitionCount,
		FullyHealthy: fullyHealthy,
		Unhealthy: unhealthy,
		WriteUnhealthy: writeUnhealthy,
		ReadUnhealthy: readUnhealthy,
	}, nil
}

func (c *MetaClient) SetMetaLevel(level string) error {
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
		return NewCommandError("set meta level to "+level+" failed", out)
	}
	return nil
}

func (c *MetaClient) RemoteCommand(command string, pattern string) error {
	cmd, err := c.buildCmd(fmt.Sprintf("remote_command -l %s %s", c.PrimaryMeta, command))
	if err != nil {
		return err
	}
	ok, out, err := checkOutputContainsOnce(cmd, true, pattern)
	if err != nil {
		return err
	}
	if !ok {
		return NewCommandError(fmt.Sprintf("remote command '%s' for %s failed", command, c.PrimaryMeta), out)
	}
	return nil
}

func (c *MetaClient) Rebalance(primaryOnly bool) error {
	if primaryOnly {
		if err := c.RemoteCommand("meta.lb.only_move_primary true", "OK"); err != nil {
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
		info, err := GetClusterInfo(c.MetaList)
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
		if err := c.RemoteCommand("meta.lb.only_move_primary false", "OK"); err != nil {
			return err
		}
	}
	return nil
}

func (c *MetaClient) Migrate(addr string) error {
	if err := runSh("migrate_node", "-c", c.MetaList, "-n", addr, "-t", "run").Run(); err != nil {
		return err
	}
	return nil
}

func (c *MetaClient) Downgrade(addr string) ([]string, error) {
	var gpids []string
	if _, err := checkOutput(runSh("downgrade_node", "-c", c.MetaList, "-n", addr, "-t run"), false, func(line string) bool {
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

func (c *MetaClient) KillPartitions(addr string, gpids []string) error {
	fmt.Println("Send kill_partition commands to node...")
	for _, gpid := range gpids {
		cmd, err := runShellInput("remote_command -l "+addr+" replica.kill_partition "+gpid, c.MetaList)
		if err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			return err
		}
	}
	fmt.Println("Sent to " + strconv.Itoa(len(gpids)) + " partitions")
	return nil
}

func (c *MetaClient) ListNodes() ([]Node, error) {
	cmd, err := c.buildCmd("nodes -d")
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+:\d+`)
	nodes := []Node{}
	_, err = checkOutput(cmd, false, func(line string) bool {
		if re.MatchString(line) {
			ss := strings.Fields(line)
			if len(ss) == 5 {
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
				info := &NodeInfo{
					Status: ss[1],
					ReplicaCount: replica,
					PrimaryCount: primary,
					SecondaryCount: secondary,
				}
				nodes = append(nodes, Node{JobReplica, "", ss[0], info})
			}
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	return nodes, nil
}
