package pegasus

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type JobType int

const (
	JobMeta      = 0
	JobReplica   = 1
	JobCollector = 2
)

func (j JobType) String() string {
	switch j {
	case JobMeta:
		return "meta"
	case JobReplica:
		return "replica"
	default:
		return "collector"
	}
}

type Node struct {
	Job    JobType
	Name   string
	IPPort string
}

type Deployment interface {
	StartNode(Node) error
	StopNode(Node) error
	RollingUpdate(Node) error
	ListAllNodes() ([]Node, error)
}

type DeployError struct {
	Msg string
	Output []byte
}

func (e *DeployError) Error() string {
	return e.Msg + ". Output:\n" + string(e.Output) + "\n"
}

func NewDeployError(msg string, out []byte) *DeployError {
	return &DeployError{msg, out}
}

var CreateDeployment func(cluster string) Deployment = nil

var nodes []Node

func initNodes(deploy Deployment) error {
	res, err := deploy.ListAllNodes()
	if err != nil {
		return err
	}
	nodes = res
	return nil
}

func findReplicaNode(name string) (Node, bool) {
	for _, node := range nodes {
		if node.Job == JobReplica && name == node.Name {
			return node, true
		}
	}
	return Node{}, false
}

func ValidateCluster(cluster string, metaList string, nodeNames []string) (string, error) {
	nodeMap := make(map[string]bool)
	for _, name := range nodeNames {
		_, prs := nodeMap[name]
		if prs {
			return "", errors.New("duplicate node '" + name + "' in node list")
		}
		nodeMap[name] = true
	}

	ok1, ok2 := false, false
	r1 := regexp.MustCompile(`/([^/]*)$`)
	r2 := regexp.MustCompile(`([0-9.:]*)\s*$`)
	cmd, err := runShellInput("cluster_info", metaList)
	if err != nil {
		return "", err
	}

	var pmeta string
	out, err := checkOutput(cmd, true, func(line string) bool {
		if strings.Contains(line, "zookeeper_root") {
			rs := r1.FindStringSubmatch(line)
			if strings.TrimSpace(rs[1]) == cluster {
				ok1 = true
			}
		} else if strings.Contains(line, "primary_meta_server") {
			rs := r2.FindStringSubmatch(line)
			if len(rs[1]) != 0 {
				ok2 = true
				pmeta = rs[1]
			}
		}
		return ok1 && ok2
	});
	if err != nil {
		return "", err
	}
	if !ok1 {
		return "", NewDeployError("cluster name and meta list not matched", out)
	} else if !ok2 {
		return "", NewDeployError("extract primary_meta_server by shell failed", out)
	} else {
		return pmeta, nil
	}
}

func AddNodes(cluster string, deploy Deployment, metaList string, nodeNames []string) error {
	if err := initNodes(deploy); err != nil {
		return err
	}
	pmeta, err := ValidateCluster(cluster, metaList, nodeNames)
	if err != nil {
		return err
	}

	if err = setMetaLevel("steady", metaList); err != nil {
		return err
	}

	for _, name := range nodeNames {
		node, ok := findReplicaNode(name)
		if !ok {
			return errors.New("replica node '" + name + "' not found")
		}
		if err := deploy.StartNode(node); err != nil {
			return err
		}
	}
	if err := rebalanceCluster(pmeta, metaList, false); err != nil {
		return err
	}

	return nil
}

func RemoveNodes(cluster string, deploy Deployment, metaList string, nodeNames []string) error {
	if err := initNodes(deploy); err != nil {
		return err
	}
	pmeta, err := ValidateCluster(cluster, metaList, nodeNames)
	if err != nil {
		return err
	}

	nodes := make([]Node, len(nodeNames))
	addrs := make([]string, len(nodeNames))
	for i, name := range nodeNames {
		node, ok := findReplicaNode(name)
		if !ok {
			return errors.New("replica node '" + name + "' not found")
		}
		nodes[i] = node
		addrs[i] = node.IPPort
	}
	if err := setRemoteCommand(pmeta, "meta.lb.assign_secondary_black_list", strings.Join(addrs, ","), metaList); err != nil {
		return err
	}
	if err := setRemoteCommand(pmeta, "meta.live_percentage", "0", metaList); err != nil {
		return err
	}

	for _, node := range nodes {
		if err := removeNode(deploy, metaList, pmeta, node); err != nil {
			return err
		}
	}
	return nil
}

func RollingUpdateNodes(cluster string, deploy Deployment, metaList string, nodeNames []string) error {
	if err := initNodes(deploy); err != nil {
		return err
	}
	pmeta, err := ValidateCluster(cluster, metaList, nodeNames)
	if err != nil {
		return err
	}

	if err := setMetaLevel("steady", metaList); err != nil {
		return err
	}

	if nodeNames == nil {
		for _, node := range nodes {
			if node.Job == JobReplica {
				if err := rollingUpdateNode(deploy, pmeta, metaList, node); err != nil {
					return err
				}
			}
		}
	} else {
		for _, name := range nodeNames {
			node, ok := findReplicaNode(name)
			if !ok {
				return errors.New("replica node '" + name + "' not found")
			}
			if err := rollingUpdateNode(deploy, pmeta, metaList, node); err != nil {
				return err
			}
		}
	}

	if err := setRemoteCommand(pmeta, "meta.lb.add_secondary_max_count_for_one_node", "DEFAULT", metaList); err != nil {
		return err
	}
	if nodeNames == nil {
		for _, node := range nodes {
			if node.Job == JobMeta {
				if err := deploy.RollingUpdate(node); err != nil {
					return err
				}
			}
		}
		for _, node := range nodes {
			if node.Job == JobCollector {
				if err := deploy.RollingUpdate(node); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func rollingUpdateNode(deploy Deployment, pmeta string, metaList string, node Node) error {
	if err := setRemoteCommand(pmeta, "meta.lb.add_secondary_max_count_for_one_node", "0", metaList); err != nil {
		return err
	}
	c := 0
	var gpids []string
	if _, err := waitFor(func() (interface{}, error) {
		if c%10 == 0 {
			if err := runSh("migrate_node", "-c", metaList, "-n", node.IPPort, "-t", "run").Start(); err != nil {
				return nil, err
			}
		}
		cmd, err := runShellInput("nodes -d", metaList)
		if err != nil {
			return nil, err
		}
		priCount := -1
		if _, err := checkOutput(cmd, false, func(line string) bool {
			if strings.HasPrefix(line, "propose ") {
				gpids = append(gpids, strings.ReplaceAll(strings.Fields(line)[2], ".", " "))
			}
			if strings.Contains(line, node.IPPort) {
				ss := strings.Fields(line)
				res, err := strconv.Atoi(ss[3])
				if err != nil {
					return false
				}
				c = res
				return true
			}
			priCount = 0
			return false
		}); err != nil {
			return nil, err
		}
		c += 1
		return priCount, nil
	}, func(v interface{}) bool { return v == 0 }, time.Second, 28); err != nil {
		return err
	}
	time.Sleep(time.Second)

	r1 := regexp.MustCompile(`replica_stub\.replica\(Count\)","type":"NUMBER","value":([0-9]*)`)
	r2 := regexp.MustCompile(`replica_stub\.opening\.replica\(Count\)","type":"NUMBER","value":([0-9]*)`)
	r3 := regexp.MustCompile(`replica_stub\.closing\.replica\(Count\)","type":"NUMBER","value":([0-9]*)`)
	if _, err := waitFor(func() (interface{}, error) {
		if c%10 == 0 {
			for _, gpid := range gpids {
				cmd, err := runShellInput("remote_command -l "+node.IPPort+" replica.kill_partition "+gpid, metaList)
				if err != nil {
					return nil, err
				}
				if err := cmd.Start(); err != nil {
					return nil, err
				}
			}
			if err := runSh("migrate_node", "-c", metaList, "-n", node.IPPort, "-t", "run").Start(); err != nil {
				return nil, err
			}
		}
		cmd, err := runShellInput("remote_command -l "+node.IPPort+" perf-counters '.*replica(Count)'", metaList)
		if err != nil {
			return nil, err
		}
		serving := -1
		opening := -1
		closing := -1
		if _, err := checkOutput(cmd, false, func(line string) bool {
			ss := r1.FindStringSubmatch(line)
			if len(ss) > 1 {
				if v, err := strconv.Atoi(ss[1]); err == nil {
					serving = v
					return false
				}
			}
			ss = r2.FindStringSubmatch(line)
			if len(ss) > 1 {
				if v, err := strconv.Atoi(ss[1]); err == nil {
					opening = v
					return false
				}
			}
			ss = r3.FindStringSubmatch(line)
			if len(ss) > 1 {
				if v, err := strconv.Atoi(ss[1]); err == nil {
					closing = v
					return true
				}
			}
			return false
		}); err != nil {
			return nil, err
		}
		if serving == -1 || opening == -1 || closing == -1 {
			return nil, errors.New("extract replica count from perf counters failed")
		}
		c += 1
		return []int{serving, opening, closing}, nil
	}, func(v interface{}) bool {
		a := v.([]int)
		return a[0]+a[1]+a[2] == 0
	}, time.Second, 28); err != nil {
		return err
	}

	if err := startRunShellInput("remote_command -l "+node.IPPort+" flush_log", metaList); err != nil {
		return err
	}
	if err := setRemoteCommand(pmeta, "meta.lb.add_secondary_max_count_for_one_node", "100", metaList); err != nil {
		return err
	}

	if err := deploy.RollingUpdate(node); err != nil {
		return err
	}

	if _, err := waitFor(func() (interface{}, error) {
		cmd, err := runShellInput("nodes -d", metaList)
		if err != nil {
			return nil, err
		}
		var status string
		if _, err := checkOutput(cmd, false, func(line string) bool {
			if strings.Contains(line, node.IPPort) {
				ss := strings.Fields(line)
				if len(ss) < 2 {
					return false
				}
				status = ss[1]
			}
			return false
		}); err != nil {
			return nil, err
		}
		return status, nil
	}, func(v interface{}) bool { return v == "ALIVE" }, time.Second, 0); err != nil {
		return err
	}

	if _, err := waitFor(func() (interface{}, error) {
		cmd, err := runShellInput("ls -d", metaList)
		if err != nil {
			return nil, err
		}
		var status string
		if _, err := checkOutput(cmd, false, func(line string) bool {
			if strings.Contains(line, node.IPPort) {
				ss := strings.Fields(line)
				if len(ss) < 2 {
					return false
				}
				status = ss[1]
			}
			return false
		}); err != nil {
			return nil, err
		}
		return status, nil
	}, func(v interface{}) bool { return v == "ALIVE" }, time.Second, 0); err != nil {
		return err
	}

	if err := waitForHealthy(metaList); err != nil {
		return err
	}

	return nil
}

func removeNode(deploy Deployment, metaList string, pmeta string, node Node) error {
	if err := setMetaLevel("steady", metaList); err != nil {
		return err
	}

	if err := setRemoteCommand(pmeta, "meta.lb.assign_delay_ms", "10", metaList); err != nil {
		return err
	}

	// migrate node
	if err := runSh("migrate_node", "-c", metaList, "-n", node.IPPort, "-t", "run").Run(); err != nil {
		return err
	}
	// wait for pri_count == 0
	if _, err := waitFor(func() (interface{}, error) {
		val := 0
		cmd, err := runShellInput("nodes -d", metaList)
		if err != nil {
			return nil, err
		}
		if _, err := checkOutput(cmd, false, func(line string) bool {
			if strings.Contains(line, node.IPPort) {
				// TODO: check field length
				val, err = strconv.Atoi(strings.Fields(line)[3])
				if err != nil {
					return false
				}
				return true
			}
			return false
		}); err != nil {
			return nil, err
		}
		return val, nil
	}, func(val interface{}) bool { return val == 0 }, time.Second, 0); err != nil {
		return err
	}
	time.Sleep(time.Second)

	// downgrade node and kill partition
	var gpid []string
	if _, err := checkOutput(runSh("downgrade_node", "-c", metaList, "-n", node.IPPort, "-t", "run"), false, func(line string) bool {
		if strings.HasPrefix(line, "propose ") {
			gpid = append(gpid, strings.ReplaceAll(strings.Fields(line)[2], ".", " "))
			return true
		}
		return false
	}); err != nil {
		return err
	}
	// wait for rep_count == 0
	if _, err := waitFor(func() (interface{}, error) {
		val := 0
		cmd, err := runShellInput("nodes -d", metaList)
		if err != nil {
			return nil, err
		}
		if _, err := checkOutput(cmd, false, func(line string) bool {
			if strings.Contains(line, node.IPPort) {
				val, err = strconv.Atoi(strings.Fields(line)[2])
				if err != nil {
					return false
				}
				return true
			}
			return false
		}); err != nil {
			return nil, err
		}
		return val, nil
	}, func(val interface{}) bool { return val == 0 }, time.Second, 0); err != nil {
		return err
	}
	time.Sleep(time.Second)

	if err := deploy.StopNode(node); err != nil {
		return err
	}
	time.Sleep(time.Second)

	if err := waitForHealthy(metaList); err != nil {
		return err
	}

	if err := setRemoteCommand(pmeta, "meta.lb.assign_delay_ms", "DEFAULT", metaList); err != nil {
		return err
	}
	return nil
}

func rebalanceCluster(pmeta string, metaList string, primaryOnly bool) error {
	if primaryOnly {
		if err := setRemoteCommand(pmeta, "meta.lb.only_move_primary", "true", metaList); err != nil {
			return err
		}
	}

	if err := setMetaLevel("lively", metaList); err != nil {
		return err
	}

	fmt.Println("Wait for 3 minutes to do load balance...")
	time.Sleep(time.Duration(180) * time.Second)

	remainTimes := 1
	r := regexp.MustCompile("total=(\\d+)")
	for {
		cmd, err := runShellInput("cluster_info", metaList)
		if err != nil {
			return err
		}
		var opCount string
		_, err = checkOutput(cmd, false, func(line string) bool {
			if strings.Contains(line, "balance_operation_count") {
				rs := r.FindStringSubmatch(line)
				opCount = rs[1]
				return true
			}
			return false
		})
		if opCount == "" {
			break
		}

		if opCount == "0" {
			if remainTimes == 0 {
				break
			} else {
				fmt.Println("cluster may be balanced, try wait 30 seconds...")
				remainTimes--
				time.Sleep(time.Duration(30) * time.Second)
			}
		} else {
			fmt.Printf("still %s balance operations to do...", opCount)
			time.Sleep(time.Duration(10) * time.Second)
		}
	}

	if err := setMetaLevel("steady", metaList); err != nil {
		return err
	}

	if primaryOnly {
		if err := setRemoteCommand(pmeta, "meta.lb.only_move_primary", "false", metaList); err != nil {
			return err
		}
	}
	return nil
}

func setMetaLevel(level string, metaList string) error {
	cmd, err := runShellInput("set_meta_level "+level, metaList)
	if err != nil {
		return err
	}
	ok, out, err := checkOutputContainsOnce(cmd, false, "control meta level ok")
	if err != nil {
		return err
	}
	if !ok {
		return NewDeployError("set meta level to " + level + " failed", out)
	}
	return nil
}

func setRemoteCommand(pmeta string, attr string, value string, metaList string) error {
	cmd, err := runShellInput(fmt.Sprintf("remote_command -l %s %s %s", pmeta, attr, value), metaList)
	if err != nil {
		return err
	}
	ok, out, err := checkOutputContainsOnce(cmd, true, "OK")
	if err != nil {
		return err
	}
	if !ok {
		return NewDeployError("set " + attr + " to " + value + " failed", out)
	}
	return nil
}

func waitForHealthy(metaList string) error {
	_, err := waitFor(func() (interface{}, error) {
		cmd, err := runShellInput("ls -d", metaList)
		if err != nil {
			return nil, err
		}
		flag := false
		count := 0
		if _, err := checkOutput(cmd, false, func(line string) bool {
			if flag {
				ss := strings.Fields(line)
				if len(ss) < 7 {
					flag = false
				} else if ss[2] != ss[3] {
					s5, err := strconv.Atoi(ss[4])
					if err != nil {
						return false
					}
					s6, err := strconv.Atoi(ss[5])
					if err != nil {
						return false
					}
					count += s5 + s6
				}
			}
			if strings.Contains(line, " fully_healthy ") {
				flag = true
			}
			return false
		}); err != nil {
			return nil, err
		}
		return count, nil
	}, func(v interface{}) bool { return v == 0 }, time.Duration(10)*time.Second, 0)
	return err
}
