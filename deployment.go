package pegasus

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type Deployment interface {
	StartNode(Node) error
	StopNode(Node) error
	RollingUpdate(Node) error
	ListAllNodes() ([]Node, error)
}

type CommandError struct {
	Msg    string
	Output []byte
}

func (e *CommandError) Error() string {
	return e.Msg + ". Output:\n" + string(e.Output)
}

func NewCommandError(msg string, out []byte) *CommandError {
	return &CommandError{msg, out}
}

var CreateDeployment func(cluster string) Deployment = nil

func ValidateCluster(cluster string, metaList string, nodeNames []string) (string, error) {
	fmt.Println("Validate cluster name and node list...")
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
			if len(rs) > 1 && strings.TrimSpace(rs[1]) == cluster {
				ok1 = true
			}
		} else if strings.Contains(line, "primary_meta_server") {
			rs := r2.FindStringSubmatch(line)
			if len(rs) > 1 && len(rs[1]) != 0 {
				ok2 = true
				pmeta = rs[1]
			}
		}
		return ok1 && ok2
	})
	if err != nil {
		return "", err
	}
	if !ok1 {
		return "", NewCommandError("cluster name and meta list not matched", out)
	} else if !ok2 {
		return "", NewCommandError("extract primary_meta_server by shell failed", out)
	} else {
		return pmeta, nil
	}
}
