package pegasus

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

var globalAllNodes []Node

func initNodes(deploy Deployment) error {
	res, err := deploy.ListAllNodes()
	if err != nil {
		return err
	}
	globalAllNodes = res
	return nil
}

func findReplicaNode(name string) (Node, bool) {
	for _, node := range globalAllNodes {
		if node.Job == JobReplica && name == node.Name {
			return node, true
		}
	}
	return Node{}, false
}
