package pegasus

import (
	"errors"
	"fmt"
)

func AddNodes(cluster string, deploy Deployment, metaList string, nodeNames []string) error {
	if err := initNodes(deploy); err != nil {
		return err
	}
	client, err := NewMetaClient(cluster, metaList)
	if err != nil {
		return err
	}

	if err = client.SetMetaLevel("steady"); err != nil {
		return err
	}

	fmt.Println()
	for _, name := range nodeNames {
		node, ok := findReplicaNode(name)
		if !ok {
			return errors.New("replica node '" + name + "' not found")
		}
		fmt.Println("Starting node " + node.IPPort + " by deployment...")
		if err := deploy.StartNode(node); err != nil {
			return err
		}
		fmt.Println("Starting node by deployment done")
		fmt.Println()
	}
	if err := client.Rebalance(false); err != nil {
		return err
	}

	return nil
}
