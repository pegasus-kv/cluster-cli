package pegasus

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

func RollingUpdateNodes(cluster string, deploy Deployment, metaList string, nodeNames []string) error {
	if err := initNodes(deploy); err != nil {
		return err
	}
	client, err := NewMetaClient(cluster, metaList)
	if err != nil {
		return err
	}

	if err := client.SetMetaLevel("steady"); err != nil {
		return err
	}

	fmt.Println()
	if nodeNames == nil {
		for _, node := range globalAllNodes {
			if node.Job == JobReplica {
				if err := rollingUpdateNode(deploy, client, node); err != nil {
					return err
				}
				fmt.Println()
			}
		}
	} else {
		for _, name := range nodeNames {
			node, ok := findReplicaNode(name)
			if !ok {
				return errors.New("replica node '" + name + "' not found")
			}
			if err := rollingUpdateNode(deploy, client, node); err != nil {
				return err
			}
			fmt.Println()
		}
	}

	if err := client.RemoteCommand("meta.lb.add_secondary_max_count_for_one_node DEFAULT", "OK"); err != nil {
		return err
	}
	if nodeNames == nil {
		fmt.Println("Rolling update meta servers...")
		for _, node := range globalAllNodes {
			if node.Job == JobMeta {
				if err := deploy.RollingUpdate(node); err != nil {
					return err
				}
			}
		}
		fmt.Println("Rolling update meta servers done")
		fmt.Println("Rolling update collectors...")
		for _, node := range globalAllNodes {
			if node.Job == JobCollector {
				if err := deploy.RollingUpdate(node); err != nil {
					return err
				}
			}
		}
		fmt.Println("Rolling update collectors done")

		if err := client.Rebalance(false); err != nil {
			return err
		}
	}

	return nil
}

func rollingUpdateNode(deploy Deployment, client MetaAPI, node Node) error {
	fmt.Printf("Rolling update replica server %s of %s...\n", node.Name, node.IPPort)

	if err := client.RemoteCommand("meta.lb.add_secondary_max_count_for_one_node 0", "OK"); err != nil {
		return err
	}

	c := 0
	fmt.Println("Migrating primary replicas out of node...")
	fin, err := waitFor(func() (bool, error) {
		if c%10 == 0 {
			if err := client.Migrate(node.IPPort); err != nil {
				return false, err
			}
			fmt.Println("Sent migrate propose")
		}
		nodes, err := client.ListNodes()
		if err != nil {
			return false, err
		}
		priCount := -1
		for _, n := range nodes {
			if n.IPPort == node.IPPort {
				priCount = node.Info.PrimaryCount
				break
			}
		}
		fmt.Println("Still " + strconv.Itoa(priCount) + " primary replicas left on " + node.IPPort)
		c++
		return priCount == 0, nil
	}, time.Second, 28)
	if err != nil {
		return err
	}
	if fin {
		fmt.Println("Migrate done")
	} else {
		fmt.Println("Migrate timeout")
	}
	time.Sleep(time.Second)

	fmt.Println("Downgrading replicas on node...")
	c = 0
	var gpids []string
	fin, err = waitFor(func() (bool, error) {
		if c%10 == 0 {
			gpids, err = client.Downgrade(node.IPPort)
			if err != nil {
				return false, err
			}
			fmt.Println("Sent downgrade propose")
		}
		nodes, err := client.ListNodes()
		if err != nil {
			return false, err
		}
		priCount := -1
		for _, n := range nodes {
			if n.IPPort == node.IPPort {
				priCount = n.Info.PrimaryCount
			}
		}
		fmt.Println("Still " + strconv.Itoa(priCount) + " primary replicas left on " + node.IPPort)
		c++
		return priCount == 0, nil
	}, time.Second, 28)
	if err != nil {
		return err
	}
	if fin {
		fmt.Println("Downgrade done")
	} else {
		fmt.Println("Downgrade timeout")
	}
	time.Sleep(time.Second)

	// TODO: Check replicas closed on node here
	// TODO: flush log

	if err := client.RemoteCommand("meta.lb.add_secondary_max_count_for_one_node 100", "OK"); err != nil {
		return err
	}

	fmt.Println("Rolling update by deployment...")
	if err := deploy.RollingUpdate(node); err != nil {
		return err
	}
	fmt.Println("Rolling update by deployment done")

	fmt.Println("Wait " + node.IPPort + " to become alive...")
	if _, err := waitFor(func() (bool, error) {
		nodes, err := client.ListNodes()
		if err != nil {
			return false, err
		}
		var status string
		for _, n := range nodes {
			if n.IPPort == node.IPPort {
				status = n.Info.Status
				break
			}
		}
		return status == "ALIVE", nil
	}, time.Second, 0); err != nil {
		return err
	}

	fmt.Println("Wait " + node.IPPort + " to become healthy...")
	if _, err := waitFor(func() (bool, error) {
		info, err := client.GetHealthyInfo()
		if err != nil {
			return false, err
		}
		if info.Unhealthy == 0 {
			fmt.Println("Cluster becomes healthy")
			return true, nil
		}
		fmt.Printf("Cluster not healthy, unhealthy_partition_count = %d\n", info.Unhealthy)
		return false, nil
	}, time.Duration(10) * time.Second, 0); err != nil {
		return err
	}

	return nil
}
