package pegasus

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func RemoveNodes(cluster string, deploy Deployment, metaList string, nodeNames []string) error {
	if err := initNodes(deploy); err != nil {
		return err
	}
	client, err := NewMetaClient(cluster, metaList)
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
	if err := client.RemoteCommand("meta.lb.assign_secondary_black_list " + strings.Join(addrs, ","), "set ok"); err != nil {
		return err
	}
	if err := client.RemoteCommand("meta.live_percentage 0", "OK"); err != nil {
		return err
	}

	fmt.Println()
	for _, node := range nodes {
		if err := removeNode(deploy, client, node); err != nil {
			return err
		}
		fmt.Println()
	}
	return nil
}

func removeNode(deploy Deployment, client MetaAPI, node Node) error {
	fmt.Println("Stopping replica node " + node.Name + " of " + node.IPPort + " ...")
	if err := client.SetMetaLevel("steady"); err != nil {
		return err
	}

	if err := client.RemoteCommand("meta.lb.assign_delay_ms 10", "OK"); err != nil {
		return err
	}

	// migrate node
	fmt.Println("Migrating primary replicas out of node...")
	if err := client.Migrate(node.IPPort); err != nil {
		return err
	}
	// wait for pri_count == 0
	fmt.Println("Wait " + node.IPPort + " to migrate done...")
	if _, err := waitFor(func() (bool, error) {
		val := 0
		nodes, err := client.ListNodes()
		if err != nil {
			return false, err
		}
		for _, n := range nodes {
			if n.IPPort == node.IPPort {
				val = n.Info.PrimaryCount
				break
			}
		}
		if val == 0 {
			fmt.Println("Migrate done.")
			return true, nil
		}
		fmt.Println("Still " + strconv.Itoa(val) + " primary replicas left on " + node.IPPort)
		return false, nil
	}, time.Second, 0); err != nil {
		return err
	}
	time.Sleep(time.Second)

	// downgrade node and kill partition
	fmt.Println("Downgrading replicas on node...")
	gpids, err := client.Downgrade(node.IPPort)
	if err != nil {
		return err
	}
	// wait for rep_count == 0
	fmt.Println("Wait " + node.IPPort + " to downgrade done...")
	if _, err := waitFor(func() (bool, error) {
		val := 0
		nodes, err := client.ListNodes()
		if err != nil {
			return false, err
		}
		for _, n := range nodes {
			if n.IPPort == node.IPPort {
				val = n.Info.ReplicaCount
				break
			}
		}
		if val == 0 {
			fmt.Println("Downgrade done.")
			return true, nil
		}
		fmt.Println("Still " + strconv.Itoa(val) + " replicas left on " + node.IPPort)
		return false, nil
	}, time.Second, 0); err != nil {
		return err
	}
	time.Sleep(time.Second)

	if err := client.KillPartitions(node.IPPort, gpids); err != nil {
		return err
	}

	fmt.Println("Stop node by deployment...")
	if err := deploy.StopNode(node); err != nil {
		return err
	}
	fmt.Println("Stop node by deployment done")
	time.Sleep(time.Second)

	fmt.Println("Wait cluster to become healthy...")
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

	if err := client.RemoteCommand("meta.lb.assign_delay_ms DEFAULT", "OK"); err != nil {
		return err
	}
	return nil
}
