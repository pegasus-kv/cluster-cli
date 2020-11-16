package pegasus

import (
	"time"
	"pegasus-cluster-cli/client"

	log "github.com/sirupsen/logrus"
)

func migratePrimariesOutOfNode(metaClient MetaClient, node *ReplicaNode) error {
	log.Print("migrating primary replicas out of node...")

	c := 0
	fin, err := waitFor(func() (bool, error) {
		if c % 10 == 0 {
			if err := metaClient.Migrate(node.IPPort()); err != nil {
				return false, err
			}
			log.Print("proposed primaries migration to MetaServer")
		}
		if err := node.updateInfo(metaClient); err != nil {
			return false, err
		}
		log.Printf("Still %d primary replicas left on %s", node.PrimaryCount, node.IPPort())
		c++
		return node.PrimaryCount == 0, nil
	}, time.Second, 28)

	if err != nil {
		return err
	}
	if fin {
		log.Print("migrate done")
	} else {
		log.Print("migrate timeout")
	}
	time.Sleep(time.Second)

	return nil
}

func downgradeReplicasOnNode(metaClient MetaClient, node *ReplicaNode) ([]string, error) {
	log.Print("Downgrading replicas on node...")

	c := 0
	var gpids []string
	var err error
	fin, err := waitFor(func() (bool, error) {
		if c % 10 == 0 {
			gpids, err = metaClient.Downgrade(node.IPPort())
			if err != nil {
				return false, err
			}
			log.Print("Sent downgrade propose")
		}
		if err := node.updateInfo(metaClient); err != nil {
			return false, err
		}
		log.Printf("Still %d primary replicas left on %s", node.PrimaryCount, node.IPPort())
		c++
		return node.PrimaryCount == 0, nil
	}, time.Second, 28)

	if err != nil {
		return nil, err
	}
	if fin {
		log.Print("Downgrade done")
	} else {
		log.Print("Downgrade timeout")
	}
	time.Sleep(time.Second)

	return gpids, nil
}

func CloseReplicasOnNode(remoteCmdClient *client.RemoteCmdClient, node *ReplicaNode, gpids []string) error {
	log.Print("Closing replicas on node...")

	c := 0
	fin, err := waitFor(func() (bool, error) {
		if c % 10 == 0 {
			log.Print("Send kill_partition commands to node...")
			for _, gpid := range gpids {
				if _, err := remoteCmdClient.KillPartition(gpid); err != nil {
					return false, err
				}
			}
			log.Printf("Sent to %d partitions.", len(gpids))
		}
		counters, err := remoteCmdClient.GetPerfCounters(".*replica(Count)")
		if err != nil {
			return false, err
		}
		count := 0
		for _, counter := range counters {
			count += int(counter.Value)
		}
		log.Printf("Still %d replicas not closed on %s", count, node.IPPort())
		c++
		return count == 0, nil
	}, time.Second, 28)

	if err != nil {
		return err
	}
	if fin {
		log.Print("Close done.")
	} else {
		log.Print("Close timeout.")
	}

	return nil
}

func waitNodeAlive(node *ReplicaNode, metaClient MetaClient) (bool, error) {
	log.Printf("Wait %s to become alive...", node.IPPort())
	return waitFor(func() (bool, error) {
		if err := node.updateInfo(metaClient); err != nil {
			return false, err
		}
		return node.Status == "ALIVE", nil
	}, time.Second, 0)
}

func waitNodeHealthy(node *ReplicaNode, metaClient MetaClient) (bool, error) {
	log.Printf("Wait %s to become healthy...", node.IPPort())
	return waitFor(func() (bool, error) {
		infos, err := metaClient.ListTableHealthInfos()
		if err != nil {
			return false, err
		}
		count := 0
		for _, info := range infos {
			if info.PartitionCount != info.FullyHealthy {
				count++
			}
		}
		if count != 0 {
			log.Printf("Cluster not healthy, unhealthy app count %d", count)
			return false, nil
		}
		log.Print("Cluster becomes healthy")
		return true, nil
	}, time.Duration(10)*time.Second, 0)
}
