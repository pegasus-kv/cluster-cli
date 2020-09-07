package pegasus

import (
	"fmt"
	"flag"
	"os"
	"strings"
)

var CreateDeployment func(cluster string) Deployment = nil

func Main() {
	var nodes NodeList
	cluster := flag.String("cluster", "", "Cluster to update. (Required)")
	flag.Var(&nodes, "node", "Nodes to take action on. (Required)")
	nodesStr := flag.String("nodes", "", "Nodes to take action on.")
	all := flag.Bool("all", false, "Whether to update all meta, replica and collector nodes.")
	metaList := flag.String("meta-list", "", "List of meta servers. (Required)")

	if len(os.Args) < 2 {
		fmt.Println("add-node, remove-node or rolling-update is required")
		os.Exit(1)
	}

	flag.CommandLine.Parse(os.Args[1:])
	for _, node := range strings.Split(*nodesStr, ",") {
		nodes = append(nodes, node)
	}
	if len(*cluster) == 0 {
		fmt.Println("cluster address not provided")
		os.Exit(1)
	}
	if !(os.Args[1] == "rolling-update" && *all) && len(nodes) == 0 {
		fmt.Println("at least one node is required")
		os.Exit(1)
	}
	if len(*metaList) == 0 {
		fmt.Println("meta list not provided")
		os.Exit(1)
	}

	minosConfPath := os.Getenv("MINOS_CONFIG_FILE")
	if minosConfPath == "" {
		fmt.Println("env MINOS_CONFIG_FILE not provided")
	}
	deploy := CreateDeployment(*cluster)
	var err error
	switch os.Args[1] {
	case "add-node":
		err = AddNodes(*cluster, deploy, *metaList, nodes)
	case "remove-node":
		err = RemoveNodes(*cluster, deploy, *metaList, nodes)
	case "rolling-update":
		if *all {
			nodes = nil
		}
		err = RollingUpdateNodes(*cluster, deploy, *metaList, nodes)
	default:
		fmt.Println("add-node, remove-node or rolling-update is required")
		os.Exit(1)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
