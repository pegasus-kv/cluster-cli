package main

import (
	"flag"
	"fmt"
	"os"
	"pegasus-cluster-cli"
	"strings"
)

func main() {
	var nodes pegasus.NodeList
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
	if !(os.Args[1] == "rolling-update" && *all) && len(nodes) == 0 {
		fmt.Println("at least one node is required")
		os.Exit(1)
	}
	if len(*metaList) == 0 {
		fmt.Println("meta list not provided")
		os.Exit(1)
	}

	pegasusConf := os.Getenv("PEGASUS_CONFIG")
	if pegasusConf == "" {
		fmt.Println("env PEGASUS_CONFIG not provided")
		os.Exit(1)
	}
	minosClientDir := os.Getenv("MINOS_CLIENT_DIR")
	if minosClientDir == "" {
		fmt.Println("env MINOS_CLIENT_DIR not provided")
		os.Exit(1)
	}
	minosConfPath := os.Getenv("MINOS_CONFIG_FILE")
	if minosConfPath == "" {
		fmt.Println("env MINOS_CONFIG_FILE not provided")
	}

	minos, err := NewMinosDeployment(*cluster, pegasusConf, minosClientDir, *metaList)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	switch os.Args[1] {
	case "add-node":
		pegasus.AddNodes(*cluster, minos, *metaList, nodes)
	case "remove-node":
		pegasus.RemoveNodes(*cluster, minos, *metaList, nodes)
	case "rolling-update":
		if *all {
			nodes = nil
		}
		pegasus.RollingUpdateNodes(*cluster, minos, *metaList, nodes)
	default:
		fmt.Println("add-node, remove-node or rolling-update is required")
		os.Exit(1)
	}
}
