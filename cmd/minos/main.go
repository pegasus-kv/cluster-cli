package main

import (
	"os"
	"pegasus-cluster-cli/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
