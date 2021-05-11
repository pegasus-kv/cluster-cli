# Minos

## Introduction

MiNOS is an internal deployment system in XiaoMi. It hides the details of operations on physical service nodes.
Its basic functions include package management, starting/stoping node, and ops-auditing, etc.
So as a service provider, Pegasus does not need to concern with how binary package and config are distributed
among nodes in a cluster, as well as how processes are "supervised".

Minos provides a set of RESTFul APIs for a service to integrate and deploy. For example, to start a node,
we merely need to send an HTTP request to the minos service, and the node will soon get bootstrapped.

## Command-line tool

We in this project also provides a CLI tool that wraps the basic functions of minos for ease of testing.

```
Minos CLI that can operates on Pegasus nodes.

Usage:
  minos [command]

Available Commands:
  help           Help about any command
  rolling-update Rolling-update a single Pegasus node
  show           Show the status of Pegasus nodes
  start          Start a single Pegasus node
  stop           Stop a single Pegasus node

Flags:
  -h, --help   help for minos

Use "minos [command] --help" for more information about a command.
```

### Start

```sh
export
./bin/minos start <cluster> --task 0 --job replica --user wutao
```
