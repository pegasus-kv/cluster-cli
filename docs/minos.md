# Minos

## Introduction

MiNOS is an internal deployment system in XiaoMi. It hides the details of operations on physical service nodes, including
StartNode, StopNode, and Rolling-Upgrade, etc. So as a service provider, Pegasus does not need to concern with how
binary package and config are distributed among nodes in a cluster, as well as how processes are "supervised".

Moreover, minos records every operation for auditing. Though this is not a requirement to implement
cluster-cli's `Deployment` API, it's still important to tracks the history in order to.

Minos provides a set of RESTFul APIs for a service to integrate and deploy. For example, to start a node,
we merely need to send an HTTP request to the minos service.

## Command-line tool

We in this project also provides a CLI tool that wraps the basic functions of minos for testing.


