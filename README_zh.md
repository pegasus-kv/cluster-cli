# pegasus-cluster-cli

这是用于Pegasus集群扩容，缩容，升级的命令行工具。

## 使用

``` sh
./pegasus-cluster-cli add-node <cluster address> --meta-list <meta list> --node <node name> [--node <node name>]
./pegasus-cluster-cli remove-node <cluster address> --meta-list <meta list> --node <node name> [--node <node name>]
./pegasus-cluster-cli rolling-update <cluster address> --meta-list <meta list> --node <node name> [--node <node name>] [--all]
```

这里的meta list是MetaServer的ip:port的列表，用逗号隔开。

上面的所有node都指的是ReplicaServer，因为只有Replica是可伸缩的。

对于rolling-update，如果指定了--all，则会升级所有的MetaServer，ReplicaServer，
Collector三种角色的节点。

对于minos来说，这里的node name指的是每个节点的task id。

## 接口

对于其他用户来说，无法直接使用提供的minos或minos2方案。所以需要自己实现一些接口。

我们定义了一个名为`Deployment`的`interface`，如下。

``` go
type Deployment interface {
	StartNode(Node) error
	StopNode(Node) error
	RollingUpdate(Node) error
	ListAllNodes() ([]Node, error)
}
```

`func StartNode(Node) error`

实现节点在一个机器上的启动。一种可能的方式为ssh到这个机器上，下载pegasus
二进制包，下载配置，启动pegasus进程。

`func StopNode(Node) error`

实现节点在某个机器上停止运行。一种可能的方式为ssh到这个机器上，kill掉对应的进程。
或者可以使用supervisord来管理进程。

`func RollingUpdate(Node) error`

实现节点在某个机器上升级。一种可能的方式为ssh登录到机器上，重新下载二进制包和配
置，kill该节点，并重新启动。

`func ListAllNodes() ([]Node, error)`

获取集群内所有节点的信息，包括MetaServer, ReplicaServer, Collector。
`Node`的结构如下。

``` go
type Node struct {
    Job JobType // one of meta, replica, collector
    Name string
    IPPort string
}
```

还有一个函数接口需要实现。

`func CreateDeployment(cluster string) Deployment`

通过命令行传入的cluster address，创建Deployment。具体可以参照minos的代码。

## License

Apache License, Version 2.0
