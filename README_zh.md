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

## License

Apache License, Version 2.0
