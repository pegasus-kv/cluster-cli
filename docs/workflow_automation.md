# Workflow Automation

After years of experience of maintaining large-scale systems in Xiaomi, we have learned
that to improve our system with an acceptable speed (aka feature velocity), we must make the
product delivery process as simple as possible.

As a critical component in XiaoMi's data processing pipeline, Pegasus has support of graceful (zero-downtime)
rolling update (via a script called `./scripts/pegasus_rolling_update.sh`), which minimizes the effect to users.

However, there were some problems of the former rolling-update:

- The entire process was too coarse-grained. We didn't have a workflow abstraction of how
to handle failure and cancel the process in mid-stage. Actually, failure handling was documented,
and kept in mind of our SRE rather than being **programmed**.

    For example, during rolling-update we firstly turn the cluster into a state that disables automatic replica migration.
    But when we observe severe unavailablity, we should terminate the script and revert
    the cluster back to normal state.

    Such a case can be generalized as:

    ```
    prepare
        |
        v
    loop: for each node, rolling-update
        |            |
        |            v failed
        |          stop and revert
        v
    finish
    ```

- We were not able to monitor the overall upgrade progress with visualization, like WebUI. We have found that
simplicity and usability should be a big concern of our ops system.

- The process was not extensible. For example, we might need to integrate some checking before and after each of the steps. Ideally, adding/removing steps could be as easy as dragging boxes in a graph. (This idea is now widely used in low-code platforms).

To resolve the above stated challenges, in Xiaomi, our SRE team develops a workflow management platform.
Although such a platform is not a requirement of `pegasus-cluster-cli`, we have built this tool to have the ability
to integrate with such a platform. 

## Commands in design

To fit in a workflow system, the rolling-update command should be divided into serveral subcommands:

```sh
./pegasus-cluster-cli rolling-update prepare <cluster_name>
./pegasus-cluster-cli rolling-update run <cluster_name> --replica --host <hostname>
./pegasus-cluster-cli rolling-update finish_replica <cluster_name>
./pegasus-cluster-cli rolling-update run <cluster_name> --meta --host <hostname>
./pegasus-cluster-cli rolling-update run <cluster_name> --collector --host <hostname>
./pegasus-cluster-cli rolling-update finish <cluster_name>
```
