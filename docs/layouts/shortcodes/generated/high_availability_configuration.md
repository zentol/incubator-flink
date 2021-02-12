| Key | Default | Type | Description |
|-----|---------|------|-------------|
| high-availability | "NONE" | String | Defines high-availability mode used for the cluster execution. To enable high-availability, set this mode to "ZOOKEEPER" or specify FQN of factory class. |
| high-availability.cluster-id | "/default" | String | The ID of the Flink cluster, used to separate multiple Flink clusters from each other. Needs to be set for standalone clusters but is automatically inferred in YARN and Mesos. |
| high-availability.jobmanager.port | "0" | String | The port (range) used by the Flink Master for its RPC connections in highly-available setups. In highly-available setups, this value is used instead of 'jobmanager.rpc.port'.A value of '0' means that a random free port is chosen. TaskManagers discover this port through the high-availability services (leader election), so a random port or a port range works without requiring any additional means of service discovery. |
| high-availability.storageDir | (none) | String | File system path (URI) where Flink persists metadata in high-availability setups. |
| high-availability.zookeeper.client.acl | "open" | String | Defines the ACL (open|creator) to be configured on ZK node. The configuration value can be set to “creator” if the ZooKeeper server configuration has the “authProvider” property mapped to use SASLAuthenticationProvider and the cluster is configured to run in secure mode (Kerberos). |
| high-availability.zookeeper.client.connection-timeout | 15000 | Integer | Defines the connection timeout for ZooKeeper in ms. |
| high-availability.zookeeper.client.max-retry-attempts | 3 | Integer | Defines the number of connection retries before the client gives up. |
| high-availability.zookeeper.client.retry-wait | 5000 | Integer | Defines the pause between consecutive retries in ms. |
| high-availability.zookeeper.client.session-timeout | 60000 | Integer | Defines the session timeout for the ZooKeeper session in ms. |
| high-availability.zookeeper.path.checkpoint-counter | "/checkpoint-counter" | String | ZooKeeper root path (ZNode) for checkpoint counters. |
| high-availability.zookeeper.path.checkpoints | "/checkpoints" | String | ZooKeeper root path (ZNode) for completed checkpoints. |
| high-availability.zookeeper.path.jobgraphs | "/jobgraphs" | String | ZooKeeper root path (ZNode) for job graphs |
| high-availability.zookeeper.path.latch | "/leaderlatch" | String | Defines the znode of the leader latch which is used to elect the leader. |
| high-availability.zookeeper.path.leader | "/leader" | String | Defines the znode of the leader which contains the URL to the leader and the current leader session ID. |
| high-availability.zookeeper.path.mesos-workers | "/mesos-workers" | String | The ZooKeeper root path for persisting the Mesos worker information. |
| high-availability.zookeeper.path.root | "/flink" | String | The root path under which Flink stores its entries in ZooKeeper. |
| high-availability.zookeeper.path.running-registry | "/running_job_registry/" | String |  |
| high-availability.zookeeper.quorum | (none) | String | The ZooKeeper quorum to use, when running Flink in a high-availability mode with ZooKeeper. |
