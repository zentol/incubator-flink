| Key | Default | Type | Description |
|-----|---------|------|-------------|
| high-availability | "NONE" | String | Defines high-availability mode used for the cluster execution. To enable high-availability, set this mode to "ZOOKEEPER" or specify FQN of factory class. |
| high-availability.cluster-id | "/default" | String | The ID of the Flink cluster, used to separate multiple Flink clusters from each other. Needs to be set for standalone clusters but is automatically inferred in YARN and Mesos. |
| high-availability.storageDir | (none) | String | File system path (URI) where Flink persists metadata in high-availability setups. |
