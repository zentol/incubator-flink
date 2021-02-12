| Key | Default | Type | Description |
|-----|---------|------|-------------|
| cluster.io-pool.size | (none) | Integer | The size of the IO executor pool used by the cluster to execute blocking IO operations (Master as well as TaskManager processes). By default it will use 4 * the number of CPU cores (hardware contexts) that the cluster process has access to. Increasing the pool size allows to run more IO operations concurrently. |
| cluster.registration.error-delay | 10000 | Long | The pause made after an registration attempt caused an exception (other than timeout) in milliseconds. |
| cluster.registration.initial-timeout | 100 | Long | Initial registration timeout between cluster components in milliseconds. |
| cluster.registration.max-timeout | 30000 | Long | Maximum registration timeout between cluster components in milliseconds. |
| cluster.registration.refused-registration-delay | 30000 | Long | The pause made after the registration attempt was refused in milliseconds. |
| cluster.services.shutdown-timeout | 30000 | Long | The shutdown timeout for cluster services like executors in milliseconds. |
| heartbeat.interval | 10000 | Long | Time interval for requesting heartbeat from sender side. |
| heartbeat.timeout | 50000 | Long | Timeout for requesting and receiving heartbeat for both sender and receiver sides. |
| jobmanager.execution.failover-strategy | "region" | String | This option specifies how the job computation recovers from task failures. Accepted values are:<ul><li>'full': Restarts all tasks to recover the job.</li><li>'region': Restarts all tasks that could be affected by the task failure. More details can be found [here]({{.Site.BaseURL}}/docs/dev/execution/task_failure_recovery#restart-pipelined-region-failover-strategy).</li></ul> |
