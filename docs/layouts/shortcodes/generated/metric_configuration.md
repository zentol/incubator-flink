| Key | Default | Type | Description |
|-----|---------|------|-------------|
| metrics.fetcher.update-interval | 10000 | Long | Update interval for the metric fetcher used by the web UI in milliseconds. Decrease this value for faster updating metrics. Increase this value if the metric fetcher causes too much load. Setting this value to 0 disables the metric fetching completely. |
| metrics.internal.query-service.port | "0" | String | The port range used for Flink's internal metric query service. Accepts a list of ports (“50100,50101”), ranges(“50100-50200”) or a combination of both. It is recommended to set a range of ports to avoid collisions when multiple Flink components are running on the same machine. Per default Flink will pick a random port. |
| metrics.internal.query-service.thread-priority | 1 | Integer | The thread priority used for Flink's internal metric query service. The thread is created by Akka's thread pool executor. The range of the priority is from 1 (MIN_PRIORITY) to 10 (MAX_PRIORITY). Warning, increasing this value may bring the main Flink components down. |
| metrics.latency.granularity | "operator" | String | Defines the granularity of latency metrics. Accepted values are:<ul><li>single - Track latency without differentiating between sources and subtasks.</li><li>operator - Track latency while differentiating between sources, but not subtasks.</li><li>subtask - Track latency while differentiating between sources and subtasks.</li></ul> |
| metrics.latency.history-size | 128 | Integer | Defines the number of measured latencies to maintain at each operator. |
| metrics.latency.interval | 0 | Long | Defines the interval at which latency tracking marks are emitted from the sources. Disables latency tracking if set to 0 or a negative value. Enabling this feature can significantly impact the performance of the cluster. |
| metrics.reporter.<name>.<parameter> | (none) | String | Configures the parameter <parameter> for the reporter named <name>. |
| metrics.reporter.<name>.class | (none) | String | The reporter class to use for the reporter named <name>. |
| metrics.reporter.<name>.interval | 10 s | Duration | The reporter interval to use for the reporter named <name>. |
| metrics.reporters | (none) | String | An optional list of reporter names. If configured, only reporters whose name matches any of the names in the list will be started. Otherwise, all reporters that could be found in the configuration will be started. |
| metrics.scope.delimiter | "." | String | Delimiter used to assemble the metric identifier. |
| metrics.scope.jm | "<host>.jobmanager" | String | Defines the scope format string that is applied to all metrics scoped to a JobManager. |
| metrics.scope.jm.job | "<host>.jobmanager.<job_name>" | String | Defines the scope format string that is applied to all metrics scoped to a job on a JobManager. |
| metrics.scope.operator | "<host>.taskmanager.<tm_id>.<job_name>.<operator_name>.<subtask_index>" | String | Defines the scope format string that is applied to all metrics scoped to an operator. |
| metrics.scope.task | "<host>.taskmanager.<tm_id>.<job_name>.<task_name>.<subtask_index>" | String | Defines the scope format string that is applied to all metrics scoped to a task. |
| metrics.scope.tm | "<host>.taskmanager.<tm_id>" | String | Defines the scope format string that is applied to all metrics scoped to a TaskManager. |
| metrics.scope.tm.job | "<host>.taskmanager.<tm_id>.<job_name>" | String | Defines the scope format string that is applied to all metrics scoped to a job on a TaskManager. |
| metrics.system-resource | false | Boolean | Flag indicating whether Flink should report system resource metrics such as machine's CPU, memory or network usage. |
| metrics.system-resource-probing-interval | 5000 | Long | Interval between probing of system resource metrics specified in milliseconds. Has an effect only when 'metrics.system-resource' is enabled. |
