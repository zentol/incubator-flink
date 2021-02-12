| Key | Default | Type | Description |
|-----|---------|------|-------------|
| akka.ask.callstack | true | Boolean | If true, call stack for asynchronous asks are captured. That way, when an ask fails (for example times out), you get a proper exception, describing to the original method call and call site. Note that in case of having millions of concurrent RPC calls, this may add to the memory footprint. |
| akka.ask.timeout | "10 s" | String | Timeout used for all futures and blocking Akka calls. If Flink fails due to timeouts then you should try to increase this value. Timeouts can be caused by slow machines or a congested network. The timeout value requires a time-unit specifier (ms/s/min/h/d). |
| akka.client-socket-worker-pool.pool-size-factor | 1.0 | Double | The pool size factor is used to determine thread pool size using the following formula: ceil(available processors * factor). Resulting size is then bounded by the pool-size-min and pool-size-max values. |
| akka.client-socket-worker-pool.pool-size-max | 2 | Integer | Max number of threads to cap factor-based number to. |
| akka.client-socket-worker-pool.pool-size-min | 1 | Integer | Min number of threads to cap factor-based number to. |
| akka.fork-join-executor.parallelism-factor | 2.0 | Double | The parallelism factor is used to determine thread pool size using the following formula: ceil(available processors * factor). Resulting size is then bounded by the parallelism-min and parallelism-max values. |
| akka.fork-join-executor.parallelism-max | 64 | Integer | Max number of threads to cap factor-based parallelism number to. |
| akka.fork-join-executor.parallelism-min | 8 | Integer | Min number of threads to cap factor-based parallelism number to. |
| akka.framesize | "10485760b" | String | Maximum size of messages which are sent between the JobManager and the TaskManagers. If Flink fails because messages exceed this limit, then you should increase it. The message size requires a size-unit specifier. |
| akka.jvm-exit-on-fatal-error | true | Boolean | Exit JVM on fatal Akka errors. |
| akka.log.lifecycle.events | false | Boolean | Turns on the Akka’s remote logging of events. Set this value to 'true' in case of debugging. |
| akka.lookup.timeout | "10 s" | String | Timeout used for the lookup of the JobManager. The timeout value has to contain a time-unit specifier (ms/s/min/h/d). |
| akka.retry-gate-closed-for | 50 | Long | Milliseconds a gate should be closed for after a remote connection was disconnected. |
| akka.server-socket-worker-pool.pool-size-factor | 1.0 | Double | The pool size factor is used to determine thread pool size using the following formula: ceil(available processors * factor). Resulting size is then bounded by the pool-size-min and pool-size-max values. |
| akka.server-socket-worker-pool.pool-size-max | 2 | Integer | Max number of threads to cap factor-based number to. |
| akka.server-socket-worker-pool.pool-size-min | 1 | Integer | Min number of threads to cap factor-based number to. |
| akka.ssl.enabled | true | Boolean | Turns on SSL for Akka’s remote communication. This is applicable only when the global ssl flag security.ssl.enabled is set to true. |
| akka.startup-timeout | (none) | String | Timeout after which the startup of a remote component is considered being failed. |
| akka.tcp.timeout | "20 s" | String | Timeout for all outbound connections. If you should experience problems with connecting to a TaskManager due to a slow network, you should increase this value. |
| akka.throughput | 15 | Integer | Number of messages that are processed in a batch before returning the thread to the pool. Low values denote a fair scheduling whereas high values can increase the performance at the cost of unfairness. |
| akka.transport.heartbeat.interval | "1000 s" | String | Heartbeat interval for Akka’s transport failure detector. Since Flink uses TCP, the detector is not necessary. Therefore, the detector is disabled by setting the interval to a very high value. In case you should need the transport failure detector, set the interval to some reasonable value. The interval value requires a time-unit specifier (ms/s/min/h/d). |
| akka.transport.heartbeat.pause | "6000 s" | String | Acceptable heartbeat pause for Akka’s transport failure detector. Since Flink uses TCP, the detector is not necessary. Therefore, the detector is disabled by setting the pause to a very high value. In case you should need the transport failure detector, set the pause to some reasonable value. The pause value requires a time-unit specifier (ms/s/min/h/d). |
| akka.transport.threshold | 300.0 | Double | Threshold for the transport failure detector. Since Flink uses TCP, the detector is not necessary and, thus, the threshold is set to a high value. |
