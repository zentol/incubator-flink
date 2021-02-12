| Key | Default | Type | Description |
|-----|---------|------|-------------|
| rest.await-leader-timeout | 30000 | Long | The time in ms that the client waits for the leader address, e.g., Dispatcher or WebMonitorEndpoint |
| rest.client.max-content-length | 104857600 | Integer | The maximum content length in bytes that the client will handle. |
| rest.connection-timeout | 15000 | Long | The maximum time in ms for the client to establish a TCP connection. |
| rest.idleness-timeout | 300000 | Long | The maximum time in ms for a connection to stay idle before failing. |
| rest.retry.delay | 3000 | Long | The time in ms that the client waits between retries (See also `rest.retry.max-attempts`). |
| rest.retry.max-attempts | 20 | Integer | The number of retries the client will attempt if a retryable operations fails. |
| rest.server.max-content-length | 104857600 | Integer | The maximum content length in bytes that the server will handle. |
| rest.server.numThreads | 4 | Integer | The number of threads for the asynchronous processing of requests. |
| rest.server.thread-priority | 5 | Integer | Thread priority of the REST server's executor for processing asynchronous requests. Lowering the thread priority will give Flink's main components more CPU time whereas increasing will allocate more time for the REST server's processing. |
