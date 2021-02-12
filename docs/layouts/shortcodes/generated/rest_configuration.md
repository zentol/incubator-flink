| Key | Default | Type | Description |
|-----|---------|------|-------------|
| rest.address | (none) | String | The address that should be used by clients to connect to the server. Attention: This option is respected only if the high-availability configuration is NONE. |
| rest.await-leader-timeout | 30000 | Long | The time in ms that the client waits for the leader address, e.g., Dispatcher or WebMonitorEndpoint |
| rest.bind-address | (none) | String | The address that the server binds itself. |
| rest.bind-port | "8081" | String | The port that the server binds itself. Accepts a list of ports (“50100,50101”), ranges (“50100-50200”) or a combination of both. It is recommended to set a range of ports to avoid collisions when multiple Rest servers are running on the same machine. |
| rest.client.max-content-length | 104857600 | Integer | The maximum content length in bytes that the client will handle. |
| rest.connection-timeout | 15000 | Long | The maximum time in ms for the client to establish a TCP connection. |
| rest.idleness-timeout | 300000 | Long | The maximum time in ms for a connection to stay idle before failing. |
| rest.port | 8081 | Integer | The port that the client connects to. If rest.bind-port has not been specified, then the REST server will bind to this port. Attention: This option is respected only if the high-availability configuration is NONE. |
| rest.retry.delay | 3000 | Long | The time in ms that the client waits between retries (See also `rest.retry.max-attempts`). |
| rest.retry.max-attempts | 20 | Integer | The number of retries the client will attempt if a retryable operations fails. |
| rest.server.max-content-length | 104857600 | Integer | The maximum content length in bytes that the server will handle. |
| rest.server.numThreads | 4 | Integer | The number of threads for the asynchronous processing of requests. |
| rest.server.thread-priority | 5 | Integer | Thread priority of the REST server's executor for processing asynchronous requests. Lowering the thread priority will give Flink's main components more CPU time whereas increasing will allocate more time for the REST server's processing. |
