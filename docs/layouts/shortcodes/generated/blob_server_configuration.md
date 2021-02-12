| Key | Default | Type | Description |
|-----|---------|------|-------------|
| blob.client.connect.timeout | 0 | Integer | The connection timeout in milliseconds for the blob client. |
| blob.client.socket.timeout | 300000 | Integer | The socket timeout in milliseconds for the blob client. |
| blob.fetch.backlog | 1000 | Integer | The config parameter defining the desired backlog of BLOB fetches on the JobManager.Note that the operating system usually enforces an upper limit on the backlog size based on the `SOMAXCONN` setting. |
| blob.fetch.num-concurrent | 50 | Integer | The config parameter defining the maximum number of concurrent BLOB fetches that the JobManager serves. |
| blob.fetch.retries | 5 | Integer | The config parameter defining number of retires for failed BLOB fetches. |
| blob.offload.minsize | 1048576 | Integer | The minimum size for messages to be offloaded to the BlobServer. |
| blob.server.port | "0" | String | The config parameter defining the server port of the blob service. |
| blob.service.cleanup.interval | 3600 | Long | Cleanup interval of the blob caches at the task managers (in seconds). |
| blob.service.ssl.enabled | true | Boolean | Flag to override ssl support for the blob service transport. |
| blob.storage.directory | (none) | String | The config parameter defining the storage directory to be used by the blob server. |
