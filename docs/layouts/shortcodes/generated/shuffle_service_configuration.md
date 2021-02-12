| Key | Default | Type | Description |
|-----|---------|------|-------------|
| shuffle-service-factory.class | "org.apache.flink.runtime.io.network.NettyShuffleServiceFactory" | String | The full class name of the shuffle service factory implementation to be used by the cluster. The default implementation uses Netty for network communication and local memory as well disk space to store results on a TaskExecutor. |
