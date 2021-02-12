| Key | Default | Type | Description |
|-----|---------|------|-------------|
| high-availability.jobmanager.port | "0" | String | The port (range) used by the Flink Master for its RPC connections in highly-available setups. In highly-available setups, this value is used instead of 'jobmanager.rpc.port'.A value of '0' means that a random free port is chosen. TaskManagers discover this port through the high-availability services (leader election), so a random port or a port range works without requiring any additional means of service discovery. |
