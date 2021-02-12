| Key | Default | Type | Description |
|-----|---------|------|-------------|
| mesos.failover-timeout | 604800 | Integer | The failover timeout in seconds for the Mesos scheduler, after which running tasks are automatically shut down. |
| mesos.master | (none) | String | The Mesos master URL. The value should be in one of the following forms: <ul><li>host:port</li><li>zk://host1:port1,host2:port2,.../path</li><li>zk://username:password@host1:port1,host2:port2,.../path</li><li>file:///path/to/file</li></ul> |
| mesos.resourcemanager.artifactserver.port | 0 | Integer | The config parameter defining the Mesos artifact server port to use. Setting the port to 0 will let the OS choose an available port. |
| mesos.resourcemanager.artifactserver.ssl.enabled | true | Boolean | Enables SSL for the Flink artifact server. Note that security.ssl.enabled also needs to be set to true encryption to enable encryption. |
| mesos.resourcemanager.declined-offer-refuse-duration | 5000 | Long | Amount of time to ask the Mesos master to not resend a declined resource offer again. This ensures a declined resource offer isn't resent immediately after being declined |
| mesos.resourcemanager.framework.name | "Flink" | String | Mesos framework name |
| mesos.resourcemanager.framework.principal | (none) | String | Mesos framework principal |
| mesos.resourcemanager.framework.role | "*" | String | Mesos framework role definition |
| mesos.resourcemanager.framework.secret | (none) | String | Mesos framework secret |
| mesos.resourcemanager.framework.user | (none) | String | Mesos framework user |
| mesos.resourcemanager.tasks.port-assignments | (none) | String | Comma-separated list of configuration keys which represent a configurable port. All port keys will dynamically get a port assigned through Mesos. |
| mesos.resourcemanager.unused-offer-expiration | 120000 | Long | Amount of time to wait for unused expired offers before declining them. This ensures your scheduler will not hoard unuseful offers. |
