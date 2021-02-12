| Key | Default | Type | Description |
|-----|---------|------|-------------|
| mesos.constraints.hard.hostattribute | (none) | String | Constraints for task placement on Mesos based on agent attributes. Takes a comma-separated list of key:value pairs corresponding to the attributes exposed by the target mesos agents. Example: az:eu-west-1a,series:t2 |
| mesos.resourcemanager.network.resource.name | "network" | String | Network resource name on Mesos cluster. |
| mesos.resourcemanager.tasks.bootstrap-cmd | (none) | String | A command which is executed before the TaskManager is started. |
| mesos.resourcemanager.tasks.container.docker.force-pull-image | false | Boolean | Instruct the docker containerizer to forcefully pull the image rather than reuse a cached version. |
| mesos.resourcemanager.tasks.container.docker.parameters | (none) | String | Custom parameters to be passed into docker run command when using the docker containerizer. Comma separated list of "key=value" pairs. The "value" may contain '='. |
| mesos.resourcemanager.tasks.container.image.name | (none) | String | Image name to use for the container. |
| mesos.resourcemanager.tasks.container.type | "mesos" | String | Type of the containerization used: “mesos” or “docker”. |
| mesos.resourcemanager.tasks.container.volumes | (none) | String | A comma separated list of [host_path:]container_path[:RO|RW]. This allows for mounting additional volumes into your container. |
| mesos.resourcemanager.tasks.cpus | 0.0 | Double | CPUs to assign to the Mesos workers. |
| mesos.resourcemanager.tasks.disk | 0 | Integer | Disk space to assign to the Mesos workers in MB. |
| mesos.resourcemanager.tasks.gpus | 0 | Integer | GPUs to assign to the Mesos workers. |
| mesos.resourcemanager.tasks.hostname | (none) | String | Optional value to define the TaskManager’s hostname. The pattern _TASK_ is replaced by the actual id of the Mesos task. This can be used to configure the TaskManager to use Mesos DNS (e.g. _TASK_.flink-service.mesos) for name lookups. |
| mesos.resourcemanager.tasks.network.bandwidth | 0 | Integer | Network bandwidth to assign to the Mesos workers in MB per sec. |
| mesos.resourcemanager.tasks.taskmanager-cmd | "$FLINK_HOME/bin/mesos-taskmanager.sh" | String |  |
| mesos.resourcemanager.tasks.uris | (none) | String | A comma separated list of URIs of custom artifacts to be downloaded into the sandbox of Mesos workers. |
