| Key | Default | Type | Description |
|-----|---------|------|-------------|
| cluster.evenly-spread-out-slots | false | Boolean | Enable the slot spread out allocation strategy. This strategy tries to spread out the slots evenly across all available `TaskExecutors`. |
| jobmanager.declarative-scheduler.min-parallelism-increase | 1 | Integer | Configure the minimum increase in parallelism for a job to scale up. |
| scheduler-mode | (none) | Enum<br>Possible values: [REACTIVE] | Determines the mode of the scheduler. Note that `scheduler-mode`=`REACTIVE` is only supported by standalone application deployments, not by active resource managers (YARN, Kubernetes) or session clusters. |
| slot.idle.timeout | 50000 | Long | The timeout in milliseconds for a idle slot in Slot Pool. |
| slot.request.timeout | 300000 | Long | The timeout in milliseconds for requesting a slot from Slot Pool. |
| slotmanager.number-of-slots.max | 2147483647 | Integer | Defines the maximum number of slots that the Flink cluster allocates. This configuration option is meant for limiting the resource consumption for batch workloads. It is not recommended to configure this option for streaming workloads, which may fail if there are not enough slots. Note that this configuration option does not take effect for standalone clusters, where how many slots are allocated is not controlled by Flink. |
