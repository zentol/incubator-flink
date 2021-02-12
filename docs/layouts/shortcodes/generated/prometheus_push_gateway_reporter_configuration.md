| Key | Default | Type | Description |
|-----|---------|------|-------------|
| deleteOnShutdown | true | Boolean | Specifies whether to delete metrics from the PushGateway on shutdown. |
| filterLabelValueCharacters | true | Boolean | Specifies whether to filter label value characters. If enabled, all characters not matching [a-zA-Z0-9:_] will be removed, otherwise no characters will be removed. Before disabling this option please ensure that your label values meet the [Prometheus requirements](https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels). |
| groupingKey | (none) | String | Specifies the grouping key which is the group and global labels of all metrics. The label name and value are separated by '=', and labels are separated by ';', e.g., `k1=v1;k2=v2`. Please ensure that your grouping key meets the [Prometheus requirements](https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels). |
| host | (none) | String | The PushGateway server host. |
| jobName | (none) | String | The job name under which metrics will be pushed |
| port | -1 | Integer | The PushGateway server port. |
| randomJobNameSuffix | true | Boolean | Specifies whether a random suffix should be appended to the job name. |
