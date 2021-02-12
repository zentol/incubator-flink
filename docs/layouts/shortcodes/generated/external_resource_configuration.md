| Key | Default | Type | Description |
|-----|---------|------|-------------|
| external-resource.<resource_name>.amount | (none) | Long | The amount for the external resource specified by <resource_name> per TaskExecutor. |
| external-resource.<resource_name>.driver-factory.class | (none) | String | Defines the factory class name for the external resource identified by <resource_name>. The factory will be used to instantiated the ExternalResourceDriver at the TaskExecutor side. For example, org.apache.flink.externalresource.gpu.GPUDriverFactory |
| external-resource.<resource_name>.param.<param> | (none) | String | The naming pattern of custom config options for the external resource specified by <resource_name>. Only the configurations that follow this pattern would be passed into the driver factory of that external resource. |
| external-resources |  | List&lt;String&gt; | List of the <resource_name> of all external resources with delimiter ";", e.g. "gpu;fpga" for two external resource gpu and fpga. The <resource_name> will be used to splice related config options for external resource. Only the <resource_name> defined here will go into effect by external resource framework. |
