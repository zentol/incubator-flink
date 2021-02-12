| Key | Default | Type | Description |
|-----|---------|------|-------------|
| execution.attached | false | Boolean | Specifies if the pipeline is submitted in attached or detached mode. |
| execution.job-listeners | (none) | List&lt;String&gt; | Custom JobListeners to be registered with the execution environment. The registered listeners cannot have constructors with arguments. |
| execution.shutdown-on-attached-exit | false | Boolean | If the job is submitted in attached mode, perform a best-effort cluster shutdown when the CLI is terminated abruptly, e.g., in response to a user interrupt, such as typing Ctrl + C. |
| execution.target | (none) | String | The deployment target for the execution. This can take one of the following values:<ul><li>remote</li><li>local</li><li>yarn-per-job</li><li>yarn-session</li><li>kubernetes-session</li></ul>. |
