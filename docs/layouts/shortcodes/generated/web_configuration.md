| Key | Default | Type | Description |
|-----|---------|------|-------------|
| web.access-control-allow-origin | "*" | String | Access-Control-Allow-Origin header for all responses from the web-frontend. |
| web.checkpoints.history | 10 | Integer | Number of checkpoints to remember for recent history. |
| web.history | 5 | Integer | Number of archived jobs for the JobManager. |
| web.log.path | (none) | String | Path to the log file (may be in /log for standalone but under log directory when using YARN). |
| web.refresh-interval | 3000 | Long | Refresh interval for the web-frontend in milliseconds. |
| web.submit.enable | true | Boolean | Flag indicating whether jobs can be uploaded and run from the web-frontend. |
| web.timeout | 600000 | Long | Timeout for asynchronous operations by the web monitor in milliseconds. |
| web.tmpdir | System.getProperty("java.io.tmpdir") | String | Flink web directory which is used by the webmonitor. |
| web.upload.dir | (none) | String | Directory for uploading the job jars. If not specified a dynamic directory will be used under the directory specified by JOB_MANAGER_WEB_TMPDIR_KEY. |
