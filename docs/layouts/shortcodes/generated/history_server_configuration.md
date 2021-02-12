| Key | Default | Type | Description |
|-----|---------|------|-------------|
| historyserver.archive.clean-expired-jobs | false | Boolean | Whether HistoryServer should cleanup jobs that are no longer present `historyserver.archive.fs.dir`. |
| historyserver.archive.fs.dir | (none) | String | Comma separated list of directories to fetch archived jobs from. The history server will monitor these directories for archived jobs. You can configure the JobManager to archive jobs to a directory via `jobmanager.archive.fs.dir`. |
| historyserver.archive.fs.refresh-interval | 10000 | Long | Interval in milliseconds for refreshing the archived job directories. |
| historyserver.archive.retained-jobs | -1 | Integer | The maximum number of jobs to retain in each archive directory defined by `historyserver.archive.fs.dir`. If set to `-1`(default), there is no limit to the number of archives. If set to `0` or less than `-1` HistoryServer will throw an `IllegalConfigurationException`.  |
| historyserver.web.address | (none) | String | Address of the HistoryServer's web interface. |
| historyserver.web.port | 8082 | Integer | Port of the HistoryServers's web interface. |
| historyserver.web.refresh-interval | 10000 | Long | The refresh interval for the HistoryServer web-frontend in milliseconds. |
| historyserver.web.ssl.enabled | false | Boolean | Enable HTTPs access to the HistoryServer web frontend. This is applicable only when the global SSL flag security.ssl.enabled is set to true. |
| historyserver.web.tmpdir | (none) | String | This configuration parameter allows defining the Flink web directory to be used by the history server web interface. The web interface will copy its static files into the directory. |
