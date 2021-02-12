| Key | Default | Type | Description |
|-----|---------|------|-------------|
| env.hadoop.conf.dir | (none) | String | Path to hadoop configuration directory. It is required to read HDFS and/or YARN configuration. You can also set it via environment variable. |
| env.hbase.conf.dir | (none) | String | Path to hbase configuration directory. It is required to read HBASE configuration. You can also set it via environment variable. |
| env.java.opts | (none) | String | Java options to start the JVM of all Flink processes with. |
| env.java.opts.client | (none) | String | Java options to start the JVM of the Flink Client with. |
| env.java.opts.historyserver | (none) | String | Java options to start the JVM of the HistoryServer with. |
| env.java.opts.jobmanager | (none) | String | Java options to start the JVM of the JobManager with. |
| env.java.opts.taskmanager | (none) | String | Java options to start the JVM of the TaskManager with. |
| env.log.dir | (none) | String | Defines the directory where the Flink logs are saved. It has to be an absolute path. (Defaults to the log directory under Flink’s home) |
| env.log.max | 5 | Integer | The maximum number of old log files to keep. |
| env.pid.dir | "/tmp" | String | Defines the directory where the flink-<host>-<process>.pid files are saved. |
| env.ssh.opts | (none) | String | Additional command line options passed to SSH clients when starting or stopping JobManager, TaskManager, and Zookeeper services (start-cluster.sh, stop-cluster.sh, start-zookeeper-quorum.sh, stop-zookeeper-quorum.sh). |
| env.yarn.conf.dir | (none) | String | Path to yarn configuration directory. It is required to run flink on YARN. You can also set it via environment variable. |
