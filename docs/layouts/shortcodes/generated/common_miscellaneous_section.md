| Key | Default | Type | Description |
|-----|---------|------|-------------|
| fs.allowed-fallback-filesystems | (none) | String | A (semicolon-separated) list of file schemes, for which Hadoop can be used instead of an appropriate Flink plugin. (example: s3;wasb) |
| fs.default-scheme | (none) | String | The default filesystem scheme, used for paths that do not declare a scheme explicitly. May contain an authority, e.g. host:port in case of an HDFS NameNode. |
| io.tmp.dirs | 'LOCAL_DIRS' on Yarn. '_FLINK_TMP_DIR' on Mesos. System.getProperty("java.io.tmpdir") in standalone. | String | Directories for temporary files, separated by",", "|", or the system's java.io.File.pathSeparator. |
