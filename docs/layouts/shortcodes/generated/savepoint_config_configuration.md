| Key | Default | Type | Description |
|-----|---------|------|-------------|
| execution.savepoint.ignore-unclaimed-state | false | Boolean | Allow to skip savepoint state that cannot be restored. Allow this if you removed an operator from your pipeline after the savepoint was triggered. |
| execution.savepoint.path | (none) | String | Path to a savepoint to restore the job from (for example hdfs:///flink/savepoint-1537). |
