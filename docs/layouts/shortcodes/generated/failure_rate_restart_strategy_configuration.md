| Key | Default | Type | Description |
|-----|---------|------|-------------|
| restart-strategy.failure-rate.delay | 1 s | Duration | Delay between two consecutive restart attempts if `restart-strategy` has been set to `failure-rate`. It can be specified using notation: "1 min", "20 s" |
| restart-strategy.failure-rate.failure-rate-interval | 1 min | Duration | Time interval for measuring failure rate if `restart-strategy` has been set to `failure-rate`. It can be specified using notation: "1 min", "20 s" |
| restart-strategy.failure-rate.max-failures-per-interval | 1 | Integer | Maximum number of restarts in given time interval before failing a job if `restart-strategy` has been set to `failure-rate`. |
