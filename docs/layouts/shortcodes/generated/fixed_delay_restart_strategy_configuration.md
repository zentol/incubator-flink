| Key | Default | Type | Description |
|-----|---------|------|-------------|
| restart-strategy.fixed-delay.attempts | 1 | Integer | The number of times that Flink retries the execution before the job is declared as failed if `restart-strategy` has been set to `fixed-delay`. |
| restart-strategy.fixed-delay.delay | 1 s | Duration | Delay between two consecutive restart attempts if `restart-strategy` has been set to `fixed-delay`. Delaying the retries can be helpful when the program interacts with external systems where for example connections or pending transactions should reach a timeout before re-execution is attempted. It can be specified using notation: "1 min", "20 s" |
