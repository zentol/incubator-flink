| Key | Default | Type | Description |
|-----|---------|------|-------------|
| taskmanager.runtime.hashjoin-bloom-filters | false | Boolean | Flag to activate/deactivate bloom filters in the hybrid hash join implementation. In cases where the hash join needs to spill to disk (datasets larger than the reserved fraction of memory), these bloom filters can greatly reduce the number of spilled records, at the cost some CPU cycles. |
| taskmanager.runtime.large-record-handler | false | Boolean | Whether to use the LargeRecordHandler when spilling. If a record will not fit into the sorting buffer. The record will be spilled on disk and the sorting will continue with only the key. The record itself will be read afterwards when merging. |
| taskmanager.runtime.max-fan | 128 | Integer | The maximal fan-in for external merge joins and fan-out for spilling hash tables. Limits the number of file handles per operator, but may cause intermediate merging/partitioning, if set too small. |
| taskmanager.runtime.sort-spilling-threshold | 0.8 | Float | A sort operation starts spilling when this fraction of its memory budget is full. |
