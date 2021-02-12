| Key | Default | Type | Description |
|-----|---------|------|-------------|
| state.backend.rocksdb.metrics.actual-delayed-write-rate | false | Boolean | Monitor the current actual delayed write rate. 0 means no delay. |
| state.backend.rocksdb.metrics.background-errors | false | Boolean | Monitor the number of background errors in RocksDB. |
| state.backend.rocksdb.metrics.block-cache-capacity | false | Boolean | Monitor block cache capacity. |
| state.backend.rocksdb.metrics.block-cache-pinned-usage | false | Boolean | Monitor the memory size for the entries being pinned in block cache. |
| state.backend.rocksdb.metrics.block-cache-usage | false | Boolean | Monitor the memory size for the entries residing in block cache. |
| state.backend.rocksdb.metrics.column-family-as-variable | false | Boolean | Whether to expose the column family as a variable. |
| state.backend.rocksdb.metrics.compaction-pending | false | Boolean | Track pending compactions in RocksDB. Returns 1 if a compaction is pending, 0 otherwise. |
| state.backend.rocksdb.metrics.cur-size-active-mem-table | false | Boolean | Monitor the approximate size of the active memtable in bytes. |
| state.backend.rocksdb.metrics.cur-size-all-mem-tables | false | Boolean | Monitor the approximate size of the active and unflushed immutable memtables in bytes. |
| state.backend.rocksdb.metrics.estimate-live-data-size | false | Boolean | Estimate of the amount of live data in bytes. |
| state.backend.rocksdb.metrics.estimate-num-keys | false | Boolean | Estimate the number of keys in RocksDB. |
| state.backend.rocksdb.metrics.estimate-pending-compaction-bytes | false | Boolean | Estimated total number of bytes compaction needs to rewrite to get all levels down to under target size. Not valid for other compactions than level-based. |
| state.backend.rocksdb.metrics.estimate-table-readers-mem | false | Boolean | Estimate the memory used for reading SST tables, excluding memory used in block cache (e.g.,filter and index blocks) in bytes. |
| state.backend.rocksdb.metrics.is-write-stopped | false | Boolean | Track whether write has been stopped in RocksDB. Returns 1 if write has been stopped, 0 otherwise. |
| state.backend.rocksdb.metrics.mem-table-flush-pending | false | Boolean | Monitor the number of pending memtable flushes in RocksDB. |
| state.backend.rocksdb.metrics.num-deletes-active-mem-table | false | Boolean | Monitor the total number of delete entries in the active memtable. |
| state.backend.rocksdb.metrics.num-deletes-imm-mem-tables | false | Boolean | Monitor the total number of delete entries in the unflushed immutable memtables. |
| state.backend.rocksdb.metrics.num-entries-active-mem-table | false | Boolean | Monitor the total number of entries in the active memtable. |
| state.backend.rocksdb.metrics.num-entries-imm-mem-tables | false | Boolean | Monitor the total number of entries in the unflushed immutable memtables. |
| state.backend.rocksdb.metrics.num-immutable-mem-table | false | Boolean | Monitor the number of immutable memtables in RocksDB. |
| state.backend.rocksdb.metrics.num-live-versions | false | Boolean | Monitor number of live versions. Version is an internal data structure. See RocksDB file version_set.h for details. More live versions often mean more SST files are held from being deleted, by iterators or unfinished compactions. |
| state.backend.rocksdb.metrics.num-running-compactions | false | Boolean | Monitor the number of currently running compactions. |
| state.backend.rocksdb.metrics.num-running-flushes | false | Boolean | Monitor the number of currently running flushes. |
| state.backend.rocksdb.metrics.num-snapshots | false | Boolean | Monitor the number of unreleased snapshots of the database. |
| state.backend.rocksdb.metrics.size-all-mem-tables | false | Boolean | Monitor the approximate size of the active, unflushed immutable, and pinned immutable memtables in bytes. |
| state.backend.rocksdb.metrics.total-sst-files-size | false | Boolean | Monitor the total size (bytes) of all SST files.WARNING: may slow down online queries if there are too many files. |
