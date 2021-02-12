| Key | Default | Type | Description |
|-----|---------|------|-------------|
| state.backend.async | true | Boolean | Option whether the state backend should use an asynchronous snapshot method where possible and configurable. Some state backends may not support asynchronous snapshots, or only support asynchronous snapshots, and ignore this option. |
| state.backend.fs.memory-threshold | 20 kb | MemorySize | The minimum size of state data files. All state chunks smaller than that are stored inline in the root checkpoint metadata file. The max memory threshold for this configuration is 1MB. |
| state.backend.fs.write-buffer-size | 4096 | Integer | The default size of the write buffer for the checkpoint streams that write to file systems. The actual write buffer size is determined to be the maximum of the value of this option and option 'state.backend.fs.memory-threshold'. |
