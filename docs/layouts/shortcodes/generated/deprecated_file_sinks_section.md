| Key | Default | Type | Description |
|-----|---------|------|-------------|
| fs.output.always-create-directory | false | Boolean | File writers running with a parallelism larger than one create a directory for the output file path and put the different result files (one per parallel writer task) into that directory. If this option is set to "true", writers with a parallelism of 1 will also create a directory and place a single result file into it. If the option is set to "false", the writer will directly create the file directly at the output path, without creating a containing directory. |
| fs.overwrite-files | false | Boolean | Specifies whether file output writers should overwrite existing files by default. Set to "true" to overwrite by default,"false" otherwise. |
