| Key | Default | Type | Description |
|-----|---------|------|-------------|
| pipeline.time-characteristic | ProcessingTime | Enum<br>Possible values: [ProcessingTime, IngestionTime, EventTime] | The time characteristic for all created streams, e.g., processingtime, event time, or ingestion time.<br /><br />If you set the characteristic to IngestionTime or EventTime this will set a default watermark update interval of 200 ms. If this is not applicable for your application you should change it using `pipeline.auto-watermark-interval`. |
