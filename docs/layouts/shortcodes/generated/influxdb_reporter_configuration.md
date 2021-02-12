| Key | Default | Type | Description |
|-----|---------|------|-------------|
| connectTimeout | 10000 | Integer | (optional) the InfluxDB connect timeout for metrics |
| consistency | ONE | Enum<br>Possible values: [ALL, ANY, ONE, QUORUM] | (optional) the InfluxDB consistency level for metrics |
| db | (none) | String | the InfluxDB database to store metrics |
| host | (none) | String | the InfluxDB server host |
| password | (none) | String | (optional) InfluxDB username's password used for authentication |
| port | 8086 | Integer | the InfluxDB server port |
| retentionPolicy | (none) | String | (optional) the InfluxDB retention policy for metrics |
| scheme | http | Enum<br>Possible values: [http, https] | the InfluxDB schema |
| username | (none) | String | (optional) InfluxDB username used for authentication |
| writeTimeout | 10000 | Integer | (optional) the InfluxDB write timeout for metrics |
