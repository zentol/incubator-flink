| Key | Default | Type | Description |
|-----|---------|------|-------------|
| security.kerberos.login.contexts | (none) | String | A comma-separated list of login contexts to provide the Kerberos credentials to (for example, `Client,KafkaClient` to use the credentials for ZooKeeper authentication and for Kafka authentication) |
| security.kerberos.login.keytab | (none) | String | Absolute path to a Kerberos keytab file that contains the user credentials. |
| security.kerberos.login.principal | (none) | String | Kerberos principal name associated with the keytab. |
| security.kerberos.login.use-ticket-cache | true | Boolean | Indicates whether to read from your Kerberos ticket cache. |
