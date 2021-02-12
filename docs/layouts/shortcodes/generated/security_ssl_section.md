| Key | Default | Type | Description |
|-----|---------|------|-------------|
| security.ssl.algorithms | "TLS_RSA_WITH_AES_128_CBC_SHA" | String | The comma separated list of standard SSL algorithms to be supported. Read more [here](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites) |
| security.ssl.internal.cert.fingerprint | (none) | String | The sha1 fingerprint of the internal certificate. This further protects the internal communication to present the exact certificate used by Flink.This is necessary where one cannot use private CA(self signed) or there is internal firm wide CA is required |
| security.ssl.internal.enabled | false | Boolean | Turns on SSL for internal network communication. Optionally, specific components may override this through their own settings (rpc, data transport, REST, etc). |
| security.ssl.internal.key-password | (none) | String | The secret to decrypt the key in the keystore for Flink's internal endpoints (rpc, data transport, blob server). |
| security.ssl.internal.keystore | (none) | String | The Java keystore file with SSL Key and Certificate, to be used Flink's internal endpoints (rpc, data transport, blob server). |
| security.ssl.internal.keystore-password | (none) | String | The secret to decrypt the keystore file for Flink's for Flink's internal endpoints (rpc, data transport, blob server). |
| security.ssl.internal.truststore | (none) | String | The truststore file containing the public CA certificates to verify the peer for Flink's internal endpoints (rpc, data transport, blob server). |
| security.ssl.internal.truststore-password | (none) | String | The password to decrypt the truststore for Flink's internal endpoints (rpc, data transport, blob server). |
| security.ssl.protocol | "TLSv1.2" | String | The SSL protocol version to be supported for the ssl transport. Note that it doesn’t support comma separated list. |
| security.ssl.rest.authentication-enabled | false | Boolean | Turns on mutual SSL authentication for external communication via the REST endpoints. |
| security.ssl.rest.cert.fingerprint | (none) | String | The sha1 fingerprint of the rest certificate. This further protects the rest REST endpoints to present certificate which is only used by proxy serverThis is necessary where once uses public CA or internal firm wide CA |
| security.ssl.rest.enabled | false | Boolean | Turns on SSL for external communication via the REST endpoints. |
| security.ssl.rest.key-password | (none) | String | The secret to decrypt the key in the keystore for Flink's external REST endpoints. |
| security.ssl.rest.keystore | (none) | String | The Java keystore file with SSL Key and Certificate, to be used Flink's external REST endpoints. |
| security.ssl.rest.keystore-password | (none) | String | The secret to decrypt the keystore file for Flink's for Flink's external REST endpoints. |
| security.ssl.rest.truststore | (none) | String | The truststore file containing the public CA certificates to verify the peer for Flink's external REST endpoints. |
| security.ssl.rest.truststore-password | (none) | String | The password to decrypt the truststore for Flink's external REST endpoints. |
| security.ssl.verify-hostname | true | Boolean | Flag to enable peer’s hostname verification during ssl handshake. |
