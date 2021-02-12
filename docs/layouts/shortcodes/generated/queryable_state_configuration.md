| Key | Default | Type | Description |
|-----|---------|------|-------------|
| queryable-state.client.network-threads | 0 | Integer | Number of network (Netty's event loop) Threads for queryable state client. |
| queryable-state.enable | false | Boolean | Option whether the queryable state proxy and server should be enabled where possible and configurable. |
| queryable-state.proxy.network-threads | 0 | Integer | Number of network (Netty's event loop) Threads for queryable state proxy. |
| queryable-state.proxy.ports | "9069" | String | The port range of the queryable state proxy. The specified range can be a single port: "9123", a range of ports: "50100-50200", or a list of ranges and ports: "50100-50200,50300-50400,51234". |
| queryable-state.proxy.query-threads | 0 | Integer | Number of query Threads for queryable state proxy. Uses the number of slots if set to 0. |
| queryable-state.server.network-threads | 0 | Integer | Number of network (Netty's event loop) Threads for queryable state server. |
| queryable-state.server.ports | "9067" | String | The port range of the queryable state server. The specified range can be a single port: "9123", a range of ports: "50100-50200", or a list of ranges and ports: "50100-50200,50300-50400,51234". |
| queryable-state.server.query-threads | 0 | Integer | Number of query Threads for queryable state server. Uses the number of slots if set to 0. |
