# example-client

This Cargo project contains example clients for interacting with `queued`.

## Performance

When using very high concurrency values on Linux, it's recommended to update `net.ipv4.ip_local_port_range` (e.g. `1024 65535`). Additional TCP and networking tuning is advised.
