# queued

Fast zero-configuration single-binary simple queue service.

- Three main operations over standard HTTP: push, poll, delete.
- Exactly once, exactly ordered delivery of messages.
- Per-message updatable visibility timeouts.
- Fast direct I/O with underlying storage, bypassing FS.

## Quick start

Currently, queued supports Linux only.

queued requires persistent storage, and it's preferred to provide a block device directly (e.g. `/dev/my_block_device`), to bypass the file system. Alternatively, a standard file can be used too (e.g. `/var/lib/queued/data`). In either case, the entire device/file will be used, and it must have a size that's a multiple of 1024 bytes.

### Install

```
# Ensure you have Rust installed.
cargo install queued
# --format WILL DESTROY EXISTING CONTENTS. Only run this on the first run.
queued --device /dev/my_block_device --format
```

### Run

```
queued --device /dev/my_block_device
```

### Call

```
🌐 POST localhost:3333/push
{
  "contents": "Hello, world!"
}
✅ 200 OK
{
  "index": 190234
}


🌐 POST localhost:3333/poll
{
  "visibility_timeout_secs": 30
}
✅ 200 OK
{
  "message": {
    "contents": "Hello, world!",
    "created": "2023-01-03T12:00:00Z",
    "index": 190234,
    "poll_count": 1,
    "poll_tag": "f914659685fcea9d60"
  }
}


🌐 POST localhost:3333/delete
{
  "index": 190234,
  "poll_tag": "f914659685fcea9d60"
}
✅ 200 OK
{}
```

## Performance

On a machine with an Intel Core i5-12400 CPU, Samsung 970 EVO Plus 1TB NVMe SSD, and Linux 5.17, with 2048 concurrent clients, queued manages around 50,000 operations (push, poll, or delete) per second.

## Safety

At the API layer, only a successful response (i.e. `2xx`) means that the request has been successfully persisted to disk. Assume any interrupted or failed requests did not safely get stored, and retry as appropriate.

Internally, queued records a hash of persisted data (including metadata and data of messages), to verify integrity when starting the server. It's recommended to use error-detecting-and-correcting durable storage when running in production, like any other stateful workload.

Performing backups can be done by stopping the process and taking a copy of the contents of the file/device. Using compression can reduce bandwidth
(when transferring) and storage usage.

## Management

`GET /metrics` returns metrics in the Prometheus format.

`GET /healthz` returns the current build version.

`POST /suspend` can suspend specific API endpoints, useful for temporary debugging or emergency intervention without stopping the server. It takes a request body like:

```json
{
  "delete": true,
  "poll": true,
  "push": false
}
```

Set a property to `true` to disable that endpoint, and `false` to re-enable it. Disabled endpoints will return `503 Service Unavailable`. Use `GET /suspend` to get currently suspended endpoints.

## Important details

- The index and poll tag values are opaque and should not be used as unique IDs.
- If you require more than one queue (e.g. channels), run multiple servers.
- Messages are limited to 1 KiB, including metadata. This will be adjustable at format time in the future.
- The server is limited to up to 2<sup>32</sup> (around 4 billion) messages at any time. This is currently a simplification optimisation, and may be adjusted in the future.
- Non-2xx responses are text only and usually contain an error message, so check the status before parsing as JSON.
