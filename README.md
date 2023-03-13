# queued

Fast zero-configuration single-binary simple queue service.

- Introspect and query contents, and push and delete in huge batches.
- Programmatic or temporary flow control with rate limiting and suspension.
- Fast I/O with underlying storage with minimal writes and API-guaranteed durability.
- Available as simple library for direct integration into larger programs.

## Quick start

queued requires persistent storage, and it's preferred to provide a block device directly (e.g. `/dev/my_block_device`), to bypass the file system. Alternatively, a standard file can be used too (e.g. `/var/lib/queued/data`). In either case, the entire device/file will be used.

### Install

```
# Ensure you have Rust installed.
cargo install queued
# --format WILL DESTROY EXISTING CONTENTS.
queued --device /dev/my_block_device --format
```

### Run

```
queued --device /dev/my_block_device
```

### Call

```jsonc
// 🌐 POST localhost:3333/push
{
  "messages": [
    { "contents": "Hello, world!", "visibility_timeout_secs": 0 }
  ]
}
// ✅ 200 OK
{
  "id": 190234
}


// 🌐 POST localhost:3333/poll
{
  "visibility_timeout_secs": 30
}
// ✅ 200 OK
{
  "message": {
    "contents": "Hello, world!",
    "created": "2023-01-03T12:00:00Z",
    "id": 190234,
    "poll_count": 1,
    "poll_tag": "f914659685fcea9d60"
  }
}


// 🌐 POST localhost:3333/update
{
  "id": 190234,
  "poll_tag": "f914659685fcea9d60",
  "visibility_timeout_secs": 15
}
// ✅ 200 OK
{}


// 🌐 POST localhost:3333/delete
{
  "id": 190234,
  "poll_tag": "f914659685fcea9d60"
}
// ✅ 200 OK
{}
```

## Performance

With a single Intel Alder Lake CPU core and NVMe SSD, queued manages around 300,000 operations (push, poll, or delete) per second with 4,096 concurrent clients and a batch size of 64. There is minimal memory usage; only a pointer to each message's storage data is stored in memory.

## Safety

At the API layer, only a successful response (i.e. `2xx`) means that the request has been successfully persisted to disk. Assume any interrupted or failed requests did not safely get stored, and retry as appropriate. Changes are immediately visible to all other callers.

It's recommended to use error-detecting-and-correcting durable storage when running in production, like any other stateful workload.

Performing backups can be done by stopping the process and taking a copy of the contents of the file/device. Using compression can reduce bandwidth (when transferring) and storage usage.

## Management

`POST /suspend` can suspend specific API endpoints, useful for temporary debugging or emergency intervention without stopping the server. It takes a request body like:

```json
{
  "delete": true,
  "poll": true,
  "push": false
}
```

Set a property to `true` to disable that endpoint, and `false` to re-enable it. Disabled endpoints will return `503 Service Unavailable`. Use `GET /suspend` to get the currently suspended endpoints.

`POST /throttle` will configure poll throttling, useful for flow control and rate limiting. It takes a request body like:

```json
{
  "throttle": {
    "max_polls_per_time_window": 100,
    "time_window_sec": 60
  }
}
```

This will rate limit poll requests to 100 every 60 seconds. No other endpoint is throttled. Throttled requests will return `429 Too Many Requests`. Use `GET /throttle` to get the current throttle setting. To disable throttling:

```json
{
  "throttle": null
}
```

`GET /healthz` returns the current build version.

`GET /metrics` returns metrics in the Prometheus or JSON (`Accept: application/json`) format:

```
# HELP queued_empty_poll Total number of poll requests that failed due to no message being available.
# TYPE queued_empty_poll counter
queued_empty_poll 0 1678525380549

# HELP queued_invisible Amount of invisible messages currently in the queue. They may have been created, polled, or updated.
# TYPE queued_invisible gauge
queued_invisible 0 1678525380549

# HELP queued_io_sync_background_loops Total number of delayed sync background loop iterations.
# TYPE queued_io_sync_background_loops counter
queued_io_sync_background_loops 19601 1678525380549

# HELP queued_io_sync Total number of fsync and fdatasync syscalls.
# TYPE queued_io_sync counter
queued_io_sync 0 1678525380549

# HELP queued_io_sync_delayed Total number of requested syncs that were delayed until a later time.
# TYPE queued_io_sync_delayed counter
queued_io_sync_delayed 0 1678525380549

# HELP queued_io_sync_longest_delay_us Total number of microseconds spent waiting for a sync by one or more delayed syncs.
# TYPE queued_io_sync_longest_delay_us counter
queued_io_sync_longest_delay_us 0 1678525380549

# HELP queued_io_sync_shortest_delay_us Total number of microseconds spent waiting after a final delayed sync before the actual sync.
# TYPE queued_io_sync_shortest_delay_us counter
queued_io_sync_shortest_delay_us 0 1678525380549

# HELP queued_io_sync_us Total number of microseconds spent in fsync and fdatasync syscalls.
# TYPE queued_io_sync_us counter
queued_io_sync_us 0 1678525380549

# HELP queued_io_write_bytes Total number of bytes written.
# TYPE queued_io_write_bytes counter
queued_io_write_bytes 0 1678525380549

# HELP queued_io_write Total number of write syscalls.
# TYPE queued_io_write counter
queued_io_write 0 1678525380549

# HELP queued_io_write_us Total number of microseconds spent in write syscalls.
# TYPE queued_io_write_us counter
queued_io_write_us 0 1678525380549

# HELP queued_missing_delete Total number of delete requests that failed due to the requested message not being found.
# TYPE queued_missing_delete counter
queued_missing_delete 0 1678525380549

# HELP queued_missing_update Total number of update requests that failed due to the requested message not being found.
# TYPE queued_missing_update counter
queued_missing_update 0 1678525380549

# HELP queued_successful_delete Total number of delete requests that did delete a message successfully.
# TYPE queued_successful_delete counter
queued_successful_delete 0 1678525380549

# HELP queued_successful_poll Total number of poll requests that did poll a message successfully.
# TYPE queued_successful_poll counter
queued_successful_poll 0 1678525380549

# HELP queued_successful_push Total number of push requests that did push a message successfully.
# TYPE queued_successful_push counter
queued_successful_push 0 1678525380549

# HELP queued_successful_update Total number of update requests that did update a message successfully.
# TYPE queued_successful_update counter
queued_successful_update 0 1678525380549

# HELP queued_suspended_delete Total number of delete requests while the endpoint was suspended.
# TYPE queued_suspended_delete counter
queued_suspended_delete 0 1678525380549

# HELP queued_suspended_poll Total number of poll requests while the endpoint was suspended.
# TYPE queued_suspended_poll counter
queued_suspended_poll 0 1678525380549

# HELP queued_suspended_push Total number of push requests while the endpoint was suspended.
# TYPE queued_suspended_push counter
queued_suspended_push 0 1678525380549

# HELP queued_suspended_update Total number of update requests while the endpoint was suspended.
# TYPE queued_suspended_update counter
queued_suspended_update 0 1678525380549

# HELP queued_throttled_poll Total number of poll requests that were throttled.
# TYPE queued_throttled_poll counter
queued_throttled_poll 0 1678525380549

# HELP queued_vacant How many more messages that can currently be pushed into the queue.
# TYPE queued_vacant gauge
queued_vacant 0 1678525380549

# HELP queued_visible Amount of visible messages currently in the queue, which can be polled. This may be delayed by a few seconds.
# TYPE queued_visible gauge
queued_visible 4000000 1678525380549
```

## Important details

- Messages are delivered in order of their visibility time. Messages visible at the same time may be delivered in any order. Messages will never be delivered before their visibility time, but may be delivered a few seconds later. Polled messages could be updated or deleted a few seconds after their visibility time for the same reason.
- The ID and poll tag values are unique and opaque.
- If you require more than one queue (e.g. channels), run multiple servers.
- Non-2xx responses are text only and usually contain an error message, so check the status before parsing as JSON.
- The process will exit when disk space is exhausted.

## Development

queued is a standard Rust project, and does not require any special build tools or system libraries.

As the design and functionality is quite simple, I/O tends to become the bottleneck at scale (and at smaller throughputs, the performance is more than enough). This is important to know when profiling and optimising.

Clients in [example-client](./example-client/) can help with running synthetic workloads for stress testing, performance tuning, and profiling.

As I/O becomes the main attention for optimisation, keep in mind:
- `write` syscall data is immediately visible to all `read` syscalls in all threads and processes.
- `write` syscalls **can** be reordered, unless `fdatasync`/`fsync` is used, which acts as both a barrier and cache-flusher. This means that a fast sequence of `write` (1 create) -> `read` (2 inspect) -> `write` (3 update) can actually cause 1 to clobber 3. Ideally there would be two different APIs for creating a barrier and flushing the cache.
