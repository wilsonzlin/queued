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
  "index": 190234
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
    "index": 190234,
    "poll_count": 1,
    "poll_tag": "f914659685fcea9d60"
  }
}


// 🌐 POST localhost:3333/update
{
  "index": 190234,
  "poll_tag": "f914659685fcea9d60",
  "visibility_timeout_secs": 15
}
// ✅ 200 OK
{}


// 🌐 POST localhost:3333/delete
{
  "index": 190234,
  "poll_tag": "f914659685fcea9d60"
}
// ✅ 200 OK
{}
```

## Performance

With a single Intel Alder Lake CPU core and NVMe SSD, queued manages around 250,000 operations (push, poll, or delete) per second with 4,096 concurrent clients and a batch size of 64. There is minimal memory usage; only a pointer to each message's storage data is stored in memory.

## Safety

At the API layer, only a successful response (i.e. `2xx`) means that the request has been successfully persisted to disk. Assume any interrupted or failed requests did not safely get stored, and retry as appropriate. Changes are immediately visible to all other callers.

It's recommended to use error-detecting-and-correcting durable storage when running in production, like any other stateful workload.

Performing backups can be done by stopping the process and taking a copy of the contents of the file/device. Using compression can reduce bandwidth (when transferring) and storage usage.

## Management

`GET /healthz` returns the current build version.

`GET /metrics` returns metrics in the Prometheus format:

```
# HELP queued_available Amount of messages currently in the queue, including both past and future visibility timestamps.
# TYPE queued_available gauge
queued_available 0 1673001337599

# HELP queued_empty_poll Total number of poll requests that failed due to no message being available.
# TYPE queued_empty_poll counter
queued_empty_poll 8192 1673001337599

# HELP queued_io_sync Total number of fsync and fdatasync syscalls.
# TYPE queued_io_sync counter
queued_io_sync 5202 1673001337599

# HELP queued_io_sync_background_loops Total number of delayed sync background loop iterations.
# TYPE queued_io_sync_background_loops counter
queued_io_sync_background_loops 818020 1673001337599

# HELP queued_io_sync_delayed Total number of requested syncs that were delayed until a later time.
# TYPE queued_io_sync_delayed counter
queued_io_sync_delayed 3000000 1673001337599

# HELP queued_io_sync_longest_delay_us Total number of microseconds spent waiting for a sync by one or more delayed syncs.
# TYPE queued_io_sync_longest_delay_us counter
queued_io_sync_longest_delay_us 40513037 1673001337599

# HELP queued_io_sync_shortest_delay_us Total number of microseconds spent waiting after a final delayed sync before the actual sync.
# TYPE queued_io_sync_shortest_delay_us counter
queued_io_sync_shortest_delay_us 1659372 1673001337599

# HELP queued_io_sync_us Total number of microseconds spent in fsync and fdatasync syscalls.
# TYPE queued_io_sync_us counter
queued_io_sync_us 39363251 1673001337599

# HELP queued_io_write_bytes Total number of bytes written.
# TYPE queued_io_write_bytes counter
queued_io_write_bytes 263888890 1673001337599

# HELP queued_io_write Total number of write syscalls.
# TYPE queued_io_write counter
queued_io_write 3000000 1673001337599

# HELP queued_io_write_us Total number of microseconds spent in write syscalls.
# TYPE queued_io_write_us counter
queued_io_write_us 246671770 1673001337599

# HELP queued_missing_delete Total number of delete requests that failed due to the requested message not being found.
# TYPE queued_missing_delete counter
queued_missing_delete 0 1673001337599

# HELP queued_successful_delete Total number of delete requests that did delete a message successfully.
# TYPE queued_successful_delete counter
queued_successful_delete 1000000 1673001337599

# HELP queued_successful_poll Total number of poll requests that did poll a message successfully.
# TYPE queued_successful_poll counter
queued_successful_poll 1000000 1673001337599

# HELP queued_successful_push Total number of push requests that did push a message successfully.
# TYPE queued_successful_push counter
queued_successful_push 1000000 1673001337599

# HELP queued_suspended_delete Total number of delete requests while the endpoint was suspended.
# TYPE queued_suspended_delete counter
queued_suspended_delete 0 1673001337599

# HELP queued_suspended_poll Total number of poll requests while the endpoint was suspended.
# TYPE queued_suspended_poll counter
queued_suspended_poll 0 1673001337599

# HELP queued_suspended_push Total number of push requests while the endpoint was suspended.
# TYPE queued_suspended_push counter
queued_suspended_push 0 1673001337599

# HELP queued_vacant How many more messages that can currently be pushed into the queue.
# TYPE queued_vacant gauge
queued_vacant 1000000 1673001337599
```

`POST /suspend` can suspend specific API endpoints, useful for temporary debugging or emergency intervention without stopping the server. It takes a request body like:

```json
{
  "delete": true,
  "poll": true,
  "push": false
}
```

Set a property to `true` to disable that endpoint, and `false` to re-enable it. Disabled endpoints will return `503 Service Unavailable`. Use `GET /suspend` to get the currently suspended endpoints.

## Important details

- The index and poll tag values are opaque and should not be used as unique or ordered IDs.
- If you require more than one queue (e.g. channels), run multiple servers.
- Non-2xx responses are text only and usually contain an error message, so check the status before parsing as JSON.

## Development

queued is a standard Rust project, and does not require any special build tools or system libraries.

As the design and functionality is quite simple, I/O tends to become the bottleneck at scale (and at smaller throughputs, the performance is more than enough). This is important to know when profiling and optimising.

Clients in [example-client](./example-client/) can help with running synthetic workloads for stress testing, performance tuning, and profiling.

As I/O becomes the main attention for optimisation, keep in mind:
- `write` syscall data is immediately visible to all `read` syscalls in all threads and processes.
- `write` syscalls **can** be reordered, unless `fdatasync`/`fsync` is used, which acts as both a barrier and cache-flusher. This means that a fast sequence of `write` (1 create) -> `read` (2 inspect) -> `write` (3 update) can actually cause 1 to clobber 3. Ideally there would be two different APIs for creating a barrier and flushing the cache.
