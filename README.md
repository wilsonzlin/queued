# queued

Fast zero-configuration single-binary simple queue service.

- Introspect and query contents, and push and delete in huge batches.
- Programmatic or temporary flow control with rate limiting and suspension.
- Fast I/O with minimal writes and strong API-guaranteed durability.
- Available as simple library for direct integration into larger programs.
- **Prometheus-compatible metrics** for monitoring and alerting.

## Quick start

### Install

Ensure you have Rust installed.

```
cargo install queued
```

### Run

```
queued --data-dir /var/lib/queued
```

### Call

```jsonc
// 🌐 PUT /queue/my-q
// (creates an empty queue)
// ✅ 200 OK


// 🌐 POST /queue/my-q/messages/push
{
  "messages": [
    { "contents": "Hello, world!", "visibility_timeout_secs": 0 }
  ]
}
// ✅ 200 OK
{
  "ids": [190234]
}


// 🌐 POST /queue/my-q/messages/poll
{
  "count": 10,
  "visibility_timeout_secs": 30
}
// ✅ 200 OK
{
  "messages": [
    {
      "contents": "Hello, world!",
      "id": 190234,
      "poll_tag": 33
    }
  ]
}


// 🌐 POST /queue/my-q/messages/update
{
  "id": 190234,
  "poll_tag": 33,
  "visibility_timeout_secs": 15
}
// ✅ 200 OK
{
  "new_poll_tag": 45
}


// 🌐 POST /queue/my-q/messages/delete
{
  "messages": [
    {
      "id": 190234,
      "poll_tag": 45
    }
  ]
}
// ✅ 200 OK
{}


// 🌐 DELETE /queue/my-q
// (deletes the queue)
// ✅ 200 OK
```

## Performance

### Single node

With a single Intel Alder Lake CPU core and NVMe SSD, queued manages around 300,000 operations (push, poll, update, or delete) per second with 4,096 concurrent clients and a batch size of 64. There is minimal memory usage; only metadata of each message is stored in memory.

As every operation is durably persisted to the underlying storage, the storage I/O performance can quickly become a bottleneck. Consider using RAID 0 and tuning the write latency for better performance.

## Safety

At the API layer, only a successful response (i.e. `2xx`) means that the request has been successfully persisted (`fdatasync`) to disk. Assume any interrupted or failed requests did not safely get stored, and retry as appropriate. Changes are immediately visible to all other callers.

It's recommended to use error-correcting durable storage when running in production, like any other stateful workload.

Performing backups can be done by stopping the process and taking a copy of the contents of the file/device.

## Management

`GET /queues` lists all queues. Returns:

```json
{
  "queues": [
    { "name": "my-q" },
    { "name": "another-q" }
  ]
}
```

`POST /queue/:queue/suspend` can suspend specific API endpoints for a queue, useful for temporary debugging or emergency intervention without stopping the server. It takes a request body like:

```json
{
  "delete": true,
  "poll": false,
  "push": false,
  "update": true
}
```

Set a property to `true` to disable that endpoint, and `false` to re-enable it. Disabled endpoints will return `503 Service Unavailable`. Use `GET /queue/:queue/suspend` to get the currently suspended endpoints.

`POST /queue/:queue/throttle` will configure poll throttling for a queue, useful for flow control and rate limiting. It takes a request body like:

```json
{
  "throttle": {
    "max_polls_per_time_window": 100,
    "time_window_sec": 60
  }
}
```

This will rate limit poll requests to 100 every 60 seconds. No other endpoint is throttled. Throttled requests will return `429 Too Many Requests`. Use `GET /queue/:queue/throttle` to get the current throttle setting. To disable throttling:

```json
{
  "throttle": null
}
```

`GET /healthz` returns the current build version.

## Metrics

Metrics are exposed in **Prometheus text format** via HTTP endpoints:

- `GET /metrics` - Global metrics for all queues
- `GET /queue/:queue/metrics` - Metrics for a specific queue (also updates visibility timeout gauges)

### Available Metrics

All metrics include a `queue` label to identify the queue.

| Metric | Type | Description |
|--------|------|-------------|
| `queued_messages` | Gauge | Current message count in the queue |
| `queued_successful_pushes_total` | Counter | Total successful push operations |
| `queued_successful_polls_total` | Counter | Total successful poll operations |
| `queued_successful_updates_total` | Counter | Total successful update operations |
| `queued_successful_deletes_total` | Counter | Total successful delete operations |
| `queued_empty_polls_total` | Counter | Poll requests with no message available |
| `queued_missing_updates_total` | Counter | Update requests where message not found |
| `queued_missing_deletes_total` | Counter | Delete requests where message not found |
| `queued_suspended_pushes_total` | Counter | Push requests while suspended |
| `queued_suspended_polls_total` | Counter | Poll requests while suspended |
| `queued_suspended_updates_total` | Counter | Update requests while suspended |
| `queued_suspended_deletes_total` | Counter | Delete requests while suspended |
| `queued_throttled_polls_total` | Counter | Poll requests that were throttled |
| `queued_visibility_timeout_first_seconds` | Gauge | Seconds until first message becomes visible |
| `queued_visibility_timeout_last_seconds` | Gauge | Seconds until last message becomes visible |
| `queued_oldest_unpolled_seconds` | Gauge | Age of oldest unpolled message |

### Example

```
$ curl http://localhost:3333/metrics

# HELP queued_messages Current message count in the queue
# TYPE queued_messages gauge
queued_messages{queue="my-queue"} 1000

# HELP queued_successful_pushes_total Successful push requests
# TYPE queued_successful_pushes_total counter
queued_successful_pushes_total{queue="my-queue"} 5000

# HELP queued_successful_polls_total Successful poll requests  
# TYPE queued_successful_polls_total counter
queued_successful_polls_total{queue="my-queue"} 4000
```

### Prometheus Integration

Add to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'queued'
    static_configs:
      - targets: ['localhost:3333']
    metrics_path: '/metrics'
```

## Important details

- Messages are delivered in order of their visibility time. Messages visible at the same time may be delivered in any order. Messages will never be delivered before their visibility time, but may be delivered a few seconds later. Polled messages could be updated or deleted a few seconds after their visibility time for the same reason.
- The ID and poll tag values are unique and opaque.
- There is no limit on the size of a message. The HTTP API has a limit of 128 MiB per request body.
- Non-2xx responses are text only and usually contain an error message, so check the status before parsing as JSON.
- The process will exit when disk space is exhausted.

## Development

Clients in [example-client](./example-client/) can help with running synthetic workloads for stress testing, performance tuning, and profiling.

As I/O becomes the main attention for optimisation, keep in mind:
- We assume [powersafe overwrites](https://www.sqlite.org/psow.html) i.e. a `write` won't affect any data outside of the target range.
- `write` syscall data is immediately visible to all `read` syscalls in all threads and processes.
- `write` syscalls **can** be reordered, unless `fdatasync`/`fsync` is used, which acts as both a barrier and cache-flusher. This means that a fast sequence of `write` (1: create) -> `read` (2: inspect) -> `write` (3: update) can actually cause 1 to clobber 3. Ideally there would be two different APIs for creating a barrier and flushing the cache.
