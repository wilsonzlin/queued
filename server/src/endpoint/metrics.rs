use crate::ctx::Ctx;
use axum::extract::State;
use axum::http::StatusCode;
use chrono::Utc;
use std::sync::Arc;

macro_rules! write_line {
  ($val:expr, $out:expr, $field_name:ident, $field_type:ident, $ts:expr, $help:expr) => {
    let field_name = stringify!($field_name);
    let field_type = stringify!($field_type);

    $out.push_str("# HELP queued_");
    $out.push_str(field_name);
    $out.push_str(" ");
    $out.push_str($help);
    $out.push_str("\n");

    $out.push_str("# TYPE queued_");
    $out.push_str(field_name);
    $out.push_str(" ");
    $out.push_str(field_type);
    $out.push_str("\n");

    $out.push_str("queued_");
    $out.push_str(field_name);
    $out.push_str(" ");
    $out.push_str(&$val.to_string());
    $out.push_str(" ");
    $out.push_str(&$ts);
    $out.push_str("\n");

    $out.push_str("\n");
  };
}

macro_rules! get_atomic_metric {
  ($ctx:expr, $field:ident) => {
    $ctx
      .metrics
      .$field
      .load(std::sync::atomic::Ordering::Relaxed)
  };
}

pub async fn endpoint_metrics(
  State(ctx): State<Arc<Ctx>>,
) -> Result<String, (StatusCode, &'static str)> {
  let mut out = String::new();
  let ts = Utc::now().timestamp_millis().to_string();

  write_line!(
    get_atomic_metric!(ctx, available_gauge), out, available, gauge, ts, "Amount of messages currently in the queue, including both past and future visibility timestamps.");

  write_line!(
    get_atomic_metric!(ctx, empty_poll_counter),
    out,
    empty_poll,
    counter,
    ts,
    "Total number of poll requests that failed due to no message being available."
  );

  write_line!(
    ctx.metrics.io.sync_background_loops_counter(),
    out,
    io_sync_background_loops,
    counter,
    ts,
    "Total number of delayed sync background loop iterations."
  );

  write_line!(
    ctx.metrics.io.sync_counter(),
    out,
    io_sync,
    counter,
    ts,
    "Total number of fsync and fdatasync syscalls."
  );

  write_line!(
    ctx.metrics.io.sync_delayed_counter(),
    out,
    io_sync_delayed,
    counter,
    ts,
    "Total number of requested syncs that were delayed until a later time."
  );

  write_line!(
    ctx.metrics.io.sync_longest_delay_us_counter(),
    out,
    io_sync_longest_delay_us,
    counter,
    ts,
    "Total number of microseconds spent waiting for a sync by one or more delayed syncs."
  );

  write_line!(
    ctx.metrics.io.sync_shortest_delay_us_counter(),
    out,
    io_sync_shortest_delay_us,
    counter,
    ts,
    "Total number of microseconds spent waiting after a final delayed sync before the actual sync."
  );

  write_line!(
    ctx.metrics.io.sync_us_counter(),
    out,
    io_sync_us,
    counter,
    ts,
    "Total number of microseconds spent in fsync and fdatasync syscalls."
  );

  write_line!(
    ctx.metrics.io.write_bytes_counter(),
    out,
    io_write_bytes,
    counter,
    ts,
    "Total number of bytes written."
  );

  write_line!(
    ctx.metrics.io.write_counter(),
    out,
    io_write,
    counter,
    ts,
    "Total number of write syscalls."
  );

  write_line!(
    ctx.metrics.io.write_us_counter(),
    out,
    io_write_us,
    counter,
    ts,
    "Total number of microseconds spent in write syscalls."
  );

  write_line!(
    get_atomic_metric!(ctx, missing_delete_counter),
    out,
    missing_delete,
    counter,
    ts,
    "Total number of delete requests that failed due to the requested message not being found."
  );

  write_line!(
    get_atomic_metric!(ctx, successful_delete_counter),
    out,
    successful_delete,
    counter,
    ts,
    "Total number of delete requests that did delete a message successfully."
  );

  write_line!(
    get_atomic_metric!(ctx, successful_poll_counter),
    out,
    successful_poll,
    counter,
    ts,
    "Total number of poll requests that did poll a message successfully."
  );

  write_line!(
    get_atomic_metric!(ctx, successful_push_counter),
    out,
    successful_push,
    counter,
    ts,
    "Total number of push requests that did push a message successfully."
  );

  write_line!(
    get_atomic_metric!(ctx, suspended_delete_counter),
    out,
    suspended_delete,
    counter,
    ts,
    "Total number of delete requests while the endpoint was suspended."
  );

  write_line!(
    get_atomic_metric!(ctx, suspended_poll_counter),
    out,
    suspended_poll,
    counter,
    ts,
    "Total number of poll requests while the endpoint was suspended."
  );

  write_line!(
    get_atomic_metric!(ctx, suspended_push_counter),
    out,
    suspended_push,
    counter,
    ts,
    "Total number of push requests while the endpoint was suspended."
  );

  write_line!(
    get_atomic_metric!(ctx, vacant_gauge),
    out,
    vacant,
    gauge,
    ts,
    "How many more messages that can currently be pushed into the queue."
  );

  Ok(out)
}
