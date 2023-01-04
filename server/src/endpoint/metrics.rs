use crate::ctx::Ctx;
use axum::extract::State;
use axum::http::StatusCode;
use chrono::Utc;
use std::sync::Arc;

macro_rules! write_line {
  ($ctx:expr, $out:expr, $field:ident, $ts:expr, $help:expr) => {
    let (field_name, field_type) = stringify!($field).rsplit_once("_").unwrap();

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
    $out.push_str(
      &$ctx
        .metrics
        .$field
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string(),
    );
    $out.push_str(" ");
    $out.push_str(&$ts);
    $out.push_str("\n");

    $out.push_str("\n");
  };
}

pub async fn endpoint_metrics(
  State(ctx): State<Arc<Ctx>>,
) -> Result<String, (StatusCode, &'static str)> {
  let mut out = String::new();
  let ts = Utc::now().timestamp_millis().to_string();

  write_line!(ctx, out, available_gauge, ts, "Amount of messages currently in the queue, including both past and future visibility timestamps.");

  write_line!(
    ctx,
    out,
    empty_poll_counter,
    ts,
    "Total number of poll requests that failed due to no message being available."
  );

  write_line!(
    ctx,
    out,
    io_sync_counter,
    ts,
    "Total number of fsync and fdatasync syscalls."
  );

  write_line!(
    ctx,
    out,
    io_sync_delay_us_counter,
    ts,
    "Total number of microseconds spent waiting for a sync by one or more delayed syncs."
  );

  write_line!(
    ctx,
    out,
    io_sync_delayed_counter,
    ts,
    "Total number of requested syncs that were delayed until a later time."
  );

  write_line!(
    ctx,
    out,
    io_sync_triggered_by_bytes_counter,
    ts,
    "Total number of syncs that were triggered due to too many written bytes from delayed syncs."
  );

  write_line!(
    ctx,
    out,
    io_sync_triggered_by_time_counter,
    ts,
    "Total number of syncs that were triggered due to too much time since last sync."
  );

  write_line!(
    ctx,
    out,
    io_sync_us_counter,
    ts,
    "Total number of microseconds spent in fsync and fdatasync syscalls."
  );

  write_line!(
    ctx,
    out,
    io_write_bytes_counter,
    ts,
    "Total number of bytes written."
  );

  write_line!(
    ctx,
    out,
    io_write_counter,
    ts,
    "Total number of write syscalls."
  );

  write_line!(
    ctx,
    out,
    io_write_us_counter,
    ts,
    "Total number of microseconds spent in write syscalls."
  );

  write_line!(
    ctx,
    out,
    missing_delete_counter,
    ts,
    "Total number of delete requests that failed due to the requested message not being found."
  );

  write_line!(
    ctx,
    out,
    successful_delete_counter,
    ts,
    "Total number of delete requests that did delete a message successfully."
  );

  write_line!(
    ctx,
    out,
    successful_poll_counter,
    ts,
    "Total number of poll requests that did poll a message successfully."
  );

  write_line!(
    ctx,
    out,
    successful_push_counter,
    ts,
    "Total number of push requests that did push a message successfully."
  );

  write_line!(
    ctx,
    out,
    suspended_delete_counter,
    ts,
    "Total number of delete requests while the endpoint was suspended."
  );

  write_line!(
    ctx,
    out,
    suspended_poll_counter,
    ts,
    "Total number of poll requests while the endpoint was suspended."
  );

  write_line!(
    ctx,
    out,
    suspended_push_counter,
    ts,
    "Total number of push requests while the endpoint was suspended."
  );

  write_line!(
    ctx,
    out,
    vacant_gauge,
    ts,
    "How many more messages that can currently be pushed into the queue."
  );

  Ok(out)
}
