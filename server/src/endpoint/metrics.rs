use crate::ctx::Ctx;
use axum::extract::State;
use axum::http::StatusCode;
use chrono::Utc;
use std::sync::Arc;

macro_rules! write_line {
  ($ctx:expr, $out:expr, $field:ident, $ts:expr) => {
    $out.push_str("queued_");
    $out.push_str(stringify!($field));
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
  };
}

pub async fn endpoint_metrics(
  State(ctx): State<Arc<Ctx>>,
) -> Result<String, (StatusCode, &'static str)> {
  let mut out = String::new();
  let ts = Utc::now().timestamp_millis().to_string();
  write_line!(ctx, out, available_gauge, ts);
  write_line!(ctx, out, empty_poll_counter, ts);
  write_line!(ctx, out, missing_delete_counter, ts);
  write_line!(ctx, out, successful_delete_counter, ts);
  write_line!(ctx, out, successful_poll_counter, ts);
  write_line!(ctx, out, successful_push_counter, ts);
  write_line!(ctx, out, suspended_delete_counter, ts);
  write_line!(ctx, out, suspended_poll_counter, ts);
  write_line!(ctx, out, suspended_push_counter, ts);
  write_line!(ctx, out, vacant_gauge, ts);
  Ok(out)
}
