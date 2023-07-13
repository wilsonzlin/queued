use super::ctx::HttpCtx;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use chrono::Utc;
use std::sync::Arc;

macro_rules! write_metric_line {
  ($json:expr, $val:expr, $out:expr, $field_name:ident, $field_type:ident, $ts:expr, $help:expr) => {
    let field_name = stringify!($field_name);
    let field_type = stringify!($field_type);

    if $json {
      $out.push('"');
      $out.push_str(field_name);
      $out.push_str("\":");
      $out.push_str(&serde_json::json!({
        "value": $val,
        "description": $help,
        "type": field_type,
      }).to_string());
      $out.push(',');
    } else {
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
  };
}

pub async fn endpoint_metrics(
  State(ctx): State<Arc<HttpCtx>>,
  headers: HeaderMap,
) -> Result<(HeaderMap, String), (StatusCode, &'static str)> {
  let mut out = String::new();
  let ts = Utc::now().timestamp_millis().to_string();
  let json = headers
    .get("accept")
    .filter(|h| h.as_bytes() == b"application/json")
    .is_some();

  if json {
    out.push_str("{\"timestamp\": ");
    out.push_str(&ts);
    out.push_str(", \"values\": {");
  };

  write_metric_line!(
    json,
    ctx.queued.metrics().empty_poll_counter(),
    out,
    empty_poll,
    counter,
    ts,
    "Total number of poll requests that failed due to no message being available."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().free_space_gauge(),
    out,
    free_space,
    gauge,
    ts,
    "How much free space remains on the device."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().invisible_gauge(),
    out,
    invisible,
    gauge,
    ts,
    "Amount of invisible messages currently in the queue. They may have been created, polled, or updated."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().missing_delete_counter(),
    out,
    missing_delete,
    counter,
    ts,
    "Total number of delete requests that failed due to the requested message not being found."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().missing_update_counter(),
    out,
    missing_update,
    counter,
    ts,
    "Total number of update requests that failed due to the requested message not being found."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().successful_delete_counter(),
    out,
    successful_delete,
    counter,
    ts,
    "Total number of delete requests that did delete a message successfully."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().successful_poll_counter(),
    out,
    successful_poll,
    counter,
    ts,
    "Total number of poll requests that did poll a message successfully."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().successful_push_counter(),
    out,
    successful_push,
    counter,
    ts,
    "Total number of push requests that did push a message successfully."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().successful_update_counter(),
    out,
    successful_update,
    counter,
    ts,
    "Total number of update requests that did update a message successfully."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().suspended_delete_counter(),
    out,
    suspended_delete,
    counter,
    ts,
    "Total number of delete requests while the endpoint was suspended."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().suspended_poll_counter(),
    out,
    suspended_poll,
    counter,
    ts,
    "Total number of poll requests while the endpoint was suspended."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().suspended_push_counter(),
    out,
    suspended_push,
    counter,
    ts,
    "Total number of push requests while the endpoint was suspended."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().suspended_update_counter(),
    out,
    suspended_update,
    counter,
    ts,
    "Total number of update requests while the endpoint was suspended."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().throttled_poll_counter(),
    out,
    throttled_poll,
    counter,
    ts,
    "Total number of poll requests that were throttled."
  );

  write_metric_line!(
    json,
    ctx.queued.metrics().visible_gauge(),
    out,
    visible,
    gauge,
    ts,
    "Amount of visible messages currently in the queue, which can be polled. This may be delayed by a few seconds."
  );

  let mut res_headers = HeaderMap::new();
  if json {
    // Remove last comma.
    out.pop().unwrap();
    out.push_str("}}");
    res_headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
  };

  Ok((res_headers, out))
}
