use crate::endpoint::HttpCtx;
use crate::endpoint::QueuedHttpResultError;
use crate::statsd::build_metrics;
use axum::extract::Path;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::HeaderMap;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) async fn endpoint_metrics(
  State(ctx): State<Arc<HttpCtx>>,
  Path(queue_name): Path<String>,
  headers: HeaderMap,
) -> Result<(HeaderMap, String), QueuedHttpResultError<()>> {
  let q = ctx.q(&queue_name)?;
  let out = build_metrics(&q);
  let (ct, raw) = match headers.get("accept").map(|h| h.as_bytes()) {
    Some(b"application/json") => ("application/json", serde_json::to_string(&out).unwrap()),
    _ => (
      "text/plain",
      serde_prometheus::to_string(&out, None, HashMap::new()).unwrap(),
    ),
  };
  let mut h = HeaderMap::new();
  h.insert(CONTENT_TYPE, ct.parse().unwrap());
  Ok((h, raw))
}
