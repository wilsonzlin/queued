use crate::endpoint::HttpCtx;
use crate::endpoint::QueuedHttpError;
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
) -> Result<(HeaderMap, Vec<u8>), QueuedHttpError> {
  let q = ctx.q(&queue_name, &headers)?;
  let out = build_metrics(&q);
  let (ct, raw) = match headers.get("accept").map(|h| h.as_bytes()) {
    Some(b"application/json") => ("application/json", serde_json::to_vec(&out).unwrap()),
    Some(b"application/msgpack") => (
      "application/msgpack",
      rmp_serde::to_vec_named(&out).unwrap(),
    ),
    _ => (
      "text/plain",
      serde_prometheus::to_string(&out, None, HashMap::new())
        .unwrap()
        .into_bytes(),
    ),
  };
  let mut h = HeaderMap::new();
  h.insert(CONTENT_TYPE, ct.parse().unwrap());
  Ok((h, raw))
}
