use super::ctx::HttpCtx;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::HeaderMap;
use std::collections::HashMap;
use std::sync::Arc;

pub async fn endpoint_metrics(
  State(ctx): State<Arc<HttpCtx>>,
  headers: HeaderMap,
) -> (HeaderMap, String) {
  let out = ctx.build_server_metrics();
  let (ct, raw) = match headers.get("accept").map(|h| h.as_bytes()) {
    Some(b"application/json") => ("application/json", serde_json::to_string(&out).unwrap()),
    _ => (
      "text/plain",
      serde_prometheus::to_string(&out, None, HashMap::new()).unwrap(),
    ),
  };
  let mut h = HeaderMap::new();
  h.insert(CONTENT_TYPE, ct.parse().unwrap());
  (h, raw)
}
