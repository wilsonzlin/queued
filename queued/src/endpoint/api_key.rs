use super::qerr;
use super::HttpCtx;
use super::QueuedHttpResult;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum_msgpack::MsgPack;
use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub(crate) struct EndpointSetApiKeyInput {
  prefix: String,
}

pub(crate) async fn endpoint_set_api_key(
  State(ctx): State<Arc<HttpCtx>>,
  Path(api_key): Path<String>,
  headers: HeaderMap,
  MsgPack(req): MsgPack<EndpointSetApiKeyInput>,
) -> QueuedHttpResult<()> {
  let Some(api_keys) = &ctx.api_keys else {
    return Err((StatusCode::NOT_FOUND, qerr("NotFound")));
  };
  ctx.verify_global_auth(&headers)?;
  api_keys.insert(api_key, req.prefix);
  Ok(MsgPack(()))
}

pub(crate) async fn endpoint_remove_api_key(
  State(ctx): State<Arc<HttpCtx>>,
  Path(api_key): Path<String>,
  headers: HeaderMap,
) -> QueuedHttpResult<()> {
  let Some(api_keys) = &ctx.api_keys else {
    return Err((StatusCode::NOT_FOUND, qerr("NotFound")));
  };
  ctx.verify_global_auth(&headers)?;
  api_keys.remove(&api_key);
  Ok(MsgPack(()))
}

#[derive(Serialize)]
pub(crate) struct EndpointListApiKeysOutputKey {
  key: String,
  prefix: String,
}

#[derive(Serialize)]
pub(crate) struct EndpointListApiKeysOutput {
  keys: Vec<EndpointListApiKeysOutputKey>,
}

pub(crate) async fn endpoint_list_api_keys(
  State(ctx): State<Arc<HttpCtx>>,
  headers: HeaderMap,
) -> QueuedHttpResult<EndpointListApiKeysOutput> {
  let Some(api_keys) = &ctx.api_keys else {
    return Err((StatusCode::NOT_FOUND, qerr("NotFound")));
  };
  ctx.verify_global_auth(&headers)?;
  let keys = api_keys
    .iter()
    .map(|e| EndpointListApiKeysOutputKey {
      key: e.key().clone(),
      prefix: e.value().clone(),
    })
    .collect_vec();
  Ok(MsgPack(EndpointListApiKeysOutput { keys }))
}
