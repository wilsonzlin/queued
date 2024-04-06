pub(crate) mod api_key;
pub(crate) mod healthz;
pub(crate) mod queue;
pub(crate) mod queues;

use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum_msgpack::MsgPack;
use dashmap::DashMap;
use libqueued::Queued;
use serde::Serialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize)]
pub(crate) struct QueuedHttpErrorBody {
  error: String,
  error_details: Option<Box<dyn erased_serde::Serialize>>,
}

pub(crate) type QueuedHttpError = (StatusCode, MsgPack<QueuedHttpErrorBody>);

pub(crate) type QueuedHttpResult<T> = Result<MsgPack<T>, QueuedHttpError>;

pub(crate) fn qerr(error: impl ToString) -> MsgPack<QueuedHttpErrorBody> {
  MsgPack(QueuedHttpErrorBody {
    error: error.to_string(),
    error_details: None,
  })
}

pub(crate) fn qerr_d(
  error: impl ToString,
  error_details: impl Serialize + 'static,
) -> MsgPack<QueuedHttpErrorBody> {
  MsgPack(QueuedHttpErrorBody {
    error: error.to_string(),
    error_details: Some(Box::new(error_details)),
  })
}

pub(crate) struct HttpCtx {
  // Map from API key to prefix. If None, auth for queues is disabled.
  pub(crate) api_keys: Option<DashMap<String, String>>,
  pub(crate) batch_sync_delay: Duration,
  pub(crate) data_dir: PathBuf,
  pub(crate) global_api_key: Option<String>,
  // We use Arc because we need to hold a ref to it (i.e. a lock to the map entry) across await points, something that would cause deadlocks in this map.
  pub(crate) queues: DashMap<String, Arc<Queued>>,
  pub(crate) statsd_endpoint: Option<SocketAddr>,
  pub(crate) statsd_prefix: String,
  pub(crate) statsd_tags: Vec<(String, String)>,
}

impl HttpCtx {
  pub(crate) fn q(&self, name: &str, headers: &HeaderMap) -> Result<Arc<Queued>, QueuedHttpError> {
    if let Some(api_keys) = &self.api_keys {
      let provided_api_key = headers.get("authorization").and_then(|v| v.to_str().ok());
      if !provided_api_key
        .and_then(|k| api_keys.get(k))
        .is_some_and(|pfx| name.starts_with(pfx.as_str()))
      {
        return Err((StatusCode::UNAUTHORIZED, qerr("NotAuthorized")));
      };
    };
    self
      .queues
      .get(name)
      .map(|q| Arc::clone(&*q))
      .ok_or_else(|| (StatusCode::NOT_FOUND, qerr("QueueNotFound")))
  }

  pub(crate) fn verify_global_auth(&self, headers: &HeaderMap) -> Result<(), QueuedHttpError> {
    if let Some(expected_api_key) = &self.global_api_key {
      let provided_api_key = headers.get("authorization").and_then(|h| h.to_str().ok());
      if !provided_api_key.is_some_and(|k| k == expected_api_key) {
        return Err((StatusCode::UNAUTHORIZED, qerr("NotAuthorized")));
      };
    };
    Ok(())
  }
}
