pub(crate) mod healthz;
pub(crate) mod queue;
pub(crate) mod queues;

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
pub(crate) struct QueuedHttpError<D: Serialize> {
  error: String,
  error_details: D,
}

pub(crate) type QueuedHttpResultError<ED> = (StatusCode, MsgPack<QueuedHttpError<ED>>);

pub(crate) type QueuedHttpResultWithED<T, ED> = Result<MsgPack<T>, QueuedHttpResultError<ED>>;

pub(crate) type QueuedHttpResult<T> = QueuedHttpResultWithED<T, ()>;

pub(crate) fn qerr(error: impl ToString) -> MsgPack<QueuedHttpError<()>> {
  MsgPack(QueuedHttpError {
    error: error.to_string(),
    error_details: (),
  })
}

pub(crate) fn qerr_d<D: Serialize>(
  error: impl ToString,
  error_details: D,
) -> MsgPack<QueuedHttpError<D>> {
  MsgPack(QueuedHttpError {
    error: error.to_string(),
    error_details,
  })
}

pub(crate) struct HttpCtx {
  pub(crate) batch_sync_delay: Duration,
  pub(crate) data_dir: PathBuf,
  // We use Arc because we need to hold a ref to it (i.e. a lock to the map entry) across await points, something that would cause deadlocks in this map.
  pub(crate) queues: DashMap<String, Arc<Queued>>,
  pub(crate) statsd_endpoint: Option<SocketAddr>,
  pub(crate) statsd_prefix: String,
  pub(crate) statsd_tags: Vec<(String, String)>,
}

impl HttpCtx {
  pub(crate) fn q(&self, name: &str) -> Result<Arc<Queued>, QueuedHttpResultError<()>> {
    self
      .queues
      .get(name)
      .map(|q| Arc::clone(&*q))
      .ok_or_else(|| (StatusCode::NOT_FOUND, qerr("QueueNotFound")))
  }
}
