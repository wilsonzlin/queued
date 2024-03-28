use axum_msgpack::MsgPack;
use chrono::Utc;
use dashmap::DashMap;
use hyper::StatusCode;
use libqueued::Queued;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::max;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub struct QueuedHttpErrorWithDetails<D> {
  pub error: String,
  pub error_details: D,
}

pub type QueuedHttpResultError<ED> = (StatusCode, MsgPack<QueuedHttpErrorWithDetails<ED>>);

pub type QueuedHttpResultWithED<T, ED> = Result<MsgPack<T>>;

pub type QueuedHttpResult<T> = QueuedHttpResultWithED<T, ()>;

pub fn qerr(error: impl ToString) -> QueuedHttpError {
  MsgPack(QueuedHttpErrorWithDetails {
    error: error.to_string(),
    error_details: (),
  })
}

pub fn qerr_d<D>(error: impl ToString, error_details: D) -> QueuedHttpError {
  MsgPack(QueuedHttpErrorWithDetails {
    error: error.to_string(),
    error_details,
  })
}

pub struct HttpCtx {
  pub batch_sync_delay: Duration,
  pub data_dir: PathBuf,
  // We use Arc because we need to hold a ref to it (i.e. a lock to the map entry) across await points, something that would cause deadlocks in this map.
  pub queues: DashMap<String, Arc<Queued>>,
  pub statsd_endpoint: Option<SocketAddr>,
  pub statsd_prefix: String,
  pub statsd_tags: Vec<String>,
}

impl HttpCtx {
  pub fn q(&self, name: &str) -> Result<Arc<Queued>, QueuedHttpResultError<()>> {
    self
      .queues
      .get(&q)
      .cloned()
      .ok_or_else(|| (StatusCode::NOT_FOUND, qerr("QueueNotFound")))
  }
}
