use super::ctx::HttpCtx;
use super::ctx::QueuedHttpResultWithED;
use crate::http::ctx::qerr_d;
use crate::statsd::spawn_statsd_emitter;
use axum::extract::Path;
use axum::extract::State;
use axum_msgpack::MsgPack;
use hyper::StatusCode;
use libqueued::Queued;
use libqueued::QueuedCfg;
use serde::Serialize;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

pub const QUEUE_CREATE_OK_MARKER_FILE: &str = ".queued";

#[derive(Serialize)]
pub struct SysErr {
  pub code: Option<i32>,
  pub kind: String,
  pub message: String,
}

impl SysErr {
  pub fn from_error(e: std::io::Error) -> SysErr {
    SysErr {
      code: e.raw_os_error(),
      kind: format!("{:?}", e.kind()),
      message: e.to_string(),
    }
  }
}

#[derive(Serialize)]
pub struct EndpointQueuesResponseQueue {
  name: String,
}

#[derive(Serialize)]
pub struct EndpointQueuesResponse {
  queues: Vec<EndpointQueuesResponseQueue>,
}

pub async fn endpoint_queues(State(ctx): State<Arc<HttpCtx>>) -> MsgPack<EndpointQueuesResponse> {
  MsgPack(EndpointQueuesResponse {
    queues: ctx
      .queues
      .iter()
      .map(|e| EndpointQueuesResponseQueue {
        name: e.key().clone(),
      })
      .collect(),
  })
}

pub async fn endpoint_queue_create(
  State(ctx): State<Arc<HttpCtx>>,
  Path(name): Path<String>,
) -> QueuedHttpResultWithED<(), Option<SysErr>> {
  // We cannot create a temporary dir, because we cannot rename the folder while RocksDB is running. Instead, we'll ensure it succeeded by writing a success file. Also, if we use a different folder name, we lose the ability to use its existence as a locking mechanism to prevent multiple simultaneous creations of the same queue.
  let dir = ctx.data_dir.join(&name);
  match tokio::fs::create_dir(&dir).await {
    Ok(()) => {}
    Err(e) => {
      return Err(match e.kind() {
        ErrorKind::AlreadyExists => (StatusCode::CONFLICT, qerr_d("QueueAlreadyExists", None)),
        _ => (
          StatusCode::INTERNAL_SERVER_ERROR,
          qerr_d("Sys", Some(SysErr::from_error(e))),
        ),
      })
    }
  };
  let q = Arc::new(
    Queued::load_and_start(&dir, QueuedCfg {
      batch_sync_delay: Duration::from_millis(10),
    })
    .await,
  );
  if let Some(addr) = ctx.statsd_endpoint {
    spawn_statsd_emitter(
      addr,
      &ctx.statsd_prefix,
      &ctx.statsd_tags,
      &name,
      Arc::downgrade(&q),
    );
  };
  match tokio::fs::write(dir.join(QUEUE_CREATE_OK_MARKER_FILE), "").await {
    Ok(()) => {}
    Err(e) => {
      return Err((
        StatusCode::INTERNAL_SERVER_ERROR,
        qerr_d("Sys", Some(SysErr::from_error(e))),
      ))
    }
  };
  assert!(ctx.queues.insert(name, q).is_none());
  Ok(MsgPack(()))
}

pub async fn endpoint_queue_delete(
  State(ctx): State<Arc<HttpCtx>>,
  Path(name): Path<String>,
) -> QueuedHttpResultWithED<(), Option<SysErr>> {
  let Some((_, mut q)) = ctx.queues.remove(&name) else {
    return Err((StatusCode::NOT_FOUND, qerr_d("NotFound", None)));
  };
  loop {
    sleep(Duration::from_secs(1)).await;
    match Arc::try_unwrap(q) {
      Ok(db) => {
        drop(db);
        break;
      }
      Err(qref) => {
        q = qref;
        warn!(
          queue = name,
          "still waiting for in-flight ops to finish before deleting"
        );
        continue;
      }
    };
  }
  let dir = ctx.data_dir.join(&name);
  // Remove marker file first in case remove_dir_all fails or doesn't complete and leaves dir in an intermediate corrupt state.
  match tokio::fs::remove_file(dir.join(QUEUE_CREATE_OK_MARKER_FILE)).await {
    Ok(()) => {}
    Err(e) => {
      return Err((
        StatusCode::INTERNAL_SERVER_ERROR,
        qerr_d("Sys", Some(SysErr::from_error(e))),
      ))
    }
  };
  match tokio::fs::remove_dir_all(name).await {
    Ok(()) => {}
    Err(e) => {
      return Err((
        StatusCode::INTERNAL_SERVER_ERROR,
        qerr_d("Sys", Some(SysErr::from_error(e))),
      ))
    }
  };
  Ok(MsgPack(()))
}
