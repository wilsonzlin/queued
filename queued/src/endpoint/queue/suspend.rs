use crate::endpoint::HttpCtx;
use crate::endpoint::QueuedHttpResult;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum_msgpack::MsgPack;
use libqueued::Queued;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize)]
pub(crate) struct SuspendState {
  delete: bool,
  poll: bool,
  push: bool,
  update: bool,
}

fn get_suspend_state(q: &Queued) -> SuspendState {
  SuspendState {
    delete: q.suspension().is_delete_suspended(),
    poll: q.suspension().is_poll_suspended(),
    push: q.suspension().is_push_suspended(),
    update: q.suspension().is_update_suspended(),
  }
}

pub(crate) async fn endpoint_get_suspend(
  State(ctx): State<Arc<HttpCtx>>,
  Path(queue_name): Path<String>,
  headers: HeaderMap,
) -> QueuedHttpResult<SuspendState> {
  let q = ctx.q(&queue_name, &headers)?;
  Ok(MsgPack(get_suspend_state(&q)))
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub(crate) struct EndpointPostSuspendInput {
  delete: Option<bool>,
  poll: Option<bool>,
  push: Option<bool>,
  update: Option<bool>,
}

pub(crate) async fn endpoint_post_suspend(
  State(ctx): State<Arc<HttpCtx>>,
  Path(queue_name): Path<String>,
  headers: HeaderMap,
  MsgPack(req): MsgPack<EndpointPostSuspendInput>,
) -> QueuedHttpResult<SuspendState> {
  let q = ctx.q(&queue_name, &headers)?;
  if let Some(s) = req.delete {
    q.suspension().set_delete_suspension(s);
  };
  if let Some(s) = req.poll {
    q.suspension().set_poll_suspension(s);
  };
  if let Some(s) = req.push {
    q.suspension().set_push_suspension(s);
  };
  if let Some(s) = req.update {
    q.suspension().set_update_suspension(s);
  };

  Ok(MsgPack(get_suspend_state(&q)))
}
