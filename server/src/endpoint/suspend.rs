use crate::ctx::Ctx;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct SuspendState {
  push: bool,
  poll: bool,
  delete: bool,
}

fn get_suspend_state(ctx: &Ctx) -> SuspendState {
  SuspendState {
    delete: ctx.suspend_delete.load(Ordering::Relaxed),
    poll: ctx.suspend_poll.load(Ordering::Relaxed),
    push: ctx.suspend_push.load(Ordering::Relaxed),
  }
}

pub async fn endpoint_get_suspend(
  State(ctx): State<Arc<Ctx>>,
) -> Result<Json<SuspendState>, (StatusCode, &'static str)> {
  Ok(Json(get_suspend_state(&ctx)))
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct EndpointPostSuspendInput {
  push: Option<bool>,
  poll: Option<bool>,
  delete: Option<bool>,
}

pub async fn endpoint_post_suspend(
  State(ctx): State<Arc<Ctx>>,
  Json(req): Json<EndpointPostSuspendInput>,
) -> Result<Json<SuspendState>, (StatusCode, &'static str)> {
  if let Some(s) = req.delete {
    ctx.suspend_delete.store(s, Ordering::Relaxed);
  };
  if let Some(s) = req.poll {
    ctx.suspend_poll.store(s, Ordering::Relaxed);
  };
  if let Some(s) = req.push {
    ctx.suspend_push.store(s, Ordering::Relaxed);
  };

  Ok(Json(get_suspend_state(&ctx)))
}
