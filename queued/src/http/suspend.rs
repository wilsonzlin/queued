use super::ctx::HttpCtx;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct SuspendState {
  delete: bool,
  poll: bool,
  push: bool,
  update: bool,
}

fn get_suspend_state(ctx: &HttpCtx) -> SuspendState {
  SuspendState {
    delete: ctx.queued.suspension().is_delete_suspended(),
    poll: ctx.queued.suspension().is_poll_suspended(),
    push: ctx.queued.suspension().is_push_suspended(),
    update: ctx.queued.suspension().is_update_suspended(),
  }
}

pub async fn endpoint_get_suspend(
  State(ctx): State<Arc<HttpCtx>>,
) -> Result<Json<SuspendState>, (StatusCode, &'static str)> {
  Ok(Json(get_suspend_state(&ctx)))
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct EndpointPostSuspendInput {
  delete: Option<bool>,
  poll: Option<bool>,
  push: Option<bool>,
  update: Option<bool>,
}

pub async fn endpoint_post_suspend(
  State(ctx): State<Arc<HttpCtx>>,
  Json(req): Json<EndpointPostSuspendInput>,
) -> Result<Json<SuspendState>, (StatusCode, &'static str)> {
  if let Some(s) = req.delete {
    ctx.queued.suspension().set_delete_suspension(s);
  };
  if let Some(s) = req.poll {
    ctx.queued.suspension().set_poll_suspension(s);
  };
  if let Some(s) = req.push {
    ctx.queued.suspension().set_push_suspension(s);
  };
  if let Some(s) = req.update {
    ctx.queued.suspension().set_update_suspension(s);
  };

  Ok(Json(get_suspend_state(&ctx)))
}
