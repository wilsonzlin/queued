use super::_common::verify_poll_tag;
use crate::ctx::Ctx;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct EndpointDeleteInput {
  index: u32,
  poll_tag: String,
}

#[derive(Serialize)]
pub struct EndpointDeleteOutput {}

pub async fn endpoint_delete(
  State(ctx): State<Arc<Ctx>>,
  Json(req): Json<EndpointDeleteInput>,
) -> Result<Json<EndpointDeleteOutput>, (StatusCode, &'static str)> {
  if ctx.suspend_delete.load(Ordering::Relaxed) {
    ctx
      .metrics
      .suspended_delete_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err((
      StatusCode::SERVICE_UNAVAILABLE,
      "this endpoint has been suspended",
    ));
  };

  verify_poll_tag(
    &ctx,
    &ctx.metrics.missing_delete_counter,
    req.index,
    &req.poll_tag,
  )
  .await?;

  if ctx.invisible.lock().await.remove(req.index).is_none() {
    ctx
      .metrics
      .missing_delete_counter
      .fetch_add(1, Ordering::Relaxed);
    // Someone else beat us to it.
    return Err((StatusCode::NOT_FOUND, "message not found"));
  };

  ctx.layout.delete_message(req.index).await;

  {
    let mut vacant = ctx.vacant.lock().await;
    vacant.add(req.index);
  };

  ctx
    .metrics
    .successful_delete_counter
    .fetch_add(1, Ordering::Relaxed);
  Ok(Json(EndpointDeleteOutput {}))
}
