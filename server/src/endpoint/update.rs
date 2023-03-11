use super::_common::verify_poll_tag;
use crate::ctx::Ctx;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use chrono::Duration;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct EndpointUpdateInput {
  index: u32,
  poll_tag: String,
  visibility_timeout_secs: i64,
}

#[derive(Serialize)]
pub struct EndpointUpdateOutput {}

pub async fn endpoint_update(
  State(ctx): State<Arc<Ctx>>,
  Json(req): Json<EndpointUpdateInput>,
) -> Result<Json<EndpointUpdateOutput>, (StatusCode, &'static str)> {
  if ctx
    .suspend_update
    .load(std::sync::atomic::Ordering::Relaxed)
  {
    ctx
      .metrics
      .suspended_update_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err((
      StatusCode::SERVICE_UNAVAILABLE,
      "this endpoint has been suspended",
    ));
  };

  verify_poll_tag(
    &ctx,
    &ctx.metrics.missing_update_counter,
    req.index,
    &req.poll_tag,
  )
  .await?;

  let new_visible_time = Utc::now() + Duration::seconds(req.visibility_timeout_secs);

  if ctx.invisible.lock().await.remove(req.index).is_none() {
    ctx
      .metrics
      .missing_update_counter
      .fetch_add(1, Ordering::Relaxed);
    // Someone else beat us to it.
    return Err((StatusCode::NOT_FOUND, "message not found"));
  };

  // Update data.
  ctx
    .layout
    .update_visibility_time(req.index, new_visible_time)
    .await;

  ctx
    .invisible
    .lock()
    .await
    .insert(req.index, new_visible_time);

  ctx
    .metrics
    .successful_update_counter
    .fetch_add(1, Ordering::Relaxed);

  Ok(Json(EndpointUpdateOutput {}))
}
