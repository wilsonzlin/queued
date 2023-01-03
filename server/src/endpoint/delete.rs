use crate::const_::SLOT_LEN;
use crate::const_::SLOT_OFFSETOF_POLL_TAG;
use crate::const_::SLOT_VACANT_TEMPLATE;
use crate::ctx::Ctx;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde::Serialize;
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
  if ctx
    .suspend_delete
    .load(std::sync::atomic::Ordering::Relaxed)
  {
    return Err((
      StatusCode::SERVICE_UNAVAILABLE,
      "this endpoint has been suspended",
    ));
  };

  let slot_offset = u64::from(req.index) * SLOT_LEN;

  let Ok(req_poll_tag) = hex::decode(req.poll_tag) else {
    return Err((StatusCode::BAD_REQUEST, "invalid poll tag"));
  };
  if req_poll_tag.len() != 30 {
    return Err((StatusCode::BAD_REQUEST, "invalid poll tag"));
  }

  // We use double-checked locking to avoid an expensive I/O read of the poll tag.
  {
    let available = ctx.available.read().await;
    if !available.has(req.index) {
      return Err((StatusCode::NOT_FOUND, "message not found"));
    };
  };

  // Note that there may be subtle race conditions here, as we're not holding a lock/in a critical section, but the poll tag is 30 bytes of crypto-strength random data, so there shouldn't be any chance of conflict anyway.
  let slot_poll_tag = ctx
    .device
    .read_at(slot_offset + SLOT_OFFSETOF_POLL_TAG, 30)
    .await;
  if slot_poll_tag != req_poll_tag {
    return Err((StatusCode::BAD_REQUEST, "invalid poll tag"));
  };

  {
    let mut available = ctx.available.write().await;
    let Some(()) = available.remove(req.index) else {
      // Someone else beat us to it.
      return Err((StatusCode::NOT_FOUND, "message not found"));
    };
  };

  ctx
    .device
    .write_at_with_delayed_sync(slot_offset, SLOT_VACANT_TEMPLATE.clone())
    .await;

  {
    let mut vacant = ctx.vacant.write().await;
    if !vacant.add_checked(req.index) {
      panic!("slot already exists");
    };
  };

  Ok(Json(EndpointDeleteOutput {}))
}
