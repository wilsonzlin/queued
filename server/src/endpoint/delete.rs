use crate::const_::SLOT_LEN;
use crate::const_::SLOT_OFFSETOF_POLL_TAG;
use crate::const_::SLOT_VACANT_TEMPLATE;
use crate::ctx::Ctx;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use seekable_async_file::WriteRequest;
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

  let slot_offset = u64::from(req.index) * SLOT_LEN;

  let Ok(req_poll_tag) = hex::decode(req.poll_tag) else {
    return Err((StatusCode::BAD_REQUEST, "invalid poll tag"));
  };
  if req_poll_tag.len() != 30 {
    return Err((StatusCode::BAD_REQUEST, "invalid poll tag"));
  }

  // We use double-checked locking to avoid an expensive I/O read of the poll tag.
  {
    let available = ctx.available.lock().await;
    if !available.has(req.index) {
      ctx
        .metrics
        .missing_delete_counter
        .fetch_add(1, Ordering::Relaxed);
      return Err((StatusCode::NOT_FOUND, "message not found"));
    };
  };

  // Note that there may be subtle race conditions here, as we're not holding a lock/in a critical section, but the poll tag is 30 bytes of crypto-strength random data, so there shouldn't be any chance of conflict anyway.
  let slot_poll_tag = ctx
    .device
    .read_at(slot_offset + SLOT_OFFSETOF_POLL_TAG, 30)
    .await;
  if slot_poll_tag != req_poll_tag {
    ctx
      .metrics
      .missing_delete_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err((StatusCode::NOT_FOUND, "invalid poll tag"));
  };

  {
    let mut available = ctx.available.lock().await;
    let Some(()) = available.remove(req.index) else {
      ctx.metrics.missing_delete_counter.fetch_add(1, Ordering::Relaxed);
      // Someone else beat us to it.
      return Err((StatusCode::NOT_FOUND, "message not found"));
    };
  };

  ctx
    .device
    .write_at_with_delayed_sync(vec![WriteRequest {
      data: SLOT_VACANT_TEMPLATE.clone(),
      offset: slot_offset,
    }])
    .await;

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
