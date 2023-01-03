use crate::const_::SlotState;
use crate::const_::MESSAGE_SLOT_CONTENT_LEN_MAX;
use crate::const_::SLOT_LEN;
use crate::ctx::Ctx;
use crate::util::as_usize;
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
pub struct EndpointPushInput {
  content: String,
  visibility_timeout_secs: i64,
}

#[derive(Serialize)]
pub struct EndpointPushOutput {
  index: u32,
}

pub async fn endpoint_push(
  State(ctx): State<Arc<Ctx>>,
  Json(req): Json<EndpointPushInput>,
) -> Result<Json<EndpointPushOutput>, (StatusCode, &'static str)> {
  if ctx.suspend_push.load(std::sync::atomic::Ordering::Relaxed) {
    ctx
      .metrics
      .suspended_push_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err((
      StatusCode::SERVICE_UNAVAILABLE,
      "this endpoint has been suspended",
    ));
  };

  if req.content.len() > as_usize!(MESSAGE_SLOT_CONTENT_LEN_MAX) {
    return Err((StatusCode::PAYLOAD_TOO_LARGE, "content is too large"));
  };

  if req.visibility_timeout_secs < 0 {
    return Err((StatusCode::BAD_REQUEST, "visibility timeout is negative"));
  }

  let visible_time = Utc::now() + Duration::seconds(req.visibility_timeout_secs);

  let index = {
    let mut vacant = ctx.vacant.lock().await;
    let Some(index) = vacant.take() else {
      return Err((StatusCode::INSUFFICIENT_STORAGE, "queue is currently full"));
    };
    index
  };
  let slot_offset = u64::from(index) * SLOT_LEN;

  let content_len: u16 = req.content.len().try_into().unwrap();

  // Populate slot.
  let mut slot_data = vec![];
  slot_data.extend_from_slice(&vec![0u8; 32]); // Placeholder for hash.
  slot_data.push(1);
  slot_data.push(SlotState::Available as u8);
  slot_data.extend_from_slice(&vec![0u8; 30]);
  slot_data.extend_from_slice(&Utc::now().timestamp().to_be_bytes());
  slot_data.extend_from_slice(&visible_time.timestamp().to_be_bytes());
  slot_data.extend_from_slice(&0u32.to_be_bytes());
  slot_data.extend_from_slice(&content_len.to_be_bytes());
  slot_data.extend_from_slice(&req.content.into_bytes());
  let hash = blake3::hash(&slot_data[32..]);
  slot_data[..32].copy_from_slice(hash.as_bytes());
  ctx
    .device
    .write_at_with_delayed_sync(slot_offset, slot_data)
    .await;

  // Only insert after write syscall has completed. Writes are immediately visible to all threads and processes, even before fsync. This also prevents a poller to mangle our write when they update the slot data.
  {
    let mut available = ctx.available.lock().await;
    available.insert(index, visible_time);
  };

  ctx
    .metrics
    .successful_push_counter
    .fetch_add(1, Ordering::Relaxed);
  Ok(Json(EndpointPushOutput { index }))
}
