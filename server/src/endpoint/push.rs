use crate::const_::SlotState;
use crate::const_::MESSAGE_SLOT_CONTENT_LEN_MAX;
use crate::const_::SLOT_LEN;
use crate::ctx::Ctx;
use crate::file::WriteRequest;
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
pub struct EndpointPushInputMessage {
  contents: String,
  visibility_timeout_secs: i64,
}

#[derive(Deserialize)]
pub struct EndpointPushInput {
  messages: Vec<EndpointPushInputMessage>,
}

#[derive(Serialize)]
pub enum EndpointPushOutputErrorType {
  ContentTooLarge,
  InvalidVisibilityTimeout,
  QueueIsFull,
}

#[derive(Serialize)]
pub struct EndpointPushOutputError {
  typ: EndpointPushOutputErrorType,
  index: usize,
}

#[derive(Serialize)]
pub struct EndpointPushOutput {
  errors: Vec<EndpointPushOutputError>,
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

  let mut errors = Vec::new();

  let indices = {
    let mut vacant = ctx.vacant.lock().await;
    vacant.take_up_to_n(req.messages.len())
  };

  let mut to_add = Vec::new();
  let mut writes = Vec::new();
  for (i, msg) in req.messages.into_iter().enumerate() {
    if msg.contents.len() > as_usize!(MESSAGE_SLOT_CONTENT_LEN_MAX) {
      errors.push(EndpointPushOutputError {
        index: i,
        typ: EndpointPushOutputErrorType::ContentTooLarge,
      });
      continue;
    };

    if msg.visibility_timeout_secs < 0 {
      errors.push(EndpointPushOutputError {
        index: i,
        typ: EndpointPushOutputErrorType::InvalidVisibilityTimeout,
      });
      continue;
    };

    if i >= indices.len() {
      errors.push(EndpointPushOutputError {
        index: i,
        typ: EndpointPushOutputErrorType::QueueIsFull,
      });
      continue;
    };

    let visible_time = Utc::now() + Duration::seconds(msg.visibility_timeout_secs);

    let index = indices[i];
    let slot_offset = u64::from(index) * SLOT_LEN;

    let content_len: u16 = msg.contents.len().try_into().unwrap();

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
    slot_data.extend_from_slice(&msg.contents.into_bytes());
    let hash = blake3::hash(&slot_data[32..]);
    slot_data[..32].copy_from_slice(hash.as_bytes());

    to_add.push((index, visible_time));
    writes.push(WriteRequest {
      data: slot_data,
      offset: slot_offset,
    });
  }

  ctx.device.write_at_with_delayed_sync(writes).await;

  {
    let mut available = ctx.available.lock().await;
    for (index, visible_time) in to_add {
      available.insert(index, visible_time);
    }
  };

  ctx
    .metrics
    .successful_push_counter
    .fetch_add(1, Ordering::Relaxed);
  Ok(Json(EndpointPushOutput { errors }))
}
