use crate::ctx::Ctx;
use crate::layout::fixed_slots::SlotState;
use crate::layout::MessageCreation;
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
  ContentsTooLarge,
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
  let mut creations = Vec::new();
  for (i, msg) in req.messages.into_iter().enumerate() {
    if msg.contents.len() > as_usize!(ctx.layout.max_contents_len()) {
      errors.push(EndpointPushOutputError {
        index: i,
        typ: EndpointPushOutputErrorType::ContentsTooLarge,
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

    to_add.push((index, visible_time));
    creations.push(MessageCreation {
      index,
      contents: msg.contents,
      state: SlotState::Available,
      visible_time,
    });
  }

  ctx.layout.create_messages(creations).await;

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
