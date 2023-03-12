use crate::ctx::Ctx;
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

  let now = Utc::now();
  let mut errors = Vec::new();

  let n: u64 = req.messages.len().try_into().unwrap();
  let base_id = ctx.id_gen.generate(n).await;
  let mut to_add_invisible = Vec::new();
  let mut to_add_visible = Vec::new();
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

    let id = base_id + u64::try_from(i).unwrap();
    let visible_time = now + Duration::seconds(msg.visibility_timeout_secs);

    if visible_time <= now {
      to_add_invisible.push((id, visible_time));
    } else {
      to_add_visible.push(id);
    };
    creations.push(MessageCreation {
      id,
      contents: msg.contents,
      visible_time,
    });
  }

  // Commit ID generator state BEFORE committing messages, as otherwise generated IDs may get reused if messages persist on disk but not the ID generator state.
  ctx.id_gen.commit(base_id, n).await;
  ctx.layout.create_messages(creations).await;

  {
    let mut invisible = ctx.invisible.lock().await;
    for (id, visible_time) in to_add_invisible {
      invisible.insert(id, visible_time);
    }
  };
  ctx.visible.push_all(to_add_visible).await;

  ctx
    .metrics
    .successful_push_counter
    .fetch_add(1, Ordering::Relaxed);
  Ok(Json(EndpointPushOutput { errors }))
}
