use super::result::OpError;
use super::result::OpResult;
use crate::ctx::Ctx;
use crate::layout::MessageCreation;
use crate::util::as_usize;
use chrono::Duration;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct OpPushInputMessage {
  contents: String,
  visibility_timeout_secs: i64,
}

#[derive(Deserialize)]
pub struct OpPushInput {
  messages: Vec<OpPushInputMessage>,
}

#[derive(Serialize)]
pub enum OpPushOutputErrorType {
  ContentsTooLarge,
  InvalidVisibilityTimeout,
}

#[derive(Serialize)]
pub struct OpPushOutputError {
  typ: OpPushOutputErrorType,
  index: usize,
}

#[derive(Serialize)]
pub struct OpPushOutput {
  errors: Vec<OpPushOutputError>,
}

pub(crate) async fn op_push(ctx: Arc<Ctx>, req: OpPushInput) -> OpResult<OpPushOutput> {
  if ctx.suspension.is_push_suspended() {
    ctx
      .metrics
      .suspended_push_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err(OpError::Suspended);
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
      errors.push(OpPushOutputError {
        index: i,
        typ: OpPushOutputErrorType::ContentsTooLarge,
      });
      continue;
    };

    if msg.visibility_timeout_secs < 0 {
      errors.push(OpPushOutputError {
        index: i,
        typ: OpPushOutputErrorType::InvalidVisibilityTimeout,
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
  Ok(OpPushOutput { errors })
}
