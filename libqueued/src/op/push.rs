use super::result::OpError;
use super::result::OpResult;
use crate::ctx::Ctx;
use crate::layout::MessageCreation;
use chrono::Duration;
use chrono::Utc;
use itertools::Itertools;
use off64::usz;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tinybuf::TinyBuf;

#[derive(Deserialize)]
pub struct OpPushInputMessage {
  pub contents: TinyBuf,
  pub visibility_timeout_secs: i64,
}

#[derive(Deserialize)]
pub struct OpPushInput {
  pub messages: Vec<OpPushInputMessage>,
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum OpPushOutputErrorType {
  ContentsTooLarge,
  InvalidVisibilityTimeout,
}

#[derive(Serialize)]
pub struct OpPushOutput {
  pub results: Vec<Result<u64, OpPushOutputErrorType>>,
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

  let n: u64 = req.messages.len().try_into().unwrap();
  let base_id = ctx.id_gen.generate(n).await;
  let mut to_add_invisible = Vec::new();
  let mut to_add_visible = Vec::new();
  let mut creations = Vec::new();
  let results = req
    .messages
    .into_iter()
    .enumerate()
    .map(|(i, msg)| {
      if msg.contents.len() > usz!(ctx.layout.max_contents_len()) {
        return Err(OpPushOutputErrorType::ContentsTooLarge);
      };

      if msg.visibility_timeout_secs < 0 {
        return Err(OpPushOutputErrorType::InvalidVisibilityTimeout);
      };

      let id = base_id + u64::try_from(i).unwrap();
      let visible_time = now + Duration::seconds(msg.visibility_timeout_secs);

      if visible_time > now {
        to_add_invisible.push((id, visible_time));
      } else {
        to_add_visible.push(id);
      };
      creations.push(MessageCreation {
        id,
        contents: msg.contents,
        visible_time,
      });
      Ok(id)
    })
    .collect_vec();

  ctx.layout.create_messages(creations).await;

  {
    let mut invisible = ctx.invisible.lock();
    for (id, visible_time) in to_add_invisible {
      invisible.insert(id, visible_time);
    }
  };
  ctx.visible.push_all(to_add_visible);

  ctx
    .metrics
    .successful_push_counter
    .fetch_add(1, Ordering::Relaxed);
  Ok(OpPushOutput { results })
}
