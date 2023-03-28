use super::result::OpError;
use super::result::OpResult;
use crate::ctx::Ctx;
use crate::layout::MessageOnDisk;
use crate::layout::MessagePoll;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use rand::thread_rng;
use rand::RngCore;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct OpPollInput {
  pub visibility_timeout_secs: i64,
}

#[derive(Serialize)]
pub struct OpPollOutputMessage {
  pub contents: String,
  pub created: DateTime<Utc>,
  pub id: u64,
  pub poll_count: u32,
  pub poll_tag: String,
}

#[derive(Serialize)]
pub struct OpPollOutput {
  pub message: Option<OpPollOutputMessage>,
}

pub(crate) async fn op_poll(ctx: Arc<Ctx>, req: OpPollInput) -> OpResult<OpPollOutput> {
  if ctx.suspension.is_poll_suspended() {
    ctx
      .metrics
      .suspended_poll_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err(OpError::Suspended);
  };

  {
    let mut throttler = ctx.throttler.lock().await;
    if throttler.is_some() && !throttler.as_mut().unwrap().increment_count() {
      ctx
        .metrics
        .throttled_poll_counter
        .fetch_add(1, Ordering::Relaxed);
      return Err(OpError::Throttled);
    };
  };

  let visible_time = Utc::now() + Duration::seconds(req.visibility_timeout_secs);

  let Some(id) = ctx.visible.pop_next().await else {
    ctx.metrics.empty_poll_counter.fetch_add(1, Ordering::Relaxed);
    return Ok(OpPollOutput { message: None });
  };

  let mut poll_tag = vec![0u8; 30];
  thread_rng().fill_bytes(&mut poll_tag);
  let poll_tag_hex = hex::encode(&poll_tag);

  let MessageOnDisk {
    contents,
    created,
    poll_count,
  } = ctx.layout.read_message(id).await;
  let new_poll_count = poll_count + 1;

  // Update data.
  ctx
    .layout
    .mark_as_polled(id, MessagePoll {
      poll_tag,
      visible_time,
      poll_count: new_poll_count,
    })
    .await;

  ctx.invisible.lock().await.insert(id, visible_time);

  ctx
    .metrics
    .successful_poll_counter
    .fetch_add(1, Ordering::Relaxed);

  Ok(OpPollOutput {
    message: Some(OpPollOutputMessage {
      contents,
      created,
      id,
      poll_count: new_poll_count,
      poll_tag: poll_tag_hex,
    }),
  })
}
