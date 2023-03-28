use super::_common::verify_poll_tag;
use super::result::OpError;
use super::result::OpResult;
use crate::ctx::Ctx;
use chrono::Duration;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct OpUpdateInput {
  pub id: u64,
  pub poll_tag: String,
  pub visibility_timeout_secs: i64,
}

#[derive(Serialize)]
pub struct OpUpdateOutput {}

pub(crate) async fn op_update(ctx: Arc<Ctx>, req: OpUpdateInput) -> OpResult<OpUpdateOutput> {
  if ctx.suspension.is_update_suspended() {
    ctx
      .metrics
      .suspended_update_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err(OpError::Suspended);
  };

  verify_poll_tag(
    &ctx,
    &ctx.metrics.missing_update_counter,
    req.id,
    &req.poll_tag,
  )
  .await?;

  let new_visible_time = Utc::now() + Duration::seconds(req.visibility_timeout_secs);

  if ctx.invisible.lock().await.remove(req.id).is_none() {
    ctx
      .metrics
      .missing_update_counter
      .fetch_add(1, Ordering::Relaxed);
    // Someone else beat us to it.
    return Err(OpError::MessageNotFound);
  };

  // Update data.
  ctx
    .layout
    .update_visibility_time(req.id, new_visible_time)
    .await;

  ctx.invisible.lock().await.insert(req.id, new_visible_time);

  ctx
    .metrics
    .successful_update_counter
    .fetch_add(1, Ordering::Relaxed);

  Ok(OpUpdateOutput {})
}
