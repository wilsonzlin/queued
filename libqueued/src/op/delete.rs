use super::_common::verify_poll_tag;
use super::result::OpError;
use super::result::OpResult;
use crate::ctx::Ctx;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tinybuf::TinyBuf;

#[derive(Serialize, Deserialize)]
pub struct OpDeleteInput {
  pub id: u64,
  pub poll_tag: TinyBuf,
}

#[derive(Serialize, Deserialize)]
pub struct OpDeleteOutput {}

pub(crate) async fn op_delete(ctx: Arc<Ctx>, req: OpDeleteInput) -> OpResult<OpDeleteOutput> {
  if ctx.suspension.is_delete_suspended() {
    ctx
      .metrics
      .suspended_delete_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err(OpError::Suspended);
  };

  verify_poll_tag(
    &ctx,
    &ctx.metrics.missing_delete_counter,
    req.id,
    &req.poll_tag,
  )
  .await?;

  if ctx.invisible.lock().remove(req.id).is_none() {
    ctx
      .metrics
      .missing_delete_counter
      .fetch_add(1, Ordering::Relaxed);
    // Someone else beat us to it.
    return Err(OpError::MessageNotFound);
  };

  ctx.layout.delete_message(req.id).await;

  ctx
    .metrics
    .successful_delete_counter
    .fetch_add(1, Ordering::Relaxed);
  Ok(OpDeleteOutput {})
}
