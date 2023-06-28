use super::result::OpError;
use super::result::OpResult;
use crate::ctx::Ctx;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

pub(crate) async fn verify_poll_tag(
  ctx: &Ctx,
  missing_metric: &AtomicU64,
  id: u64,
  req_poll_tag: &[u8],
) -> OpResult<()> {
  if req_poll_tag.len() != 30 {
    return Err(OpError::InvalidPollTag);
  }

  // We use double-checked locking to avoid an expensive I/O read of the poll tag.
  if !ctx.invisible.lock().await.has(id) {
    missing_metric.fetch_add(1, Ordering::Relaxed);
    return Err(OpError::MessageNotFound);
  };

  // Note that there may be subtle race conditions here, as we're not holding a lock/in a critical section, but the poll tag is 30 bytes of crypto-strength random data, so there shouldn't be any chance of conflict anyway.
  let slot_poll_tag = ctx.layout.read_poll_tag(id).await;
  if slot_poll_tag != req_poll_tag {
    missing_metric.fetch_add(1, Ordering::Relaxed);
    return Err(OpError::MessageNotFound);
  };

  Ok(())
}
