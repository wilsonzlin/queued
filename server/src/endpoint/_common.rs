use crate::ctx::Ctx;
use axum::http::StatusCode;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

pub async fn verify_poll_tag(
  ctx: &Ctx,
  missing_metric: &AtomicU64,
  index: u32,
  poll_tag_hex: &str,
) -> Result<(), (StatusCode, &'static str)> {
  let Ok(req_poll_tag) = hex::decode(poll_tag_hex) else {
    return Err((StatusCode::BAD_REQUEST, "invalid poll tag"));
  };
  if req_poll_tag.len() != 30 {
    return Err((StatusCode::BAD_REQUEST, "invalid poll tag"));
  }

  // We use double-checked locking to avoid an expensive I/O read of the poll tag.
  if !ctx.available.lock().await.has(index) {
    missing_metric.fetch_add(1, Ordering::Relaxed);
    return Err((StatusCode::NOT_FOUND, "message not found"));
  };

  // Note that there may be subtle race conditions here, as we're not holding a lock/in a critical section, but the poll tag is 30 bytes of crypto-strength random data, so there shouldn't be any chance of conflict anyway.
  let slot_poll_tag = ctx.layout.read_poll_tag(index).await;
  if slot_poll_tag != req_poll_tag {
    missing_metric.fetch_add(1, Ordering::Relaxed);
    return Err((StatusCode::NOT_FOUND, "message not found"));
  };

  Ok(())
}
