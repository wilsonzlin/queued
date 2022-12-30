use crate::const_::MESSAGE_SLOT_CONTENT_LEN_MAX;
use crate::const_::SLOT_OFFSETOF_NEXT;
use crate::const_::STATE_OFFSETOF_VACANT_HEAD;
use crate::ctx::Ctx;
use crate::slice::as_usize;
use crate::time::now;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct EndpointPushInput {
  content: String,
}

#[derive(Serialize)]
pub struct EndpointPushOutput {
  offset: u64,
}

pub async fn endpoint_push(
  State(ctx): State<Arc<Ctx>>,
  Json(req): Json<EndpointPushInput>,
) -> Result<Json<EndpointPushOutput>, (StatusCode, &'static str)> {
  if req.content.len() > as_usize!(MESSAGE_SLOT_CONTENT_LEN_MAX) {
    return Err((StatusCode::PAYLOAD_TOO_LARGE, "content is too large"));
  };

  // We must hold the lock until we journal-write. We can get a consistent view of the updated heads across lists, but we still need to ensure this update is written before any other API does any update.
  let (offset, pending_write_future) = {
    let mut slots = ctx.lists.write().await;

    let slot = slots.vacant.ready.pop_front().unwrap();
    let offset = slot.offset;
    let prev_avail_offset = slots
      .available
      .pending
      .back()
      .map(|p| p.offset)
      .or_else(|| slots.available.ready.back().map(|p| p.offset))
      .unwrap_or(0);
    slots.available.pending.push_back(slot);

    let mut journal_writes = vec![];

    let content_len: u16 = req.content.len().try_into().unwrap();

    // Populate slot.
    let mut slot_data = vec![];
    slot_data.extend_from_slice(&0i64.to_be_bytes());
    slot_data.extend_from_slice(&now().to_be_bytes());
    slot_data.extend_from_slice(&0u32.to_be_bytes());
    slot_data.extend_from_slice(&0u64.to_be_bytes());
    slot_data.extend_from_slice(&content_len.to_be_bytes());
    slot_data.extend_from_slice(&req.content.into_bytes());
    journal_writes.push((offset, slot_data));

    // Update vacant list head.
    journal_writes.push((
      STATE_OFFSETOF_VACANT_HEAD,
      slots
        .vacant
        .ready
        .front()
        .unwrap()
        .offset
        .to_be_bytes()
        .to_vec(),
    ));

    // Update available list tail.
    journal_writes.push((
      prev_avail_offset + SLOT_OFFSETOF_NEXT,
      offset.to_be_bytes().to_vec(),
    ));

    // Drop the lock AFTER creating the journal-write but BEFORE the future completes.
    (offset, ctx.journal_pending.write(journal_writes))
  };

  pending_write_future.await;

  Ok(Json(EndpointPushOutput { offset }))
}
