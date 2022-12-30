use crate::const_::SLOT_LEN_MAX;
use crate::const_::SLOT_OFFSETOF_CONTENTS;
use crate::const_::SLOT_OFFSETOF_CREATED_TS;
use crate::const_::SLOT_OFFSETOF_LEN;
use crate::const_::SLOT_OFFSETOF_NEXT;
use crate::const_::SLOT_OFFSETOF_POLL_COUNT;
use crate::const_::SLOT_OFFSETOF_VISIBLE_TS;
use crate::const_::STATE_OFFSETOF_AVAILABLE_HEAD;
use crate::ctx::Ctx;
use crate::util::now;
use crate::util::u64_slice;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct EndpointPollInput {}

#[derive(Serialize)]
pub struct EndpointPollOutputMessage {
  offset: u64,
  created: DateTime<Utc>,
  poll_count: u32,
  contents: String,
}

#[derive(Serialize)]
pub struct EndpointPollOutput {
  message: Option<EndpointPollOutputMessage>,
}

pub async fn endpoint_poll(
  State(ctx): State<Arc<Ctx>>,
  Json(_req): Json<EndpointPollInput>,
) -> Result<Json<EndpointPollOutput>, (StatusCode, &'static str)> {
  let polled = {
    let mut slots = ctx.lists.write().await;

    if let Some(slot) = slots.available.ready.pop_front() {
      let offset = slot.offset;
      let prev_invis_offset = slots
        .invisible
        .pending
        .back()
        .map(|s| s.offset)
        .or_else(|| slots.invisible.ready.back().map(|p| p.offset))
        .unwrap_or(0);
      slots.invisible.pending.push_back(slot);

      let mut journal_writes = vec![];

      let raw_data = ctx.data_fd.read_at(offset, SLOT_LEN_MAX).await;

      let next_avail_offset = u64::from_be_bytes(
        u64_slice(&raw_data, SLOT_OFFSETOF_NEXT, 8)
          .try_into()
          .unwrap(),
      );
      let new_poll_count = u32::from_be_bytes(
        u64_slice(&raw_data, SLOT_OFFSETOF_POLL_COUNT, 4)
          .try_into()
          .unwrap(),
      ) + 1;

      // Update visible timestamp and poll count.
      journal_writes.push((
        offset + SLOT_OFFSETOF_VISIBLE_TS,
        now().to_be_bytes().to_vec(),
      ));
      journal_writes.push((
        offset + SLOT_OFFSETOF_POLL_COUNT,
        new_poll_count.to_be_bytes().to_vec(),
      ));

      // Update available list head.
      journal_writes.push((
        STATE_OFFSETOF_AVAILABLE_HEAD,
        next_avail_offset.to_be_bytes().to_vec(),
      ));

      // Update invisible list tail.
      journal_writes.push((
        prev_invis_offset + SLOT_OFFSETOF_NEXT,
        offset.to_be_bytes().to_vec(),
      ));

      // Don't read raw data yet, to save some unnecessary time spent holding lock.
      Some((
        offset,
        new_poll_count,
        raw_data,
        ctx.journal_pending.write(journal_writes),
      ))
    } else {
      None
    }
  };

  let message = if let Some((offset, poll_count, raw_data, pending_write_future)) = polled {
    pending_write_future.await;
    let len: u64 = u16::from_be_bytes(
      u64_slice(&raw_data, SLOT_OFFSETOF_LEN, 2)
        .try_into()
        .unwrap(),
    )
    .into();
    Some(EndpointPollOutputMessage {
      contents: String::from_utf8(u64_slice(&raw_data, SLOT_OFFSETOF_CONTENTS, len).to_vec())
        .unwrap(),
      created: Utc
        .timestamp_millis_opt(
          i64::from_be_bytes(
            u64_slice(&raw_data, SLOT_OFFSETOF_CREATED_TS, 8)
              .try_into()
              .unwrap(),
          ) * 1000,
        )
        .unwrap(),
      offset,
      poll_count,
    })
  } else {
    None
  };

  Ok(Json(EndpointPollOutput { message }))
}
