use crate::const_::SlotState;
use crate::const_::SLOT_FIXED_FIELDS_LEN;
use crate::const_::SLOT_LEN;
use crate::const_::SLOT_OFFSETOF_CONTENTS;
use crate::const_::SLOT_OFFSETOF_CREATED_TS;
use crate::const_::SLOT_OFFSETOF_HASH;
use crate::const_::SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS;
use crate::const_::SLOT_OFFSETOF_LEN;
use crate::const_::SLOT_OFFSETOF_POLL_COUNT;
use crate::const_::SLOT_OFFSETOF_POLL_TAG;
use crate::const_::SLOT_OFFSETOF_STATE;
use crate::const_::SLOT_OFFSETOF_VISIBLE_TS;
use crate::ctx::Ctx;
use crate::util::as_usize;
use crate::util::u64_slice;
use crate::util::u64_slice_write;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use chrono::DateTime;
use chrono::Duration;
use chrono::TimeZone;
use chrono::Utc;
use rand::thread_rng;
use rand::RngCore;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct EndpointPollInput {
  visibility_timeout_secs: i64,
}

#[derive(Serialize)]
pub struct EndpointPollOutputMessage {
  contents: String,
  created: DateTime<Utc>,
  index: u64,
  poll_count: u32,
  poll_tag: String,
}

#[derive(Serialize)]
pub struct EndpointPollOutput {
  message: Option<EndpointPollOutputMessage>,
}

pub async fn endpoint_poll(
  State(ctx): State<Arc<Ctx>>,
  Json(req): Json<EndpointPollInput>,
) -> Result<Json<EndpointPollOutput>, (StatusCode, &'static str)> {
  let poll_time = Utc::now();

  let visible_time = Utc::now() + Duration::seconds(req.visibility_timeout_secs);

  let index: u64 = {
    let mut available = ctx.available.write().await;
    let Some(index) = available.poll(&poll_time, visible_time.clone()) else {
      return Ok(Json(EndpointPollOutput { message: None }));
    };
    index.into()
  };

  let slot_offset = index * SLOT_LEN;
  let mut slot_data = ctx.device.read_at(slot_offset, SLOT_LEN).await;

  let mut poll_tag = vec![0u8; 30];
  thread_rng().fill_bytes(&mut poll_tag);

  let created = Utc
    .timestamp_millis_opt(
      i64::from_be_bytes(
        u64_slice(&slot_data, SLOT_OFFSETOF_CREATED_TS, 8)
          .try_into()
          .unwrap(),
      ) * 1000,
    )
    .unwrap();
  let new_poll_count = u32::from_be_bytes(
    u64_slice(&slot_data, SLOT_OFFSETOF_POLL_COUNT, 4)
      .try_into()
      .unwrap(),
  ) + 1;
  let len: u64 = u16::from_be_bytes(
    u64_slice(&slot_data, SLOT_OFFSETOF_LEN, 2)
      .try_into()
      .unwrap(),
  )
  .into();
  let contents =
    String::from_utf8(u64_slice(&slot_data, SLOT_OFFSETOF_CONTENTS, len).to_vec()).unwrap();

  // Update data.
  // For efficiency, hash does not cover contents, as contents have already been durabilty persisted. This also saves wasting writes on rewriting contents.
  slot_data.truncate(as_usize!(SLOT_FIXED_FIELDS_LEN));
  u64_slice_write(&mut slot_data, SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS, &[0]);
  u64_slice_write(&mut slot_data, SLOT_OFFSETOF_STATE, &[
    SlotState::Available as u8
  ]);
  u64_slice_write(&mut slot_data, SLOT_OFFSETOF_POLL_TAG, &poll_tag);
  u64_slice_write(
    &mut slot_data,
    SLOT_OFFSETOF_VISIBLE_TS,
    &visible_time.timestamp().to_be_bytes(),
  );
  u64_slice_write(
    &mut slot_data,
    SLOT_OFFSETOF_POLL_COUNT,
    &new_poll_count.to_be_bytes(),
  );
  let hash = blake3::hash(&slot_data[32..]);
  u64_slice_write(&mut slot_data, SLOT_OFFSETOF_HASH, hash.as_bytes());
  ctx.device.write_at(slot_offset, slot_data).await;
  ctx.device.sync_all().await;

  Ok(Json(EndpointPollOutput {
    message: Some(EndpointPollOutputMessage {
      contents,
      created,
      index,
      poll_count: new_poll_count,
      poll_tag: hex::encode(poll_tag),
    }),
  }))
}
