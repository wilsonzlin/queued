use crate::ctx::Ctx;
use crate::layout::MessageOnDisk;
use crate::layout::MessagePoll;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
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
pub struct EndpointPollInput {
  visibility_timeout_secs: i64,
}

#[derive(Serialize)]
pub struct EndpointPollOutputMessage {
  contents: String,
  created: DateTime<Utc>,
  index: u32,
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
  if ctx.suspend_poll.load(std::sync::atomic::Ordering::Relaxed) {
    ctx
      .metrics
      .suspended_poll_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err((
      StatusCode::SERVICE_UNAVAILABLE,
      "this endpoint has been suspended",
    ));
  };

  {
    let mut throttler = ctx.throttler.lock().await;
    if throttler.is_some() && !throttler.as_mut().unwrap().increment_count() {
      ctx
        .metrics
        .throttled_poll_counter
        .fetch_add(1, Ordering::Relaxed);
      return Err((
        StatusCode::TOO_MANY_REQUESTS,
        "this poll has been throttled",
      ));
    };
  };

  let visible_time = Utc::now() + Duration::seconds(req.visibility_timeout_secs);

  let Some(index) = ctx.visible.pop_next().await else {
    ctx.metrics.empty_poll_counter.fetch_add(1, Ordering::Relaxed);
    return Ok(Json(EndpointPollOutput { message: None }));
  };

  let mut poll_tag = vec![0u8; 30];
  thread_rng().fill_bytes(&mut poll_tag);
  let poll_tag_hex = hex::encode(&poll_tag);

  let MessageOnDisk {
    contents,
    created,
    poll_count,
  } = ctx.layout.read_message(index).await;
  let new_poll_count = poll_count + 1;

  // Update data.
  ctx
    .layout
    .mark_as_polled(index, MessagePoll {
      poll_tag,
      created_time: created,
      visible_time,
      poll_count: new_poll_count,
    })
    .await;

  ctx.invisible.lock().await.insert(index, visible_time);

  ctx
    .metrics
    .successful_poll_counter
    .fetch_add(1, Ordering::Relaxed);

  Ok(Json(EndpointPollOutput {
    message: Some(EndpointPollOutputMessage {
      contents,
      created,
      index,
      poll_count: new_poll_count,
      poll_tag: poll_tag_hex,
    }),
  }))
}
