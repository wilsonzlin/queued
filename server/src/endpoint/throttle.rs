use crate::ctx::Ctx;
use crate::throttler::Throttler;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct ThrottleState {
  max_polls_per_time_window: u64,
  time_window_sec: i64,
}

async fn get_throttle_state(ctx: &Ctx) -> Option<ThrottleState> {
  let throttler = ctx.throttler.lock().await;
  throttler.as_ref().map(|t| ThrottleState {
    max_polls_per_time_window: t.get_max_reqs_per_time_window(),
    time_window_sec: t.get_time_window_sec(),
  })
}

#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub struct EndpointIO {
  throttle: Option<ThrottleState>,
}

pub async fn endpoint_get_throttle(
  State(ctx): State<Arc<Ctx>>,
) -> Result<Json<EndpointIO>, (StatusCode, &'static str)> {
  Ok(Json(EndpointIO {
    throttle: get_throttle_state(&ctx).await,
  }))
}

pub async fn endpoint_post_throttle(
  State(ctx): State<Arc<Ctx>>,
  Json(req): Json<EndpointIO>,
) -> Result<Json<EndpointIO>, (StatusCode, &'static str)> {
  *ctx.throttler.lock().await = req
    .throttle
    .map(|t| Throttler::new(t.max_polls_per_time_window, t.time_window_sec));

  Ok(Json(EndpointIO {
    throttle: get_throttle_state(&ctx).await,
  }))
}
