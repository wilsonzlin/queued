use super::ctx::HttpCtx;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use libqueued::ThrottleState;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub struct EndpointIO {
  throttle: Option<ThrottleState>,
}

pub async fn endpoint_get_throttle(
  State(ctx): State<Arc<HttpCtx>>,
) -> Result<Json<EndpointIO>, (StatusCode, &'static str)> {
  Ok(Json(EndpointIO {
    throttle: ctx.queued.get_throttle_state(),
  }))
}

pub async fn endpoint_post_throttle(
  State(ctx): State<Arc<HttpCtx>>,
  Json(req): Json<EndpointIO>,
) -> Result<Json<EndpointIO>, (StatusCode, &'static str)> {
  ctx.queued.set_throttle(req.throttle);
  Ok(Json(EndpointIO {
    throttle: ctx.queued.get_throttle_state(),
  }))
}
