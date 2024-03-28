use super::ctx::HttpCtx;
use super::ctx::QueuedHttpResult;
use axum::extract::State;
use axum::http::StatusCode;
use axum_msgpack::MsgPack;
use libqueued::ThrottleState;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub struct EndpointIO {
  throttle: Option<ThrottleState>,
}

pub async fn endpoint_get_throttle(State(ctx): State<Arc<HttpCtx>>) -> QueuedHttpResult<EndpointIO> {
  let q = ctx.q(&queue_name)?;
  Ok(MsgPack(EndpointIO {
    throttle: q.get_throttle_state(),
  }))
}

pub async fn endpoint_post_throttle(
  State(ctx): State<Arc<HttpCtx>>,
  MsgPack(req): MsgPack<EndpointIO>,
) -> QueuedHttpResult<EndpointIO> {
  let q = ctx.q(&queue_name)?;
  q.set_throttle(req.throttle);
  Ok(MsgPack(EndpointIO {
    throttle: q.get_throttle_state(),
  }))
}
