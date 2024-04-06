use crate::endpoint::HttpCtx;
use crate::endpoint::QueuedHttpResult;
use axum::extract::Path;
use axum::extract::State;
use axum_msgpack::MsgPack;
use libqueued::ThrottleState;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct EndpointIO {
  throttle: Option<ThrottleState>,
}

pub(crate) async fn endpoint_get_throttle(
  State(ctx): State<Arc<HttpCtx>>,
  Path(queue_name): Path<String>,
) -> QueuedHttpResult<EndpointIO> {
  let q = ctx.q(&queue_name)?;
  Ok(MsgPack(EndpointIO {
    throttle: q.get_throttle_state(),
  }))
}

pub(crate) async fn endpoint_post_throttle(
  State(ctx): State<Arc<HttpCtx>>,
  Path(queue_name): Path<String>,
  MsgPack(req): MsgPack<EndpointIO>,
) -> QueuedHttpResult<EndpointIO> {
  let q = ctx.q(&queue_name)?;
  q.set_throttle(req.throttle);
  Ok(MsgPack(EndpointIO {
    throttle: q.get_throttle_state(),
  }))
}
