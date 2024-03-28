use super::ctx::qerr;
use super::ctx::HttpCtx;
use super::ctx::QueuedHttpResult;
use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum_msgpack::MsgPack;
use libqueued::op::delete::OpDeleteInput;
use libqueued::op::delete::OpDeleteOutput;
use libqueued::op::poll::OpPollInput;
use libqueued::op::poll::OpPollOutput;
use libqueued::op::push::OpPushInput;
use libqueued::op::push::OpPushOutput;
use libqueued::op::result::OpError;
use libqueued::op::result::OpResult;
use libqueued::op::update::OpUpdateInput;
use libqueued::op::update::OpUpdateOutput;
use serde::Serialize;
use std::sync::Arc;

fn transform_op_result<R: Serialize>(result: OpResult<R>) -> QueuedHttpResult<R> {
  result.map(|res| MsgPack(res)).map_err(|err| {
    let status = match err {
      OpError::InvalidPollTag => StatusCode::BAD_REQUEST,
      OpError::MessageNotFound => StatusCode::NOT_FOUND,
      OpError::Suspended => StatusCode::SERVICE_UNAVAILABLE,
      OpError::Throttled => StatusCode::TOO_MANY_REQUESTS,
    };
    (status, qerr(format!("{err:?}")))
  })
}

pub async fn endpoint_delete(
  State(ctx): State<Arc<HttpCtx>>,
  Path(q): Path<String>,
  MsgPack(req): MsgPack<OpDeleteInput>,
) -> QueuedHttpResult<OpDeleteOutput> {
  transform_op_result(ctx.q(&q)?.delete(req).await)
}

pub async fn endpoint_poll(
  State(ctx): State<Arc<HttpCtx>>,
  Path(q): Path<String>,
  MsgPack(req): MsgPack<OpPollInput>,
) -> QueuedHttpResult<OpPollOutput> {
  transform_op_result(ctx.q(&q)?.poll(req).await)
}

pub async fn endpoint_push(
  State(ctx): State<Arc<HttpCtx>>,
  Path(q): Path<String>,
  MsgPack(req): MsgPack<OpPushInput>,
) -> QueuedHttpResult<OpPushOutput> {
  transform_op_result(ctx.q(&q)?.push(req).await)
}

pub async fn endpoint_update(
  State(ctx): State<Arc<HttpCtx>>,
  Path(q): Path<String>,
  MsgPack(req): MsgPack<OpUpdateInput>,
) -> QueuedHttpResult<OpUpdateOutput> {
  transform_op_result(ctx.q(&q)?.update(req).await)
}
