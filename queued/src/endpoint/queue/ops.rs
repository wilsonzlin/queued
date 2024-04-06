use crate::endpoint::qerr;
use crate::endpoint::HttpCtx;
use crate::endpoint::QueuedHttpResult;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
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

pub(crate) async fn endpoint_delete(
  State(ctx): State<Arc<HttpCtx>>,
  Path(q): Path<String>,
  headers: HeaderMap,
  MsgPack(req): MsgPack<OpDeleteInput>,
) -> QueuedHttpResult<OpDeleteOutput> {
  let q = ctx.q(&q, &headers)?;
  transform_op_result(q.delete(req).await)
}

pub(crate) async fn endpoint_poll(
  State(ctx): State<Arc<HttpCtx>>,
  Path(q): Path<String>,
  headers: HeaderMap,
  MsgPack(req): MsgPack<OpPollInput>,
) -> QueuedHttpResult<OpPollOutput> {
  let q = ctx.q(&q, &headers)?;
  transform_op_result(q.poll(req).await)
}

pub(crate) async fn endpoint_push(
  State(ctx): State<Arc<HttpCtx>>,
  Path(q): Path<String>,
  headers: HeaderMap,
  MsgPack(req): MsgPack<OpPushInput>,
) -> QueuedHttpResult<OpPushOutput> {
  let q = ctx.q(&q, &headers)?;
  transform_op_result(q.push(req).await)
}

pub(crate) async fn endpoint_update(
  State(ctx): State<Arc<HttpCtx>>,
  Path(q): Path<String>,
  headers: HeaderMap,
  MsgPack(req): MsgPack<OpUpdateInput>,
) -> QueuedHttpResult<OpUpdateOutput> {
  let q = ctx.q(&q, &headers)?;
  transform_op_result(q.update(req).await)
}
