use super::ctx::HttpCtx;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
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

fn transform_op_result<R: Serialize>(
  result: OpResult<R>,
) -> Result<Json<R>, (StatusCode, &'static str)> {
  result.map(|res| Json(res)).map_err(|err| match err {
    OpError::InvalidPollTag => (StatusCode::BAD_REQUEST, "invalid poll tag"),
    OpError::MessageNotFound => (StatusCode::NOT_FOUND, "message not found"),
    OpError::Suspended => (
      StatusCode::SERVICE_UNAVAILABLE,
      "this endpoint has been suspended",
    ),
    OpError::Throttled => (
      StatusCode::TOO_MANY_REQUESTS,
      "this poll has been throttled",
    ),
  })
}

pub async fn endpoint_delete(
  State(ctx): State<Arc<HttpCtx>>,
  Json(req): Json<OpDeleteInput>,
) -> Result<Json<OpDeleteOutput>, (StatusCode, &'static str)> {
  transform_op_result(ctx.queued.delete(req).await)
}

pub async fn endpoint_poll(
  State(ctx): State<Arc<HttpCtx>>,
  Json(req): Json<OpPollInput>,
) -> Result<Json<OpPollOutput>, (StatusCode, &'static str)> {
  transform_op_result(ctx.queued.poll(req).await)
}

pub async fn endpoint_push(
  State(ctx): State<Arc<HttpCtx>>,
  Json(req): Json<OpPushInput>,
) -> Result<Json<OpPushOutput>, (StatusCode, &'static str)> {
  transform_op_result(ctx.queued.push(req).await)
}

pub async fn endpoint_update(
  State(ctx): State<Arc<HttpCtx>>,
  Json(req): Json<OpUpdateInput>,
) -> Result<Json<OpUpdateOutput>, (StatusCode, &'static str)> {
  transform_op_result(ctx.queued.update(req).await)
}
