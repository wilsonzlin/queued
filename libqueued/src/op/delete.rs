use super::result::OpError;
use super::result::OpResult;
use crate::ctx::Ctx;
use crate::db::rocksdb_key;
use crate::db::rocksdb_write_opts;
use crate::db::RocksDbKeyPrefix;
use rocksdb::WriteBatchWithTransaction;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::task::spawn_blocking;

#[derive(Serialize, Deserialize)]
pub struct OpDeleteInput {
  pub id: u64,
  pub poll_tag: u32,
}

#[derive(Serialize, Deserialize)]
pub struct OpDeleteOutput {}

pub(crate) async fn op_delete(ctx: Arc<Ctx>, req: OpDeleteInput) -> OpResult<OpDeleteOutput> {
  if ctx.suspension.is_delete_suspended() {
    ctx
      .metrics
      .suspended_delete_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err(OpError::Suspended);
  };

  if !ctx
    .messages
    .lock()
    .remove_if_poll_tag_matches(req.id, req.poll_tag)
  {
    ctx
      .metrics
      .missing_delete_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err(OpError::MessageNotFound);
  };

  let db = ctx.db.clone();
  spawn_blocking(move || {
    let mut b = WriteBatchWithTransaction::default();
    b.delete(rocksdb_key(RocksDbKeyPrefix::MessageData, req.id));
    b.delete(rocksdb_key(RocksDbKeyPrefix::MessagePollTag, req.id));
    b.delete(rocksdb_key(
      RocksDbKeyPrefix::MessageVisibleTimestampSec,
      req.id,
    ));
    db.write_opt(b, &rocksdb_write_opts()).unwrap();
  })
  .await
  .unwrap();
  ctx.batch_sync.submit_and_wait().await;

  ctx
    .metrics
    .successful_delete_counter
    .fetch_add(1, Ordering::Relaxed);
  Ok(OpDeleteOutput {})
}
