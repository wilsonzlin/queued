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
use tokio::task::spawn_blocking;

#[derive(Serialize, Deserialize)]
pub struct OpDeleteInputMessage {
  pub id: u64,
  pub poll_tag: u32,
}

#[derive(Serialize, Deserialize)]
pub struct OpDeleteInput {
  pub messages: Vec<OpDeleteInputMessage>,
}

#[derive(Serialize, Deserialize)]
pub struct OpDeleteOutput {}

pub(crate) async fn op_delete(ctx: &Ctx, req: OpDeleteInput) -> OpResult<OpDeleteOutput> {
  if ctx.suspension.is_delete_suspended() {
    ctx
      .metrics
      .suspended_delete_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err(OpError::Suspended);
  };

  let mut b = WriteBatchWithTransaction::default();
  {
    let mut msgs = ctx.messages.lock();
    for m in req.messages {
      if !msgs.remove_if_poll_tag_matches(m.id, m.poll_tag) {
        ctx
          .metrics
          .missing_delete_counter
          .fetch_add(1, Ordering::Relaxed);
        continue;
      };
      b.delete(rocksdb_key(RocksDbKeyPrefix::MessageData, m.id));
      b.delete(rocksdb_key(RocksDbKeyPrefix::MessagePollTag, m.id));
      b.delete(rocksdb_key(
        RocksDbKeyPrefix::MessageVisibleTimestampSec,
        m.id,
      ));
      ctx
        .metrics
        .successful_delete_counter
        .fetch_add(1, Ordering::Relaxed);
    }
  };
  let db = ctx.db.clone();
  spawn_blocking(move || db.write_opt(b, &rocksdb_write_opts()).unwrap())
    .await
    .unwrap();
  ctx.batch_sync.submit_and_wait(0).await;

  Ok(OpDeleteOutput {})
}
