use super::result::OpError;
use super::result::OpResult;
use crate::ctx::Ctx;
use crate::db::rocksdb_key;
use crate::db::rocksdb_write_opts;
use crate::db::RocksDbKeyPrefix;
use chrono::Utc;
use off64::int::create_i40_le;
use off64::int::create_u32_le;
use rocksdb::WriteBatchWithTransaction;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use tokio::task::spawn_blocking;

#[derive(Deserialize)]
pub struct OpUpdateInput {
  pub id: u64,
  pub poll_tag: u32,
  pub visibility_timeout_secs: i64,
}

#[derive(Serialize)]
pub struct OpUpdateOutput {
  pub new_poll_tag: u32,
}

pub(crate) async fn op_update(ctx: &Ctx, req: OpUpdateInput) -> OpResult<OpUpdateOutput> {
  if ctx.suspension.is_update_suspended() {
    ctx
      .metrics
      .suspended_update_counter
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
      .missing_update_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err(OpError::MessageNotFound);
  };
  let new_visible_time = Utc::now().timestamp() + req.visibility_timeout_secs as i64;
  let new_poll_tag = req.poll_tag + 1;

  let db = ctx.db.clone();
  spawn_blocking(move || {
    let mut b = WriteBatchWithTransaction::default();
    b.put(
      rocksdb_key(RocksDbKeyPrefix::MessagePollTag, req.id),
      create_u32_le(new_poll_tag),
    );
    b.put(
      rocksdb_key(RocksDbKeyPrefix::MessageVisibleTimestampSec, req.id),
      create_i40_le(new_visible_time),
    );
    db.write_opt(b, &rocksdb_write_opts()).unwrap();
  })
  .await
  .unwrap();
  ctx.batch_sync.submit_and_wait(0).await;

  ctx
    .messages
    .lock()
    .insert(req.id, new_visible_time, new_poll_tag);

  ctx
    .metrics
    .successful_update_counter
    .fetch_add(1, Ordering::Relaxed);

  Ok(OpUpdateOutput { new_poll_tag })
}
