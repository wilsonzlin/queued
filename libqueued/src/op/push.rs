use super::result::OpError;
use super::result::OpResult;
use crate::ctx::Ctx;
use crate::db::rocksdb_key;
use crate::db::rocksdb_write_opts;
use crate::db::RocksDbKeyPrefix;
use chrono::Utc;
use itertools::Itertools;
use off64::int::create_i40_le;
use off64::int::create_u64_le;
use rocksdb::WriteBatchWithTransaction;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::task::spawn_blocking;

#[derive(Deserialize)]
pub struct OpPushInputMessage {
  #[serde(with = "serde_bytes")]
  pub contents: Vec<u8>,
  pub visibility_timeout_secs: u32,
}

#[derive(Deserialize)]
pub struct OpPushInput {
  pub messages: Vec<OpPushInputMessage>,
}

#[derive(Serialize)]
pub struct OpPushOutput {
  pub ids: Vec<u64>,
}

pub(crate) async fn op_push(ctx: Arc<Ctx>, req: OpPushInput) -> OpResult<OpPushOutput> {
  if ctx.suspension.is_push_suspended() {
    ctx
      .metrics
      .suspended_push_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err(OpError::Suspended);
  };

  let n = req.messages.len() as u64;
  let base_id = ctx.next_id.fetch_add(n, Ordering::Relaxed);
  let mut to_add = Vec::new();
  let mut b = WriteBatchWithTransaction::default();
  b.put("next_id", create_u64_le(base_id + n));
  for (i, msg) in req.messages.into_iter().enumerate() {
    let id = base_id + i as u64;
    let visible_time = Utc::now().timestamp() + msg.visibility_timeout_secs as i64;
    b.put(rocksdb_key(RocksDbKeyPrefix::MessageData, id), msg.contents);
    b.put(
      rocksdb_key(RocksDbKeyPrefix::MessageVisibleTimestampSec, id),
      create_i40_le(visible_time),
    );
    to_add.push((id, visible_time));
  }
  let db = ctx.db.clone();
  spawn_blocking(move || db.write_opt(b, &rocksdb_write_opts()).unwrap())
    .await
    .unwrap();
  ctx.batch_sync.submit_and_wait().await;

  {
    let mut messages = ctx.messages.lock();
    for (id, vt) in to_add {
      messages.insert(id, vt, 0);
    }
  }

  ctx
    .metrics
    .successful_push_counter
    .fetch_add(n, Ordering::Relaxed);

  Ok(OpPushOutput {
    ids: (0..n).map(|i| base_id + i).collect_vec(),
  })
}
