use super::result::OpError;
use super::result::OpResult;
use crate::ctx::Ctx;
use crate::db::rocksdb_key;
use crate::db::rocksdb_write_opts;
use crate::db::RocksDbKeyPrefix;
use chrono::Utc;
use dashmap::DashMap;
use futures::stream::iter;
use futures::StreamExt;
use itertools::Itertools;
use off64::int::create_i40_le;
use off64::int::create_u32_le;
use rocksdb::WriteBatchWithTransaction;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::task::spawn_blocking;

#[derive(Deserialize)]
pub struct OpPollInput {
  pub count: usize,
  pub visibility_timeout_secs: i64,
  #[serde(default)]
  pub ignore_existing_visibility_timeouts: bool, // This can be used for debugging purposes e.g. visibility timeout was set incorrectly.
}

#[derive(Serialize, Default)]
pub struct OpPollOutputMessage {
  #[serde(with = "serde_bytes")]
  pub contents: Vec<u8>,
  pub id: u64,
  pub poll_tag: u32,
}

#[derive(Serialize)]
pub struct OpPollOutput {
  pub messages: Vec<OpPollOutputMessage>,
}

pub(crate) async fn op_poll(ctx: &Ctx, req: OpPollInput) -> OpResult<OpPollOutput> {
  if ctx.suspension.is_poll_suspended() {
    ctx
      .metrics
      .suspended_poll_counter
      .fetch_add(1, Ordering::Relaxed);
    return Err(OpError::Suspended);
  };

  {
    let mut throttler = ctx.throttler.lock();
    if throttler.is_some() && !throttler.as_mut().unwrap().increment_count() {
      ctx
        .metrics
        .throttled_poll_counter
        .fetch_add(1, Ordering::Relaxed);
      return Err(OpError::Throttled);
    };
  };

  let new_visible_time = Utc::now().timestamp() + req.visibility_timeout_secs as i64;

  let msgs = ctx
    .messages
    .lock()
    .remove_earliest_n(req.count, req.ignore_existing_visibility_timeouts);
  assert!(msgs.len() <= req.count);

  let mut b = WriteBatchWithTransaction::default();
  for &(id, old_poll_tag) in msgs.iter() {
    b.put(
      rocksdb_key(RocksDbKeyPrefix::MessagePollTag, id),
      create_u32_le(old_poll_tag + 1),
    );
    b.put(
      rocksdb_key(RocksDbKeyPrefix::MessageVisibleTimestampSec, id),
      create_i40_le(new_visible_time),
    );
  }
  let db = ctx.db.clone();
  spawn_blocking(move || db.write_opt(b, &rocksdb_write_opts()).unwrap())
    .await
    .unwrap();

  let msg_contents = Arc::new(DashMap::new());
  iter(msgs.iter())
    .for_each_concurrent(None, |&(id, _)| {
      let db = ctx.db.clone();
      let msg_datas = msg_contents.clone();
      async move {
        spawn_blocking(move || {
          let data = db
            .get(rocksdb_key(RocksDbKeyPrefix::MessageData, id))
            .unwrap()
            .unwrap();
          msg_datas.insert(id, data);
        })
        .await
        .unwrap();
      }
    })
    .await;
  ctx.batch_sync.submit_and_wait(0).await;

  {
    let mut messages = ctx.messages.lock();
    for &(id, old_poll_tag) in msgs.iter() {
      messages.insert(id, new_visible_time, old_poll_tag + 1);
    }
  };

  ctx
    .metrics
    .successful_poll_counter
    .fetch_add(msgs.len() as u64, Ordering::Relaxed);

  Ok(OpPollOutput {
    messages: msgs
      .into_iter()
      .map(|(id, old_poll_tag)| OpPollOutputMessage {
        contents: msg_contents.remove(&id).unwrap().1,
        id,
        poll_tag: old_poll_tag + 1,
      })
      .collect_vec(),
  })
}
