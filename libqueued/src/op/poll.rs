use super::result::OpError;
use super::result::OpResult;
use crate::ctx::Ctx;
use crate::db::rocksdb_key;
use crate::db::rocksdb_write_opts;
use crate::db::RocksDbKeyPrefix;
use chrono::Utc;
use futures::stream::iter;
use futures::StreamExt;
use itertools::Itertools;
use off64::int::create_i40_le;
use off64::int::create_u32_le;
use parking_lot::Mutex;
use rocksdb::WriteBatchWithTransaction;
use serde::Deserialize;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct OpPollInput {
  pub count: usize,
  pub visibility_timeout_secs: i64,
}

#[derive(Serialize, Default)]
pub struct OpPollOutputMessage {
  pub contents: Vec<u8>,
  pub id: u64,
  pub poll_tag: u32,
}

#[derive(Serialize)]
pub struct OpPollOutput {
  pub messages: Vec<OpPollOutputMessage>,
}

pub(crate) async fn op_poll(ctx: Arc<Ctx>, req: OpPollInput) -> OpResult<OpPollOutput> {
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

  let msgs = ctx.messages.lock().remove_earliest_n(req.count);

  let b = Arc::new(Mutex::new(WriteBatchWithTransaction::default()));
  let out = Arc::new(Mutex::new(
    msgs
      .iter()
      .map(|&(id, _)| OpPollOutputMessage {
        id,
        ..Default::default()
      })
      .collect_vec(),
  ));
  iter(msgs.into_iter().enumerate())
    .for_each_concurrent(None, |(i, (id, old_poll_tag))| {
      let b = b.clone();
      let db = ctx.db.clone();
      let out = out.clone();
      async move {
        let data = db
          .get(rocksdb_key(RocksDbKeyPrefix::MessageData, id))
          .unwrap()
          .unwrap();
        let new_poll_tag = old_poll_tag + 1;
        {
          let mut b = b.lock();
          b.put(
            rocksdb_key(RocksDbKeyPrefix::MessagePollTag, id),
            create_u32_le(new_poll_tag),
          );
          b.put(
            rocksdb_key(RocksDbKeyPrefix::MessageVisibleTimestampSec, id),
            create_i40_le(new_visible_time),
          );
        };
        {
          let mut o = &mut out.lock()[i];
          o.contents = data;
          o.poll_tag = new_poll_tag;
        };
      }
    })
    .await;
  ctx
    .db
    .write_opt(
      Arc::into_inner(b).unwrap().into_inner(),
      &rocksdb_write_opts(),
    )
    .unwrap();
  ctx.batch_sync.submit_and_wait().await;

  let out = Arc::into_inner(out).unwrap().into_inner();

  {
    let mut messages = ctx.messages.lock();
    for m in out.iter() {
      messages.insert(m.id, new_visible_time, m.poll_tag);
    }
  };

  ctx
    .metrics
    .successful_poll_counter
    .fetch_add(out.len() as u64, Ordering::Relaxed);

  Ok(OpPollOutput { messages: out })
}
