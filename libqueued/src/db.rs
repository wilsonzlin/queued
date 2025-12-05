use crate::messages::Messages;
use num_derive::FromPrimitive;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use rocksdb::BlockBasedOptions;
use rocksdb::Cache;
use rocksdb::Direction;
use rocksdb::IteratorMode;
use rocksdb::WriteOptions;
use rocksdb::DB;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, FromPrimitive)]
#[repr(u8)]
pub(crate) enum RocksDbKeyPrefix {
  MessagePollTag = 1,
  MessageVisibleTimestampSec = 2,
  MessageData = 3,
}

pub(crate) fn rocksdb_key(p: RocksDbKeyPrefix, id: u64) -> [u8; 9] {
  let mut out = [0u8; 9];
  out[0] = p as u8;
  out.write_u64_le_at(1, id);
  out
}

fn rocksdb_opts() -> rocksdb::Options {
  let mut opt = rocksdb::Options::default();
  opt.create_if_missing(true);
  opt.set_max_background_jobs(num_cpus::get() as i32 * 2);
  opt.set_bytes_per_sync(1024 * 1024 * 4);
  opt.set_write_buffer_size(1024 * 1024 * 1024 * 1);
  opt.set_manual_wal_flush(true);
  opt.set_compression_type(rocksdb::DBCompressionType::None);

  let block_cache = Cache::new_lru_cache(1024 * 1024 * 1024 * 1);
  let mut bbt_opt = BlockBasedOptions::default();
  bbt_opt.set_block_size(1024 * 64);
  bbt_opt.set_block_cache(&block_cache);
  bbt_opt.set_cache_index_and_filter_blocks(true);
  bbt_opt.set_pin_l0_filter_and_index_blocks_in_cache(true);
  bbt_opt.set_format_version(5);
  opt.set_block_based_table_factory(&bbt_opt);
  opt
}

pub(crate) fn rocksdb_open(data_dir: &Path) -> Arc<DB> {
  Arc::new(DB::open(&rocksdb_opts(), data_dir).unwrap())
}

pub(crate) struct LoadedData {
  pub next_id: u64,
  pub messages: Messages,
}

pub(crate) fn rocksdb_load(db: &DB, queue_name: String) -> LoadedData {
  let mut messages = Messages::new(queue_name);
  let mut next_id = db
    .get("next_id")
    .unwrap()
    .map(|raw| raw.read_u64_le_at(0))
    .unwrap_or(0);
  for e in db.iterator(IteratorMode::From(
    &[RocksDbKeyPrefix::MessageVisibleTimestampSec as u8],
    Direction::Forward,
  )) {
    let (k, v) = e.unwrap();
    if k[0] != RocksDbKeyPrefix::MessageVisibleTimestampSec as u8 {
      break;
    };
    let id = k.read_u64_le_at(1);
    if id >= next_id {
      next_id = id + 1;
    };
    let visible_time = v.read_i40_le_at(0);
    let poll_tag = db
      .get(rocksdb_key(RocksDbKeyPrefix::MessagePollTag, id))
      .unwrap()
      .map(|raw| raw.read_u32_le_at(0))
      .unwrap_or(0);
    messages.insert(id, visible_time, poll_tag);
  }
  LoadedData { messages, next_id }
}

pub(crate) fn rocksdb_write_opts() -> WriteOptions {
  WriteOptions::default()
}
