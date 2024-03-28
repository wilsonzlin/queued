use crate::messages::Messages;
use crate::metrics::Metrics;
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
  MessagePollTag = 1, // Only exists for messages that have been polled at least once.
  MessageVisibleTimestampSec = 2,
  MessageData = 3,
}

pub(crate) fn rocksdb_key(p: RocksDbKeyPrefix, id: u64) -> [u8; 9] {
  let mut out = [0u8; 9];
  out[0] = p as u8;
  out.write_u64_le_at(1, id);
  out
}

// There's no need to optimise for point lookups as our keys are always sequential 8-byte integers with (almost) no skips inserted in order, and our workload is write heavy with almost 1 write for every read.
// - (Almost) every key exists, so adding bloom filters, hash indices, or in-memory structures only consumes more memory and index space and slows down inserts without much gain in total system performance.
// - These options generally require careful tuning and come with sensitive tradeoffs.
// - We still need to be able to scan the entire database initially, so using a prefix extractor isn't applicable; a prefix extractor also wouldn't work well given our key distribution (we insert sequential IDs, so the prefix will be very unbalanced until literally the entire keyspace is used i.e. we run out of IDs).
// TODO Consider using separate column family for MessageData with blob files enabled.
fn rocksdb_opts() -> rocksdb::Options {
  // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#other-general-options.
  let mut opt = rocksdb::Options::default();
  opt.create_if_missing(true);
  opt.set_max_background_jobs(num_cpus::get() as i32 * 2);
  opt.set_bytes_per_sync(1024 * 1024 * 4);
  // https://github.com/facebook/rocksdb/wiki/BlobDB#performance-tuning
  opt.set_write_buffer_size(1024 * 1024 * 1024 * 1);
  // By default, RocksDB does not fsync WAL after fwrite, so we can lose data even when Put()/Write() returns with success, which is not OK for us. However, requiring fsync() after every Put()/Write() kills performance; therefore, we instead take over responsibility of both fwrite() and fsync() for the WAL, and do so in the background at intervals.
  opt.set_manual_wal_flush(true);
  opt.set_compression_type(rocksdb::DBCompressionType::None);

  // https://github.com/facebook/rocksdb/wiki/Block-Cache.
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

pub(crate) fn rocksdb_load(db: &DB, metrics: Arc<Metrics>) -> LoadedData {
  let mut messages = Messages::new(metrics);
  // WARNING: We must use next_id instead of simply getting the maximum ID, as that would cause ID reuse if a message is deleted and then a new one is created in quick succession.
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
    // In some rare situations, it's possible for some pushed messages to persist to the WAL but not yet reach `BatchSync::submit_and_wait` and update the `next_id` key; therefore, we must also update `next_id` to be above any existing ID. This is safe to do as, because if they did not complete `submit_and_wait`, they were never acknowledged nor inserted into the in-memory messages, so could not be polled and deleted and therefore have their IDs reused.
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

// This exists in case we need to override options for all writes in the future.
pub(crate) fn rocksdb_write_opts() -> WriteOptions {
  WriteOptions::default()
}
