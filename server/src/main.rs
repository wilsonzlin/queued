#[cfg(feature = "alloc_jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub mod const_;
pub mod ctx;
pub mod endpoint;
pub mod file;
pub mod util;

use crate::const_::SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS;
use crate::const_::SLOT_VACANT_TEMPLATE;
use crate::file::SeekableAsyncFile;
use crate::util::get_device_size;
use axum::routing::post;
use axum::Router;
use axum::Server;
use chrono::TimeZone;
use chrono::Utc;
use clap::arg;
use clap::command;
use clap::Parser;
use const_::SlotState;
use const_::SLOT_FIXED_FIELDS_LEN;
use const_::SLOT_LEN;
use const_::SLOT_OFFSETOF_LEN;
use const_::SLOT_OFFSETOF_STATE;
use const_::SLOT_OFFSETOF_VISIBLE_TS;
use croaring::Bitmap;
use ctx::AvailableSlots;
use ctx::Ctx;
use endpoint::delete::endpoint_delete;
use endpoint::poll::endpoint_poll;
use endpoint::push::endpoint_push;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::join;
use tokio::sync::RwLock;
use util::as_usize;
use util::u64_slice;

async fn start_server_loop(
  available: RwLock<AvailableSlots>,
  device: SeekableAsyncFile,
  vacant: RwLock<Bitmap>,
) {
  let ctx = Arc::new(Ctx {
    available,
    device,
    vacant,
  });

  let app = Router::new()
    .route("/delete", post(endpoint_delete))
    .route("/poll", post(endpoint_poll))
    .route("/push", post(endpoint_push))
    .with_state(ctx.clone());

  let addr = SocketAddr::from(([0, 0, 0, 0], 3333));

  Server::bind(&addr)
    .serve(app.into_make_service())
    .await
    .unwrap();
}

async fn format_device(dev: &SeekableAsyncFile, dev_size: u64) {
  let mut next = 0;
  let end = dev_size;
  while next < end {
    dev.write_at(next, SLOT_VACANT_TEMPLATE.clone()).await;
    next += SLOT_LEN;
  }

  dev.sync_all().await;

  println!("Formatted device");
}

struct LoadedData {
  available: AvailableSlots,
  vacant: Bitmap,
}

async fn load_data_from_device(dev: &SeekableAsyncFile, dev_size: u64) -> LoadedData {
  let mut available = AvailableSlots::new();
  let mut vacant = Bitmap::create();

  let mut offset = 0;
  while offset < dev_size {
    let mut slot_data = dev.read_at(offset, SLOT_LEN).await;

    let hash_includes_contents = slot_data[as_usize!(SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS)];
    match hash_includes_contents {
      0 => slot_data.truncate(as_usize!(SLOT_FIXED_FIELDS_LEN)),
      1 => {
        let content_len: u64 = u16::from_be_bytes(
          u64_slice(&slot_data, SLOT_OFFSETOF_LEN, 2)
            .try_into()
            .unwrap(),
        )
        .into();
        if content_len > SLOT_LEN - SLOT_FIXED_FIELDS_LEN {
          panic!(
            "data corruption: slot at {} contains invalid content length",
            offset
          );
        }
        slot_data.truncate(as_usize!(SLOT_FIXED_FIELDS_LEN + content_len));
      }
      _ => panic!(
        "data corruption: slot at {} contains invalid content hashing indicator",
        offset
      ),
    };

    let expected_hash = blake3::hash(&slot_data[32..]);
    let actual_hash = &slot_data[..32];
    if actual_hash != expected_hash.as_bytes() {
      panic!(
        "data corruption: slot at {} contains hash {:x?} but data hashes to {:x?}",
        offset,
        actual_hash,
        expected_hash.as_bytes()
      );
    }

    let state = SlotState::try_from(slot_data[as_usize!(SLOT_OFFSETOF_STATE)]).unwrap();
    let index: u32 = (offset / SLOT_LEN).try_into().unwrap();
    match state {
      SlotState::Available => {
        let visible_time = Utc
          .timestamp_millis_opt(
            i64::from_be_bytes(
              u64_slice(&slot_data, SLOT_OFFSETOF_VISIBLE_TS, 8)
                .try_into()
                .unwrap(),
            ) * 1000,
          )
          .unwrap();
        available.insert(index, visible_time);
      }
      SlotState::Vacant => {
        if !vacant.add_checked(index) {
          panic!("slot already exists");
        }
      }
    };

    offset += SLOT_LEN;
  }

  println!("Verified and loaded data on device");
  println!("Vacant slots: {}", vacant.cardinality());
  println!("Available slots: {}", available.len());
  println!("Total device slots: {}", dev_size / SLOT_LEN);
  LoadedData { available, vacant }
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
  #[arg(long)]
  device: PathBuf,

  #[arg(long)]
  format: bool,
}

#[tokio::main]
async fn main() {
  let cli = Cli::parse();

  let mut device = SeekableAsyncFile::open(&cli.device).await;

  let device_size = get_device_size(&cli.device).await;
  if device_size % SLOT_LEN != 0 {
    panic!("device must be an exact multiple of {} bytes", SLOT_LEN);
  };

  if cli.format {
    format_device(&mut device, device_size).await;
    // To avoid accidentally reusing --format command for starting long-running server process, quit immediately so it's not possible to do so.
    return;
  }

  let LoadedData { available, vacant } = load_data_from_device(&device, device_size).await;

  let server_fut = start_server_loop(RwLock::new(available), device.clone(), RwLock::new(vacant));

  #[cfg(feature = "fsync_delayed")]
  join! {
    server_fut,
    device.start_delayed_data_sync_background_loop(),
  };

  #[cfg(not(feature = "fsync_delayed"))]
  server_fut.await;
}
