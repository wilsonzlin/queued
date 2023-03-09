#[cfg(feature = "alloc_jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub mod const_;
pub mod ctx;
pub mod endpoint;
pub mod util;

use crate::const_::SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS;
use crate::const_::SLOT_VACANT_TEMPLATE;
use crate::util::get_device_size;
use crate::util::repeated_copy;
use axum::extract::DefaultBodyLimit;
use axum::routing::get;
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
use ctx::AvailableSlots;
use ctx::Ctx;
use ctx::Metrics;
use ctx::VacantSlots;
use endpoint::delete::endpoint_delete;
use endpoint::healthz::endpoint_healthz;
use endpoint::metrics::endpoint_metrics;
use endpoint::poll::endpoint_poll;
use endpoint::push::endpoint_push;
use endpoint::suspend::endpoint_get_suspend;
use endpoint::suspend::endpoint_post_suspend;
use futures::StreamExt;
use seekable_async_file::SeekableAsyncFile;
use std::cmp::min;
use std::fs::File;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::join;
use tokio::sync::Mutex;
use tokio::time::Instant;
use util::as_usize;
use util::u64_slice;

async fn start_server_loop(
  interface: Ipv4Addr,
  port: u16,
  available: Mutex<AvailableSlots>,
  device: SeekableAsyncFile,
  metrics: Arc<Metrics>,
  vacant: Mutex<VacantSlots>,
) {
  let ctx = Arc::new(Ctx {
    available,
    device,
    metrics,
    suspend_delete: AtomicBool::new(false),
    suspend_poll: AtomicBool::new(false),
    suspend_push: AtomicBool::new(false),
    vacant,
  });

  let app = Router::new()
    .route("/delete", post(endpoint_delete))
    .route("/healthz", get(endpoint_healthz))
    .route("/metrics", get(endpoint_metrics))
    .route("/poll", post(endpoint_poll))
    .route("/push", post(endpoint_push))
    .route(
      "/suspend",
      get(endpoint_get_suspend).post(endpoint_post_suspend),
    )
    .layer(DefaultBodyLimit::max(1024 * 1024 * 128))
    .with_state(ctx.clone());

  let addr = SocketAddr::from((interface, port));

  Server::bind(&addr)
    .serve(app.into_make_service())
    .await
    .unwrap();
}

// For performance, as we're writing huge chunks, this is synchronous.
fn format_device(dev: &mut File, dev_size: u64) {
  let mut template_base_padded = vec![0u8; as_usize!(SLOT_LEN)];
  template_base_padded[..as_usize!(SLOT_FIXED_FIELDS_LEN)].copy_from_slice(&SLOT_VACANT_TEMPLATE);

  let mut template = vec![0u8; min(as_usize!(dev_size), 1024 * 1024 * 1024)];
  repeated_copy(&mut template, &template_base_padded);
  let template_len: u64 = template.len().try_into().unwrap();

  let mut next = 0;
  while next < dev_size {
    dev
      .write_all_at(
        &template[..min(template.len(), as_usize!(dev_size - next))],
        next,
      )
      .unwrap();
    next += template_len;
  }

  dev.sync_all().unwrap();

  println!("Formatted device");
}

struct LoadedData {
  available: AvailableSlots,
  vacant: VacantSlots,
}

async fn load_data_from_device(
  metrics: Arc<Metrics>,
  dev: &SeekableAsyncFile,
  dev_size: u64,
) -> LoadedData {
  let available = Arc::new(Mutex::new(AvailableSlots::new(metrics.clone())));
  let vacant = Arc::new(Mutex::new(VacantSlots::new(metrics.clone())));
  let progress = Arc::new(AtomicU64::new(0));
  let started = Instant::now();

  futures::stream::iter((0..dev_size).step_by(as_usize!(SLOT_LEN)))
    .for_each_concurrent(None, |offset| {
      let available = available.clone();
      let vacant = vacant.clone();
      let progress = progress.clone();

      async move {
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

        let actual_hash = &slot_data[..32];
        let expected_hash = blake3::hash(&slot_data[32..]);
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
            available.lock().await.insert(index, visible_time);
          }
          SlotState::Vacant => {
            vacant.lock().await.add(index);
          }
        };

        // Use a counter instead of `offset / dev_size` as slots are processed out of order.
        let completed = progress.fetch_add(1, Ordering::Relaxed);
        if completed % 4194304 == 0 {
          println!(
            "Loaded {:.2}%",
            completed as f64 / (dev_size / SLOT_LEN) as f64 * 100.0
          );
        };
      }
    })
    .await;

  let available = Arc::try_unwrap(available)
    .unwrap_or_else(|_| unreachable!())
    .into_inner();
  let vacant = Arc::try_unwrap(vacant)
    .unwrap_or_else(|_| unreachable!())
    .into_inner();
  let elapsed = started.elapsed();
  println!(
    "Verified and loaded data on device in {:.2} seconds",
    elapsed.as_secs_f64()
  );
  println!("Vacant slots: {}", vacant.count());
  println!("Available slots: {}", available.len());
  println!("Total device slots: {}", dev_size / SLOT_LEN);
  LoadedData { available, vacant }
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
  /// Path to the device or file to use as persistent storage.
  #[arg(long)]
  device: PathBuf,

  /// Optional override of amount of storage to use. Note that once provided, all future invocations must consistently provide the same value for integrity; this value is not recorded anywhere.
  #[arg(long)]
  device_size: Option<u64>,

  /// Format the device or file. WARNING: All existing data will be erased.
  #[arg(long)]
  format: bool,

  /// Interface for server to listen on. Defaults to 127.0.0.1.
  #[arg(long, default_value = "127.0.0.1")]
  interface: Ipv4Addr,

  /// [Advanced] Use O_DIRECT.
  #[arg(long, default_value_t = false)]
  io_direct: bool,

  /// [Advanced] Use O_DSYNC.
  #[arg(long, default_value_t = false)]
  io_dsync: bool,

  /// Port for server to listen on. Defaults to 3333.
  #[arg(long, default_value_t = 3333)]
  port: u16,
}

const DELAYED_SYNC_US: u64 = 100;

#[tokio::main]
async fn main() {
  let cli = Cli::parse();

  let metrics = Arc::new(Metrics::default());

  let device_size = match cli.device_size {
    Some(s) => s,
    None => get_device_size(&cli.device).await,
  };
  if device_size % SLOT_LEN != 0 {
    panic!("device must be an exact multiple of {} bytes", SLOT_LEN);
  };

  if cli.format {
    format_device(
      &mut File::options().write(true).open(&cli.device).unwrap(),
      device_size,
    );
    // To avoid accidentally reusing --format command for starting long-running server process, quit immediately so it's not possible to do so.
    return;
  };

  let device = SeekableAsyncFile::open(
    &cli.device,
    device_size,
    metrics.io.clone(),
    std::time::Duration::from_micros(DELAYED_SYNC_US),
    cli.io_direct,
    cli.io_dsync,
  )
  .await;

  let LoadedData { available, vacant } =
    load_data_from_device(metrics.clone(), &device, device_size).await;

  let server_fut = start_server_loop(
    cli.interface,
    cli.port,
    Mutex::new(available),
    device.clone(),
    metrics,
    Mutex::new(vacant),
  );

  join! {
    server_fut,
    device.start_delayed_data_sync_background_loop(),
  };
}
