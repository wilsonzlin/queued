#[cfg(feature = "alloc_jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub mod http;

use crate::http::ctx::HttpCtx;
use crate::http::start_http_server_loop;
use clap::arg;
use clap::command;
use clap::Parser;
use libqueued::QueuedLayoutType;
use libqueued::QueuedLoader;
use seekable_async_file::get_file_len_via_seek;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::SeekableAsyncFileMetrics;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::join;

// TODO Tune.
const DELAYED_SYNC_US: u64 = 100;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
  /// Path to the device or file to use as persistent storage.
  #[arg(long)]
  device: PathBuf,

  /// Optional override of amount of storage to use. Note that once provided, all future invocations must consistently provide the same value for integrity; this value is not recorded anywhere.
  #[arg(long)]
  device_size: Option<u64>,

  /// [Advanced] Use fixed slots layout.
  #[arg(long)]
  fixed_slots_layout: bool,

  /// Format the device or file. WARNING: All existing data will be erased.
  #[arg(long)]
  format: bool,

  /// Interface for server to listen on. Defaults to 127.0.0.1.
  #[arg(long, default_value = "127.0.0.1")]
  interface: Ipv4Addr,

  /// Port for server to listen on. Defaults to 3333.
  #[arg(long, default_value_t = 3333)]
  port: u16,
}

#[tokio::main]
async fn main() {
  let cli = Cli::parse();

  let device_size = match cli.device_size {
    Some(s) => s,
    None => get_file_len_via_seek(&cli.device)
      .await
      .expect("seek device file"),
  };

  let io_metrics = Arc::new(SeekableAsyncFileMetrics::default());

  let device = SeekableAsyncFile::open(
    &cli.device,
    device_size,
    io_metrics.clone(),
    std::time::Duration::from_micros(DELAYED_SYNC_US),
    0,
  )
  .await;

  let queued = QueuedLoader::new(
    device.clone(),
    device_size,
    if cli.fixed_slots_layout {
      QueuedLayoutType::FixedSlots
    } else {
      QueuedLayoutType::LogStructured
    },
    // TODO Tune.
    std::time::Duration::from_micros(DELAYED_SYNC_US),
  );

  if cli.format {
    queued.format().await;
    // To avoid accidentally reusing --format command for starting long-running server process, quit immediately so it's not possible to do so.
    return;
  };

  let queued = queued.load().await;

  let ctx = Arc::new(HttpCtx {
    io_metrics: io_metrics.clone(),
    queued: queued.clone(),
  });

  join! {
    start_http_server_loop(cli.interface, cli.port, ctx),
    device.start_delayed_data_sync_background_loop(),
    queued.start(),
  };
}
