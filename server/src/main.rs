#[cfg(feature = "alloc_jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub mod ctx;
pub mod endpoint;
pub mod http;
pub mod id_gen;
pub mod invisible;
pub mod journal;
pub mod layout;
pub mod metrics;
pub mod throttler;
pub mod util;
pub mod vacant;
pub mod visible;

use crate::http::start_http_server_loop;
use crate::id_gen::IdGenerator;
use crate::journal::Journal;
use crate::layout::fixed_slots::FixedSlotsLayout;
use crate::layout::log_structured::LogStructuredLayout;
use crate::util::get_device_size;
use clap::arg;
use clap::command;
use clap::Parser;
use layout::LoadedData;
use layout::StorageLayout;
use metrics::Metrics;
use seekable_async_file::SeekableAsyncFile;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::join;
use tokio::sync::Mutex;
use tokio::time::Instant;

const DELAYED_SYNC_US: u64 = 100;
const OFFSETOF_JOURNAL: u64 = 0;
const JOURNAL_CAPACITY: u64 = 1024 * 1024;
const OFFSETOF_ID_GEN: u64 = OFFSETOF_JOURNAL + JOURNAL_CAPACITY;
const OFFSETOF_DATA: u64 = OFFSETOF_ID_GEN + 8;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
  /// Path to the device or file to use as persistent storage.
  #[arg(long)]
  device: PathBuf,

  /// Optional override of amount of storage to use. Note that once provided, all future invocations must consistently provide the same value for integrity; this value is not recorded anywhere.
  #[arg(long)]
  device_size: Option<u64>,

  /// [Advanced] Use log structured layout.
  #[arg(long)]
  log_structured_layout: bool,

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

#[tokio::main]
async fn main() {
  let cli = Cli::parse();

  let metrics = Arc::new(Metrics::default());

  let device_size = match cli.device_size {
    Some(s) => s,
    None => get_device_size(&cli.device).await,
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

  let journal = Arc::new(Journal::new(
    device.clone(),
    OFFSETOF_JOURNAL,
    JOURNAL_CAPACITY,
  ));
  let id_gen =
    IdGenerator::load_from_device(device.clone(), journal.clone(), OFFSETOF_ID_GEN).await;

  let layout: Arc<dyn StorageLayout + Send + Sync> = if !cli.log_structured_layout {
    Arc::new(FixedSlotsLayout::new(
      device.clone(),
      device_size,
      OFFSETOF_DATA,
      metrics.clone(),
    ))
  } else {
    Arc::new(LogStructuredLayout::new(
      device.clone(),
      device_size,
      OFFSETOF_DATA,
    ))
  };

  if cli.format {
    journal.format_device().await;
    id_gen.format_device(device.clone()).await;
    layout.format_device().await;
    println!("Formatted device");
    // To avoid accidentally reusing --format command for starting long-running server process, quit immediately so it's not possible to do so.
    return;
  };

  journal.recover().await;

  let load_started = Instant::now();
  let LoadedData { invisible, visible } = layout.load_data_from_device(metrics.clone()).await;
  println!(
    "Verified and loaded data on device in {:.2} seconds",
    load_started.elapsed().as_secs_f64()
  );
  println!("Invisible messages: {}", invisible.len());
  println!("Visible messages: {}", visible.len());

  let invisible = Arc::new(Mutex::new(invisible));
  let visible = Arc::new(visible);

  let server_fut = start_http_server_loop(
    cli.interface,
    cli.port,
    invisible.clone(),
    visible.clone(),
    id_gen,
    device.clone(),
    layout.clone(),
    metrics,
  );

  join! {
    server_fut,
    device.start_delayed_data_sync_background_loop(),
    layout.start_background_loops(),
    visible.start_invisible_consumption_background_loop(invisible.clone()),
    journal.start_commit_background_loop(),
  };
}
