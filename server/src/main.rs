#[cfg(feature = "alloc_jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub mod available;
pub mod ctx;
pub mod endpoint;
pub mod layout;
pub mod metrics;
pub mod throttler;
pub mod util;
pub mod vacant;

use crate::layout::fixed_slots::FixedSlotsLayout;
use crate::layout::log_structured::LogStructuredLayout;
use crate::util::get_device_size;
use available::AvailableMessages;
use axum::extract::DefaultBodyLimit;
use axum::routing::get;
use axum::routing::post;
use axum::Router;
use axum::Server;
use clap::arg;
use clap::command;
use clap::Parser;
use ctx::Ctx;
use endpoint::delete::endpoint_delete;
use endpoint::healthz::endpoint_healthz;
use endpoint::metrics::endpoint_metrics;
use endpoint::poll::endpoint_poll;
use endpoint::push::endpoint_push;
use endpoint::suspend::endpoint_get_suspend;
use endpoint::suspend::endpoint_post_suspend;
use endpoint::throttle::endpoint_get_throttle;
use endpoint::throttle::endpoint_post_throttle;
use endpoint::update::endpoint_update;
use layout::LoadedData;
use layout::StorageLayout;
use metrics::Metrics;
use seekable_async_file::SeekableAsyncFile;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::join;
use tokio::sync::Mutex;
use tokio::time::Instant;
use vacant::VacantSlots;

async fn start_server_loop(
  interface: Ipv4Addr,
  port: u16,
  available: Mutex<AvailableMessages>,
  device: SeekableAsyncFile,
  layout: Arc<dyn StorageLayout + Send + Sync>,
  metrics: Arc<Metrics>,
  vacant: Mutex<VacantSlots>,
) {
  let ctx = Arc::new(Ctx {
    available,
    device,
    layout,
    metrics,
    suspend_delete: AtomicBool::new(false),
    suspend_poll: AtomicBool::new(false),
    suspend_push: AtomicBool::new(false),
    suspend_update: AtomicBool::new(false),
    throttler: Mutex::new(None),
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
    .route(
      "/throttle",
      get(endpoint_get_throttle).post(endpoint_post_throttle),
    )
    .route("/update", post(endpoint_update))
    .layer(DefaultBodyLimit::max(1024 * 1024 * 128))
    .with_state(ctx.clone());

  let addr = SocketAddr::from((interface, port));

  Server::bind(&addr)
    .serve(app.into_make_service())
    .await
    .unwrap();
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

const DELAYED_SYNC_US: u64 = 100;

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

  let mut layout: Arc<dyn StorageLayout + Send + Sync> = if !cli.log_structured_layout {
    Arc::new(FixedSlotsLayout::new(device.clone(), device_size))
  } else {
    Arc::new(LogStructuredLayout::new(device.clone(), device_size))
  };

  if cli.format {
    layout.format_device().await;
    println!("Formatted device");
    // To avoid accidentally reusing --format command for starting long-running server process, quit immediately so it's not possible to do so.
    return;
  };

  let load_started = Instant::now();
  let LoadedData { available, vacant } = Arc::get_mut(&mut layout)
    .unwrap()
    .load_data_from_device(metrics.clone())
    .await;
  println!(
    "Verified and loaded data on device in {:.2} seconds",
    load_started.elapsed().as_secs_f64()
  );
  println!("Vacant slots: {}", vacant.count());
  println!("Available messages: {}", available.len());

  let server_fut = start_server_loop(
    cli.interface,
    cli.port,
    Mutex::new(available),
    device.clone(),
    layout.clone(),
    metrics,
    Mutex::new(vacant),
  );

  join! {
    server_fut,
    device.start_delayed_data_sync_background_loop(),
    layout.start_background_loops(),
  };
}
