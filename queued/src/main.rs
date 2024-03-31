#[cfg(feature = "alloc_jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub(crate) mod http;
pub(crate) mod statsd;

use crate::http::ctx::HttpCtx;
use crate::http::healthz::endpoint_healthz;
use crate::http::metrics::endpoint_metrics;
use crate::http::ops::endpoint_delete;
use crate::http::ops::endpoint_poll;
use crate::http::ops::endpoint_push;
use crate::http::ops::endpoint_update;
use crate::http::queues::QUEUE_CREATE_OK_MARKER_FILE;
use crate::http::suspend::endpoint_get_suspend;
use crate::http::suspend::endpoint_post_suspend;
use crate::http::throttle::endpoint_get_throttle;
use crate::http::throttle::endpoint_post_throttle;
use crate::statsd::spawn_statsd_emitter;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::Request;
use axum::middleware;
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::delete;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
use axum::Router;
use axum::Server;
use clap::arg;
use clap::command;
use clap::Parser;
use dashmap::DashMap;
use http::queues::endpoint_queue_create;
use http::queues::endpoint_queue_delete;
use http::queues::endpoint_queues;
use hyper::HeaderMap;
use hyper::StatusCode;
use libqueued::Queued;
use std::backtrace::Backtrace;
use std::io::stderr;
use std::io::ErrorKind;
use std::io::Write;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::os::unix::prelude::PermissionsExt;
use std::panic;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::remove_file;
use tokio::fs::set_permissions;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tracing::info;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
  /// Path to the data directory.
  #[arg(long)]
  data_dir: PathBuf,

  /// Optional API key that clients must use to authenticate.
  #[arg(long, default_value = "")]
  api_key: String,

  /// Interface for server to listen on. Defaults to 127.0.0.1.
  #[arg(long, default_value = "127.0.0.1")]
  interface: Ipv4Addr,

  /// Port for server to listen on. Defaults to 3333.
  #[arg(long, default_value_t = 3333)]
  port: u16,

  /// If provided, the server will create and listen on this Unix socket; `interface` and `port` will be ignored.
  #[arg(long)]
  unix_socket: Option<PathBuf>,

  /// Optional StatsD server to send metrics to.
  #[arg(long)]
  statsd: Option<SocketAddr>,

  /// StatsD prefix. Defaults to "queued".
  #[arg(long, default_value = "queued")]
  statsd_prefix: String,

  /// Tags to add to all StatsD metric values sent. Use the format: `name1:value1,name2:value2,name3:value3`.
  #[arg(long, default_value = "")]
  statsd_tags: String,

  /// Batch sync delay time, in microseconds. For advanced usage only.
  #[arg(long, default_value_t = 10_000)]
  batch_sync_delay_us: u64,
}

async fn auth_middleware<B>(
  h: HeaderMap,
  api_key: State<String>,
  req: Request<B>,
  next: Next<B>,
) -> Result<Response, StatusCode> {
  if !api_key.is_empty()
    && !h
      .get("authorization")
      .and_then(|v| v.to_str().ok())
      .is_some_and(|v| v == api_key.as_str())
  {
    return Err(StatusCode::UNAUTHORIZED);
  };
  Ok(next.run(req).await)
}

#[tokio::main]
async fn main() {
  // Currently, even with `panic = abort`, thread panics will still not kill the process, so we'll install our own handler to ensure that any panic exits the process, as most likely some internal state/invariant has become corrupted and it's not safe to continue. Copied from https://stackoverflow.com/a/36031130.
  let _orig_hook = panic::take_hook();
  panic::set_hook(Box::new(move |panic_info| {
    let bt = Backtrace::force_capture();
    // Don't use `tracing::*` as it may be dangerous to do so from within a panic handler (e.g. it could itself panic).
    // Do not lock stderr as we could deadlock.
    // Build string first so we (hopefully) do one write syscall to stderr and don't get it mangled.
    // Prepend with a newline to avoid mangling with any existing half-written stderr line.
    let json = format!(
      "\r\n{}\r\n",
      serde_json::json!({
        "level": "CRITICAL",
        "panic": true,
        "message": panic_info.to_string(),
        "stack_trace": bt.to_string(),
      })
    );
    // Try our best to write all and then flush, but don't panic if we don't.
    let mut out = stderr();
    let _ = out.write_all(json.as_bytes());
    let _ = out.flush();
    process::exit(1);
  }));

  tracing_subscriber::fmt().json().init();

  let cli = Cli::parse();
  let statsd_tags = cli
    .statsd_tags
    .split(',')
    .filter_map(|p| p.split_once(':'))
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect::<Vec<_>>();

  let batch_sync_delay = Duration::from_micros(cli.batch_sync_delay_us);
  let queues = DashMap::<String, Arc<Queued>>::new();
  for d in std::fs::read_dir(&cli.data_dir).expect("read data dir") {
    let d = d.expect("read data dir entry");
    let m = d.metadata().expect("get data dir entry metadata");
    if !m.is_dir() {
      continue;
    };
    match std::fs::read_to_string(d.path().join(QUEUE_CREATE_OK_MARKER_FILE)) {
      Ok(c) => assert_eq!(&c, "", "unexpected {QUEUE_CREATE_OK_MARKER_FILE} contents in {d:?}"),
      Err(e) if e.kind() == ErrorKind::NotFound => panic!("no {QUEUE_CREATE_OK_MARKER_FILE} found in {d:?}, which could indicate corruption, external tampering, or a failed creation/deletion (in which case the containing folder can be safely deleted, and must be deleted in order to proceed)"),
      Err(e) => panic!("failed to read {QUEUE_CREATE_OK_MARKER_FILE} in {d:?}: {e}"),
    };
    let name = d
      .file_name()
      .into_string()
      .expect("data dir entry as UTF-8 string");
    let q =
      Arc::new(Queued::load_and_start(&d.path(), libqueued::QueuedCfg { batch_sync_delay }).await);
    if let Some(addr) = cli.statsd {
      spawn_statsd_emitter(
        addr,
        &cli.statsd_prefix,
        &statsd_tags,
        &name,
        Arc::downgrade(&q),
      );
    };
    assert!(queues.insert(name, q).is_none());
  }

  let ctx = Arc::new(HttpCtx {
    batch_sync_delay,
    data_dir: cli.data_dir,
    queues,
    statsd_endpoint: cli.statsd,
    statsd_prefix: cli.statsd_prefix,
    statsd_tags,
  });

  #[rustfmt::skip]
  let app = Router::new()
    .route("/healthz", get(endpoint_healthz))
    .route("/queue/:queue", delete(endpoint_queue_delete))
    .route("/queue/:queue", put(endpoint_queue_create))
    .route("/queue/:queue/messages/delete", post(endpoint_delete))
    .route("/queue/:queue/messages/poll", post(endpoint_poll))
    .route("/queue/:queue/messages/push", post(endpoint_push))
    .route("/queue/:queue/messages/update", post(endpoint_update))
    .route("/queue/:queue/metrics", get(endpoint_metrics))
    .route("/queue/:queue/suspend", get(endpoint_get_suspend).post(endpoint_post_suspend))
    .route("/queue/:queue/throttle", get(endpoint_get_throttle).post(endpoint_post_throttle))
    .route("/queues", get(endpoint_queues))
    .layer(DefaultBodyLimit::max(1024 * 1024 * 128))
    .with_state(ctx.clone())
    .route_layer(middleware::from_fn_with_state(cli.api_key, auth_middleware));

  match cli.unix_socket {
    Some(socket_path) => {
      let _ = remove_file(&socket_path).await;
      let unix_listener = UnixListener::bind(&socket_path).expect("failed to bind UNIX socket");
      let stream = UnixListenerStream::new(unix_listener);
      let acceptor = hyper::server::accept::from_stream(stream);
      set_permissions(&socket_path, PermissionsExt::from_mode(0o777))
        .await
        .unwrap();
      info!(
        unix_socket_path = socket_path.to_string_lossy().to_string(),
        "server started"
      );
      Server::builder(acceptor)
        .serve(app.into_make_service())
        .await
        .unwrap();
    }
    None => {
      let addr = SocketAddr::from((cli.interface, cli.port));
      info!(
        interface = cli.interface.to_string(),
        port = cli.port,
        "server started"
      );
      Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
    }
  };
}
