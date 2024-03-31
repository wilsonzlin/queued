#[cfg(feature = "alloc_jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod cfg;
mod http;
mod panic;
mod statsd;

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
use axum::middleware::from_fn_with_state;
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::delete;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
use axum::Router;
use axum::Server;
use cfg::load_cfg;
use dashmap::DashMap;
use http::queues::endpoint_queue_create;
use http::queues::endpoint_queue_delete;
use http::queues::endpoint_queues;
use hyper::HeaderMap;
use hyper::StatusCode;
use libqueued::Queued;
use panic::set_up_panic_hook;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::os::unix::prelude::PermissionsExt;
use std::sync::Arc;
use tokio::fs::remove_file;
use tokio::fs::set_permissions;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tracing::info;

async fn auth_middleware<B>(
  h: HeaderMap,
  State(api_key): State<Option<String>>,
  req: Request<B>,
  next: Next<B>,
) -> Result<Response, StatusCode> {
  if let Some(expected_key) = api_key {
    if !h
      .get("authorization")
      .and_then(|v| v.to_str().ok())
      .is_some_and(|v| v == expected_key.as_str())
    {
      return Err(StatusCode::UNAUTHORIZED);
    };
  };
  Ok(next.run(req).await)
}

#[tokio::main]
async fn main() {
  set_up_panic_hook();
  tracing_subscriber::fmt().json().init();

  let cfg = load_cfg();
  let queues = DashMap::<String, Arc<Queued>>::new();
  for d in std::fs::read_dir(&cfg.data_dir).expect("read data dir") {
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
    let q = Arc::new(
      Queued::load_and_start(&d.path(), libqueued::QueuedCfg {
        batch_sync_delay: cfg.batch_sync_delay,
      })
      .await,
    );
    if let Some(addr) = cfg.statsd {
      spawn_statsd_emitter(
        addr,
        &cfg.statsd_prefix,
        &cfg.statsd_tags,
        &name,
        Arc::downgrade(&q),
      );
    };
    assert!(queues.insert(name, q).is_none());
  }

  let ctx = Arc::new(HttpCtx {
    batch_sync_delay: cfg.batch_sync_delay,
    data_dir: cfg.data_dir,
    queues,
    statsd_endpoint: cfg.statsd,
    statsd_prefix: cfg.statsd_prefix,
    statsd_tags: cfg.statsd_tags,
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
    .route_layer(from_fn_with_state(cfg.api_key, auth_middleware));

  match cfg.unix_socket {
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
      let addr = SocketAddr::from((cfg.interface, cfg.port));
      info!(
        interface = cfg.interface.to_string(),
        port = cfg.port,
        "server started"
      );
      Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
    }
  };
}
