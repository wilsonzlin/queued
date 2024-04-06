#[cfg(feature = "alloc_jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod cfg;
mod endpoint;
mod statsd;

use crate::endpoint::api_key::endpoint_list_api_keys;
use crate::endpoint::api_key::endpoint_remove_api_key;
use crate::endpoint::api_key::endpoint_set_api_key;
use crate::endpoint::healthz::endpoint_healthz;
use crate::endpoint::queue::metrics::endpoint_metrics;
use crate::endpoint::queue::ops::endpoint_delete;
use crate::endpoint::queue::ops::endpoint_poll;
use crate::endpoint::queue::ops::endpoint_push;
use crate::endpoint::queue::ops::endpoint_update;
use crate::endpoint::queue::suspend::endpoint_get_suspend;
use crate::endpoint::queue::suspend::endpoint_post_suspend;
use crate::endpoint::queue::throttle::endpoint_get_throttle;
use crate::endpoint::queue::throttle::endpoint_post_throttle;
use crate::endpoint::queues::QUEUE_CREATE_OK_MARKER_FILE;
use crate::endpoint::HttpCtx;
use crate::statsd::spawn_statsd_emitter;
use axum::extract::DefaultBodyLimit;
use axum::routing::delete;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
use axum::Router;
use cfg::load_cfg;
use dashmap::DashMap;
use endpoint::queues::endpoint_queue_create;
use endpoint::queues::endpoint_queue_delete;
use endpoint::queues::endpoint_queues;
use libqueued::Queued;
use service_toolkit::panic::set_up_panic_hook;
use service_toolkit::server::build_port_server;
use service_toolkit::server::build_port_server_with_tls;
use service_toolkit::server::build_unix_socket_server;
use service_toolkit::server::TlsCfg;
use std::fs::read;
use std::io::ErrorKind;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() {
  set_up_panic_hook();
  tracing_subscriber::fmt().json().init();

  let cfg = load_cfg();
  let queues = DashMap::<String, Arc<Queued>>::new();
  info!(
    dir = format!("{:?}", cfg.data_dir),
    "loading queues from data dir"
  );
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
    info!(name, "loaded queue");
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
  info!(count = queues.len(), "loaded all queues");

  let ctx = Arc::new(HttpCtx {
    api_keys: cfg.enable_auth.then(|| DashMap::new()),
    batch_sync_delay: cfg.batch_sync_delay,
    data_dir: cfg.data_dir,
    global_api_key: cfg.global_api_key,
    queues,
    statsd_endpoint: cfg.statsd,
    statsd_prefix: cfg.statsd_prefix,
    statsd_tags: cfg.statsd_tags,
  });

  #[rustfmt::skip]
  let app = Router::new()
    .route("/healthz", get(endpoint_healthz))
    .route("/api-keys", get(endpoint_list_api_keys))
    .route("/api-key/:apiKey", put(endpoint_set_api_key).delete(endpoint_remove_api_key))
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
    .with_state(ctx.clone());

  match cfg.unix_socket {
    Some(socket_path) => {
      info!(
        unix_socket_path = socket_path.to_string_lossy().to_string(),
        "server started"
      );
      build_unix_socket_server(&socket_path, cfg.unix_socket_mode)
        .await
        .serve(app.into_make_service())
        .await
        .unwrap();
    }
    None => {
      match (cfg.ssl_cert, cfg.ssl_key, cfg.ssl_ca) {
        (Some(cert), Some(key), ca) => {
          info!(
            interface = cfg.interface.to_string(),
            port = cfg.port,
            mtls = ca.is_some(),
            "HTTPS server started"
          );
          build_port_server_with_tls(cfg.interface, cfg.port, &TlsCfg {
            cert: read(cert).expect("read SSL certificate file"),
            key: read(key).expect("read SSL key file"),
            ca: ca.map(|ca| read(ca).expect("read SSL CA file")),
          })
          .serve(app.into_make_service())
          .await
          .unwrap();
        }
        (None, None, None) => {
          info!(
            interface = cfg.interface.to_string(),
            port = cfg.port,
            "HTTP server started"
          );
          build_port_server(cfg.interface, cfg.port)
            .serve(app.into_make_service())
            .await
            .unwrap();
        }
        _ => panic!("invalid SSL configuration"),
      };
    }
  };
}
