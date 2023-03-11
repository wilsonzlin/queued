use crate::ctx::Ctx;
use crate::endpoint::delete::endpoint_delete;
use crate::endpoint::healthz::endpoint_healthz;
use crate::endpoint::metrics::endpoint_metrics;
use crate::endpoint::poll::endpoint_poll;
use crate::endpoint::push::endpoint_push;
use crate::endpoint::suspend::endpoint_get_suspend;
use crate::endpoint::suspend::endpoint_post_suspend;
use crate::endpoint::throttle::endpoint_get_throttle;
use crate::endpoint::throttle::endpoint_post_throttle;
use crate::endpoint::update::endpoint_update;
use crate::id_gen::IdGenerator;
use crate::invisible::InvisibleMessages;
use crate::layout::StorageLayout;
use crate::metrics::Metrics;
use crate::vacant::VacantSlots;
use crate::visible::VisibleMessages;
use axum::extract::DefaultBodyLimit;
use axum::routing::get;
use axum::routing::post;
use axum::Router;
use axum::Server;
use seekable_async_file::SeekableAsyncFile;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn start_http_server_loop(
  interface: Ipv4Addr,
  port: u16,
  invisible: Arc<Mutex<InvisibleMessages>>,
  visible: Arc<VisibleMessages>,
  id_gen: IdGenerator,
  device: SeekableAsyncFile,
  layout: Arc<dyn StorageLayout + Send + Sync>,
  metrics: Arc<Metrics>,
) {
  let ctx = Arc::new(Ctx {
    device,
    id_gen,
    invisible,
    layout,
    metrics,
    suspend_delete: AtomicBool::new(false),
    suspend_poll: AtomicBool::new(false),
    suspend_push: AtomicBool::new(false),
    suspend_update: AtomicBool::new(false),
    throttler: Mutex::new(None),
    visible,
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
