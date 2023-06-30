pub mod ctx;
pub mod healthz;
pub mod metrics;
pub mod ops;
pub mod suspend;
pub mod throttle;

use self::ctx::HttpCtx;
use self::healthz::endpoint_healthz;
use self::metrics::endpoint_metrics;
use self::ops::endpoint_delete;
use self::ops::endpoint_poll;
use self::ops::endpoint_push;
use self::ops::endpoint_update;
use self::suspend::endpoint_get_suspend;
use self::suspend::endpoint_post_suspend;
use self::throttle::endpoint_get_throttle;
use self::throttle::endpoint_post_throttle;
use axum::extract::DefaultBodyLimit;
use axum::routing::get;
use axum::routing::post;
use axum::Router;
use axum::Server;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

pub async fn start_http_server_loop(interface: Ipv4Addr, port: u16, ctx: Arc<HttpCtx>) {
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

  info!(interface = interface.to_string(), port, "server started");

  Server::bind(&addr)
    .serve(app.into_make_service())
    .await
    .unwrap();
}
