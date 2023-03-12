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
use axum::extract::DefaultBodyLimit;
use axum::routing::get;
use axum::routing::post;
use axum::Router;
use axum::Server;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;

pub async fn start_http_server_loop(interface: Ipv4Addr, port: u16, ctx: Arc<Ctx>) {
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
