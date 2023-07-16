#[cfg(feature = "alloc_jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub mod http;

use crate::http::ctx::HttpCtx;
use crate::http::start_http_server_loop;
use cadence::Counted;
use cadence::Gauged;
use cadence::QueuingMetricSink;
use cadence::StatsdClient;
use cadence::UdpMetricSink;
use clap::arg;
use clap::command;
use clap::Parser;
use libqueued::Queued;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::UdpSocket;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::join;
use tokio::spawn;
use tokio::time::sleep;

async fn sample_counter(
  statsd: &StatsdClient,
  name: &'static str,
  f: impl Fn() -> u64,
  interval: Duration,
) {
  let mut last_value = f();
  loop {
    sleep(interval).await;
    let value = f();
    let diff = i64::try_from(value).unwrap() - i64::try_from(last_value).unwrap();
    statsd.count(name, diff).unwrap();
    last_value = value;
  }
}

async fn sample_gauge(
  statsd: &StatsdClient,
  name: &'static str,
  f: impl Fn() -> u64,
  interval: Duration,
) {
  loop {
    sleep(interval).await;
    statsd.gauge(name, f()).unwrap();
  }
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
  /// Path to the data directory.
  #[arg(long)]
  data_dir: PathBuf,

  /// Interface for server to listen on. Defaults to 127.0.0.1.
  #[arg(long, default_value = "127.0.0.1")]
  interface: Ipv4Addr,

  /// Port for server to listen on. Defaults to 3333.
  #[arg(long, default_value_t = 3333)]
  port: u16,

  /// Optional StatsD server to send metrics to.
  #[arg(long)]
  statsd: Option<SocketAddr>,

  /// StatsD prefix. Defaults to "queued".
  #[arg(long, default_value = "queued")]
  statsd_prefix: String,
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt::init();

  let cli = Cli::parse();

  let queued = Queued::load_and_start(&cli.data_dir).await;

  let ctx = Arc::new(HttpCtx { queued });

  if let Some(addr) = cli.statsd {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let sink = UdpMetricSink::from(addr, socket).unwrap();
    let sink = QueuingMetricSink::from(sink);
    let s = StatsdClient::from_sink(&cli.statsd_prefix, sink);

    #[rustfmt::skip]
    spawn({
      let ctx = ctx.clone();
      let i = Duration::from_millis(1000);
      async move {
        loop {
          let m = ctx.build_server_metrics();
          join! {
            sample_counter(&s, "empty_poll", || m.empty_poll_counter, i),
            sample_counter(&s, "message", || m.message_counter, i),
            sample_counter(&s, "missing_delete", || m.missing_delete_counter, i),
            sample_counter(&s, "missing_update", || m.missing_update_counter, i),
            sample_counter(&s, "successful_delete", || m.successful_delete_counter, i),
            sample_counter(&s, "successful_poll", || m.successful_poll_counter, i),
            sample_counter(&s, "successful_push", || m.successful_push_counter, i),
            sample_counter(&s, "successful_update", || m.successful_update_counter, i),
            sample_counter(&s, "suspended_delete", || m.suspended_delete_counter, i),
            sample_counter(&s, "suspended_poll", || m.suspended_poll_counter, i),
            sample_counter(&s, "suspended_push", || m.suspended_push_counter, i),
            sample_counter(&s, "suspended_update", || m.suspended_update_counter, i),
            sample_counter(&s, "throttled_poll", || m.throttled_poll_counter, i),
            sample_gauge(&s, "first_message_visibility_timeout_sec", || m.first_message_visibility_timeout_sec_gauge, i),
            sample_gauge(&s, "last_message_visibility_timeout_sec", || m.last_message_visibility_timeout_sec_gauge, i),
            sample_gauge(&s, "longest_unpolled_message_sec", || m.longest_unpolled_message_sec_gauge, i),
          };
        };
      }
    });
  }

  start_http_server_loop(cli.interface, cli.port, ctx).await;
}
