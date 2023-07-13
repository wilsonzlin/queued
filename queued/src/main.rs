#[cfg(feature = "alloc_jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub mod http;

use crate::http::ctx::HttpCtx;
use crate::http::start_http_server_loop;
use clap::arg;
use clap::command;
use clap::Parser;
use libqueued::Queued;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

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
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt::init();

  let cli = Cli::parse();

  let queued = Queued::load_and_start(&cli.data_dir).await;

  let ctx = Arc::new(HttpCtx { queued });

  start_http_server_loop(cli.interface, cli.port, ctx).await;
}
