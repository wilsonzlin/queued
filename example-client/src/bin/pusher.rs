#![feature(int_roundings)]

use clap::command;
use clap::Parser;
use futures::future::join_all;
use serde_json::json;
use serde_json::Value;
use std::cmp::min;
use tokio::time::Instant;

async fn execute(start: u32, end: u32) -> () {
  let client = reqwest::Client::new();
  for id in start..end {
    client
      .post("http://127.0.0.1:3333/push")
      .json(&json!({
        "content": id.to_string(),
      }))
      .send()
      .await
      .unwrap()
      .error_for_status()
      .unwrap()
      .json::<Value>()
      .await
      .unwrap();
  }
}

#[derive(Parser, Debug)]
#[command(author, about)]
struct Cli {
  #[arg(long)]
  concurrency: u32,

  #[arg(long)]
  count: u32,
}

#[tokio::main]
async fn main() {
  let args = Cli::parse();
  let started = Instant::now();
  let mut tasks = Vec::with_capacity(args.concurrency.try_into().unwrap());
  let count_per_concurrency = args.count.div_ceil(args.concurrency);
  for c in 0..args.concurrency {
    let first = c * count_per_concurrency;
    let last = min(args.count, (c + 1) * count_per_concurrency);
    tasks.push(execute(first, last));
  }
  join_all(tasks).await;
  let exec_dur = started.elapsed();
  println!(
    "Pushed {} messages with {} concurrency in {} seconds",
    args.count,
    args.concurrency,
    exec_dur.as_secs_f64()
  );
}