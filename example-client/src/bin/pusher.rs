use clap::command;
use clap::Parser;
use futures::future::join_all;
use itertools::Itertools;
use serde_json::json;
use serde_json::Value;
use std::cmp::min;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::time::Instant;

async fn execute(
  hostname: &str,
  connect_timeout: u64,
  request_timeout: u64,
  start: u32,
  end: u32,
  batch_size: usize,
  conn_err_cnt: Arc<AtomicU64>,
) -> () {
  let client = reqwest::Client::builder()
    .connect_timeout(std::time::Duration::from_secs(connect_timeout))
    .timeout(std::time::Duration::from_secs(request_timeout))
    .pool_max_idle_per_host(1048576)
    .build()
    .unwrap();
  let url = format!("http://{}:3333/push", hostname);
  for batch in (start..end).chunks(batch_size).into_iter() {
    let messages = batch
      .map(|id| {
        json!({
          "contents": id.to_string(),
          "visibility_timeout_secs": 0,
        })
      })
      .collect::<Vec<_>>();
    loop {
      let Ok(req) = client
        .post(&url)
        .json(&json!({
          "messages": messages,
        }))
        .send()
        .await else {
          conn_err_cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          continue;
        };
      req
        .error_for_status()
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
      break;
    }
  }
}

#[derive(Parser, Debug)]
#[command(author, about)]
struct Cli {
  #[arg(long)]
  hostname: String,

  #[arg(long)]
  connect_timeout_secs: u64,

  #[arg(long)]
  request_timeout_secs: u64,

  #[arg(long)]
  concurrency: u32,

  #[arg(long)]
  count: u32,

  #[arg(long)]
  batch_size: usize,
}

// https://stackoverflow.com/a/72442854/6249022
fn div_ceil(a: u32, b: u32) -> u32 {
  // This only works if addition doesn't overflow, so let's upsize to u64 to ensure this.
  let a: u64 = a.into();
  let b: u64 = b.into();
  ((a + b - 1) / b).try_into().unwrap()
}

#[cfg(test)]
mod tests {
  use crate::div_ceil;

  #[test]
  fn test_div_ceil() {
    assert_eq!(div_ceil(0, 1), 0);
    assert_eq!(div_ceil(0, 2), 0);
    assert_eq!(div_ceil(0, 3), 0);
    assert_eq!(div_ceil(1, 1), 1);
    assert_eq!(div_ceil(1, 2), 1);
    assert_eq!(div_ceil(1, 3), 1);
    assert_eq!(div_ceil(2, 2), 1);
    assert_eq!(div_ceil(2, 3), 1);
    assert_eq!(div_ceil(3, 4), 1);
    assert_eq!(div_ceil(4, 3), 2);
    assert_eq!(div_ceil(5, 3), 2);
    assert_eq!(div_ceil(5, 4), 2);
    assert_eq!(div_ceil(5, 5), 1);
    assert_eq!(div_ceil(5, 6), 1);
    assert_eq!(div_ceil(6, 5), 2);
  }
}

#[tokio::main]
async fn main() {
  let args = Cli::parse();
  let started = Instant::now();
  let mut tasks = Vec::with_capacity(args.concurrency.try_into().unwrap());
  let connection_error_counter = Arc::new(AtomicU64::new(0));
  let count_per_concurrency = div_ceil(args.count, args.concurrency);
  for c in 0..args.concurrency {
    let first = c * count_per_concurrency;
    let last = min(args.count, (c + 1) * count_per_concurrency);
    tasks.push(execute(
      &args.hostname,
      args.connect_timeout_secs,
      args.request_timeout_secs,
      first,
      last,
      args.batch_size,
      connection_error_counter.clone(),
    ));
  }
  join_all(tasks).await;
  let exec_dur = started.elapsed();
  println!(
    "Pushed {} messages with {} concurrency in {} seconds ({} network errors)",
    args.count,
    args.concurrency,
    exec_dur.as_secs_f64(),
    connection_error_counter.load(std::sync::atomic::Ordering::Relaxed),
  );
}
