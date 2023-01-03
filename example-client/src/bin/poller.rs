use clap::command;
use clap::Parser;
use futures::future::join_all;
use roaring::RoaringBitmap;
use serde::Deserialize;
use serde_json::json;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::time::Instant;

#[derive(Deserialize)]
struct PollOutputMessage {
  contents: String,
  index: u32,
  poll_tag: String,
}

#[derive(Deserialize)]
struct PollOutput {
  message: Option<PollOutputMessage>,
}

async fn execute(
  hostname: &str,
  connect_timeout: u64,
  request_timeout: u64,
  conn_err_cnt: Arc<AtomicU64>,
) -> RoaringBitmap {
  let mut bitmap = RoaringBitmap::new();
  let client = reqwest::Client::builder()
    .connect_timeout(std::time::Duration::from_secs(connect_timeout))
    .timeout(std::time::Duration::from_secs(request_timeout))
    .build()
    .unwrap();
  let url = format!("http://{}:3333/poll", hostname);
  loop {
    let Ok(req) = client
      .post(&url)
      .json(&json!({
        "visibility_timeout_secs": 60,
      }))
      .send()
      .await else {
        conn_err_cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        continue;
      };
    let res = req
      .error_for_status()
      .unwrap()
      .json::<PollOutput>()
      .await
      .unwrap();
    if let Some(msg) = res.message {
      let Ok(id) = msg.contents.parse::<u32>() else {
        panic!("failed to parse: {}", msg.contents);
      };
      bitmap.insert(id);
      client
        .post("http://127.0.0.1:3333/delete")
        .json(&json!({
          "index": msg.index,
          "poll_tag": msg.poll_tag,
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    } else {
      break;
    };
  }
  bitmap
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
}

struct RoaringBitmapRevOrdByMin(RoaringBitmap);

impl PartialEq for RoaringBitmapRevOrdByMin {
  fn eq(&self, other: &Self) -> bool {
    self.0 == other.0
  }
}

impl Eq for RoaringBitmapRevOrdByMin {}

impl PartialOrd for RoaringBitmapRevOrdByMin {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.0.min().unwrap().cmp(&other.0.min().unwrap()).reverse())
  }
}

impl Ord for RoaringBitmapRevOrdByMin {
  fn cmp(&self, other: &Self) -> Ordering {
    self.partial_cmp(other).unwrap()
  }
}

#[tokio::main]
async fn main() {
  let args = Cli::parse();
  let started = Instant::now();
  let mut tasks = Vec::with_capacity(args.concurrency.try_into().unwrap());
  let connection_error_counter = Arc::new(AtomicU64::new(0));
  for _ in 0..args.concurrency {
    tasks.push(execute(
      &args.hostname,
      args.connect_timeout_secs,
      args.request_timeout_secs,
      connection_error_counter.clone(),
    ));
  }
  let polled_ids = join_all(tasks).await;
  let exec_dur = started.elapsed();

  let mut bitmaps = BinaryHeap::new();
  for bitmap in polled_ids {
    if !bitmap.is_empty() {
      bitmaps.push(RoaringBitmapRevOrdByMin(bitmap));
    };
  }
  let mut expected_next_id = 0;
  while let Some(mut bitmap) = bitmaps.pop() {
    let id = bitmap.0.min().unwrap();
    assert_eq!(id, expected_next_id);
    expected_next_id += 1;
    bitmap.0.remove(id);
    if !bitmap.0.is_empty() {
      bitmaps.push(bitmap);
    };
  }
  assert_eq!(args.count, expected_next_id);

  println!(
    "Polled and deleted {} messages with {} concurrency in {} seconds ({} network errors)",
    args.count,
    args.concurrency,
    exec_dur.as_secs_f64(),
    connection_error_counter.load(std::sync::atomic::Ordering::Relaxed),
  );
}
