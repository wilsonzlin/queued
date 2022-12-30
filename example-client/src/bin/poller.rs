use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::Deref;
use std::ops::DerefMut;

use chrono::DateTime;
use chrono::Utc;
use clap::Parser;
use clap::command;
use futures::future::join_all;
use roaring::RoaringBitmap;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use serde_json::Value;
use tokio::time::Instant;

#[derive(Deserialize)]
struct PollOutputMessage {
  offset: u64,
  created: DateTime<Utc>,
  poll_count: u32,
  contents: String,
}

#[derive(Deserialize)]
struct PollOutput {
  message: Option<PollOutputMessage>,
}

async fn execute() -> RoaringBitmap {
  let mut bitmap = RoaringBitmap::new();
  let client = reqwest::Client::new();
  loop {
    let res = client
      .post("http://127.0.0.1:3333/poll")
      .json(&json!({}))
      .send()
      .await
      .unwrap()
      .error_for_status()
      .unwrap()
      .json::<PollOutput>()
      .await
      .unwrap();
    if let Some(msg) = res.message {
      let id: u32 = msg.contents.parse().unwrap();
      bitmap.insert(id);
    } else {
      break;
    };
  };
  bitmap
}

#[derive(Parser, Debug)]
#[command(author, about)]
struct Cli {
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
  for _ in 0..args.concurrency {
    tasks.push(execute());
  }
  let polled_ids = join_all(tasks).await;
  let exec_dur = started.elapsed();

  let mut bitmaps = BinaryHeap::new();
  for bitmap in polled_ids {
    if !bitmap.is_empty() {
      bitmaps.push(RoaringBitmapRevOrdByMin(bitmap));
    };
  };
  let mut expected_next_id = 0;
  while let Some(mut bitmap) = bitmaps.pop() {
    let id = bitmap.0.min().unwrap();
    assert_eq!(id, expected_next_id);
    expected_next_id += 1;
    bitmap.0.remove(id);
    if !bitmap.0.is_empty() {
      bitmaps.push(bitmap);
    };
  };
  assert_eq!(args.count, expected_next_id);

  println!(
    "Polled {} messages with {} concurrency in {} seconds",
    args.count,
    args.concurrency,
    exec_dur.as_secs_f64()
  );
}
