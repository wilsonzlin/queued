use bytesize::ByteSize;
use futures::stream::iter;
use futures::StreamExt;
use libqueued::op::poll::OpPollInput;
use libqueued::op::push::OpPushInput;
use libqueued::op::push::OpPushInputMessage;
use libqueued::Queued;
use libqueued::QueuedCfg;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u64;
use off64::usz;
use parking_lot::Mutex;
use roaring::RoaringTreemap;
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::create_dir;
use tokio::fs::remove_dir_all;
use tokio::time::Instant;
use tracing::info;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
  data_dir: PathBuf,

  /// Messages to create.
  messages: usize,

  /// Size of a created message.
  message_size: ByteSize,

  /// Concurrency level. Defaults to 64.
  concurrency: Option<usize>,

  /// Skips clearing the data directory.
  #[serde(default)]
  skip_data_dir_reset: bool,

  /// Skips pushing messages. Useful for benchmarking across invocations, where a previous invocation has already created all messages but didn't poll or delete them.
  #[serde(default)]
  skip_push: bool,

  /// Skips polling messages.
  #[serde(default)]
  skip_poll: bool,
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt::init();

  let cli: Config = serde_yaml::from_str(
    &fs::read_to_string(env::args().nth(1).expect("config file path argument"))
      .expect("read config file"),
  )
  .expect("parse config file");
  let concurrency = cli.concurrency.unwrap_or(64);
  let message_count = cli.messages;
  let message_size = usz!(cli.message_size.as_u64());

  if !cli.skip_data_dir_reset {
    remove_dir_all(&cli.data_dir).await.unwrap();
    create_dir(&cli.data_dir).await.unwrap();
    info!("cleared data dir");
  };
  let queued = Arc::new(
    Queued::load_and_start(&cli.data_dir, QueuedCfg {
      batch_sync_delay: Duration::from_millis(10),
    })
    .await,
  );
  info!("queued loaded");

  if !cli.skip_push {
    let now = Instant::now();
    iter(0..message_count)
      .for_each_concurrent(Some(concurrency), |i| {
        let queued = queued.clone();
        let mut contents = vec![0u8; message_size];
        contents.write_u64_le_at(0, u64!(i));
        async move {
          queued
            .push(OpPushInput {
              messages: vec![OpPushInputMessage {
                contents: contents.into(),
                visibility_timeout_secs: 0,
              }],
            })
            .await
            .unwrap();
        }
      })
      .await;
    let push_exec_secs = now.elapsed().as_secs_f64();
    info!(
      push_exec_secs,
      push_ops_per_second = (message_count as f64) / push_exec_secs,
      push_mib_per_second =
        (message_count * message_size) as f64 / push_exec_secs / 1024.0 / 1024.0,
      "completed all push ops",
    );
  };

  if !cli.skip_poll {
    let now = Instant::now();
    let unseen: Arc<Mutex<RoaringTreemap>> = Default::default();
    unseen.lock().insert_range(0..u64!(message_count));
    iter(0..message_count)
      .for_each_concurrent(Some(concurrency), |_| {
        let queued = queued.clone();
        let unseen = unseen.clone();
        async move {
          let msg = &queued
            .poll(OpPollInput {
              count: 1,
              visibility_timeout_secs: 3600,
              ignore_existing_visibility_timeouts: false,
            })
            .await
            .unwrap()
            .messages[0]
            .contents;
          assert_eq!(msg.len(), message_size);
          let id = msg.read_u64_le_at(0);
          assert!(unseen.lock().remove(id));
        }
      })
      .await;
    // Use `min` instead of `is_empty` so the panic will show the ID if it exists.
    assert_eq!(unseen.lock().min(), None);
    let poll_exec_secs = now.elapsed().as_secs_f64();
    info!(
      poll_exec_secs,
      poll_ops_per_second = (message_count as f64) / poll_exec_secs,
      "completed all poll ops",
    );
  };

  info!("all done");
}
