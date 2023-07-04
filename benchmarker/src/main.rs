use bytesize::ByteSize;
use futures::stream::iter;
use futures::StreamExt;
use libqueued::op::poll::OpPollInput;
use libqueued::op::push::OpPushInput;
use libqueued::op::push::OpPushInputMessage;
use libqueued::QueuedLayoutType;
use libqueued::QueuedLoader;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u64;
use off64::usz;
use parking_lot::Mutex;
use roaring::RoaringTreemap;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::SeekableAsyncFileMetrics;
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::time::Instant;
use tracing::info;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
  device_path: PathBuf,

  device_size: u64,

  #[serde(default)]
  fixed_slots_layout: bool,

  /// Messages to create.
  messages: usize,

  /// Size of a created message.
  message_size: ByteSize,

  /// Concurrency level. Defaults to 64.
  concurrency: Option<usize>,

  /// Skips formatting the device.
  #[serde(default)]
  skip_device_format: bool,

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

  let device = SeekableAsyncFile::open(
    &cli.device_path,
    cli.device_size,
    Arc::new(SeekableAsyncFileMetrics::default()),
    Duration::from_micros(10),
    0,
  )
  .await;

  let queued = QueuedLoader::new(
    device.clone(),
    cli.device_size,
    if cli.fixed_slots_layout {
      QueuedLayoutType::FixedSlots
    } else {
      QueuedLayoutType::LogStructured
    },
    Duration::from_micros(10),
  );

  if !cli.skip_device_format {
    queued.format().await;
    info!("queued formatted");
  };
  let queued = queued.load().await;
  info!("queued loaded");

  spawn({
    let device = device.clone();
    let queued = queued.clone();
    async move {
      tokio::join! {
        queued.start(),
        device.start_delayed_data_sync_background_loop(),
      };
    }
  });

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
          let msg = queued
            .poll(OpPollInput {
              visibility_timeout_secs: 3600,
            })
            .await
            .unwrap()
            .message
            .unwrap()
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
