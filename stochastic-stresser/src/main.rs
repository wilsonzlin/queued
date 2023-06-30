use bytesize::ByteSize;
use dashmap::DashMap;
use itertools::Itertools;
use libqueued::op::delete::OpDeleteInput;
use libqueued::op::poll::OpPollInput;
use libqueued::op::push::OpPushInput;
use libqueued::op::push::OpPushInputMessage;
use libqueued::op::update::OpUpdateInput;
use libqueued::QueuedLayoutType;
use libqueued::QueuedLoader;
use off64::usz;
use rand::thread_rng;
use rand::Rng;
use rand::RngCore;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::SeekableAsyncFileMetrics;
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use stochastic_queue::mpmc_sync::stochastic_channel;
use stochastic_queue::mpmc_sync::StochasticMpmcRecvError;
use tinybuf::TinyBuf;
use tokio::spawn;
use tokio::time::sleep;
use tracing::info;

#[derive(Deserialize)]
struct Config {
  device_path: PathBuf,

  device_size: u64,

  #[serde(default)]
  fixed_slots_layout: bool,

  /// Messages to create.
  messages: u64,

  /// Maximum size of a created message.
  maximum_message_size: ByteSize,

  /// Concurrency level. Defaults to 64.
  concurrency: Option<u64>,

  /// Size of random bytes pool. Defaults to 1 GiB.
  pool_size: Option<ByteSize>,
}

#[derive(Clone)]
struct Pool {
  data: Arc<Vec<u8>>,
}

enum Task {
  Push { data_len: u64, data_offset: u64 },
  Poll {},
  Update { id: u64, poll_tag: TinyBuf },
  Delete { id: u64, poll_tag: TinyBuf },
}

#[derive(Default)]
struct TaskProgress {
  push: AtomicU64,
  poll: AtomicU64,
  update: AtomicU64,
  delete: AtomicU64,
}

impl Pool {
  fn new(size: u64) -> Self {
    let mut data = vec![0u8; usize::try_from(size).unwrap()];
    thread_rng().fill_bytes(&mut data);
    Self {
      data: Arc::new(data),
    }
  }

  fn get(&self, (offset, len): (u64, u64)) -> &[u8] {
    let start = usz!(offset);
    let end = usz!(offset + len);
    &self.data[start..end]
  }
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt::init();

  let cli: Config = serde_yaml::from_str(
    &fs::read_to_string(env::args().nth(1).expect("config file path argument"))
      .expect("read config file"),
  )
  .expect("parse config file");

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

  queued.format().await;
  info!("queued formatted");
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

  let pool_size = cli
    .pool_size
    .map(|s| s.as_u64())
    .unwrap_or(1024 * 1024 * 1024 * 1);
  let maximum_message_size = cli.maximum_message_size.as_u64();
  let concurrency = cli.concurrency.unwrap_or(64);
  let pool = Pool::new(pool_size);

  let (tasks_sender, tasks_receiver) = stochastic_channel::<Task>();
  for _ in 0..cli.messages {
    let data_len = thread_rng().gen_range(0..maximum_message_size);
    let data_offset = thread_rng().gen_range(0..pool_size - data_len);
    tasks_sender
      .send(Task::Push {
        data_len,
        data_offset,
      })
      .unwrap();
  }

  let progress = Arc::new(TaskProgress::default());
  let complete = Arc::new(AtomicBool::new(false));
  // Background loop to regularly print out progress.
  spawn({
    let complete = complete.clone();
    let task_progress = progress.clone();
    async move {
      while !complete.load(Ordering::Relaxed) {
        sleep(Duration::from_secs(3)).await;
        info!(
          push = task_progress.push.load(Ordering::Relaxed),
          update = task_progress.update.load(Ordering::Relaxed),
          poll = task_progress.poll.load(Ordering::Relaxed),
          delete = task_progress.delete.load(Ordering::Relaxed),
          "progress"
        );
      }
    }
  });

  let msgs = Arc::new(DashMap::<u64, (u64, u64)>::new());
  let workers = (0..concurrency)
    .map(|_| {
      let complete = complete.clone();
      let msgs = msgs.clone();
      let pool = pool.clone();
      let progress = progress.clone();
      let queued = queued.clone();
      let tasks_receiver = tasks_receiver.clone();
      let tasks_sender = tasks_sender.clone();
      spawn(async move {
        while !complete.load(Ordering::Relaxed) {
          // We must use a timeout and regularly check the completion count, as we hold a sender so the channel won't naturally end.
          // WARNING: We cannot use `recv_timeout` as it's blocking.
          let t = match tasks_receiver.try_recv() {
            Ok(Some(t)) => t,
            Err(StochasticMpmcRecvError::NoSenders) => break,
            Ok(None) => {
              // Keep this timeout small so that total execution time is accurate.
              sleep(Duration::from_millis(100)).await;
              continue;
            }
          };
          match t {
            Task::Push {
              data_len,
              data_offset,
            } => {
              let contents = pool.get((data_offset, data_len));
              let res = queued
                .push(OpPushInput {
                  messages: vec![OpPushInputMessage {
                    contents: TinyBuf::from_slice(contents),
                    visibility_timeout_secs: 0,
                  }],
                })
                .await
                .unwrap();
              assert!(msgs
                .insert(res.results[0].unwrap(), (data_offset, data_len))
                .is_none());
              tasks_sender.send(Task::Poll {}).unwrap();
              progress.push.fetch_add(1, Ordering::Relaxed);
            }
            Task::Poll {} => {
              let visibility_timeout_secs = thread_rng().gen_range(1800..3600);
              let res = queued
                .poll(OpPollInput {
                  visibility_timeout_secs,
                })
                .await
                .unwrap();
              let msg = res.message.unwrap();
              assert_eq!(
                pool.get(msgs.remove(&msg.id).unwrap().1),
                msg.contents.as_slice(),
              );
              tasks_sender
                .send(Task::Update {
                  id: msg.id,
                  poll_tag: msg.poll_tag,
                })
                .unwrap();
              progress.poll.fetch_add(1, Ordering::Relaxed);
            }
            Task::Update { id, poll_tag } => {
              let visibility_timeout_secs = thread_rng().gen_range(1800..3600);
              queued
                .update(OpUpdateInput {
                  id,
                  poll_tag: poll_tag.clone(),
                  visibility_timeout_secs,
                })
                .await
                .unwrap();
              tasks_sender.send(Task::Delete { id, poll_tag }).unwrap();
              progress.update.fetch_add(1, Ordering::Relaxed);
            }
            Task::Delete { id, poll_tag } => {
              queued.delete(OpDeleteInput { id, poll_tag }).await.unwrap();
              if progress.delete.fetch_add(1, Ordering::Relaxed) + 1 == cli.messages {
                complete.store(true, Ordering::Relaxed);
              };
            }
          }
        }
      })
    })
    .collect_vec();
  drop(tasks_sender);
  drop(tasks_receiver);
  for t in workers {
    t.await.unwrap();
  }

  info!("all done");
}
