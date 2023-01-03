use itertools::Itertools;
use std::future::Future;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Instant;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
use tokio::time::sleep;

const DELAYED_SYNC_BYTES_THRESHOLD: usize = 4 * 1024 * 1024;
const DELAYED_SYNC_US: u64 = 100;

struct PendingSyncState {
  unsynced_bytes: usize,
  unsynced_since: Instant,
  pending_sync_fut_states: Vec<Arc<std::sync::Mutex<PendingSyncFutureState>>>,
}

// Tokio has still not implemented read_at and write_at: https://github.com/tokio-rs/tokio/issues/1529. We need these to be able to share a file descriptor across threads (e.g. use from within async function).
// Apparently spawn_blocking is how Tokio does all file operations (as not all platforms have native async I/O), so our use is not worse but not optimised for async I/O either.
#[derive(Clone)]
pub struct SeekableAsyncFile {
  fd: Arc<std::fs::File>,
  pending_sync_state: Arc<Mutex<PendingSyncState>>,
}

struct PendingSyncFutureState {
  completed: bool,
  waker: Option<Waker>,
}

struct PendingSyncFuture {
  shared_state: Arc<std::sync::Mutex<PendingSyncFutureState>>,
}

impl Future for PendingSyncFuture {
  type Output = ();

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let mut shared_state = self.shared_state.lock().unwrap();
    if shared_state.completed {
      Poll::Ready(())
    } else {
      shared_state.waker = Some(cx.waker().clone());
      Poll::Pending
    }
  }
}

impl SeekableAsyncFile {
  fn from_fd(fd: std::fs::File) -> Self {
    SeekableAsyncFile {
      fd: Arc::new(fd),
      pending_sync_state: Arc::new(Mutex::new(PendingSyncState {
        unsynced_bytes: 0,
        unsynced_since: Instant::now(),
        pending_sync_fut_states: Vec::new(),
      })),
    }
  }

  pub async fn open(path: &Path) -> Self {
    let async_fd = OpenOptions::new()
      .read(true)
      .write(true)
      .open(path)
      .await
      .unwrap();
    let fd = async_fd.into_std().await;
    SeekableAsyncFile::from_fd(fd)
  }

  pub async fn create(path: &Path) -> Self {
    let async_fd = File::create(path).await.unwrap();
    let fd = async_fd.into_std().await;
    SeekableAsyncFile::from_fd(fd)
  }

  // Since spawn_blocking requires 'static lifetime, we don't have a read_into_at function taht takes a &mut [u8] buffer, as it would be more like a Arc<Mutex<Vec<u8>>>, at which point the overhead is not really worth it for small reads.
  pub async fn read_at(&self, offset: u64, len: u64) -> Vec<u8> {
    let fd = self.fd.clone();
    spawn_blocking(move || {
      let mut buf = vec![0u8; len.try_into().unwrap()];
      fd.read_exact_at(&mut buf, offset).unwrap();
      buf
    })
    .await
    .unwrap()
  }

  pub async fn read_u16_at(&self, offset: u64) -> u16 {
    let bytes = self.read_at(offset, 2).await;
    u16::from_be_bytes(bytes.try_into().unwrap())
  }

  pub async fn read_u64_at(&self, offset: u64) -> u64 {
    let bytes = self.read_at(offset, 8).await;
    u64::from_be_bytes(bytes.try_into().unwrap())
  }

  pub async fn write_at(&self, offset: u64, data: Vec<u8>) {
    let fd = self.fd.clone();
    spawn_blocking(move || fd.write_all_at(&data, offset).unwrap())
      .await
      .unwrap();
  }

  #[cfg(feature = "fsync_delayed")]
  async fn maybe_perform_delayed_data_sync_now(
    &self,
    increment_unsynced_bytes: usize,
    keep_waiting: bool,
  ) {
    let (fut_states, created_fut) = {
      let mut state = self.pending_sync_state.lock().await;
      state.unsynced_bytes += increment_unsynced_bytes;

      let has_pending_futs = !state.pending_sync_fut_states.is_empty();
      let met_bytes_threshold = state.unsynced_bytes >= DELAYED_SYNC_BYTES_THRESHOLD;
      let met_deadline = state.unsynced_since.elapsed().as_micros() >= u128::from(DELAYED_SYNC_US);

      if has_pending_futs && (met_bytes_threshold || met_deadline) {
        state.unsynced_bytes = 0;
        state.unsynced_since = Instant::now();
        (
          Some(state.pending_sync_fut_states.drain(..).collect_vec()),
          None,
        )
      } else if keep_waiting {
        let fut_state = Arc::new(std::sync::Mutex::new(PendingSyncFutureState {
          completed: false,
          waker: None,
        }));
        state.pending_sync_fut_states.push(fut_state.clone());
        (
          None,
          Some(PendingSyncFuture {
            shared_state: fut_state,
          }),
        )
      } else {
        (None, None)
      }
    };
    if let Some(fut_states) = fut_states {
      assert!(!fut_states.is_empty());
      self.sync_data().await;
      for ft in fut_states {
        let mut ft = ft.lock().unwrap();
        ft.completed = true;
        if let Some(waker) = ft.waker.take() {
          waker.wake();
        };
      }
    };
    if let Some(fut) = created_fut {
      fut.await;
    };
  }

  #[cfg(feature = "fsync_delayed")]
  pub async fn write_at_with_delayed_sync(&self, offset: u64, data: Vec<u8>) {
    let len = data.len();
    self.write_at(offset, data).await;
    self.maybe_perform_delayed_data_sync_now(len, true).await;
  }

  #[cfg(feature = "fsync_immediate")]
  pub async fn sync_data_delayed(&self) {
    self.sync_data().await;
  }

  #[cfg(feature = "unsafe_fsync_none")]
  pub async fn sync_data_delayed(&self) {}

  #[cfg(feature = "fsync_delayed")]
  pub async fn start_delayed_data_sync_background_loop(&self) {
    loop {
      sleep(std::time::Duration::from_micros(DELAYED_SYNC_US)).await;
      self.maybe_perform_delayed_data_sync_now(0, false).await;
    }
  }

  pub async fn sync_data(&self) {
    let fd = self.fd.clone();
    spawn_blocking(move || fd.sync_data().unwrap())
      .await
      .unwrap();
  }

  pub async fn sync_all(&self) {
    let fd = self.fd.clone();
    spawn_blocking(move || fd.sync_all().unwrap())
      .await
      .unwrap();
  }

  pub async fn truncate(&self, len: u64) {
    let fd = self.fd.clone();
    spawn_blocking(move || fd.set_len(len).unwrap())
      .await
      .unwrap();
  }
}
