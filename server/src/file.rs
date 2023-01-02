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

// Tokio has still not implemented read_at and write_at: https://github.com/tokio-rs/tokio/issues/1529. We need these to be able to share a file descriptor across threads (e.g. use from within async function).
// Apparently spawn_blocking is how Tokio does all file operations (as not all platforms have native async I/O), so our use is not worse but not optimised for async I/O either.
#[derive(Clone)]
pub struct SeekableAsyncFile {
  fd: Arc<std::fs::File>,
  unsynced: Arc<Mutex<(usize, Instant)>>,
  pending_sync_fut_states: Arc<Mutex<Vec<Arc<std::sync::Mutex<PendingSyncFutureState>>>>>,
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
      unsynced: Arc::new(Mutex::new((0, Instant::now()))),
      pending_sync_fut_states: Arc::new(Mutex::new(Vec::new())),
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

  pub async fn size(&self) -> u64 {
    let fd = self.fd.clone();
    spawn_blocking(move || fd.metadata().unwrap().len())
      .await
      .unwrap()
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
    let len = data.len();
    spawn_blocking(move || fd.write_all_at(&data, offset).unwrap())
      .await
      .unwrap();
    {
      let mut state = self.unsynced.lock().await;
      state.0 += len;
    };
  }

  #[cfg(feature = "fsync_delayed")]
  async fn perform_delayed_data_sync_now(&self) {
    let fut_states = {
      let mut state = self.pending_sync_fut_states.lock().await;
      state.drain(..).collect_vec()
    };
    // Handle possible race condition, where multiple calls to this function happened simultaneously.
    if fut_states.is_empty() {
      return;
    };
    self.sync_data().await;
    for ft in fut_states {
      let mut ft = ft.lock().unwrap();
      ft.completed = true;
      if let Some(waker) = ft.waker.take() {
        waker.wake();
      };
    }
  }

  #[cfg(feature = "fsync_delayed")]
  pub async fn sync_data_delayed(&self) {
    let should_sync = {
      let mut state = self.unsynced.lock().await;
      if state.0 >= DELAYED_SYNC_BYTES_THRESHOLD {
        state.0 = 0;
        state.1 = Instant::now();
        true
      } else {
        false
      }
    };
    if should_sync {
      self.perform_delayed_data_sync_now().await;
    } else {
      let fut_state = Arc::new(std::sync::Mutex::new(PendingSyncFutureState {
        completed: false,
        waker: None,
      }));
      {
        let mut fut_states = self.pending_sync_fut_states.lock().await;
        fut_states.push(fut_state.clone());
      };
      PendingSyncFuture {
        shared_state: fut_state.clone(),
      }
      .await
    };
  }

  #[cfg(feature = "fsync_immediate")]
  pub async fn sync_data_delayed(&self) {
    self.sync_data().await;
  }

  #[cfg(feature = "unsafe_fsync_none")]
  pub async fn sync_data_delayed(&self) {}

  #[cfg(feature = "fsync_delayed")]
  pub async fn start_delayed_data_sync_background_loop(&self) {
    const INTERVAL_US: u64 = 100;
    loop {
      sleep(std::time::Duration::from_micros(INTERVAL_US)).await;
      let should_sync = {
        let mut state = self.unsynced.lock().await;
        if state.0 > 0 && state.1.elapsed().as_micros() >= u128::from(INTERVAL_US) {
          state.0 = 0;
          state.1 = Instant::now();
          true
        } else {
          false
        }
      };
      if should_sync {
        self.perform_delayed_data_sync_now().await;
      };
    }
  }

  #[cfg(not(feature = "fsync_delayed"))]
  pub async fn start_delayed_data_sync_background_loop(&self) {
    loop {
      sleep(std::time::Duration::from_secs(2)).await;
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
