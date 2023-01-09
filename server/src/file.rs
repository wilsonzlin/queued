use crate::ctx::Metrics;
use crate::util::as_usize;
use std::future::Future;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use tokio::fs::OpenOptions;
use tokio::spawn;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tokio::time::Instant;

const DELAYED_SYNC_US: u64 = 100;

struct PendingSyncState {
  earliest_unsynced: Option<Instant>, // Only set when first pending_sync_fut_states is created; otherwise, metrics are misleading as we'd count time when no one is waiting for a sync as delayed sync time.
  latest_unsynced: Option<Instant>,
  pending_sync_fut_states: Vec<Arc<std::sync::Mutex<PendingSyncFutureState>>>,
}

// Tokio has still not implemented read_at and write_at: https://github.com/tokio-rs/tokio/issues/1529. We need these to be able to share a file descriptor across threads (e.g. use from within async function).
// Apparently spawn_blocking is how Tokio does all file operations (as not all platforms have native async I/O), so our use is not worse but not optimised for async I/O either.
#[derive(Clone)]
pub struct SeekableAsyncFile {
  #[cfg(feature = "io_tokio_file")]
  fd: Arc<std::fs::File>,
  #[cfg(feature = "io_mmap")]
  mmap: Arc<memmap2::MmapRaw>,
  #[cfg(feature = "io_mmap")]
  mmap_len: usize,
  metrics: Arc<Metrics>,
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
  pub async fn open(
    path: &Path,
    size: u64,
    metrics: Arc<Metrics>,
    io_direct: bool,
    io_dsync: bool,
  ) -> Self {
    let mut flags = 0;
    if io_direct {
      flags |= libc::O_DIRECT;
    };
    if io_dsync {
      flags |= libc::O_DSYNC;
    };

    let async_fd = OpenOptions::new()
      .read(true)
      .write(true)
      .custom_flags(flags)
      .open(path)
      .await
      .unwrap();

    let fd = async_fd.into_std().await;

    SeekableAsyncFile {
      #[cfg(feature = "io_tokio_file")]
      fd: Arc::new(fd),
      #[cfg(feature = "io_mmap")]
      mmap: Arc::new(memmap2::MmapRaw::map_raw(&fd).unwrap()),
      #[cfg(feature = "io_mmap")]
      mmap_len: as_usize!(size),
      metrics,
      pending_sync_state: Arc::new(Mutex::new(PendingSyncState {
        earliest_unsynced: None,
        latest_unsynced: None,
        pending_sync_fut_states: Vec::new(),
      })),
    }
  }

  // Since spawn_blocking requires 'static lifetime, we don't have a read_into_at function taht takes a &mut [u8] buffer, as it would be more like a Arc<Mutex<Vec<u8>>>, at which point the overhead is not really worth it for small reads.
  #[cfg(feature = "io_tokio_file")]
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

  #[cfg(feature = "io_mmap")]
  pub async fn read_at(&self, offset: u64, len: u64) -> Vec<u8> {
    let offset = as_usize!(offset);
    let len = as_usize!(len);
    let memory = unsafe { std::slice::from_raw_parts(self.mmap.as_ptr(), self.mmap_len) };
    memory[offset..offset + len].to_vec()
  }

  pub async fn read_u16_at(&self, offset: u64) -> u16 {
    let bytes = self.read_at(offset, 2).await;
    u16::from_be_bytes(bytes.try_into().unwrap())
  }

  pub async fn read_u64_at(&self, offset: u64) -> u64 {
    let bytes = self.read_at(offset, 8).await;
    u64::from_be_bytes(bytes.try_into().unwrap())
  }

  #[cfg(feature = "io_tokio_file")]
  pub async fn write_at(&self, offset: u64, data: Vec<u8>) {
    let fd = self.fd.clone();
    let len: u64 = data.len().try_into().unwrap();
    let started = Instant::now();
    spawn_blocking(move || fd.write_all_at(&data, offset).unwrap())
      .await
      .unwrap();
    // Yes, we're including the overhead of Tokio's spawn_blocking.
    let call_us: u64 = started.elapsed().as_micros().try_into().unwrap();
    self
      .metrics
      .io_write_bytes_counter
      .fetch_add(len, Ordering::Relaxed);
    self
      .metrics
      .io_write_counter
      .fetch_add(1, Ordering::Relaxed);
    self
      .metrics
      .io_write_us_counter
      .fetch_add(call_us, Ordering::Relaxed);
  }

  #[cfg(feature = "io_mmap")]
  pub async fn write_at(&self, offset: u64, data: Vec<u8>) {
    let offset = as_usize!(offset);
    let len = data.len();

    let memory = unsafe { std::slice::from_raw_parts_mut(self.mmap.as_mut_ptr(), self.mmap_len) };
    memory[offset..offset + len].copy_from_slice(&data);
  }

  #[cfg(feature = "fsync_delayed")]
  pub async fn write_at_with_delayed_sync(&self, offset: u64, data: Vec<u8>) {
    self.write_at(offset, data).await;

    let fut_state = Arc::new(std::sync::Mutex::new(PendingSyncFutureState {
      completed: false,
      waker: None,
    }));

    {
      let mut state = self.pending_sync_state.lock().await;
      let now = Instant::now();
      state.earliest_unsynced.get_or_insert(now);
      state.latest_unsynced = Some(now);
      state.pending_sync_fut_states.push(fut_state.clone());
    };

    self
      .metrics
      .io_sync_delayed_counter
      .fetch_add(1, Ordering::Relaxed);

    PendingSyncFuture {
      shared_state: fut_state,
    }
    .await;
  }

  #[cfg(feature = "fsync_immediate")]
  pub async fn write_at_with_delayed_sync(&self, offset: u64, data: Vec<u8>) {
    self.write_at(offset, data).await;
    self.sync_data().await;
  }

  #[cfg(feature = "unsafe_fsync_none")]
  pub async fn write_at_with_delayed_sync(&self, offset: u64, data: Vec<u8>) {
    self.write_at(offset, data).await;
  }

  #[cfg(feature = "fsync_delayed")]
  pub async fn start_delayed_data_sync_background_loop(&self) {
    let mut futures_to_wake = Vec::new();
    loop {
      sleep(std::time::Duration::from_micros(DELAYED_SYNC_US)).await;

      struct SyncNow {
        longest_delay_us: u64,
        shortest_delay_us: u64,
      }

      let sync_now = {
        let mut state = self.pending_sync_state.lock().await;

        if !state.pending_sync_fut_states.is_empty() {
          let longest_delay_us: u64 = state
            .earliest_unsynced
            .unwrap()
            .elapsed()
            .as_micros()
            .try_into()
            .unwrap();

          let shortest_delay_us: u64 = state
            .latest_unsynced
            .unwrap()
            .elapsed()
            .as_micros()
            .try_into()
            .unwrap();

          state.earliest_unsynced = None;
          state.latest_unsynced = None;

          futures_to_wake.extend(state.pending_sync_fut_states.drain(..));

          Some(SyncNow {
            longest_delay_us,
            shortest_delay_us,
          })
        } else {
          None
        }
      };

      if let Some(SyncNow {
        longest_delay_us,
        shortest_delay_us,
      }) = sync_now
      {
        // OPTIMISATION: Don't perform these atomic operations while unnecessarily holding up the lock.
        self
          .metrics
          .io_sync_longest_delay_us_counter
          .fetch_add(longest_delay_us, Ordering::Relaxed);
        self
          .metrics
          .io_sync_shortest_delay_us_counter
          .fetch_add(shortest_delay_us, Ordering::Relaxed);

        assert!(!futures_to_wake.is_empty());
        let file = self.clone();
        spawn(async move { file.sync_data().await }).await.unwrap();

        for ft in futures_to_wake.drain(..) {
          let mut ft = ft.lock().unwrap();
          ft.completed = true;
          if let Some(waker) = ft.waker.take() {
            waker.wake();
          };
        }
      };

      self
        .metrics
        .io_sync_background_loops_counter
        .fetch_add(1, Ordering::Relaxed);
    }
  }

  pub async fn sync_data(&self) {
    #[cfg(feature = "io_tokio_file")]
    let fd = self.fd.clone();
    #[cfg(feature = "io_mmap")]
    let mmap = self.mmap.clone();

    let started = Instant::now();
    spawn_blocking(move || {
      #[cfg(feature = "io_tokio_file")]
      fd.sync_data().unwrap();

      #[cfg(feature = "io_mmap")]
      mmap.flush().unwrap();
    })
    .await
    .unwrap();
    // Yes, we're including the overhead of Tokio's spawn_blocking.
    let sync_us: u64 = started.elapsed().as_micros().try_into().unwrap();
    self.metrics.io_sync_counter.fetch_add(1, Ordering::Relaxed);
    self
      .metrics
      .io_sync_us_counter
      .fetch_add(sync_us, Ordering::Relaxed);
  }
}
