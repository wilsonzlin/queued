use crate::journal::Journal;
use futures::stream::iter;
use futures::StreamExt;
use seekable_async_file::SeekableAsyncFile;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::sleep;

pub struct IdGenerator {
  journal: Arc<Journal>,
  device_offset: u64,
  next: AtomicU64,
  uncommitted_ids: Mutex<BTreeMap<u64, Option<SignalFutureController>>>,
}

impl IdGenerator {
  pub async fn load_from_disk(
    device: SeekableAsyncFile,
    journal: Arc<Journal>,
    device_offset: u64,
  ) -> Self {
    let next = device.read_u64_at(device_offset).await;
    Self {
      journal,
      device_offset,
      next: AtomicU64::new(next),
      uncommitted_ids: Mutex::new(BTreeMap::new()),
    }
  }

  pub async fn generate(&self, n: u64) -> u64 {
    let base = self.next.fetch_add(n, Ordering::Relaxed);
    self.uncommitted_ids.lock().await.insert(base + n, None);
    base
  }

  pub async fn commit(&self, base: u64, n: u64) {
    let (fut, fut_ctl) = SignalFuture::new();
    *self
      .uncommitted_ids
      .lock()
      .await
      .get_mut(&(base + n))
      .unwrap() = Some(fut_ctl);
    fut.await;
  }

  pub async fn start_background_commit_loop(&self) {
    loop {
      sleep(std::time::Duration::from_micros(200)).await;
      let id = self.next.load(Ordering::Relaxed);
      self
        .journal
        .write(self.device_offset, id.to_be_bytes().to_vec())
        .await;
      let mut uncommited_ids = self.uncommitted_ids.lock().await;
      loop {
        let Some(e) = uncommited_ids.first_entry() else {
          break;
        };
        // The stored disk state represents the next ID available, so if we use ID 8 we cannot resolve its commit if we're committing 8. However, the ID we store in `uncommitted_ids` is actually the one subsequent to the ID we use, so it's correct to break on `>` and not `>=`.
        if *e.key() > id || e.get().is_none() {
          break;
        };
        e.get().as_ref().unwrap().signal();
      }
    }
  }
}
