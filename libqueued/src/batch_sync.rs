use off64::int::create_u64_le;
use rocksdb::DB;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::timeout_at;
use tokio::time::Instant;

pub(crate) struct BatchSync {
  sender: UnboundedSender<(u64, SignalFutureController<()>)>,
}

impl BatchSync {
  pub fn start(batch_sync_delay: Duration, db: Arc<DB>, mut persisted_next_id: u64) -> Self {
    let (sender, mut receiver) = unbounded_channel::<(u64, SignalFutureController<()>)>();
    spawn(async move {
      let mut signals = Vec::new();
      while let Some((nid, sig)) = receiver.recv().await {
        let mut next_id_requires_update = false;
        if nid > persisted_next_id {
          persisted_next_id = nid;
          next_id_requires_update = true;
        };
        signals.push(sig);
        // TODO Tune, allow configuration. The optimal value is the highest number while remaining as close to original (i.e. no `flush_wal()`) performance as possible.
        let deadline = Instant::now() + batch_sync_delay;
        while let Ok(Some((nid, sig))) = timeout_at(deadline, receiver.recv()).await {
          if nid > persisted_next_id {
            persisted_next_id = nid;
            next_id_requires_update = true;
          };
          signals.push(sig);
        }
        if next_id_requires_update {
          db.put("next_id", create_u64_le(persisted_next_id)).unwrap();
        };
        db.flush_wal(true).unwrap();
        for sig in signals.drain(..) {
          sig.signal(());
        }
      }
    });
    Self { sender }
  }

  pub async fn submit_and_wait(&self, new_next_id_or_zero: u64) {
    let (fut, fut_ctl) = SignalFuture::new();
    self.sender.send((new_next_id_or_zero, fut_ctl)).unwrap();
    fut.await;
  }
}
