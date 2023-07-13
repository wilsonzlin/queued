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
  sender: UnboundedSender<SignalFutureController<()>>,
}

impl BatchSync {
  pub fn start(db: Arc<DB>) -> Self {
    let (sender, mut receiver) = unbounded_channel::<SignalFutureController<()>>();
    spawn(async move {
      let mut signals = Vec::new();
      while let Some(sig) = receiver.recv().await {
        signals.push(sig);
        // TODO Tune, allow configuration. The optimal value is the highest number while remaining as close to original (i.e. no `flush_wal()`) performance as possible.
        let deadline = Instant::now() + Duration::from_millis(10);
        while let Ok(Some(sig)) = timeout_at(deadline, receiver.recv()).await {
          signals.push(sig);
        }
        db.flush_wal(true).unwrap();
        for sig in signals.drain(..) {
          sig.signal(());
        }
      }
    });
    Self { sender }
  }

  pub async fn submit_and_wait(&self) {
    let (fut, fut_ctl) = SignalFuture::new();
    self.sender.send(fut_ctl).unwrap();
    fut.await;
  }
}
