use crate::file::SeekableAsyncFile;
use crate::slot::SlotLists;
use crate::JournalPending;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Ctx {
  pub data_fd: SeekableAsyncFile,
  pub journal_pending: Arc<JournalPending>,
  pub lists: RwLock<SlotLists>,
}
