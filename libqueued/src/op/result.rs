pub enum OpError {
  InvalidPollTag,
  MessageNotFound,
  Suspended,
  Throttled,
}

pub type OpResult<T> = Result<T, OpError>;
