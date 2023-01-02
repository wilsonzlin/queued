use crate::util::as_usize;
use lazy_static::lazy_static;
use num_enum::TryFromPrimitive;

#[derive(Debug, Eq, PartialEq, TryFromPrimitive)]
#[repr(u8)]
pub enum SlotState {
  Vacant = 0,
  Available = 1,
}

pub const SLOT_LEN: u64 = 1024;
pub const SLOT_OFFSETOF_HASH: u64 = 0;
pub const SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS: u64 = SLOT_OFFSETOF_HASH + 32;
pub const SLOT_OFFSETOF_STATE: u64 = SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS + 1;
pub const SLOT_OFFSETOF_POLL_TAG: u64 = SLOT_OFFSETOF_STATE + 1;
pub const SLOT_OFFSETOF_CREATED_TS: u64 = SLOT_OFFSETOF_POLL_TAG + 30;
pub const SLOT_OFFSETOF_VISIBLE_TS: u64 = SLOT_OFFSETOF_CREATED_TS + 8;
pub const SLOT_OFFSETOF_POLL_COUNT: u64 = SLOT_OFFSETOF_VISIBLE_TS + 8;
pub const SLOT_OFFSETOF_LEN: u64 = SLOT_OFFSETOF_POLL_COUNT + 4;
pub const SLOT_OFFSETOF_CONTENTS: u64 = SLOT_OFFSETOF_LEN + 2;
pub const SLOT_FIXED_FIELDS_LEN: u64 = SLOT_OFFSETOF_CONTENTS;
pub const MESSAGE_SLOT_CONTENT_LEN_MAX: u64 = SLOT_LEN - SLOT_FIXED_FIELDS_LEN;

lazy_static! {
  pub static ref SLOT_VACANT_TEMPLATE: Vec<u8> = {
    let mut slot_template = vec![0u8; as_usize!(SLOT_FIXED_FIELDS_LEN)];
    let hash = blake3::hash(&slot_template[32..]);
    slot_template[..32].copy_from_slice(hash.as_bytes());
    slot_template
  };
}
