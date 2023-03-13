use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use std::cmp::min;

// Use this over `as usize` for safety without verbosity of `.try_into::<usize>().unwrap()`.
macro_rules! as_usize {
  ($v:expr) => {{
    let v: usize = $v.try_into().unwrap();
    v
  }};
}

pub(crate) use as_usize;

// This method has two conveniences:
// - Takes u64 instead of usize as we use u64 nearly everywhere else.
// - Takes a length instead of end, which is more common everywhere else.
pub(crate) fn u64_slice<T>(slice: &[T], offset: u64, len: u64) -> &[T] {
  let offset = as_usize!(offset);
  let len = as_usize!(len);
  &slice[offset..offset + len]
}

pub(crate) fn u64_slice_write<T: Copy>(slice: &mut [T], offset: u64, new: &[T]) {
  let offset = as_usize!(offset);
  let len = as_usize!(new.len());
  slice[offset..offset + len].copy_from_slice(new);
}

pub(crate) fn read_u16(slice: &[u8], offset: u64) -> u16 {
  u16::from_be_bytes(u64_slice(slice, offset, 2).try_into().unwrap())
}

pub(crate) fn read_u32(slice: &[u8], offset: u64) -> u32 {
  u32::from_be_bytes(u64_slice(slice, offset, 4).try_into().unwrap())
}

pub(crate) fn read_u64(slice: &[u8], offset: u64) -> u64 {
  u64::from_be_bytes(u64_slice(slice, offset, 8).try_into().unwrap())
}

pub(crate) fn read_ts(slice: &[u8], offset: u64) -> DateTime<Utc> {
  Utc
    .timestamp_millis_opt(
      i64::from_be_bytes(u64_slice(slice, offset, 8).try_into().unwrap()) * 1000,
    )
    .unwrap()
}

pub(crate) fn repeated_copy<T: Copy>(dest: &mut [T], src: &[T]) {
  assert_eq!(dest.len() % src.len(), 0);
  assert!(dest.len() > src.len());
  dest[..src.len()].copy_from_slice(src);

  let mut next = src.len();
  while next < dest.len() {
    let end = min(next * 2, dest.len());
    dest.copy_within(..end - next, next);
    next = end;
  }
}

#[cfg(test)]
mod tests {
  use super::repeated_copy;

  #[test]
  fn test_repeated_copy() {
    let mut dest = vec![0u8; 25];
    let src = [0, 1, 2, 3, 4];
    repeated_copy(&mut dest, &src);
    assert_eq!(dest, vec![
      0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4,
    ]);
  }
}
