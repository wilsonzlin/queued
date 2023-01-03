use chrono::Utc;
use std::cmp::min;
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;

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
pub fn u64_slice<T>(slice: &[T], offset: u64, len: u64) -> &[T] {
  let offset = as_usize!(offset);
  let len = as_usize!(len);
  &slice[offset..offset + len]
}

pub fn u64_slice_write<T: Copy>(slice: &mut [T], offset: u64, new: &[T]) {
  let offset = as_usize!(offset);
  let len = as_usize!(new.len());
  slice[offset..offset + len].copy_from_slice(new);
}

pub fn u64_len<T>(slice: &[T]) -> u64 {
  slice.len().try_into().unwrap()
}

pub fn now() -> i64 {
  Utc::now().timestamp()
}

pub async fn get_device_size(path: &Path) -> u64 {
  let mut file = File::open(path).await.unwrap();
  // Note that `file.metadata().len()` is 0 for device files.
  file.seek(SeekFrom::End(0)).await.unwrap()
}

pub fn repeated_copy<T: Copy>(dest: &mut [T], src: &[T]) {
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
