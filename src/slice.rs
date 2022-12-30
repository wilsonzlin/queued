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

pub fn u64_len<T>(slice: &[T]) -> u64 {
  slice.len().try_into().unwrap()
}
