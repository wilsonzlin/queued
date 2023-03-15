use std::cmp::min;

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
