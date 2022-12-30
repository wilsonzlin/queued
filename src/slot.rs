use std::collections::LinkedList;

pub struct Slot {
  pub offset: u64,
}

// For all lists (available, invisible, vacant), we require two in-memory lists. Many mutating operations (pushing, polling, deleting) involve moving across lists. We use one list that can be consumed/introspected from, but we don't immediately move to the new list, as the underlying data to be changed hasn't been written to the file yet.
// For example, when pushing, we immediately pop from `vacant.ready`, to prevent someone else from also taking the same slot. However, we cannot immediately move it into `available.ready`, as the slot data (metadata and contents) is still awaiting write and cannot be read yet (e.g. by a poller). We also don't want writes to go out of order (e.g. updating metadata of a slot before it's populated), causing corruption.
// An alternative is to store everything in memory and rely on atomic memory operations, but that's not memory efficient. Treating operations as if they're successful despite the data not having been safely written yet also seems like it will cause lots of complexity and/or subtle safety issues.
pub struct SlotList {
  pub ready: LinkedList<Slot>,
  pub pending: LinkedList<Slot>,
}

// The problem with using an individual lock per list is that it works fine for in-memory representations, but starts to get tricky and complex when also trying to keep journal-writes ordered and atomic, due to most mutating operations (push, poll, delete) interacting with more than one list at once. For example, when calling the push API, we pop from the vacant list and push to the available list. If two people are calling the push API at the same, it's possible for one to acquire the vacant list lock first and the other to acquire the available list lock first, and now don't have a consistent view of the new linked list offset values to write to disk. Holding the first lock (vacant list) for the entire operation only solves it for the push API, but does not solve interactions with other APIs. We can't just create multiple journal-write entries for whoever-is-first-wins, since all writes must be atomic across the entire push API operation. Serialising all API calls (e.g. placing in a MPSC queue) is another possibility, but this is mostly a less-efficient global lock.
// Having one lock across all lists is the most simple and less-prone-to-subtle-bugs option, and it's unlikely to cause much of a performance slowdown given we must perform I/O for each operation anyway (and I/O is much slower than acquiring even contentious locks).
pub struct SlotLists {
  pub available: SlotList,
  pub invisible: SlotList,
  pub vacant: SlotList,
}
