use crate::import::{Arc, AtomicUsize, Ordering};

#[derive(Debug)]
struct Mpsc<T> {
    mem: Box<[Option<T>]>,
    // The mask is written when this structure is created and is then only read.
    // Therefore, we do not need Atomic here.
    mask: usize,
    allocated: AtomicUsize,
    completed: AtomicUsize,
}

impl<T> Mpsc<T> {
    fn new(capacity: usize) -> Self {
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(None);
        }
        let buffer: Box<[Option<T>]> = buffer.into_boxed_slice();
        Mpsc {
            mem: buffer,
            mask: capacity - 1,
            allocated: 0.into(),
            completed: 0.into(),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.mask + 1
    }
    #[inline]
    fn next_allocation(&self) -> usize {
        self.allocated.fetch_add(1, Ordering::Acquire)
    }
    #[inline]
    fn next_completion(&self) -> usize {
        self.completed.fetch_add(1, Ordering::Acquire)
    }
}

struct Occupied<T> {
    mem: [T; usize::BITS as usize],
    occupied: AtomicUsize,
}

#[derive(Debug)]
pub struct Receiver<T> {
    spsc: Arc<Mpsc<T>>,
    read: usize,
}
unsafe impl<T: Send> Send for Receiver<T> {}
unsafe impl<T: Send> Sync for Receiver<T> {}

impl<T> Receiver<T> {
    fn new(spsc: Arc<Mpsc<T>>) -> Self {
        Receiver { spsc, read: 0 }
    }
}
