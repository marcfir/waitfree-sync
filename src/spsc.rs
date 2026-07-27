//! A wait-free single-producer single-consumer (SPSC) queue to send data to another thread.
//! It is based on the improved FastForward queue.
//!
//! # Example
//! ```rust
//! use waitfree_sync::spsc;
//!
//! //                            Type ──╮   ╭─ Capacity
//! let (mut tx, mut rx) = spsc::spsc::<u64>(8);
//! tx.try_send(234);
//! assert_eq!(rx.try_recv(), Ok(234u64));
//! ```
//!
//! # Behavior for full and empty queue.
//! If the queue is full, the [Sender] returns a [NoSpaceLeftError].
//! If the queue is empty, the [Receiver] returns `None`

//!
use crate::import::{Arc, AtomicBool, Ordering, UnsafeCell};
use core::error::Error;
use crossbeam_utils::CachePadded;
#[cfg(feature = "async")]
use highres_async_timer::HighResultionTimer;
use std::{fmt::Debug, sync::atomic::AtomicUsize};
#[cfg(feature = "async")]
use std::{
    task::{Context, Poll},
    time::Duration,
};

/// Create a new wait-free SPSC queue. The `capacity` must be a power of two, which is validate during runtime.
/// # Panic
/// Panics if the `capacity` is not a power of two.
/// # Example
/// ```rust
/// use waitfree_sync::spsc;
///
/// //               Data type ──╮   ╭─ Capacity
/// let (tx, rx) = spsc::spsc::<u64>(8);
/// ```
pub fn spsc<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    if !is_power_of_two(capacity) {
        panic!("The SIZE must be a power of 2")
    }

    let chan = Arc::new(Spsc::new(capacity));

    let r = Receiver::new(chan.clone());
    let w = Sender::new(chan);

    (w, r)
}

const fn is_power_of_two(x: usize) -> bool {
    let c = x.wrapping_sub(1);
    (x != 0) && (x != 1) && ((x & c) == 0)
}

/// Indicates that a queue is full.
#[derive(Clone, Debug, PartialEq)]
pub enum SendError<T> {
    NoSpaceLeft(T),
    ReceiverSideDropped(T),
}
impl<T: Debug> Error for SendError<T> {}
impl<T> core::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::NoSpaceLeft(_) => write!(f, "No space left in the SPSC queue."),
            SendError::ReceiverSideDropped(_) => {
                write!(f, "Receiver side of the  SPSC queue dropped.")
            }
        }
    }
}
impl<T> SendError<T> {
    pub fn into_value(self) -> T {
        match self {
            SendError::NoSpaceLeft(val) => val,
            SendError::ReceiverSideDropped(val) => val,
        }
    }
}

/// This enumeration is the list of the possible reasons that [Receiver::try_recv] could not return data when called.
#[derive(Clone, Debug, PartialEq)]
pub enum TryRecvError {
    /// This queue is currently empty, but the Sender(s) have not yet disconnected, so data may yet become available.
    Empty,
    /// The queues sending half has become disconnected, and there will never be any more data received on it.
    Disconnected,
}
impl Error for TryRecvError {}
impl core::fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "No space left in the SPSC queue."),
            TryRecvError::Disconnected => {
                write!(f, "Receiver side of the  SPSC queue dropped.")
            }
        }
    }
}

/// The recv operation can only fail if the sending half of a channel is disconnected and the queue is empty, implying that no further messages will ever be received.
#[derive(Clone, Debug, PartialEq)]
pub struct RecvError {}
impl Error for RecvError {}
impl core::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Receiver side of the  SPSC queue dropped.")
    }
}

#[derive(Debug)]
struct Slot<T> {
    value: UnsafeCell<Option<T>>,
    occupied: CachePadded<AtomicBool>,
}
impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            value: UnsafeCell::new(None),
            occupied: CachePadded::new(false.into()),
        }
    }
}

#[derive(Debug)]
struct Spsc<T> {
    mem: Box<[Slot<T>]>,
    // The mask is written when this structure is created and is then only read.
    // Therefore, we do not need Atomic here.
    mask: usize,
    read: CachePadded<AtomicUsize>,
    write: CachePadded<AtomicUsize>,
    alive_counter: CachePadded<AtomicUsize>,
}

impl<T> Spsc<T> {
    fn new(size: usize) -> Self {
        let mut buffer = Vec::with_capacity(size);
        for _ in 0..size {
            buffer.push(Slot::new());
        }
        let buffer: Box<[Slot<T>]> = buffer.into_boxed_slice();
        Spsc {
            mem: buffer,
            mask: size - 1,
            read: CachePadded::new(0.into()),
            write: CachePadded::new(0.into()),
            alive_counter: CachePadded::new(2.into()),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.mask + 1
    }

    #[inline]
    fn len(&self) -> usize {
        self.write
            .load(Ordering::Relaxed)
            .saturating_sub(self.read.load(Ordering::Relaxed))
    }
}

/// The receiving side of the [spsc] queue.
#[derive(Debug)]
pub struct Receiver<T> {
    spsc: Arc<Spsc<T>>,
}
unsafe impl<T: Send> Send for Receiver<T> {}
unsafe impl<T: Send> Sync for Receiver<T> {}

impl<T> Receiver<T> {
    fn new(spsc: Arc<Spsc<T>>) -> Self {
        Receiver { spsc }
    }
}
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.spsc.alive_counter.fetch_sub(1, Ordering::Relaxed);
    }
}

impl<T> Receiver<T> {
    /// Retrieve the next available element from the queue.
    /// Returns [None] if the queue is empty.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let read = self.spsc.read.load(Ordering::Relaxed);
        let rpos = read & self.spsc.mask;
        let slot = unsafe { self.spsc.mem.get_unchecked(rpos) };
        if !slot.occupied.load(Ordering::Acquire) {
            if self.spsc.alive_counter.load(Ordering::Relaxed) < 2 {
                Err(TryRecvError::Disconnected)
            } else {
                Err(TryRecvError::Empty)
            }
        } else {
            #[cfg(not(loom))]
            let val = unsafe { slot.value.get().replace(None) };
            #[cfg(loom)]
            let val = unsafe { slot.value.get_mut().with(|ptr| ptr.replace(None)) };

            slot.occupied.store(false, Ordering::Release);
            // self.read = self.read.wrapping_add(1);
            self.spsc
                .read
                .store(read.wrapping_add(1), Ordering::Relaxed);
            Ok(val.unwrap())
        }
    }
    /// Peeks the next element in the queue without removing it.
    #[cfg(not(loom))] // We can't return a reference to an UnsafeCell of loom.
    pub fn peek(&self) -> Option<&T> {
        let rpos = self.spsc.read.load(Ordering::Relaxed) & self.spsc.mask;
        let slot = unsafe { self.spsc.mem.get_unchecked(rpos) };
        if !slot.occupied.load(Ordering::Acquire) {
            None
        } else {
            let val = unsafe { &*slot.value.get() };
            val.as_ref()
        }
    }

    /// Returns the total number of items that the queue can hold at most.
    #[inline]
    pub fn capacity(&self) -> usize {
        // SAFETY: This is safe because we only read size which is never written.
        self.spsc.capacity()
    }

    /// Returns the number of items in the queue.
    /// # WARNING
    /// This length is only a best-effort estimate.
    /// It is computed from relaxed atomic and is NOT a linearizable value.
    /// It may be temporarily incorrect (including over/under-counting) due to
    /// reordering and visibility delays across threads.
    #[inline]
    pub fn len(&self) -> usize {
        self.spsc.len()
    }

    /// Returns true if the queue is empty.
    /// # WARNING
    /// This length is only a best-effort estimate.
    /// It is computed from relaxed atomic and is NOT a linearizable value.
    /// It may be temporarily incorrect (including over/under-counting) due to
    /// reordering and visibility delays across threads.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.spsc.len() == 0
    }

    /// Converts this [Receiver] into an [AsyncReceiver] that can be awaited on.
    /// # Errors
    /// Returns an [std::io::Error] if the underlying timer could not be created or configured.
    #[cfg(feature = "async")]
    pub async fn into_async(
        self,
        poll_intervall: Duration,
    ) -> Result<AsyncReceiver<T>, std::io::Error> {
        let timer = HighResultionTimer::interval(poll_intervall)?;

        Ok(AsyncReceiver {
            receiver: self,
            timer,
        })
    }
}

/// The async receiving side of the [spsc] queue which can be created with [Receiver::into_async].
/// The `AsyncReceiver` uses internally a polling mechanism base on timerfd.
#[cfg(feature = "async")]
#[derive(Debug)]
pub struct AsyncReceiver<T> {
    receiver: Receiver<T>,
    timer: HighResultionTimer,
}
#[cfg(feature = "async")]
impl<T> AsyncReceiver<T> {
    /// Asynchronously waits for and returns the next available element from the queue.
    /// This polls the queue at the configured polling rate (see [AsyncReceiver::set_polling_rate])
    /// until an element becomes available.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        loop {
            match self.receiver.try_recv() {
                Ok(val) => return Ok(val),
                Err(TryRecvError::Disconnected) => return Err(RecvError {}),
                Err(_) => (),
            }
            self.timer.tick().await;
        }
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match self.receiver.try_recv() {
            Ok(val) => Poll::Ready(Some(val)),
            Err(TryRecvError::Disconnected) => Poll::Ready(None),
            Err(_) => {
                let _ = self.timer.poll_tick(cx);
                Poll::Pending
            }
        }

        // self.timer.poll_tick();
    }

    /// Returns the total number of items that the queue can hold at most.
    #[inline]
    pub fn capacity(&self) -> usize {
        // SAFETY: This is safe because we only read size which is never written.
        self.receiver.spsc.capacity()
    }

    /// Returns the number of items in the queue.
    /// # WARNING
    /// This length is only a best-effort estimate.
    /// It is computed from relaxed atomic and is NOT a linearizable value.
    /// It may be temporarily incorrect (including over/under-counting) due to
    /// reordering and visibility delays across threads.
    #[inline]
    pub fn len(&self) -> usize {
        self.receiver.spsc.len()
    }

    /// Returns true if the queue is empty.
    /// # WARNING
    /// This length is only a best-effort estimate.
    /// It is computed from relaxed atomic and is NOT a linearizable value.
    /// It may be temporarily incorrect (including over/under-counting) due to
    /// reordering and visibility delays across threads.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.receiver.spsc.len() == 0
    }
}

/// The sending side of the [spsc] queue.
#[derive(Debug)]
pub struct Sender<T> {
    spsc: Arc<Spsc<T>>,
}
unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Send> Sync for Sender<T> {}
impl<T> Sender<T> {
    fn new(spsc: Arc<Spsc<T>>) -> Self {
        Sender { spsc }
    }
}
impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.spsc.alive_counter.fetch_sub(1, Ordering::Relaxed);
    }
}

impl<T> Sender<T> {
    /// Attempts to send a value to the queue without blocking.
    /// Returns a [NoSpaceLeftError] if the queue is full.
    pub fn try_send(&mut self, data: T) -> Result<(), SendError<T>> {
        let write = self.spsc.write.load(Ordering::Relaxed);
        let wpos = write & self.spsc.mask;

        if self.spsc.alive_counter.load(Ordering::Relaxed) < 2 {
            return Err(SendError::ReceiverSideDropped(data));
        }

        let slot = unsafe { self.spsc.mem.get_unchecked(wpos) };
        if slot.occupied.load(Ordering::Acquire) {
            Err(SendError::NoSpaceLeft(data))
        } else {
            #[cfg(not(loom))]
            unsafe {
                slot.value.get().write(Some(data))
            };
            #[cfg(loom)]
            unsafe {
                slot.value.get_mut().with(|ptr| ptr.write(Some(data)))
            };
            slot.occupied.store(true, Ordering::Release);
            self.spsc
                .write
                .store(write.wrapping_add(1), Ordering::Relaxed);
            Ok(())
        }
    }

    /// Returns the total number of items that the queue can hold at most.
    #[inline]
    pub fn capacity(&self) -> usize {
        // SAFETY: This is safe because we only read size which is never written.
        self.spsc.capacity()
    }

    /// Returns the number of items in the queue.
    /// # WARNING
    /// This length is only a best-effort estimate.
    /// It is computed from relaxed atomic and is NOT a linearizable value.
    /// It may be temporarily incorrect (including over/under-counting) due to
    /// reordering and visibility delays across threads.
    #[inline]
    pub fn len(&self) -> usize {
        self.spsc.len()
    }

    /// Returns true if the queue is empty.
    /// # WARNING
    /// This length is only a best-effort estimate.
    /// It is computed from relaxed atomic and is NOT a linearizable value.
    /// It may be temporarily incorrect (including over/under-counting) due to
    /// reordering and visibility delays across threads.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.spsc.len() == 0
    }
}

#[cfg(not(loom))]
#[cfg(test)]
mod test {
    #[cfg(loom)]
    use loom::thread;
    #[cfg(not(loom))]
    use std::thread;

    use super::*;

    #[test]
    fn smoke() {
        let (mut w, mut r) = spsc(4);
        w.try_send(vec![0; 15]).unwrap();
        w.try_send(vec![0; 16]).unwrap();
        w.try_send(vec![0; 17]).unwrap();
        w.try_send(vec![0; 18]).unwrap();

        assert_eq!(r.try_recv(), Ok(vec![0; 15]));
        assert_eq!(r.try_recv(), Ok(vec![0; 16]));
        assert_eq!(r.try_recv(), Ok(vec![0; 17]));
        assert_eq!(r.try_recv(), Ok(vec![0; 18]));
    }

    #[test]
    fn test_is_power_of_two() {
        assert!(!is_power_of_two(0));
        assert!(!is_power_of_two(1));
        assert!(is_power_of_two(2));
        assert!(!is_power_of_two(3));
        assert!(is_power_of_two(4));
        assert!(!is_power_of_two(5));
        assert!(!is_power_of_two(6));
        assert!(!is_power_of_two(7));
        assert!(is_power_of_two(8));
        assert!(!is_power_of_two(9));

        assert!(!is_power_of_two(15));
        assert!(is_power_of_two(16));
        assert!(!is_power_of_two(17));

        assert!(!is_power_of_two(31));
        assert!(is_power_of_two(32));
        assert!(!is_power_of_two(33));
    }

    #[test]
    fn test_full_empty() {
        let (mut write, mut read) = spsc::<i32>(4);
        assert_eq!(write.try_send(1), Ok(()));
        assert_eq!(write.len(), 1);
        assert_eq!(write.try_send(2), Ok(()));
        assert_eq!(write.len(), 2);
        assert_eq!(write.try_send(3), Ok(()));
        assert_eq!(write.len(), 3);
        assert_eq!(write.try_send(4), Ok(()));
        assert_eq!(write.len(), 4);
        assert_eq!(write.try_send(5), Err(SendError::NoSpaceLeft(5)));
        assert_eq!(write.len(), 4);

        assert_eq!(read.try_recv(), Ok(1));
        assert_eq!(write.len(), 3);
        assert_eq!(write.try_send(6), Ok(()));
        assert_eq!(write.len(), 4);
        assert_eq!(read.try_recv(), Ok(2));
        assert_eq!(write.len(), 3);
        assert_eq!(read.try_recv(), Ok(3));
        assert_eq!(write.len(), 2);
        assert_eq!(read.try_recv(), Ok(4));
        assert_eq!(write.len(), 1);
        assert_eq!(read.try_recv(), Ok(6));
        assert_eq!(read.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn test_drop_read_side() {
        let (mut write, read) = spsc::<i32>(4);

        assert_eq!(write.try_send(1), Ok(()));
        assert_eq!(write.len(), 1);
        assert_eq!(write.try_send(2), Ok(()));
        assert_eq!(write.len(), 2);
        drop(read);
        assert_eq!(write.try_send(3), Err(SendError::ReceiverSideDropped(3)));
        assert_eq!(write.len(), 2);
        assert_eq!(write.try_send(4), Err(SendError::ReceiverSideDropped(4)));
        assert_eq!(write.len(), 2);
        assert_eq!(write.try_send(5), Err(SendError::ReceiverSideDropped(5)));
        assert_eq!(write.len(), 2);
    }

    #[test]
    fn test_drop_write_side() {
        let (mut write, mut read) = spsc::<i32>(4);

        write.try_send(0).unwrap();
        write.try_send(1).unwrap();
        assert_eq!(read.try_recv(), Ok(0));
        drop(write);
        assert_eq!(read.try_recv(), Ok(1));
    }

    #[test]
    fn test_peek() {
        let (mut w, mut r) = spsc(4);
        w.try_send(vec![0; 15]).unwrap();
        w.try_send(vec![0; 16]).unwrap();
        w.try_send(vec![0; 17]).unwrap();
        w.try_send(vec![0; 18]).unwrap();

        assert_eq!(r.peek(), Some(&vec![0; 15]));
        assert_eq!(r.try_recv(), Ok(vec![0; 15]));
        assert_eq!(r.peek(), Some(&vec![0; 16]));
        assert_eq!(r.try_recv(), Ok(vec![0; 16]));
        assert_eq!(r.peek(), Some(&vec![0; 17]));
        assert_eq!(r.try_recv(), Ok(vec![0; 17]));
        assert_eq!(r.peek(), Some(&vec![0; 18]));
        assert_eq!(r.peek(), Some(&vec![0; 18]));
        assert_eq!(r.peek(), Some(&vec![0; 18]));
        assert_eq!(r.try_recv(), Ok(vec![0; 18]));
        assert_eq!(r.peek(), None);
    }

    #[test]
    fn test_peek_threaded() {
        let (mut sender, mut receiver) = spsc(4);

        let writer_thread = thread::spawn(move || {
            thread::park();
            for i in 0..4 {
                assert_eq!(sender.try_send([i; 50]), Ok(()));
            }
        });
        let reader_thread = thread::spawn(move || {
            thread::park();
            let mut i = 0;
            while i < 4 {
                if let Some(val) = receiver.peek() {
                    let first_entry = val[0];
                    for entry in val {
                        assert_eq!(*entry, first_entry);
                    }
                    let val = receiver.try_recv().unwrap();
                    let first_entry = val[0];
                    for entry in val {
                        assert_eq!(entry, first_entry);
                    }
                    i += 1;
                }
            }
        });
        writer_thread.thread().unpark();
        reader_thread.thread().unpark();
        assert!(writer_thread.join().is_ok());
        assert!(reader_thread.join().is_ok());
    }

    #[cfg(feature = "async")]
    #[test]
    fn test_tokio_smoke() {
        use std::time::Duration;

        let (mut sender, receiver) = spsc(2);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let writer_thread = thread::spawn(move || {
            thread::park();
            for i in 0..1000 {
                assert_eq!(sender.try_send([i; 50]), Ok(()));
                std::thread::sleep(Duration::from_micros(500));
            }
        });
        let notify = std::sync::Arc::new(tokio::sync::Notify::new());
        let reader_thread = rt.spawn({
            let notify = notify.clone();
            async move {
                println!("now running on a worker thread");
                let mut receiver = receiver.into_async(Duration::from_millis(1)).await.unwrap();

                notify.notified().await;
                for i in 0..1000 {
                    assert_eq!(receiver.recv().await, Ok([i; 50]));
                }
            }
        });
        notify.notify_one();
        writer_thread.thread().unpark();
        assert!(writer_thread.join().is_ok());
        rt.block_on(async move {
            reader_thread.await.unwrap();
        });
        // assert!(reader_thread..is_ok());
    }
}
