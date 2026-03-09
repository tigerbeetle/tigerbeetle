use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Shared<T> {
    value: Option<T>,
    waker: Option<Waker>,
}

pub struct Sender<T>(Arc<Mutex<Shared<T>>>);
pub struct Receiver<T>(Arc<Mutex<Shared<T>>>);

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Mutex::new(Shared {
        value: None,
        waker: None,
    }));
    (Sender(shared.clone()), Receiver(shared))
}

impl<T> Sender<T> {
    pub fn send(self, value: T) {
        let mut shared = self.0.lock().unwrap();
        shared.value = Some(value);
        if let Some(waker) = shared.waker.take() {
            waker.wake();
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut shared = self.0.lock().unwrap();
        if let Some(value) = shared.value.take() {
            Poll::Ready(value)
        } else {
            shared.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{RawWaker, RawWakerVTable};

    // Minimal waker that tracks wake count.
    fn counting_waker(count: &Arc<AtomicUsize>) -> Waker {
        let data = Arc::into_raw(count.clone()) as *const ();

        unsafe fn clone(data: *const ()) -> RawWaker {
            Arc::increment_strong_count(data as *const AtomicUsize);
            RawWaker::new(data, &VTABLE)
        }
        unsafe fn wake(data: *const ()) {
            let arc = Arc::from_raw(data as *const AtomicUsize);
            arc.fetch_add(1, Ordering::SeqCst);
        }
        unsafe fn wake_by_ref(data: *const ()) {
            let arc = &*(data as *const AtomicUsize);
            arc.fetch_add(1, Ordering::SeqCst);
        }
        unsafe fn drop(data: *const ()) {
            Arc::decrement_strong_count(data as *const AtomicUsize);
        }

        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

        unsafe { Waker::from_raw(RawWaker::new(data, &VTABLE)) }
    }

    fn poll_once<T>(rx: &mut Receiver<T>, waker: &Waker) -> Poll<T> {
        let mut cx = Context::from_waker(waker);
        Pin::new(rx).poll(&mut cx)
    }

    #[test]
    fn send_before_poll() {
        let (tx, mut rx) = channel::<u64>();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(&wake_count);

        tx.send(42);

        // Value already present — should resolve immediately.
        match poll_once(&mut rx, &waker) {
            Poll::Ready(v) => assert_eq!(v, 42),
            Poll::Pending => panic!("expected Ready"),
        }
        // No waker should have been registered or woken.
        assert_eq!(wake_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn poll_before_send() {
        let (tx, mut rx) = channel::<u64>();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(&wake_count);

        // First poll — no value yet.
        assert!(poll_once(&mut rx, &waker).is_pending());
        assert_eq!(wake_count.load(Ordering::SeqCst), 0);

        // Send wakes the registered waker.
        tx.send(99);
        assert_eq!(wake_count.load(Ordering::SeqCst), 1);

        // Second poll picks up the value.
        match poll_once(&mut rx, &waker) {
            Poll::Ready(v) => assert_eq!(v, 99),
            Poll::Pending => panic!("expected Ready"),
        }
    }

    #[test]
    fn multiple_polls_replace_waker() {
        let (tx, mut rx) = channel::<&str>();
        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));
        let waker1 = counting_waker(&count1);
        let waker2 = counting_waker(&count2);

        // Register waker1, then replace with waker2.
        assert!(poll_once(&mut rx, &waker1).is_pending());
        assert!(poll_once(&mut rx, &waker2).is_pending());

        tx.send("hello");

        // Only the most recent waker should fire.
        assert_eq!(count1.load(Ordering::SeqCst), 0);
        assert_eq!(count2.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn send_without_prior_poll() {
        // No waker registered — send should not panic.
        let (tx, _rx) = channel::<i32>();
        tx.send(7);
    }

    #[test]
    fn send_from_another_thread() {
        let (tx, mut rx) = channel::<Vec<u8>>();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(&wake_count);

        assert!(poll_once(&mut rx, &waker).is_pending());

        std::thread::spawn(move || {
            tx.send(vec![1, 2, 3]);
        })
        .join()
        .unwrap();

        match poll_once(&mut rx, &waker) {
            Poll::Ready(v) => assert_eq!(v, vec![1, 2, 3]),
            Poll::Pending => panic!("expected Ready"),
        }
    }

    #[test]
    fn zero_sized_type() {
        let (tx, mut rx) = channel::<()>();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(&wake_count);

        assert!(poll_once(&mut rx, &waker).is_pending());
        tx.send(());
        assert!(poll_once(&mut rx, &waker).is_ready());
    }

    #[test]
    fn drop_sender_without_sending() {
        // Dropping sender without sending shouldn't panic or wake.
        let (_tx, _rx) = channel::<String>();
    }
}
