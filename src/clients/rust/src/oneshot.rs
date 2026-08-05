use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// An embeddable single-producer, single-consumer completion cell.
/// The enclosing allocation owns it and the cell doesn't allocate.
pub(crate) struct CompletionCell<Awaiting, Complete> {
    state: Mutex<State<Awaiting, Complete>>,
}

enum State<Awaiting, Complete> {
    Awaiting {
        value: Awaiting,
        waker: Option<Waker>,
    },
    Updating,
    Complete(Complete),
    Reusable,
}

impl<Awaiting, Complete> CompletionCell<Awaiting, Complete> {
    pub(crate) fn new(value: Awaiting) -> Self {
        Self {
            state: Mutex::new(State::Awaiting { value, waker: None }),
        }
    }

    pub(crate) fn reusable() -> Self {
        Self {
            state: Mutex::new(State::Reusable),
        }
    }

    pub(crate) fn with_awaiting<R>(&self, f: impl FnOnce(&Awaiting) -> R) -> R {
        let state = self.state.lock().unwrap();
        match &*state {
            State::Awaiting { value, .. } => f(value),
            _ => panic!("completion is no longer pending"),
        }
    }

    pub(crate) fn complete_with(&self, f: impl FnOnce(Awaiting) -> Complete) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            assert!(
                matches!(*state, State::Awaiting { .. }),
                "completion completed twice"
            );
            let previous = std::mem::replace(&mut *state, State::Updating);
            let (value, waker) = match previous {
                State::Awaiting { value, waker } => (value, waker),
                _ => unreachable!(),
            };
            *state = State::Complete(f(value));
            waker
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<Complete> {
        let mut state = self.state.lock().unwrap();
        match &mut *state {
            State::Awaiting { waker, .. } => {
                if waker
                    .as_ref()
                    .map_or(true, |registered| !registered.will_wake(cx.waker()))
                {
                    *waker = Some(cx.waker().clone());
                }
                Poll::Pending
            }
            State::Complete(_) => match std::mem::replace(&mut *state, State::Reusable) {
                State::Complete(value) => Poll::Ready(value),
                _ => unreachable!(),
            },
            State::Updating => panic!("polled completion while completion is in progress"),
            State::Reusable => panic!("polled completion after completion"),
        }
    }

    pub(crate) fn reuse_with(&self, f: impl FnOnce() -> Awaiting) {
        let mut state = self.state.lock().unwrap();
        assert!(matches!(*state, State::Reusable),);
        *state = State::Awaiting {
            value: f(),
            waker: None,
        };
    }
}

/// oneshot channel sender
pub struct Sender<T>(Arc<CompletionCell<(), T>>);

/// oneshot channel receiver
pub struct Receiver<T>(Arc<CompletionCell<(), T>>);

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let cell = Arc::new(CompletionCell::new(()));
    (Sender(Arc::clone(&cell)), Receiver(cell))
}

impl<T> Sender<T> {
    pub fn send(self, value: T) {
        self.0.complete_with(|()| value);
    }
}

impl<T> Future for Receiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        self.0.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    struct CountingWake(Arc<AtomicUsize>);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counting_waker(count: &Arc<AtomicUsize>) -> Waker {
        Waker::from(Arc::new(CountingWake(Arc::clone(count))))
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

    #[test]
    fn completion_cell_can_be_rearmed() {
        let cell = CompletionCell::<u64, u64>::reusable();
        let count = Arc::new(AtomicUsize::new(0));
        let waker = counting_waker(&count);
        let mut cx = Context::from_waker(&waker);

        cell.reuse_with(|| 2);
        assert_eq!(cell.with_awaiting(|value| *value), 2);
        assert!(cell.poll(&mut cx).is_pending());

        cell.complete_with(|value| value * 3);
        assert_eq!(cell.poll(&mut cx), Poll::Ready(6));

        cell.reuse_with(|| 5);
        assert!(cell.poll(&mut cx).is_pending());

        cell.complete_with(|value| value * 7);
        assert_eq!(cell.poll(&mut cx), Poll::Ready(35));
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    struct DropCount(Arc<AtomicUsize>);

    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn completion_cell_drops_owned_values() {
        let count = Arc::new(AtomicUsize::new(0));
        let cell = CompletionCell::<_, ()>::new(DropCount(Arc::clone(&count)));
        drop(cell);
        assert_eq!(count.load(Ordering::SeqCst), 1);

        let cell = CompletionCell::new(DropCount(Arc::clone(&count)));
        cell.complete_with(|value| value);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        drop(cell);
        assert_eq!(count.load(Ordering::SeqCst), 2);

        let cell = CompletionCell::new(DropCount(Arc::clone(&count)));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cell.complete_with(|_value| panic!("completion failed"));
        }));
        assert!(result.is_err());
        drop(cell);
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn reuse_checks_state_before_initializing() {
        let count = Arc::new(AtomicUsize::new(0));
        let cell = CompletionCell::new(DropCount(Arc::clone(&count)));
        cell.complete_with(|value| value);

        let initialized = AtomicUsize::new(0);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cell.reuse_with(|| {
                initialized.fetch_add(1, Ordering::SeqCst);
                DropCount(Arc::clone(&count))
            });
        }));

        assert!(result.is_err());
        assert_eq!(initialized.load(Ordering::SeqCst), 0);
        drop(cell);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn completion_wakes_after_unlocking() {
        struct ReentrantWake(std::sync::Weak<CompletionCell<(), u32>>);

        impl std::task::Wake for ReentrantWake {
            fn wake(self: Arc<Self>) {
                let cell = self.0.upgrade().unwrap();
                let state = cell.state.try_lock().expect("completion still locked");
                assert!(matches!(*state, State::Complete(42)));
                drop(state);

                let waker = Waker::from(self);
                assert_eq!(cell.poll(&mut Context::from_waker(&waker)), Poll::Ready(42));
                cell.reuse_with(|| ());
                cell.complete_with(|()| 7);
            }
        }

        let cell = Arc::new(CompletionCell::new(()));
        let waker = Waker::from(Arc::new(ReentrantWake(Arc::downgrade(&cell))));
        let mut cx = Context::from_waker(&waker);

        assert!(cell.poll(&mut cx).is_pending());
        cell.complete_with(|()| 42);
        assert_eq!(cell.poll(&mut cx), Poll::Ready(7));
    }
}
