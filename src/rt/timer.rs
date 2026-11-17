//! Provides a timer trait with timer-like functions.
//!
//! Example using tokio timer:
//! ```rust
//! use std::{
//!     future::Future,
//!     pin::Pin,
//!     task::{Context, Poll},
//!     time::{Duration, Instant},
//! };
//!
//! use pin_project_lite::pin_project;
//! use hyper::rt::{Timer, Sleep};
//!
//! #[derive(Clone, Debug)]
//! pub struct TokioTimer;
//!
//! impl Timer for TokioTimer {
//!     fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>> {
//!         Box::pin(TokioSleep {
//!             inner: tokio::time::sleep(duration),
//!         })
//!     }
//!
//!     fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>> {
//!         Box::pin(TokioSleep {
//!             inner: tokio::time::sleep_until(deadline.into()),
//!         })
//!     }
//!
//!     fn reset(&self, sleep: &mut Pin<Box<dyn Sleep>>, new_deadline: Instant) {
//!         if let Some(sleep) = sleep.as_mut().downcast_mut_pin::<TokioSleep>() {
//!             sleep.reset(new_deadline.into())
//!         }
//!     }
//! }
//!
//! pin_project! {
//!     pub(crate) struct TokioSleep {
//!         #[pin]
//!         pub(crate) inner: tokio::time::Sleep,
//!     }
//! }
//!
//! impl Future for TokioSleep {
//!     type Output = ();
//!
//!     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//!         self.project().inner.poll(cx)
//!     }
//! }
//!
//! impl Sleep for TokioSleep {}
//!
//! impl TokioSleep {
//!     pub fn reset(self: Pin<&mut Self>, deadline: Instant) {
//!         self.project().inner.as_mut().reset(deadline.into());
//!     }
//! }
//! ```

use std::{
    any::TypeId,
    future::Future,
    pin::Pin,
    time::{Duration, Instant},
};

/// A timer which provides timer-like functions.
pub trait Timer {
    /// Return a future that resolves in `duration` time.
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>>;

    /// Return a future that resolves at `deadline`.
    ///
    /// `deadline` is interpreted according to the same clock that
    /// [`Timer::now`] uses, so deadlines can be constructed as
    /// `timer.now() + duration`.
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>>;

    /// Return an `Instant` representing the current time.
    ///
    /// The returned instant is meant to be compared against other instants
    /// produced by this same `Timer` (for example, instants obtained from
    /// [`Timer::now`], or passed to [`Timer::sleep_until`]); it is not
    /// necessarily comparable to instants from other clocks.
    ///
    /// Note that the default implementation returns [`Instant::now()`].
    /// On Linux, `std::time::Instant` is backed by `CLOCK_MONOTONIC`, which
    ///—unlike `CLOCK_BOOTTIME`—does not include time spent while the system
    ///is suspended. As a result, deadlines constructed from
    ///`Instant::now()` may not expire while the system is suspended, and
    ///durations measured against it may underestimate real elapsed time
    ///across a suspend. Implementations that need to account for suspend
    ///time (such as timers used to evict expired idle connections) should
    ///base their `now`/`sleep_until` on a clock that includes suspend time,
    ///and keep both functions on the same clock.
    fn now(&self) -> Instant {
        Instant::now()
    }

    /// Reset a future to resolve at `new_deadline` instead.
    fn reset(&self, sleep: &mut Pin<Box<dyn Sleep>>, new_deadline: Instant) {
        *sleep = self.sleep_until(new_deadline);
    }
}

/// A future returned by a `Timer`.
pub trait Sleep: Send + Sync + Future<Output = ()> {
    #[doc(hidden)]
    /// This method is private and can not be implemented by downstream crates.
    fn __type_id(&self, _: private::Sealed) -> TypeId
    where
        Self: 'static,
    {
        TypeId::of::<Self>()
    }
}

impl dyn Sleep {
    // This is a re-implementation of downcast methods from `std::any::Any`.

    /// Check whether the type is the same as `T`.
    pub fn is<T>(&self) -> bool
    where
        T: Sleep + 'static,
    {
        self.__type_id(private::Sealed) == TypeId::of::<T>()
    }

    /// Downcast a pinned `&mut Sleep` object to its original type.
    pub fn downcast_mut_pin<T>(self: Pin<&mut Self>) -> Option<Pin<&mut T>>
    where
        T: Sleep + 'static,
    {
        if self.is::<T>() {
            // SAFETY:
            // 1. `self.is::<T>()` guarantees that the underlying object is indeed of type `T`.
            // 2. We use `Pin::into_inner_unchecked` to gain mutable access to the underlying
            //    value because we are maintaining the pinning contract.
            // 3. We convert the reference to `dyn Sleep` to a reference to `T` via raw pointers.
            //    Since we've verified the type, this is a sound downcast.
            // 4. `Pin::new_unchecked` is safe here because the original object was already pinned,
            //    and by pinning the downcasted reference, we continue to uphold the pinning guarantee
            //    that the object will not be moved.
            unsafe {
                let inner = Pin::into_inner_unchecked(self);
                Some(Pin::new_unchecked(
                    &mut *(&mut *inner as *mut dyn Sleep).cast(),
                ))
            }
        } else {
            None
        }
    }
}

mod private {
    #![allow(missing_debug_implementations)]
    pub struct Sealed;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll, Waker};

    // A minimal timer implementation implementing every required method, to
    // ensure the trait remains object-safe-ish and usable as documented.
    struct TestTimer;

    struct TestSleep {
        deadline: Instant,
    }

    impl Future for TestSleep {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if Instant::now() >= self.deadline {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    impl Sleep for TestSleep {}

    impl Timer for TestTimer {
        fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>> {
            Box::pin(TestSleep {
                deadline: Instant::now() + duration,
            })
        }

        fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>> {
            Box::pin(TestSleep { deadline })
        }
    }

    #[test]
    fn default_now_is_monotonic() {
        // `Instant` is guaranteed to be monotonic, and the default `Timer`
        // implementation uses it, so successive `now()` calls must not
        // go backwards.
        let timer = TestTimer;
        let start = timer.now();
        for _ in 0..1000 {
            let now = timer.now();
            assert!(now >= start);
        }
    }

    #[test]
    fn sleep_until_uses_same_clock_as_now() {
        // A deadline constructed from `now()` must lie in the same clock
        // domain observed by `sleep_until`; otherwise the sleep could fire
        // immediately or never.
        let timer = TestTimer;
        let deadline = timer.now() + Duration::from_millis(5);
        let mut sleep = timer.sleep_until(deadline);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(sleep.as_mut().poll(&mut cx), Poll::Pending));
        assert!(timer.now() < deadline);
    }
}
