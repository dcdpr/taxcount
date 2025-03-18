//! Writers do not block readers unless the keys have a hash collision.
//!
//! This module runs concurrency tests with [`loom`](https://docs.rs/loom), attempting to check
//! every possible thread scheduling permutation deterministically. This means that we can verify
//! with high confidence that concurrency either can or cannot be observed (in all scheduling
//! scenarios) for varying tasks.

use super::*;
use loom::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use loom::sync::Arc;
use loom::thread;

// Known collisions from https://github.com/pstibrany/fnv-1a-64bit-collisions
const KEY1: &str = "7mohtcOFVz";
const KEY2: &str = "c1E51sSEyx";

#[test]
fn test_non_blocking() {
    // Ensure that non-colliding keys "a" and "b" do not block each other, and that concurrency is
    // always observed.
    loom::model(|| {
        let counter = Arc::new(AtomicU8::new(0));
        let ack1 = Arc::new(AtomicBool::new(false));
        let ack2 = Arc::new(AtomicBool::new(false));
        let hasher = fnv::FnvBuildHasher::default();
        assert_ne!(hasher.hash_one("a"), hasher.hash_one("b"));

        let fett = Arc::new(Fett::with_hasher(hasher, {
            let counter = counter.clone();
            let ack1 = ack1.clone();
            let ack2 = ack2.clone();
            move |key| {
                let count = counter.fetch_add(1, Ordering::Relaxed);

                // Busy-loop until both threads are synchronized.
                // This ensures that the `get()` calls are in fact running concurrently.
                if count == 0 {
                    // The first thread executes this block.
                    // Acknowledge to thread 2 that thread 1 ready.
                    ack1.store(true, Ordering::Release);
                    // Wait for thread 2 to acknowledge its readiness.
                    while !ack2.load(Ordering::Acquire) {
                        thread::yield_now();
                    }
                } else if count == 1 {
                    // The second thread executes this block.
                    // Acknowledge to thread 1 that thread 2 ready.
                    ack2.store(true, Ordering::Release);
                    // Wait for thread 1 to acknowledge its readiness.
                    while !ack1.load(Ordering::Acquire) {
                        thread::yield_now();
                    }
                }

                match *key {
                    "a" => 0,
                    "b" => 1,
                    _ => unreachable!(),
                }
            }
        }));

        // Get in a new thread.
        let b = thread::spawn({
            let fett = fett.clone();
            move || {
                assert_eq!(fett.get("b"), 1);
            }
        });

        // Get in current thread.
        assert_eq!(fett.get("a"), 0);
        b.join().unwrap();

        assert_eq!(counter.load(Ordering::Relaxed), 2);
    });
}

#[test]
fn test_blocking_same() {
    // Ensure that identical keys "a" and "a" DO block each other and that the `create` function is
    // only called once, and therefore no concurrency.
    loom::model(|| {
        let counter = Arc::new(AtomicU8::new(0));

        let fett = Arc::new(Fett::new({
            let counter = counter.clone();
            move |_key| {
                counter.fetch_add(1, Ordering::Relaxed);

                0
            }
        }));

        // Get in a new thread.
        let a = thread::spawn({
            let fett = fett.clone();
            move || {
                fett.get("a");
            }
        });

        // Get in current thread.
        fett.get("a");
        a.join().unwrap();

        assert_eq!(counter.load(Ordering::Relaxed), 1);
    });
}

#[test]
fn test_blocking_collision() {
    // Ensure that colliding keys `KEY1` and `KEY2` DO block each other and that the `create`
    // function is called twice, but that no concurrency is ever observed.
    loom::model(|| {
        let counter = Arc::new(AtomicU8::new(0));
        let observed_concurrency = Arc::new(AtomicBool::new(false));
        let hasher = fnv::FnvBuildHasher::default();
        assert_eq!(hasher.hash_one(KEY1), hasher.hash_one(KEY2));

        let fett = Arc::new(Fett::with_hasher(hasher, {
            let counter = counter.clone();
            let observed_concurrency = observed_concurrency.clone();
            move |key| {
                let old = counter.load(Ordering::Acquire);

                // Yield to give the other thread a chance to observe concurrency.
                thread::yield_now();

                // Neither thread will increment `observed_concurrency`.
                if counter.fetch_add(1, Ordering::Release) > old {
                    observed_concurrency.store(true, Ordering::Relaxed);
                }

                match *key {
                    KEY1 => 0,
                    KEY2 => 1,
                    _ => unreachable!(),
                }
            }
        }));

        // Get in a new thread.
        let key1 = thread::spawn({
            let fett = fett.clone();
            move || {
                assert_eq!(fett.get(KEY1), 0);
            }
        });

        // Get in current thread.
        assert_eq!(fett.get(KEY2), 1);
        key1.join().unwrap();

        assert_eq!(counter.load(Ordering::Relaxed), 2);
        assert_eq!(observed_concurrency.load(Ordering::Relaxed), false);
    });
}
