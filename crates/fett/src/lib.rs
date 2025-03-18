//! This is the way to do concurrent memoizing maps.
//!
//! # Examples
//!
//! A simple memoizing "x + 1" map:
//!
//! ```
//! # use fett::Fett;
//! // The constructor accepts a function or closure which we call the "value constructor".
//! // It takes a reference to a key and returns a new value associated with that key.
//! let fett = Fett::new(|x| *x + 1);
//!
//! // Getting a key will call the value constructor the first time that key is accessed.
//! assert_eq!(fett.get(3), 4);
//! assert_eq!(fett.get(12), 13);
//! ```
//!
//! A basic file cache:
//!
//! ```no_run
//! # use fett::Fett;
//! # use std::sync::Arc;
//! // The value constructor returns file contents as `Arc<str>`, panicking on I/O errors.
//! let fett = Fett::new(|path| {
//!     let content = std::fs::read_to_string(path).expect("File must be readable");
//!     Arc::<str>::from(content)
//! });
//!
//! // Assuming `echo 'some contents' >/tmp/some_file`
//! assert_eq!(&*fett.get("/tmp/some_file"), "some contents");
//! ```
//!
//! Caching behavior:
//!
//! ```
//! # use fett::Fett;
//! # use std::cell::Cell;
//! // Demonstrate the caching ability with a counter.
//! let counter = Cell::new(0);
//!
//! let fett = Fett::new(|x| {
//!     counter.set(counter.get() + 1);
//!     *x + 1
//! });
//!
//! // The value constructor is only called once for each unique key.
//! assert_eq!(counter.get(), 0);
//! assert_eq!(fett.get(3), 4);
//! assert_eq!(fett.get(3), 4);
//! assert_eq!(counter.get(), 1);
//!
//! // We'll call the value constructor only one additional time.
//! assert_eq!(fett.get(12), 13);
//! assert_eq!(fett.get(12), 13);
//! assert_eq!(fett.get(3), 4);
//! assert_eq!(counter.get(), 2);
//! ```
//!
//! Thread safe and robust:
//!
//! ```
//! # use fett::Fett;
//! # use rayon::prelude::*;
//! # use std::sync::atomic::{AtomicU8, Ordering};
//! let counter = AtomicU8::new(0);
//!
//! let fett = Fett::new(|_| {
//!     // Increment the counter on each call, and return its old value.
//!     counter.fetch_add(1, Ordering::Relaxed)
//! });
//!
//! // Use the rayon crate to attack our poor cache with many threads.
//! [0_i32; 32].par_iter().for_each(|_| {
//!     assert_eq!(fett.get(0), 0);
//! });
//!
//! assert_eq!(counter.load(Ordering::Relaxed), 1);
//! ```
//!
//! # Deadlock
//!
//! Unlike most other concurrent map implementations, [`Fett`] can only deadlock if the value
//! constructor never returns. Common causes would include infinite loops, I/O requests missing a
//! timeout, resource requests that are busy forever, etc.
//!
//! Because there is only a single value constructor, this issue is very controllable. Threads
//! competing for values within the map can otherwise never lead to deadlock as there are no mutual
//! dependencies between them.
//!
//! `Fett` affords this capability by requiring that the values it stores implements [`Clone`]. For
//! this reason, it should only store [`Copy`] types or non-`Copy` types that are wrapped in
//! [`Arc`].
//!
//! For instance, if you would typically store `String` values, consider `Arc<str>` instead. This
//! makes cloning the value inexpensive while remaining thread-safe.
//!
//! [`Clone`]: std::clone::Clone
//! [`Copy`]: std::marker::Copy
//! [`Arc`]: std::sync::Arc

#![forbid(unsafe_code)]

use crate::sync::RwLock;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};

mod sync;

const DEFAULT_CAPACITY: usize = 1_024;

/// A concurrent `HashMap` implementation with lazy value construction.
///
/// # Limitations
///
/// - Insert-once: The constructor function `create` should be memoizable; always producing the same
///   result for any given key. This means that values cannot be updated like a normal map.
/// - The map does dynamically grow, but the number of buckets is fixed to 1/16 of initial capacity.
///   - See [`Fett::with_capacity_and_hasher`] for capacity details.
///   - Re-hashing the map in-place is not supported, but it can be rehashed if you are able to move
///     it by combining [`Fett::into_inner`] and [`Fett::from::<(H, F, I)>`].
///
/// [`Fett::from::<(H, F, I)>`]: Fett#impl-From<(H,+F,+I)>-for-Fett<K,+V,+F,+H>
#[derive(Debug)]
pub struct Fett<K, V, F, H = RandomState> {
    buckets: Vec<RwLock<Vec<(K, V)>>>,
    hasher: H,
    create: F,
}

impl<K, V, F> Fett<K, V, F>
where
    K: Eq + Hash + PartialEq,
    V: Clone,
    F: Fn(&K) -> V,
{
    /// Create a new memoizer with default capacity and hasher.
    ///
    /// Default capacity is 1,024. See [`Fett::with_capacity_and_hasher`] for capacity details.
    ///
    /// Take notice that the value returned by the constructor function must implement `Clone`. This
    /// means `Copy` types can be used directly, but everything else should probably be wrapped in
    /// [`Arc`]. See the [crate root] documentation for more info.
    ///
    /// [`Arc`]: std::sync::Arc
    /// [crate root]: crate#Deadlock
    pub fn new(create: F) -> Self {
        Self::with_capacity(DEFAULT_CAPACITY, create)
    }

    /// Create a new fixed-sized memoizer with specified capacity and default hasher.
    ///
    /// # Example
    ///
    /// ```
    /// # use fett::Fett;
    /// let fett = Fett::with_capacity(1024, |key| *key);
    /// assert_eq!(fett.get(42), 42);
    /// ```
    pub fn with_capacity(capacity: usize, create: F) -> Fett<K, V, F, RandomState> {
        Self::with_capacity_and_hasher(capacity, RandomState::new(), create)
    }
}

impl<K, V, F, H> Fett<K, V, F, H>
where
    K: Eq + Hash + PartialEq,
    V: Clone,
    F: Fn(&K) -> V,
    H: BuildHasher,
{
    /// Create a new fixed-sized memoizer with the default capacity and specified hasher.
    ///
    /// Default capacity is 1,024. See [`Fett::with_capacity_and_hasher`] for capacity details.
    ///
    /// # Example
    ///
    /// ```
    /// # use fett::Fett;
    /// let fett = Fett::with_hasher(ahash::RandomState::new(), |key| *key);
    /// assert_eq!(fett.get(13), 13);
    ///
    /// ```
    pub fn with_hasher(hasher: H, create: F) -> Fett<K, V, F, H> {
        Self::with_capacity_and_hasher(DEFAULT_CAPACITY, hasher, create)
    }

    /// Create a new fixed-sized memoizer with the specified capacity and hasher.
    ///
    /// # Capacity
    ///
    /// - The number of buckets will be fixed to 1/16 of the `capacity` for the lifetime of `Self`.
    /// - The initial capacity for each bucket is 16, and will grow as needed.
    ///
    /// # Panics
    ///
    /// The `capacity` is required to be greater than or equal to `256`.
    ///
    /// # Example
    ///
    /// ```
    /// # use fett::Fett;
    /// let fett = Fett::with_capacity_and_hasher(1024, ahash::RandomState::new(), |key| *key);
    /// assert_eq!(fett.get(13), 13);
    ///
    /// ```
    pub fn with_capacity_and_hasher(capacity: usize, hasher: H, create: F) -> Fett<K, V, F, H> {
        assert!(capacity >= 256);
        let max_buckets = capacity / 16;
        let max_pairs = 16;

        let mut buckets = Vec::with_capacity(max_buckets);
        for _ in 0..max_buckets {
            let pairs = Vec::with_capacity(max_pairs);
            buckets.push(RwLock::new(pairs));
        }

        Fett {
            buckets,
            hasher,
            create,
        }
    }
}

impl<K, V, F, H> Fett<K, V, F, H>
where
    K: Eq + Hash + PartialEq,
    V: Clone,
    F: Fn(&K) -> V,
    H: BuildHasher,
{
    /// Get a value by key, blocking if the constructor has not been called for it yet.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fett::Fett;
    /// use std::{fs, sync::Arc};
    ///
    /// let fett = Fett::new(|path| {
    ///     let content = fs::read_to_string(path).expect("Missing file");
    ///     Arc::<str>::from(content)
    /// });
    /// assert_eq!(&*fett.get("/tmp/some-file"), "some file contents");
    /// ```
    ///
    /// [`Arc`]: std::sync::Arc
    pub fn get(&self, key: K) -> V {
        let bucket = self.bucket(&key);

        // Fast path: Return value if available.
        let guard = self.buckets[bucket].read();
        if let Some(value) = guard
            .iter()
            .find_map(|(k, value)| (k == &key).then_some(value))
        {
            return value.clone();
        }
        drop(guard);

        // Slow path: Acquire a write lock on the hash bucket.
        let mut write_guard = self.buckets[bucket].write();

        // Another thread may have raced for the write lock. Attempt to early return.
        if let Some(value) = write_guard
            .iter()
            .rev()
            .find_map(|(k, value)| (k == &key).then_some(value))
        {
            return value.clone();
        }

        // Run the constructor, which can take an arbitrarily long time.
        let value = (self.create)(&key);

        // Insert the value into the pairs list.
        write_guard.push((key, value.clone()));

        value
    }

    /// Remove a value by key.
    ///
    /// Returns `None` when the key does not exist.
    pub fn remove(&self, key: &K) -> Option<V> {
        let bucket = self.bucket(key);

        let mut write_guard = self.buckets[bucket].write();

        write_guard
            .iter()
            .position(|(k, _)| k == key)
            .map(|index| write_guard.swap_remove(index).1)
    }

    /// Returns `true` if the key exists in this collection.
    pub fn contains(&self, key: &K) -> bool {
        let bucket = self.bucket(key);

        let guard = self.buckets[bucket].read();

        guard.iter().any(|(k, _)| k == key)
    }

    /// Split the map into its inner hasher, `create` function, and collection of key-value pairs.
    ///
    /// The map can be reconstructed (and internally re-hashed) with [`Fett::from::<(H, F, I)>`].
    /// Even choosing a different hasher entirely.
    ///
    /// [`Fett::from::<(H, F, I)>`]: Fett#impl-From<(H,+F,+I)>-for-Fett<K,+V,+F,+H>
    pub fn into_inner(self) -> (H, F, Vec<(K, V)>) {
        let kv = self
            .buckets
            .into_iter()
            .flat_map(|lock| lock.into_inner())
            .collect();

        (self.hasher, self.create, kv)
    }

    /// Return the key's bucket index.
    fn bucket(&self, key: &K) -> usize {
        self.hasher.hash_one(key) as usize % self.buckets.len()
    }
}

impl<K, V, F, I> From<(F, I)> for Fett<K, V, F>
where
    K: Eq + Hash + PartialEq,
    V: Clone,
    F: Fn(&K) -> V,
    I: IntoIterator<Item = (K, V)>,
    <I as IntoIterator>::IntoIter: ExactSizeIterator,
{
    /// Construct a memoizing map from a `create` function and an iterator. This is primarily used
    /// in conjunction with [`Fett::into_inner`].
    ///
    /// See [`Fett::from::<(H, F, I)>`] for a correctness warning related to a possible logic bug.
    ///
    /// [`Fett::from::<(H, F, I)>`]: Fett#impl-From<(H,+F,+I)>-for-Fett<K,+V,+F,+H>
    fn from((create, iter): (F, I)) -> Self {
        let iter = iter.into_iter();
        let fett = Self::with_capacity(iter.len().max(DEFAULT_CAPACITY), create);

        for (key, value) in iter {
            let bucket = fett.bucket(&key);
            let mut write_guard = fett.buckets[bucket].write();
            write_guard.push((key, value));
        }

        fett
    }
}

impl<K, V, F, H, I> From<(H, F, I)> for Fett<K, V, F, H>
where
    K: Eq + Hash + PartialEq,
    V: Clone,
    F: Fn(&K) -> V,
    H: BuildHasher,
    I: IntoIterator<Item = (K, V)>,
    <I as IntoIterator>::IntoIter: ExactSizeIterator,
{
    /// Construct a memoizing map from a hasher, a `create` function, and an iterator. This is
    /// primarily used in conjunction with [`Fett::into_inner`].
    ///
    /// # Correctness
    ///
    /// This allows you to create a map with values that have not been produced by the `create`
    /// function. Any differences between the values in the iterator and what the `create` function
    /// would have produced had it been called is considered a logic bug in the caller.
    ///
    /// This cannot lead to memory safety issues or Undefined Behavior, but it could allow
    /// [`Fett::get`] to return unexpected values.
    fn from((hasher, create, iter): (H, F, I)) -> Self {
        let iter = iter.into_iter();
        let fett = Self::with_capacity_and_hasher(iter.len().max(DEFAULT_CAPACITY), hasher, create);

        for (key, value) in iter {
            let bucket = fett.bucket(&key);
            let mut write_guard = fett.buckets[bucket].write();
            write_guard.push((key, value));
        }

        fett
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::time::Duration;

    // Known collisions from https://github.com/pstibrany/fnv-1a-64bit-collisions
    const KEY1: &str = "7mohtcOFVz";
    const KEY2: &str = "c1E51sSEyx";

    #[test]
    fn test_create_many() {
        let counter = AtomicU8::new(0);
        let fett = Fett::new(|_| {
            std::thread::sleep(Duration::from_millis(100));

            let count = counter.fetch_add(1, Ordering::Relaxed);
            assert_eq!(count, 0);

            count
        });

        // Pummel a single key with threads to attempt to run the constructor more than once.
        [0_i32; 32].par_iter().for_each(|_| {
            assert_eq!(fett.get(0), 0);
        });
    }

    #[test]
    fn test_remove() {
        let hasher = fnv::FnvBuildHasher::default();
        assert_eq!(hasher.hash_one(KEY1), hasher.hash_one(KEY2));

        let fett = Fett::with_hasher(hasher.clone(), |_key| 0);

        assert_eq!(fett.get(KEY1), 0);
        assert!(fett.contains(&KEY1));
        assert!(!fett.contains(&KEY2));
        assert!(fett.remove(&KEY2).is_none());

        assert_eq!(fett.get(KEY2), 0);
        assert!(fett.contains(&KEY1));
        assert!(fett.contains(&KEY2));
        assert_eq!(fett.remove(&KEY2), Some(0));

        assert!(fett.contains(&KEY1));
        assert!(!fett.contains(&KEY2));
        assert!(fett.remove(&KEY2).is_none());

        let fett = Fett::with_hasher(hasher, |_key| 0);

        assert_eq!(fett.get(KEY1), 0);
        assert_eq!(fett.get(KEY2), 0);

        assert_eq!(fett.remove(&KEY1), Some(0));
        assert!(!fett.contains(&KEY1));
        assert!(fett.contains(&KEY2));
    }

    #[test]
    fn test_rehash() {
        let hasher = fnv::FnvBuildHasher::default();
        assert_eq!(hasher.hash_one(KEY1), hasher.hash_one(KEY2));

        let fett = Fett::with_hasher(hasher, |key| format!("id {key}"));

        fett.get(0);
        fett.get(13);
        fett.get(42);

        // Sanity check inner state
        assert_eq!(fett.buckets.len(), 64);
        assert!(fett.contains(&0));
        assert!(fett.contains(&13));
        assert!(fett.contains(&42));
        for i in 1000..3000 {
            assert!(!fett.contains(&i));
        }
        assert!(!fett.contains(&3000));

        // Deconstruct and rebuild the memo
        let (hasher, create, kv) = fett.into_inner();
        let fett = Fett::from((hasher, create, kv.into_iter()));

        // Inspect the inner state, should be identical to above
        assert_eq!(fett.buckets.len(), 64);
        assert!(fett.contains(&0));
        assert!(fett.contains(&13));
        assert!(fett.contains(&42));
        for i in 1000..3000 {
            assert!(!fett.contains(&i));
        }
        assert!(!fett.contains(&3000));

        // Re-hash with a larger KV data set
        let (hasher, create, mut kv) = fett.into_inner();
        kv.extend((1000..3000).map(|i| (i, create(&i))));
        let fett = Fett::from((hasher, create, kv.into_iter()));

        // Inspect the inner state for re-hashing
        assert_eq!(fett.buckets.len(), 125);
        assert!(fett.contains(&0));
        assert!(fett.contains(&13));
        assert!(fett.contains(&42));
        for i in 1000..3000 {
            assert!(fett.contains(&i));
        }
        assert!(!fett.contains(&3000));
    }
}

#[cfg(all(test, loom))]
mod loom_tests;
