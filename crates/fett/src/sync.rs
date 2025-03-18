#[cfg(not(loom))]
pub(crate) use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

#[cfg(loom)]
pub(crate) use loom::sync::{RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug)]
pub(crate) struct RwLock<T>(
    #[cfg(not(loom))] parking_lot::RwLock<T>,
    #[cfg(loom)] loom::sync::RwLock<T>,
);

impl<T> RwLock<T> {
    pub(crate) fn new(value: T) -> Self {
        #[cfg(not(loom))]
        let lock = parking_lot::RwLock::new(value);
        #[cfg(loom)]
        let lock = loom::sync::RwLock::new(value);

        Self(lock)
    }

    pub(crate) fn read(&self) -> RwLockReadGuard<'_, T> {
        let guard = self.0.read();

        #[cfg(loom)]
        let guard = guard.unwrap();

        guard
    }

    pub(crate) fn write(&self) -> RwLockWriteGuard<'_, T> {
        let guard = self.0.write();

        #[cfg(loom)]
        let guard = guard.unwrap();

        guard
    }

    #[allow(clippy::let_and_return)]
    pub(crate) fn into_inner(self) -> T {
        let inner = self.0.into_inner();

        #[cfg(loom)]
        let inner = inner.unwrap();

        inner
    }
}
