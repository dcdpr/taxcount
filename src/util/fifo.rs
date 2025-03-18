//! FIFO implemented using VecDeque
use serde::{Deserialize, Serialize};
use std::collections::vec_deque::{Drain, IntoIter, Iter, IterMut};
use std::collections::VecDeque;
use std::ops::{Index, RangeBounds};

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct FIFO<A> {
    deq: VecDeque<A>,
}

impl<A> Default for FIFO<A> {
    fn default() -> Self {
        Self {
            deq: VecDeque::new(),
        }
    }
}

impl<A> FIFO<A> {
    pub fn new() -> Self {
        Self {
            deq: VecDeque::new(),
        }
    }
    pub fn iter(&self) -> Iter<'_, A> {
        self.deq.iter()
    }
    pub fn iter_mut(&mut self) -> IterMut<'_, A> {
        self.deq.iter_mut()
    }
    pub fn drain<R>(&mut self, range: R) -> Drain<'_, A>
    where
        R: RangeBounds<usize>,
    {
        self.deq.drain(range)
    }
    pub fn append_back(&mut self, e: A) {
        self.deq.push_back(e);
    }
    pub fn pop_front(&mut self) -> Option<A> {
        self.deq.pop_front()
    }
    pub fn push_front(&mut self, e: A) {
        // a LIFO thing, used for splits
        //   (intended sequence: pop; split -> (a,b); consume a; push_front b;)
        self.deq.push_front(e);
    }
    pub fn peek_front(&self) -> Option<&A> {
        self.deq.front()
    }
    pub fn len(&self) -> usize {
        self.deq.len()
    }
    pub fn is_empty(&self) -> bool {
        self.deq.is_empty()
    }
}

impl<A> Index<usize> for FIFO<A> {
    type Output = A;

    fn index(&self, index: usize) -> &Self::Output {
        self.deq.index(index)
    }
}

impl<A> FromIterator<A> for FIFO<A> {
    // see https://docs.rs/from_iter/latest/from_iter/trait.FromIterator.html
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let iterator = iter.into_iter();
        let mut deq = FIFO::<A>::new();
        deq.extend(iterator);
        deq
    }
}

impl<A> IntoIterator for FIFO<A> {
    type Item = A;
    type IntoIter = IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.deq.into_iter()
    }
}

impl<A> Extend<A> for FIFO<A> {
    fn extend<T: IntoIterator<Item = A>>(&mut self, iter: T) {
        for item in iter.into_iter() {
            self.append_back(item);
        }
    }
}
