pub mod fifo;
pub mod year_ext;

pub struct HasSplit<A: Sized> {
    pub(crate) take: A,
    pub(crate) leave: A, // non-maybe.  consequence of split.
}
