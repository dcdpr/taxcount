use crate::util::fifo::FIFO;
use std::collections::BTreeSet;

pub(crate) trait GetYear {
    fn get_year(&self) -> i32;
}

pub trait CheckYearsExt {
    fn get_first_year(&self) -> Option<i32>;
    fn check_years(&self, year: i32) -> Result<(), BTreeSet<i32>>;
}

/// Blanket implementation for every FIFO whose items implement `GetYear`.
impl<T> CheckYearsExt for FIFO<T>
where
    T: GetYear,
{
    fn get_first_year(&self) -> Option<i32> {
        self.peek_front().map(|row| row.get_year())
    }

    fn check_years(&self, year: i32) -> Result<(), BTreeSet<i32>> {
        let errors: BTreeSet<_> = self
            .iter()
            .filter_map(|item| {
                let item_year = item.get_year();

                (item_year != year).then_some(item_year)
            })
            .collect();

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}
