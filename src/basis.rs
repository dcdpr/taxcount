pub use self::poolasset::{Asset, AssetName, AssetNameError, PoolAsset, SplitBasisError};
pub use self::split::{Events, PriceError};
pub(crate) use self::{bucket::*, lifecycle::*, poolasset::*, split::*};
use crate::model::events::Event;
use error_iter::ErrorIter as _;
use std::{collections::HashMap, fmt::Display, rc::Rc};
use thiserror::Error;

mod bucket;
mod lifecycle;
pub(crate) mod lookup;
mod poolasset;
mod split;

#[derive(Debug, Error)]
pub enum CheckListError {
    #[error("Did not pass CheckList")]
    Failed,
}

/// The checklist is an intermediate type (as in a typestate) for handling Taxcount inputs. Used
/// after [`State`] resolution to ensure that all required inputs have been provided.
///
/// [`State`]: crate::model::checkpoint::State
pub struct CheckList {
    resolved: Vec<Event>,
    errors: Vec<PriceError>,
}

impl Display for CheckList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Basis CheckList")?;
        writeln!(f, "===== =========")?;
        writeln!(f)?;

        if self.is_passing() {
            writeln!(f, "No issues detected! 🎉")?;
        } else {
            for err in &self.errors {
                writeln!(f, "❌ {err}")?;
                for source in err.sources().skip(1) {
                    writeln!(f, "     Caused by {source}")?;
                }
            }
        }

        Ok(())
    }
}

impl CheckList {
    /// Consume resolved events and execute the checklist, separating errors from events.
    ///
    /// Prints the checklist and returns the events if there were no errors encountered. The hashmap
    /// returned maps worksheets to events (once again advancing typestate).
    pub fn execute(events: Events) -> Result<HashMap<Rc<str>, Vec<Event>>, CheckListError> {
        let (events, errors): (Vec<_>, Vec<_>) =
            events.inner.into_iter().partition(|res| res.is_ok());

        let checklist = Self {
            resolved: events.into_iter().map(|res| res.unwrap()).collect(),
            errors: errors.into_iter().map(|res| res.unwrap_err()).collect(),
        };
        println!("{checklist}");

        if checklist.is_passing() {
            let mut events = HashMap::<_, Vec<Event>>::new();
            for event in checklist.resolved.into_iter() {
                events
                    .entry(event.worksheet_name.clone())
                    .and_modify(|entry| entry.push(event.clone()))
                    .or_insert_with(|| vec![event]);
            }

            Ok(events)
        } else {
            Err(CheckListError::Failed)
        }
    }

    fn is_passing(&self) -> bool {
        self.errors.is_empty()
    }
}
