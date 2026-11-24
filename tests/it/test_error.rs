use std::io;
use std::path::{Path, PathBuf};
use taxcount::errors::CheckpointError;
use taxcount::run::Error as RunError;
use thiserror::Error;

/// An error that stops an Integration Test before its assertions.
#[derive(Debug, Error)]
pub enum TestError {
    /// A file operation on `path` failed.
    #[error("a file operation on {path:?} failed")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// The function run() returned an error.
    #[error("run() failed")]
    Run(#[from] RunError),

    /// The name of each file in `dir` is without a run prefix in the format YYYYMMDD-.
    #[error("the files of a run are missing from {dir:?}")]
    NoFixtureRun { dir: PathBuf },

    /// Checkpoint::load() failed for the checkpoint file at `path`.
    #[error("the checkpoint file {path:?} cannot be read")]
    Checkpoint {
        path: PathBuf,
        #[source]
        source: CheckpointError,
    },
}

/// Returns a function that makes a [`TestError::Io`] for `path` from an [`io::Error`].
pub fn io_error(path: &Path) -> impl FnOnce(io::Error) -> TestError + '_ {
    move |source| TestError::Io {
        path: path.to_path_buf(),
        source,
    }
}
