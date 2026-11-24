//! Functions that compare the output files of a test run with the output files of a fixture.

use crate::test_error::{io_error, TestError};
use similar_asserts::assert_eq;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use taxcount::model::Checkpoint;

/// If a test run wrote files that are different from the last run in `fixture_dir`, the test fails.
/// The name of each file of the test run is in `run_dir` and starts with `run_prefix`.
///
/// For a RON file, assert_eq_runs() reads the two files as checkpoints and calls
/// [`Checkpoint::assert_eq_checkpoint()`].  For each other file, assert_eq_runs() compares
/// the lines of the two files.
pub fn assert_eq_runs(
    fixture_dir: &Path,
    run_dir: &Path,
    run_prefix: &str,
) -> Result<(), TestError> {
    let fixture_prefix = last_prefix(fixture_dir)?;
    let fixture_names = unprefixed_file_names(fixture_dir, &fixture_prefix)?;
    let run_names = unprefixed_file_names(run_dir, run_prefix)?;
    assert_eq!(fixture: &fixture_names, run: &run_names);

    for name in &fixture_names {
        let fixture_path = fixture_dir.join(format!("{fixture_prefix}{name}"));
        let run_path = run_dir.join(format!("{run_prefix}{name}"));
        if Path::new(name).extension().is_some_and(|ext| ext == "ron") {
            load_checkpoint(&fixture_path)?.assert_eq_checkpoint(&load_checkpoint(&run_path)?);
        } else {
            assert_eq!(
                fixture: read_lines(&fixture_path)?,
                run: read_lines(&run_path)?,
                "{name} is different from {}",
                fixture_path.display(),
            );
        }
    }

    Ok(())
}

/// If `name` starts with a date in the format YYYYMMDD and a hyphen, returns that date.
fn run_date(name: &str) -> Option<&str> {
    let date = name.get(..8)?;
    let is_date = date.bytes().all(|byte| byte.is_ascii_digit());

    (is_date && name[8..].starts_with('-')).then_some(date)
}

/// Returns the prefix `YYYYMMDD-` of the last run in `dir`.  The name of each file of a run in `dir`
/// starts with the date of that run.
fn last_prefix(dir: &Path) -> Result<String, TestError> {
    let mut dates = BTreeSet::new();
    for entry in fs::read_dir(dir).map_err(io_error(dir))? {
        let name = entry.map_err(io_error(dir))?.file_name();
        if let Some(date) = run_date(&name.to_string_lossy()) {
            dates.insert(date.to_string());
        }
    }

    dates
        .pop_last()
        .map(|date| format!("{date}-"))
        .ok_or_else(|| TestError::NoFixtureRun {
            dir: dir.to_path_buf(),
        })
}

/// Returns the name of each file in `dir` that starts with `prefix`, without `prefix`.
fn unprefixed_file_names(dir: &Path, prefix: &str) -> Result<BTreeSet<String>, TestError> {
    let mut found = BTreeSet::new();
    for entry in fs::read_dir(dir).map_err(io_error(dir))? {
        let entry = entry.map_err(io_error(dir))?;
        let is_file = entry
            .file_type()
            .map_err(io_error(&entry.path()))?
            .is_file();
        if let Some(name) = entry.file_name().to_string_lossy().strip_prefix(prefix) {
            if is_file {
                found.insert(name.to_string());
            }
        }
    }

    Ok(found)
}

/// Reads the checkpoint file at `path`.
fn load_checkpoint(path: &Path) -> Result<Checkpoint, TestError> {
    Checkpoint::load(path).map_err(|source| TestError::Checkpoint {
        path: path.to_path_buf(),
        source,
    })
}

/// Returns the lines of the file at `path`, with one newline between each two lines.  The line
/// ends of the file and its last newline do not change the result.
fn read_lines(path: &Path) -> Result<String, TestError> {
    let text = fs::read_to_string(path).map_err(io_error(path))?;

    Ok(text.lines().collect::<Vec<_>>().join("\n"))
}
