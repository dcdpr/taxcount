//  [rgrant 20220220 11:47 UTC] taxcount

#![forbid(unsafe_code)]

use error_iter::ErrorIter as _;
use std::process::ExitCode;
use taxcount::{env::ENV, run::run};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::prelude::*;

fn main() -> ExitCode {
    // Initialize the tracing subscriber for instrumentation.
    // Uses the `RUST_LOG` environment var for configuration. E.g. `RUST_LOG=debug cargo run`
    // This is very useful to see the input CSV row that caused a panic.
    //
    // See: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/struct.EnvFilter.html#directives
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let term_color = ENV.term_color.is_enabled();
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_ansi(term_color))
        .with(env_filter)
        .init();

    match run(onlyargs::parse()) {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("Error: {err}");
            for source in err.sources().skip(1) {
                eprintln!("  Caused by: {source}");
            }

            ExitCode::FAILURE
        }
    }
}
