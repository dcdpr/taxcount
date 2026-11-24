#![forbid(unsafe_code)]
#![allow(rustdoc::private_intra_doc_links)]

pub use bdk;

pub mod basis;
pub mod cli;
pub mod client;
pub mod env;
pub mod errors;
pub mod imports;
pub mod model;
pub mod run;
pub mod util;

pub mod gitver_hashes {
    include!(concat!(env!("OUT_DIR"), "/gitver_hashes.rs"));

    #[cfg(test)]
    mod tests {
        use super::*;
        use tracing_test::traced_test;

        #[test]
        #[traced_test]
        fn print_gitvers() {
            let _ = tracing_log::LogTracer::init();

            // try: cargo test -- --nocapture
            print_all();
        }
    }
}
