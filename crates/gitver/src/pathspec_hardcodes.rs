/// [rgrant 20210712 02:12 GMT] build process that makes available some git file revisions

#[cfg(not(debug_assertions))]
const PATHS: [&str; 10] = [
    ".gitignore",
    "build.rs",
    "Cargo.lock",
    "Cargo.toml",
    "bin/",
    "fixtures/",
    "crates/",
    "references/",
    "src/",
    "tools/",
];

#[cfg(not(debug_assertions))]
pub fn cargotime_emit_external_dependencies() {
    // see `cat ./target/debug/build/taxcount-*/output`
    if let (true, _, _) = super::git_is_clean(".") {
        // When the CWD is clean, rebuild only when specific file paths change.
        for path in &PATHS {
            println!("cargo:rerun-if-changed={path}");
        }
    } else {
        // When the CWD is dirty, always rebuild.
        println!("cargo:rerun-if-changed=.");
    }
}

#[cfg(not(debug_assertions))]
pub fn init_pathspecs(map: &mut super::GitverMap) {
    use walkdir::WalkDir;

    let cwd = std::env::current_dir().unwrap();

    for path in &PATHS {
        for entry in WalkDir::new(path) {
            let pathspec = entry.unwrap().into_path();

            if pathspec.is_dir() {
                continue;
            }

            let pathspec = pathspec
                .strip_prefix(&cwd)
                .unwrap_or(&pathspec)
                .to_path_buf();

            if !super::git_is_ignored(&pathspec) {
                super::add_pathspec(map, pathspec);
            }
        }
    }
}
