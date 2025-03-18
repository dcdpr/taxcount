//! [rgrant 20210712 02:12 GMT] build process that makes available some git file revisions
//!
//! explanation of implementation techniques
//!   see <https://stackoverflow.com/a/44407625>
//!       <https://stackoverflow.com/a/51620853>
//!       <https://bitshifter.github.io/2020/05/07/conditional-compilation-in-rust/>
//!       <https://unix.stackexchange.com/a/155077>

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    env,
    ffi::OsStr,
    fmt::{Display, Formatter},
    path::{Path, PathBuf},
    process::Command,
};

#[cfg(not(debug_assertions))]
use std::{
    fs::File,
    io::{BufReader, Error, Read},
};

mod codegen;
mod pathspec_hardcodes; // <-- all your hardcoded pathspecs

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GitHash {
    //TODO provide constructors to make these fields crate-private
    pub pathspec: PathBuf,
    pub commit: String,
    pub tree: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Default))]
pub struct GitHead {
    //TODO provide constructors to make these fields crate-private
    pub commit: String,
    pub tree: String,
}

/// Mapping pathspecs to hashes.
pub type GitverMap = BTreeMap<PathBuf, GitHash>;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GitverHashes {
    //TODO provide constructors to make these fields crate-private
    pub map: GitverMap,
    pub head: GitHead,
}

#[cfg(not(debug_assertions))]
fn githash(pathspec: impl AsRef<Path>) -> GitHash {
    let pathspec = pathspec.as_ref();

    let mut args = ["log", "--pretty=format:%H/%T", "--max-count=1"]
        .map(OsStr::new)
        .to_vec();
    args.push(pathspec.as_os_str());

    let output = Command::new("git").args(args).output().unwrap_or_else(|_| {
        let cwd = env::current_dir().unwrap();
        panic!(
            "failed to execute git log.\n  cwd: {}\n  pathspec: {:?}",
            cwd.display(),
            pathspec
        )
    });
    let pretty = String::from_utf8(output.stdout).unwrap();
    if pretty.is_empty() {
        eprintln!(
            "error: invalid (blank) git commit for\n  git log --pretty=format:%H/%T --max-count=1 {}",
            pathspec.display()
        );
        std::process::exit(1);
    }

    let (commit, tree) = pretty.split_once('/').unwrap();

    GitHash {
        pathspec: pathspec.to_path_buf(),
        commit: commit.to_string(),
        tree: tree.to_string(),
        sha256: sha256_string(pathspec).unwrap(),
    }
}

#[cfg(not(debug_assertions))]
fn git_rev_parse(args: &[&str]) -> String {
    let os_args = args.iter().map(OsStr::new);

    let output = Command::new("git")
        .args(os_args)
        .output()
        .unwrap_or_else(|_| {
            let cwd = env::current_dir().unwrap();
            panic!("failed to execute git {args:?}.\n  cwd: {}", cwd.display())
        });
    let hash = String::from_utf8(output.stdout)
        .unwrap()
        .trim_end()
        .to_string();
    if hash.is_empty() {
        eprintln!("error: invalid (blank) return value for\n  git {args:?}");
        std::process::exit(1);
    }
    hash
}

#[cfg(not(debug_assertions))]
use bitcoin_hashes::sha256::{Hash, HashEngine};

#[cfg(not(debug_assertions))]
fn sha256_digest<R: Read>(mut reader: R) -> Result<Hash, Error> {
    use bitcoin_hashes::{Hash as _, HashEngine as _};

    let mut engine = HashEngine::default();
    let mut buffer = [0; 4096];

    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        engine.input(&buffer[..count]);
    }

    Ok(Hash::from_engine(engine))
}

#[cfg(not(debug_assertions))]
fn sha256_string(pathspec: impl AsRef<Path>) -> Result<String, Error> {
    use data_encoding::HEXLOWER;

    let pathspec = pathspec.as_ref();
    let digest = sha256_digest(BufReader::new(File::open(pathspec)?))?;

    Ok(HEXLOWER.encode(digest.as_ref()))
}

/// regarding use of --porcelain see <https://unix.stackexchange.com/a/155077>
pub fn git_is_clean(pathspec: impl AsRef<Path>) -> (bool, PathBuf, String) {
    let pathspec = pathspec.as_ref();
    let cwd = env::current_dir().unwrap();

    let mut args = ["status", "--porcelain"].map(OsStr::new).to_vec();
    args.push(pathspec.as_os_str());

    let output = Command::new("git").args(args).output().unwrap_or_else(|_| {
        panic!(
            "failed to execute git status.\n  cwd: {}\n  pathspec: {}",
            cwd.display(),
            pathspec.display()
        )
    });
    let lines = String::from_utf8(output.stdout).unwrap();
    let clean = output.status.success() && lines.is_empty();

    (clean, cwd, lines)
}

pub fn git_is_ignored(pathspec: impl AsRef<Path>) -> bool {
    let pathspec = pathspec.as_ref();

    let mut args = ["check-ignore", "--quiet"].map(OsStr::new).to_vec();
    args.push(pathspec.as_os_str());

    let output = Command::new("git").args(args).output().unwrap_or_else(|_| {
        let cwd = env::current_dir().unwrap();
        panic!(
            "failed to execute git status.\n  cwd: {}\n  pathspec: {}",
            cwd.display(),
            pathspec.display()
        )
    });

    output.status.success()
}

/// nb. ignores git status on debug builds.
pub fn git_assert_clean(pathspec: impl AsRef<Path>) {
    let pathspec = pathspec.as_ref();

    let (clean, cwd, lines) = git_is_clean(pathspec);
    if clean {
        eprintln!("Git status clean.");
    } else {
        #[cfg(not(debug_assertions))]
        panic!(
            "error: Git status unclean.\n  cwd: {}\n  pathspec: {}\n  dirty:\n{}",
            cwd.display(),
            pathspec.display(),
            lines
        );
        #[cfg(debug_assertions)]
        eprintln!("warning: Git status unclean.  Allowing for debug build.\n  cwd: {}\n  pathspec: {}\n  dirty:\n{}",
                 cwd.display(), pathspec.display(), lines);
    }
}

#[cfg(not(debug_assertions))]
fn add_pathspec(map: &mut GitverMap, pathspec: PathBuf) {
    let githash = {
        eprintln!("{}", pathspec.display());

        //  do not show hashes out of sync with git
        let (clean, _, _) = git_is_clean(&pathspec);
        if clean {
            githash(&pathspec)
        } else {
            GitHash {
                pathspec: pathspec.clone(),
                commit: "------".to_string(),
                tree: "------".to_string(),
                sha256: "------".to_string(),
            }
        }
    };
    map.insert(pathspec.clone(), githash);
}

#[cfg(not(debug_assertions))]
impl Default for GitHead {
    fn default() -> Self {
        let commit = git_rev_parse(&["rev-parse", "HEAD"]);
        let tree = git_rev_parse(&["rev-parse", "HEAD:./"]);

        Self { commit, tree }
    }
}

#[cfg(not(feature = "verbose"))]
impl Display for GitverHashes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Gitver hashes are stored in any saved checkpoint.  Enable verbose feature to print.",
        )?;
        Ok(())
    }
}

#[cfg(feature = "verbose")]
impl Display for GitverHashes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let path_width: usize = 60; //results in total width < 100 columns
        let commit_width: usize = 6;
        let hash_width: usize = 6;

        writeln!(
            f,
            "git {trimmed:>path_width$} : {commit:commit_width$}/{tree:commit_width$}/{sha256:hash_width$}",
            trimmed = "",
            commit = "commit",
            tree = "tree",
            sha256 = "sha256",
        )?;

        for gh in self.map.values() {
            let pathspec = gh.pathspec.to_str().unwrap();
            let len_ps = pathspec.len();
            let len_fin = std::cmp::min(len_ps, path_width);
            let mut trimmed_path = pathspec[len_ps - len_fin..].to_string();
            if len_fin == path_width {
                trimmed_path = "...".to_string() + &trimmed_path[3..];
                //  TODO remove anything between ... and first /  [rgrant 20220418 22:18 UTC]
            }
            writeln!(
                f,
                "git {trimmed_path:>path_width$} : {commit:.commit_width$}/{tree:.commit_width$}/{sha256:.hash_width$}",
                trimmed_path = trimmed_path,
                commit = &gh.commit,
                tree = &gh.tree,
                sha256 = &gh.sha256,
            )?;
        }

        writeln!(
            f,
            "git {trimmed:>path_width$} : {head_commit:.commit_width$}/{head_tree:.commit_width$}",
            trimmed = "HEAD",
            head_commit = self.head.commit,
            head_tree = self.head.tree,
        )?;

        Ok(())
    }
}

#[cfg(debug_assertions)]
pub fn cargotime_init() {
    // calm down build.rs in debug builds.  technique:
    //   https://doc.rust-lang.org/cargo/reference/build-scripts.html#rerun-if-changed

    println!("cargo:rerun-if-changed=build.rs");
    codegen::write_mod(GitverHashes::default());
}

#[cfg(not(debug_assertions))]
pub fn cargotime_init() {
    // use full process in release builds.
    git_assert_clean(".");

    pathspec_hardcodes::cargotime_emit_external_dependencies();

    let mut gitver_hashes = GitverHashes::default();
    pathspec_hardcodes::init_pathspecs(&mut gitver_hashes.map);
    codegen::write_mod(gitver_hashes);
}
