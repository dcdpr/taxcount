# Module Declaration Architecture

This document explains Rust module architecture when a project has both a binary (main.rs) and a library (lib.rs).

## Motivation

Integration tests in `tests/` can only import from a library crate, not from a binary. Every project should have integration tests (see [integration-testing-guide.md](integration-testing-guide.md)), so every project needs `lib.rs`.

Additional benefits of this architecture:
- **API control**: lib.rs decides what's public, preventing accidental dependencies on internals
- **Stability**: public API can remain stable while internals change
- **Flexibility**: binary can use internal APIs without exposing them
- **Documentation**: lib.rs serves as living documentation of the public API
- **Testing integrity**: integration tests verify the public contract works

This document explains the module architecture that results from having both `main.rs` and `lib.rs`.

## Overview

The standard Rust pattern is:
- **main.rs** has direct, privileged access to all modules
- **lib.rs** defines a curated public API for external consumers
- **tests/** access the crate through the public API only

Both main.rs and lib.rs declare the same modules, but with different visibility and purposes.

## The Pattern

### src/lib.rs - Public API Definition

**Option A: Selective exports with `pub use`**

```rust
mod config;  // private module
mod parser;  // private module

// Re-export only specific types
pub use config::Config;
pub use parser::Parser;
```

**Option B: Expose entire modules with `pub mod`**

```rust
pub mod config;
pub mod parser;
```

Use Option A for fine-grained control over your public API. Use Option B if you don't want to micro-manage exports.

**Purpose**: Define what external crates and integration tests can access.

**Characteristics**:
- Controls exactly what implementation details are exposed
- Creates a clean, documented public API

### src/main.rs - Binary with Direct Access

```rust
// Declare modules as private to the binary
mod config;
mod parser;
mod commands;
// ... etc

// Import specific items directly from modules
use config::Config;
use parser::Parser;
// ... etc
```

**Purpose**: Provide the binary executable with full access to all functionality.

**Characteristics**:
- Uses private `mod` declarations (no `pub`)
- Has direct access to all module internals
- Can access private functions and types not exposed in lib.rs
- Independent of lib.rs - doesn't import the library

### tests/ - External Integration Tests

```rust
// Import from the crate's public API
use my_crate::Config;
use my_crate::Parser;
use my_crate::commands::run_command;
```

**Purpose**: Test the crate as an external consumer would use it.

**Characteristics**:
- Must use the crate name (`my_crate::`) to import
- Only has access to items made public in lib.rs
- Cannot access private functions or types
- Tests the public API contract

## Key Insight: Separate Module Trees

The crucial understanding is that **main.rs and lib.rs create separate module trees**:

```
main.rs binary:
  ├─ mod config (private, direct access)
  ├─ mod parser (private, direct access)
  └─ mod commands (private, direct access)

lib.rs library:
  ├─ pub mod config (public, controlled access)
  ├─ pub mod parser (public, controlled access)
  └─ pub mod commands (public, controlled access)
```

Both trees point to the same source files in `src/`, but provide different access patterns.

## Access Levels Comparison

| Location | Access Pattern | Visibility |
|----------|---------------|------------|
| main.rs | `use config::Config` | Full access to module internals (public and private) |
| lib.rs | `pub use config::Config` | Only exposes what it explicitly marks as `pub` |
| tests/ | `use my_crate::Config` | Only what lib.rs makes public |
| External crates | `use my_crate::Config` | Only what lib.rs makes public |

## Example: Private vs Public

Consider a hypothetical private helper function in `src/parser.rs`:

```rust
// In src/parser.rs
pub struct Parser { /* ... */ }

// Public function
pub fn parse(input: &str) -> Result<Parser, Error> { /* ... */ }

// Private helper (not marked pub)
fn validate_format(input: &str) -> bool { /* ... */ }
```

**Access from main.rs**:
```rust
use parser::{Parser, parse, validate_format}; // ✅ All accessible
```

**Access from lib.rs**:
```rust
pub use parser::{Parser, parse}; // ✅ Can re-export public items
// validate_format is not accessible here
```

**Access from tests**:
```rust
use my_crate::{Parser, parse}; // ✅ Only public API
// validate_format is not accessible from tests
```

## Common Misconception

**WRONG**: "main.rs imports functionality from lib.rs"

**CORRECT**: "main.rs and lib.rs both independently declare and access the same underlying modules in src/"

main.rs does NOT depend on lib.rs. They are sibling targets that happen to share source files.

## Critical Gotcha: Module Declaration Sync

**IMPORTANT**: Because main.rs and lib.rs independently declare modules, they must both declare any module that source files depend on via `crate::`.

**Example of the problem**:
- lib.rs declares: `pub mod utils;`
- main.rs is missing: `mod utils;`
- Files like `src/parser.rs` use: `use crate::utils::Helper;`
- Build error when compiling the binary:
  ```
  error[E0432]: unresolved import `crate::utils`
   --> src/parser.rs:8:12
    |
  8 | use crate::utils::Helper;
    |            ^^^^^
    |            |
    |            unresolved import
    |            help: a similar path exists: `my_crate::utils`
  ```

**Key diagnostic clues**:
1. The error suggests using `my_crate::utils` instead of `crate::utils`
2. Building with `cargo build --lib` succeeds, but `cargo build` (binary) fails

These clues indicate:
- The module exists in the library crate (lib.rs has declared it)
- But it doesn't exist in the current binary crate tree (main.rs hasn't declared it)

When a source file uses `crate::some_module`, the `crate::` refers to:
- The lib.rs module tree when building the library
- The main.rs module tree when building the binary

**Solution**: Ensure both main.rs and lib.rs declare all modules that are referenced via `crate::` anywhere in the codebase.

**Note**: Rust's build system is reliable - if you see a build error, it's a real code error, not a build artifact. You never need to run `cargo clean` to fix spurious build errors. If the compiler says a module is missing, the module declaration is actually missing.
