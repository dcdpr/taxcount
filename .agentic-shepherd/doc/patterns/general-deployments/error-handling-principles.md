# Error Handling Principles

## Core Philosophy

Error types are foundational: design the error hierarchy FIRST, and every
function returns Result with these types. You cannot write correct code
without understanding the error architecture.

### The Explicitness Principle

**Define failure modes with typed errors, not Option.** When a function cannot
fulfill its purpose, return `Err` with a specific error type that describes what
went wrong. Callers decide how to handle the failure — retrying, falling back,
or propagating with `?`. When an error propagates all the way to `main()`, it
halts the program (see The Single Exit Principle below).

`Option` is appropriate when absence is expected and informative — such as a
search returning no results for user-provided input.

### The Single Exit Principle

**Only main() prints errors.** The role of main() is to eprintln the error with
its source chain. No other level should eprintln—use tracing for diagnostics.
Errors propagate up via `?` until main() handles them.

```rust
use error_iter::ErrorIter as _;  // provides .sources() method

fn main() -> ExitCode {
    match run() {
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

fn run() -> Result<(), Error> {
    // All application logic here, propagating errors with ?
    let config = load_config()?;
    let result = process(&config)?;
    output(result)?;
    Ok(())
}
```

### The Type-Safety Principle

**Use real types, not strings.** Error data should use PathBuf for paths,
ErrorKind for IO errors, numeric types for counts. Stringly-typed errors
that encode information in formatted strings are forbidden.

```rust
// Bad: information hidden in strings
#[error("Failed to read file: {0}")]
ReadFailed(String),

// Good: structured data
#[error("Failed to read {path:?}: {source}")]
ReadFailed {
    path: PathBuf,
    #[source]
    source: IoError,
},
```

### The Self-Describing Principle

**Errors carry structured data to explain themselves.** Each error variant
includes typed fields that provide context: affected paths, error kinds,
failure reasons. This enables proper error handling and meaningful user
messages without string parsing.

## onlyerror DSL

This codebase uses `onlyerror`, NOT `thiserror`. Key syntax differences:

| Feature | onlyerror | thiserror |
|---------|-----------|-----------|
| Struct errors | No (enums only) | Yes |
| `#[error(transparent)]` | No | Yes |
| Display fallback | Doc comments | None |
| Compile time | Faster | Slower |

**DSL patterns:**

```rust
use onlyerror::Error;

#[derive(Debug, Error)]
pub enum MyError {
    /// IO error during file read
    // ^ Doc comment becomes Display text (no interpolation)
    Io(#[from] std::io::Error),

    #[error("Parse failed at line {line}: {reason}")]
    // ^ Use #[error] when you need interpolation
    Parse { line: usize, reason: String },

    #[error("Invalid value: {0}")]
    // ^ {0} for tuple variant fields
    Invalid(String),
}
```

**Key rules:**
- Use doc comments (`///`) when no interpolation needed
- Use `#[error("...")]` only when you need field interpolation
- `#[from]` derives From impl for automatic conversion
- `#[source]` marks the underlying cause without deriving From

## Error Architecture Patterns

### 1. Top-Level Aggregator

The main entry point aggregates all module errors:

```rust
#[derive(Debug, Error)]
pub enum Error {
    /// Argument parsing error
    Args(#[from] ArgsError),

    /// Configuration error
    Config(#[from] ConfigError),

    /// IO error with context
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: IoError,
    },
}
```

### 2. Module-Specific Errors

Each module defines its own error type:

```rust
#[derive(Debug, Error)]
pub enum CommandError {
    /// IO error reading/writing file
    #[error("IO error: {0}")]
    Io(#[from] IoError),

    /// Validation failed
    #[error("Validation error: {0}")]
    Validation(#[from] ValidationError),
}
```

### 3. Layered Logic/IO Split

Separate planning errors (pure logic, testable) from execution errors (I/O):

```rust
/// Logic layer errors - no I/O operations
#[derive(Debug, Error)]
pub enum PlanError {
    /// Required configuration missing
    #[error("Config key not found: {0}")]
    ConfigMissing(String),

    /// Invalid parameter combination
    #[error("Invalid parameters: {0}")]
    InvalidParams(String),
}

/// Execution layer errors - I/O operations only
#[derive(Debug, Error)]
pub enum ExecutionError {
    /// Failed to write output file
    #[error("Failed to write {path:?}: {source}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: IoError,
    },

    /// Failed to create directory
    #[error("Failed to create directory {path:?}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: IoError,
    },
}
```

### 4. Hierarchical Categories

For complex operations, group errors by category:

```rust
#[derive(Debug, Error)]
pub enum OperationError {
    #[error("Operation failed: {0}")]
    Operation(#[source] OperationKindError),

    #[error("System error: {0}")]
    System(#[source] SystemError),

    #[error("Data error: {0}")]
    Data(#[source] DataError),
}

#[derive(Debug, Error)]
pub enum OperationKindError {
    #[error("Required resource missing: {0:?}")]
    ResourceMissing(PathBuf),

    #[error("Permission denied: {0:?}")]
    PermissionDenied(PathBuf),
}

#[derive(Debug, Error)]
pub enum DataError {
    #[error("Failed to parse at line {line}: {reason}")]
    Parse { line: usize, reason: String },

    #[error("Invalid format: {0}")]
    InvalidFormat(String),
}
```

## Decision Framework

When adding error handling, ask:

1. **Result or Option?**
   - Can the function fail in ways the caller needs to understand? → `Result<T, E>` with a specific error type
   - Is absence a normal, expected outcome (e.g., search with no matches)? → `Option<T>` is fine

2. **What type should the error be?**
   - Use typed fields, not strings
   - Include all context needed for debugging
   - Consider if it should be a new variant or fit an existing one

3. **Where does it belong in the hierarchy?**
   - Logic error (testable without I/O) → PlanError or similar
   - I/O error → ExecutionError or similar
   - Domain-specific → Module's own error type
