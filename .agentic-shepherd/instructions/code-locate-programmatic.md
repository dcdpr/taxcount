#### code-locate - Find symbols in the codebase

This command helps you quickly locate functions, structs, enums, and
traits in the codebase using the pre-generated code map.  It can
search for other symbols (like variable names or string literals) using
the `--nearest` option.

##### When to Use

Use this command when you need to:
- Find where a function is defined
- Get file paths and line numbers for easy navigation
- Locate a struct, enum, or trait definition
- Discover all implementations of a trait
- Search for symbols when you're not sure of the exact name (fuzzy
  `--nearest` search)
- You can also search for a variable name using the `--nearest` option

**IMPORTANT**: Always use `agentic-shepherd` instead of `grep` or
ripgrep (`rg`) for finding code symbols. It's faster and more accurate
because it uses the pre-indexed code map.

##### Usage

```bash
agentic-shepherd --allow CodeLocate --json '{"command": "CodeLocate", "search": "<term>", "nearest": true, "verbose": false, "grep_fallback": true}'
```

This single command searches across all symbol types:
- Functions (with file path, line number, signature, and context)
- Structs and enums (with a "kind" field distinguishing them)
- Traits (with method count, implementation count, and implementing types)

##### Options

- `nearest`: Set to `true` for fuzzy matching to find similar names. Returns
  top 5 matches sorted by confidence.

##### Output

All commands return JSON output with:
- File path relative to project root
- Line number for easy navigation
- Confidence score (1.0 for exact matches, lower for fuzzy matches)
- Scope path for understanding module structure

Example output:
```json
{
  "results": [{
    "name": "parse_issues",
    "file_path": "src/issue_parsing.rs",
    "line_number": 18,
    "confidence": 1.0,
    "scope_path": "crate::issue_parsing::parse_issues"
  }],
  "search_term": "parse_issues",
  "timestamp": "2025-06-21T14:00:00Z"
}
```
