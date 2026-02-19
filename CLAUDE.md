# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GraphLite is a lightweight, embedded ISO GQL (Graph Query Language) graph database written in Rust. It follows the SQLite philosophy -- embedded, zero-configuration, serverless -- but for graph data. Storage is backed by Sled (embedded key-value store).

## Build and Development Commands

```bash
# Build
cargo build --all                  # Debug build
cargo build --all --release        # Release build

# Test
cargo test --all                   # All tests (unit + integration)
cargo test --package graphlite --lib  # Unit tests only (~2-3 sec)
cargo test test_name               # Single test by name
cargo test --package graphlite --lib ast::tests  # Tests in specific module
cargo test --test '*'              # Integration tests only
cargo test -- --nocapture          # Tests with stdout output
RUST_LOG=debug cargo test          # Tests with logging

# Integration tests (parallel, recommended)
./scripts/run_integration_tests_parallel.sh --release --jobs=8

# Lint and format
cargo fmt --all                    # Format code
cargo fmt --all -- --check         # Check formatting only
./scripts/clippy_all.sh --all      # Clippy on all targets (required before commit)
./scripts/clippy_all.sh --fix      # Auto-fix clippy suggestions
./scripts/clippy_all.sh --all --strict  # Treat warnings as errors

# Validation
./scripts/check_code_patterns.sh   # Check 11 architectural rules (~5 sec)
./scripts/validate_ci.sh --quick   # CI simulation before push (~30 sec)
./scripts/validate_ci.sh --full    # Full CI simulation (~10 min)

# CLI
cargo run --bin graphlite -- install --path ./mydb --admin-user admin
cargo run --bin graphlite -- gql --path ./mydb -u admin
cargo run --bin graphlite -- query --path ./mydb -u admin "MATCH (n) RETURN n"
```

## Workspace Structure

Four crates in the workspace:

- **graphlite** -- Core library: parser, executor, storage, catalog, sessions, transactions, query planning
- **sdk-rust** -- High-level ergonomic Rust SDK wrapping `QueryCoordinator`
- **gql-cli** -- CLI/REPL tool (binary: `graphlite`)
- **graphlite-ffi** -- C-compatible FFI layer for Python/Java bindings

Supporting directories: `bindings/` (Python/Java), `sdk-python/`, `examples/`, `benches/`, `docs/`, `scripts/`

## Architecture

### Query Pipeline

```
User Input -> QueryCoordinator -> Parser/Lexer -> AST -> Planner -> Executor -> StorageManager -> Sled
```

### Key Architectural Rules

`QueryCoordinator` is the **only public API entry point**. All internal modules (`ast`, `exec`, `storage`, `catalog`, `session`, `txn`, `cache`, `plan`, `schema`, `types`, `functions`) are `pub(crate)`.

Public re-exports from `graphlite`: `QueryCoordinator`, `QueryResult`, `QueryInfo`, `QueryPlan`, `QueryType`, `Row`, `SessionMode`, `Value`.

### Critical Rules (Enforced by `check_code_patterns.sh` and pre-commit hooks)

1. Never create new `ExecutionContext` instances -- pass existing ones
2. Never create new `StorageManager` during query execution -- singleton
3. Use read locks for reads, write locks only for writes
4. Get `CatalogManager` from session context, don't create new instances
5. Don't create new Tokio runtimes repeatedly
6. Access fields directly in helper methods, not recursively
7. Check if in async context before using `block_on()`
8. Fix bugs in code, not in tests (unless the test itself is wrong)
9. Use global `SessionManager`, don't create test-specific instances
10. Use `QueryCoordinator` as the only public API
11. No emojis in markdown documentation files

### Session Modes

- **Instance mode** (default) -- each `QueryCoordinator` has isolated session pool (for embedded use)
- **Global mode** -- shared process-wide session pool (for server/daemon use)

## Testing Patterns

Integration tests live in `graphlite/tests/`. Two test fixture types:

- **`TestFixture`** -- creates isolated temp database via `QueryCoordinator::from_path()`, generates unique schema names for test isolation. Preferred for most tests.
- **`CliFixture`** -- spawns actual CLI processes, tests full end-to-end behavior through the binary.

Both are in `graphlite/tests/testutils/`.

## Commit Conventions

**ALWAYS** use conventional commits: `type(scope): message`

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `perf`, `chore`
