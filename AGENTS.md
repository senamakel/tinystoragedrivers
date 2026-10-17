# Repository Guidelines

This file is the single source of truth for how humans and coding agents work
in this repository. `CLAUDE.md` is a symlink to this file, so every agent reads
the same instructions.

## Project Structure

This is a Rust 2024 cargo workspace rooted at a virtual `Cargo.toml`. Every
crate lives under `crates/`, one directory per package, each directory named for
the package it holds.

```text
Cargo.toml                    # virtual workspace: members, [workspace.package],
                              # [workspace.dependencies], [workspace.lints]
clippy.toml                   # test-code allowances, doc identifier list
crates/
├── tinystoragedrivers-core/  # the ports and everything driver-independent
│   └── src/
│       ├── lib.rs            # crate docs + the entire public surface
│       ├── error/            # StorageError, ErrorKind, Result
│       ├── scope/            # Scope, the tenant key
│       ├── capabilities/     # optional driver abilities
│       ├── value/            # JSON ordering, lookup, merge patch, tokens
│       ├── filter/           # Filter, Sort, reference evaluation
│       ├── document/         # DocumentStore + types + typed extension
│       ├── stream/           # StreamStore
│       ├── blob/             # BlobStore
│       ├── backend/          # StorageBackend, ScopedStorage
│       ├── memory/           # the reference driver
│       ├── blocking/         # sync bridge (feature `blocking`)
│       └── conformance/      # the driver suite (feature `testkit`)
└── tinystoragedrivers/       # facade: StorageConfig, open(), driver features
docs/
├── specs/                    # storage-ports.md: the contract
├── plans/                    # storage-rollout.md: delivery order
└── adr/                      # immutable architecture decision records
```

### Where code goes

- A port, a value type every driver shares, or behavior every driver must agree
  on (filter evaluation, value ordering) belongs in `tinystoragedrivers-core`.
- Anything that links a database client belongs in its own driver crate,
  `crates/tinystoragedrivers-<driver>/`. The facade forwards it behind a
  feature of the same name and adds its URL form to `StorageConfig`.
- `tinystoragedrivers-core` never depends on a database client or a transport.
  CI asserts this with `cargo tree`.
- No typed repository for a particular record type (sessions, approvals) lives
  here. Those belong to the crate that owns the record.

Every driver crate runs `tinystoragedrivers_core::conformance::run` against
itself. A driver that disagrees with the memory driver is wrong unless the spec
says the behavior is driver-defined.

Add a crate by creating `crates/<name>/`. `members = ["crates/*"]` picks it up
automatically. Inherit `version`, `edition`, `rust-version`, `license`, and
`repository` from `[workspace.package]`, take shared dependencies from
`[workspace.dependencies]`, and opt into the shared lint set with:

```toml
[lints]
workspace = true
```

`tinystoragedrivers-bus` is the deliberate MSRV exception: it declares Rust
1.85 because its wire types and default compatibility client compile there. The
workspace and implementation crates remain at Rust 1.88; CI checks the bus and
its isolated consumer separately at 1.85.

Each feature area belongs in a focused module directory under a crate's `src/`.
A module root explains the module, wires its pieces together, and exposes the
smallest useful API. Move substantial type definitions into `types.rs`, and put
module-local unit tests in a sibling `mod_tests.rs` wired from the bottom of the
module root:

```rust
#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
```

OpenHuman vendors this repository and its layout check rejects inline
`mod tests { ... }` blocks and files named `test.rs` or `tests.rs`, so keep to
this form.

Keep public exports centralized in each crate's `src/lib.rs` so downstream users
have one predictable surface.

## Build And Test

Run every command from the repository root. These four are the contract; CI
runs exactly them, so a green local run should mean a green CI run.

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets --all-features
cargo test --all-features
```

Supporting commands:

- `cargo fmt --all` — format before committing.
- `cargo test <filter>` — run a focused subset while iterating.
- `cargo test -p tinystoragedrivers-core` — run one crate's suite.
- `cargo doc --no-deps --all-features` — build the rustdoc CI also builds with
  `RUSTDOCFLAGS="-D warnings"`.
- `cargo test --doc` — run doctests alone when editing documentation examples.

Never skip, ignore, or delete a failing test to make a command pass. Fix the
root cause, or stop and report the blocker.

## Coding Style

Use standard `rustfmt` output and Rust 2024 idioms. Do not hand-format around
`rustfmt`, and do not add `#[rustfmt::skip]` without a comment explaining why.

- `snake_case` for modules, files, functions, methods, fields, and locals.
- `PascalCase` for types, traits, and enum variants; `SCREAMING_SNAKE_CASE` for
  constants and statics.
- Name things for what they are, not for their layer: `RetryPolicy`, not
  `RetryHelper`.
- Prefer small, typed APIs over stringly-typed ones. Accept `&str` and generic
  `impl Into<String>` at boundaries; return owned, concrete types.
- Keep the public surface minimal: default to private, and export deliberately
  from `src/lib.rs`.
- `unsafe` is forbidden workspace-wide by `[workspace.lints]` in the root
  `Cargo.toml`. If a project genuinely needs it, relax the lint in its own
  commit and document every invariant with a `// SAFETY:` comment.

### Errors

- Every port and driver returns `tinystoragedrivers_core::Result<T>` with a
  `StorageError`. Callers branch on `ErrorKind`, so map a backend failure to the
  kind a caller can act on, and keep the backend error as the source.
- Error messages are lowercase, without trailing punctuation, and never contain
  credentials.
- Do not `unwrap()`, `expect()`, or `panic!` in library code paths. They are
  fine in tests, examples, and genuinely unreachable states — where `expect`
  must carry a message explaining the invariant.
- Document a `# Errors` section on every public fallible function and a
  `# Panics` section on anything that can panic.

### Dependencies

Adding a dependency is a design decision. Before adding one, check whether the
standard library or an existing dependency already covers the need. When you do
add one:

- pin a caret range (`serde = "1"`), not an exact version;
- enable only the features you need, with `default-features = false` when that
  meaningfully trims the tree;
- gate anything optional behind a Cargo feature, documented in `Cargo.toml`;
- declare it once in the root `[workspace.dependencies]` when more than one
  crate needs it, and take it with `{ workspace = true }`;
- never add one to `crates/tinystoragedrivers-core` that pulls in a database
  client or a transport. CI fails the build if you do;
- leave a comment above the entry explaining *why* the crate is needed and what
  uses it — see the existing entries for the expected tone;
- prefer well-maintained crates with a compatible license.

Keep `Cargo.lock` committed; this workspace ships a single lockfile so CI and
releases are reproducible.

## Testing

- Module-local unit tests live in `crates/<crate>/src/<feature>/mod_tests.rs` and
  may touch private items.
- Integration tests live in `crates/<crate>/tests/` and exercise only the public
  API — they are the regression suite for the crate's contract.
- Payload types pin their serde representation in a unit test. That
  representation is the wire form: a host and a module that disagree about a
  field name fail at runtime with a decode error.
- Use descriptive, behavioral test names: `rejects_an_empty_name`, not
  `test_greet_2`.
- Cover the failure paths, not just the happy path. Every new error variant
  needs a test that produces it.
- For async behavior, standardize on one runtime (`tokio` as a dev-dependency
  for tests) rather than mixing runtimes.
- Tests must be deterministic and independent of network, wall-clock time, and
  execution order. Gate any live/network test behind a feature or an env var and
  name it `live_*` so it is easy to exclude.
- Maintain at least 90% line coverage in every source file. Add or update tests
  with every behavior change, and note any deliberately untested edge case in
  the pull request description.

Write the test first when fixing a bug: a failing test that reproduces the
report, then the fix that turns it green.

## Documentation

Write documentation for the reader who has never seen the code.

- Every public item gets a rustdoc comment. `missing_docs` is a warning that CI
  treats as an error.
- Start every `mod.rs` and `mod_tests.rs` with a concise module-level `//!`
  description.
- Each crate's `src/lib.rs` carries its crate-level overview: what the crate
  does, the primary entry points, and a short runnable example. It should also
  say what the crate deliberately does *not* hold, and why.
- Prefer concrete examples over vague description. Doc examples are compiled and
  run by `cargo test`, so they cannot drift.
- Complex modules must include a module-level `README.md` covering their design,
  public surface, and important operational constraints.
- Keep `README.md`, `docs/`, and module docs aligned with code changes in the
  same commit that changes behavior.
- Write accepted behavior and constraints in `docs/specs/` before creating a
  linked, implementation-ordered plan in `docs/plans/`. Specs define what and
  why; plans define how and in what sequence.
- Keep every Markdown file, including this one, at 500 lines or fewer. When a
  topic outgrows that, split it into focused files and link them from the
  nearest `README.md`.

## Git Workflow

- Never commit directly to `main`. Branch first, one branch per logical change.
- Do feature work in a git worktree so the main checkout stays clean.
- Commit subjects are concise and imperative: `Add retry policy to the client`.
  Keep the subject specific to the change and under ~72 characters.
- Make small, focused commits. Each commit should cover one logical change,
  build independently, and avoid mixing formatting, refactors, and behavior
  changes unless they are inseparable.
- Never commit secrets. `.env` is git-ignored; document new variables in
  `.env.example` with placeholder values.
- Never force-push a shared branch, rewrite published history, or bypass hooks
  with `--no-verify`.

## Pull Requests

Open pull requests ready for review, not as drafts, unless the work genuinely
must not merge yet. A pull request should:

- summarize what changed and why, in a few sentences;
- call out public API or behavior changes explicitly, or state "None";
- list the validation commands actually run, with their outcome;
- link the related issue;
- include updated tests, docs, and examples in the same change.

The template in `.github/PULL_REQUEST_TEMPLATE.md` encodes this checklist.
Address review feedback by fixing it, and reply on each thread describing what
changed. Do not resolve a thread whose feedback you have not addressed or
explicitly declined with a reason.

## Releases

Releases run from `.github/workflows/release.yml` via a manual
`workflow_dispatch` with a `patch` / `minor` / `major` bump; `current` resumes
an interrupted release after its version commit and tag exist. The workflow
re-runs the full validation suite, computes the next version, updates
the root `[workspace.package]` version and `Cargo.lock`, commits and tags
`vX.Y.Z`, pushes, and creates a GitHub release. Consumers pin the tag through a
git dependency or a submodule gitlink, so there are no binary assets.

Consequently:

- Do not hand-edit the `version` field in the root `[workspace.package]`; the
  release workflow owns it. Every member inherits it with
  `version.workspace = true`, so the whole workspace releases as one version.
- Follow semantic versioning. Any change to the public surface that is not
  purely additive is a breaking change and needs a major bump (pre-1.0: a minor
  bump).
- `main` should always be green.

## Agent Working Agreement

For automated contributors specifically:

1. **Read before writing.** Inspect the surrounding module and match its
   conventions, comment density, and idiom rather than importing a house style.
2. **Verify, do not assume.** Run the four contract commands and read their
   output before reporting a task complete. Report failures with the output;
   never claim a check passed that you did not run.
3. **Stay in scope.** Implement what was asked. Do not opportunistically
   refactor, reformat, upgrade dependencies, or "fix" unrelated code — raise it
   instead.
4. **No placeholders in delivered code.** No `todo!()`, no stubbed functions, no
   commented-out alternatives left behind. If something cannot be finished, say
   so explicitly.
5. **Do not weaken the guardrails.** Never add blanket `#[allow(...)]`, relax a
   lint, mark a test `#[ignore]`, or loosen CI to get a green run. Fix the
   cause.
6. **Secrets stay out.** Never read, echo, or commit `.env` contents, tokens, or
   credentials, and never paste them into a pull request or issue.
7. **Ask only when blocked.** Make routine judgment calls yourself; escalate
   only irreversible decisions or genuine forks with no clear default.
