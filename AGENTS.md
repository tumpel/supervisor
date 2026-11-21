# Agent rules

Always follow `rules/errors.md` and `rules/coding-standards.md`.

- When writing Rust, follow `rules/rust.md`.
- When writing or changing a test, follow `rules/tests.md`.
- When committing or opening a pull request, follow `rules/commits-and-prs.md`.
- When writing text a user reads, docs, an issue or a PR body, follow `rules/writing.md`.
- No comments in code.
- The API types come from `tumpel-api`, pinned exactly. Change them in tumpel/protocol, release it, then bump the pin.
- Consume a tumpel/protocol release candidate from crates.io, pinned exactly as `tumpel-api = "=X.Y.Z-rc.N"`, with the `@tumpel/schemas` tarball of the same release. Never use a git dependency: `npm run crates:check` refuses it.

## Commands

- `npm install` installs dependencies.
- `npm run check` runs every check. `npm run fix` formats.
- `npm test` runs the Rust and release script tests. The `host-tests` feature needs KVM and runs by hand.
- `npm run supervisor:build` builds the static guest, packs the guest drive and builds the release supervisor.
- Releases are mechanical; never bump a version, tag or publish by hand. A merge to `main` that changes a shipped file publishes a GitHub pre-release `vX.Y.Z-rc.N`, versioned from the conventional commit types (breaking major, `feat` minor, `fix` and `perf` patch). Only the manual `promote` workflow ships a release candidate as the stable GitHub release `vX.Y.Z`. A rerun releases the same version.
