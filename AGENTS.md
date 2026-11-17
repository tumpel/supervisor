# Agent rules

Always follow `rules/errors.md` and `rules/coding-standards.md`.

- When writing Rust, follow `rules/rust.md`.
- When writing or changing a test, follow `rules/tests.md`.
- When committing or opening a pull request, follow `rules/commits-and-prs.md`.
- When writing text a user reads, docs, an issue or a PR body, follow `rules/writing.md`.
- No comments in code.
- The API types come from `tumpel-api`, pinned exactly. Change them in tumpel/protocol, release it, then bump the pin.

## Commands

- `npm install` installs dependencies and enables the pre-commit hook.
- `npm run check` runs every check. `npm run fix` formats.
- `npm test` runs the Rust tests. The `host-tests` feature needs KVM and runs by hand.
- `npm run supervisor:build` builds the static guest, packs the guest drive and builds the release supervisor.
- A push to `main` that changes the supervisor releases the next `vX.Y.Z`; a manual run of the release workflow can bump minor or major.
