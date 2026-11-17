# Errors

1. Fail fast. Let an error propagate to the caller that can act on it.
2. Catch an error only where the catch holds a concrete recovery. Never catch to log, rethrow or wrap.
3. Never swallow an error. No empty catch, no catch that returns a default, no silent fallback.
4. Never hedge a downstream call. No retries, no fallbacks, no catch-and-continue. A failed call fails the operation.
5. Never throw to branch. Return a result type for an expected outcome. Throw, or return `Err`, only for a fault that aborts the operation.
6. Validate external input once, at the boundary. External input is a request body, a channel frame, a config file, an environment variable or a third-party response. Inside the boundary, trust the parsed type.
7. Every error a user or an operator sees states what failed and the fix. A user-facing failure is never a silent no-op.
8. An auth failure (401 or 403) never becomes empty data. Only a real not-found becomes empty.
9. Every phase has a time limit and a named failure.

TypeScript specifics are in `rules/typescript.md`. Rust specifics are in `rules/rust.md`.
