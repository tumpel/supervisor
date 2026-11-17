# Rust

1. Propagate errors with `?`. In `tumpel-supervisor`, add context with `anyhow`'s `.context()` or `.with_context()`.
2. A library crate (`tumpel-channel`) returns a typed error enum that implements `std::error::Error`.
3. Never call `unwrap()`, `expect()`, `panic!`, `todo!` or `unimplemented!` outside tests. The workspace lints in `Cargo.toml` deny them.
4. A proven invariant is the one exception to rule 3. Mark that item with `#[expect(clippy::expect_used, reason = "...")]`, and name the invariant in the reason.
5. Never discard a `Result` with `let _ =`, `.ok()` or `.unwrap_or_default()`. Best-effort cleanup inside `Drop` is the one exception.
6. Parse external input with serde into a typed struct at the boundary. Never pass a `serde_json::Value` past it.
7. Use the API types from `tumpel-api`. Never hand-write one. Change them in tumpel/protocol, release it, then bump the exact `tumpel-api` pin here.
8. Silence a lint with `#[expect(lint, reason = "...")]` on the narrowest item. Only generated code carries `#[allow]`.
9. `npm run clippy` (pedantic, warnings as errors) and `npm run fmt:check` pass. `npm run fix` formats.
10. Every type the supervisor decodes from the control plane tolerates a field an older control plane lacks: the field is optional or defaulted, or the change bumps the channel protocol. `src/update/tests.rs` decodes the oldest release manifest the supervisor accepts.
