# Tests

1. Rust tests run with `cargo test --workspace` (`npm test`).
2. CI runs the full suite on every pull request.
3. A test proves behaviour that is hard to get right without it: a parser, a limit, an ordering, a state machine.
4. Every bug fix ships with a regression test that fails before the fix and passes after it.
5. Never mock a library. Spy on one operation of the real client. End-to-end tests use no mocks.
6. Never test what the typecheck or the linter catches: a shape, a type, an export, a required field.
7. Never test a code path that no real input reaches.
8. A unit test feeds its own small inputs. It never reads live config.
9. Keep the input and the expected output visible in the test body. Repeat setup instead of hiding it in a helper. When fixture code is more than half the test, simplify the code under test.
10. Never skip a test. Fix it or delete it. Delete a low-value test when it breaks.
