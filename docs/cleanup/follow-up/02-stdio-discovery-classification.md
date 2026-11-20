# 02: Cover stdio discovery classifications

**Scope.** Distinguish complete discovery of a missing operation from a transport
closing before the final inventory reply, without relying on the database or
scheduler. Keep the existing durable E2E assertions.

**Acceptance.** Through the public native adapter, a valid empty final tool page
returns `McpDescriptor`, while EOF before that reply returns `McpTransport`.
The local peer observes the requested cursor in both cases and no tool call.
Trigger each outcome at a protocol boundary rather than after a sleep.

**Verification.** Run `mcp_transport`, strict all-target lint for the affected
test and the complete `deep_e2e` target on SQLite and PostgreSQL 16 and 18.
Repeat `deep_e2e` on PostgreSQL 18 and check the native fixture.

## Results

The database-free test lives in `deep_e2e` to reuse its independent Node peer and
request trace. It connects through the public native adapter, observes both
inventory requests and the exact cursor, and asserts that no call was sent.
The EOF peer exits immediately after logging the second request. An initial
fixture that ended Node's stdout stream without exiting left the pipe open and
produced a timeout; the final fixture produces the intended EOF at the same
protocol checkpoint. No production runtime or native identity change was needed.

| Check | Result |
| --- | --- |
| `cargo test -p htlk-runtime --locked --test mcp_transport` | 13 passed. |
| `cargo test -p htlk --locked --test deep_e2e` | 9 passed on SQLite. |
| `HTLK_TEST_BACKEND=postgres cargo test -p htlk --all-features --locked --test deep_e2e` | 9 passed on PostgreSQL 16.15, and 9 passed on each of two consecutive PostgreSQL 18.6 runs. |
| `cargo clippy -p htlk --all-features --test deep_e2e --locked -- -D warnings`, `cargo fmt --all -- --check`, and `node --check tests/support/mcp_peer.mjs` | Passed. |
| `node tools/validate-executable-fixtures.mjs` | Passed; the native fixture fingerprint is unchanged. |
