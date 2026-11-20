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
