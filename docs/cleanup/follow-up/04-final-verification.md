# 04: Verify the completed cleanup

**Scope and acceptance.** Run the workspace verification suite and review the
committed diff for changes to public APIs, behavior, dependencies, or native
executable identity. Record failures and limitations with the observed evidence.

**Verification.** Run formatting, workspace check and strict Clippy across the
backend matrix, ordinary workspace tests and doctests with default and all
features, supported example checks, both PostgreSQL campaigns and the PostgreSQL-only
embedding and host example, strict rustdoc, dependency policy and notices, fresh
workspace packaging, Node validation and tests, and workflow lint. Report hosted
coverage status separately.
