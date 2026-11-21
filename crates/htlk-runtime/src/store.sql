-- Private version-1 SQLite layout, journal records use HTLK record_version 0.1.
CREATE TABLE runs (
    run_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    root_scope TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running','succeeded','failed','cancelled')),
    sequence INTEGER NOT NULL CHECK(sequence >= 1),
    owner TEXT,
    fence INTEGER NOT NULL CHECK(fence >= 0),
    lease_expires_ms INTEGER,
    eligible_at_ms INTEGER NOT NULL DEFAULT 0 CHECK(eligible_at_ms>=0),
    last_time_ms INTEGER NOT NULL CHECK(last_time_ms >= 0),
    CHECK((owner IS NULL AND lease_expires_ms IS NULL) OR
          (owner IS NOT NULL AND lease_expires_ms IS NOT NULL))
) STRICT;
CREATE INDEX running_run_ids ON runs(run_id) WHERE status='running';
CREATE INDEX ready_run_ids ON runs(eligible_at_ms,run_id) WHERE status='running';
CREATE TABLE journal (
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    sequence INTEGER NOT NULL CHECK(sequence >= 1),
    event_type TEXT NOT NULL,
    record_version TEXT NOT NULL CHECK(record_version = '0.1'),
    actor TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL CHECK(timestamp_ms >= 0),
    causation TEXT NOT NULL,
    command_id TEXT,
    payload BLOB NOT NULL,
    PRIMARY KEY(run_id,sequence)
) STRICT;
CREATE TABLE outbox (
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    span INTEGER NOT NULL DEFAULT 1 CHECK(span BETWEEN 1 AND 8 AND sequence>=span),
    pending INTEGER NOT NULL DEFAULT 1 CHECK(pending>=0 AND pending<(1 << span)),
    PRIMARY KEY(run_id,sequence),
    FOREIGN KEY(run_id,sequence) REFERENCES journal(run_id,sequence)
) STRICT;
CREATE INDEX outbox_pending ON outbox(run_id,sequence) WHERE pending<>0;
-- Each family retains its typed runtime record schema; these rows are not a
-- second graph. Immutable/final records cannot be revised by normal transitions.
CREATE TABLE checkpoint_payloads (
    payload_digest TEXT PRIMARY KEY,
    payload BLOB NOT NULL
) STRICT;
CREATE TABLE record_heads (
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    family TEXT NOT NULL,
    record_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision >= 1),
    final INTEGER NOT NULL CHECK(final IN (0,1)),
    payload_digest TEXT NOT NULL REFERENCES checkpoint_payloads(payload_digest),
    PRIMARY KEY(run_id,family,record_id)
) STRICT;
CREATE INDEX artifact_tombstones_by_id ON record_heads(record_id) WHERE family='tombstone';
CREATE VIEW records AS
    SELECT h.run_id,h.family,h.record_id,h.revision,h.final,p.payload,h.payload_digest
    FROM record_heads h LEFT JOIN checkpoint_payloads p USING(payload_digest);
CREATE TABLE commands (
    run_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    PRIMARY KEY(run_id,command_id),
    FOREIGN KEY(run_id,sequence) REFERENCES journal(run_id,sequence)
) STRICT;
