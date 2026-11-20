# Performance evaluation

The [release-mode driver](../examples/performance/main.rs) runs actual compiler,
runtime, and durable storage paths. The [campaign runner](../tools/performance.py)
runs one trial at a time and keeps raw CSV, stderr, per-trial JSON, and a context
file. Keep results per backend and hardware configuration.

## Reproduce a campaign

On macOS or Linux, use Python 3.11+, Rust, and Node.js for the local MCP
fixtures. For PostgreSQL, start a disposable database reachable only on loopback:

```console
docker run --rm -d --name htlk-perf --publish 127.0.0.1:55432:5432 --env POSTGRES_PASSWORD=htlk-perf --env POSTGRES_DB=htlk_perf postgres:18
export HTLK_PERF_POSTGRES_URL='host=127.0.0.1 port=55432 user=postgres password=htlk-perf dbname=htlk_perf sslmode=disable'
cargo build --locked --release --all-features --example performance
python3 tools/performance.py --suite smoke --trials 1 --output target/perf-smoke --postgres-container htlk-perf
python3 tools/performance.py --suite baseline --trials 3 --output target/perf-baseline --postgres-container htlk-perf
```

Use `--backend sqlite` to run without a database server, or `--backend postgres`
to test a dedicated deployment. Each trial initializes a fresh SQLite file or
PostgreSQL schema. SQLite scratch files are removed at exit. Removing the disposable
container removes the PostgreSQL test schemas. The optional `--postgres-container`
queries PostgreSQL statistics through `docker exec`; those statistics require an
otherwise idle database. The tool does not log the connection string.

The baseline sweeps worker and coordinator contention, scheduler polling, mixed
history reads, chain and fan-out/join shapes, loop feedback, host replies, distinct
16 KiB artifacts, fixed-rate arrivals, and native MCP transport with and without
a fixed peer delay. MCP workloads use the independent local Node peer and validate one
dispatch per run. The extended suite adds larger shapes, longer history, cache and
observer sensitivity, an empty-engine polling check, and shutdown/reopen at a
durable wait. Select cases with
`--cases` and use `--runs` to shorten a smoke/profile trial or extend an arrival-rate
trial. An overloaded trial can fail with the graph's ordinary execution deadline;
the runner records the failure, continues the sweep, and exits nonzero.

To run one workload with custom settings:

```console
target/release/examples/performance --help
target/release/examples/performance --backend sqlite --shape chain --nodes 8 --runs 256 --concurrency 16
target/release/examples/performance --backend postgres --runs 120 --rate 2 --observe-ms 10
```

## How to interpret the measurements

- **Latency** is wall time from planned arrival to first observation of a durable
  terminal header. A separate connection reads one bounded committed change feed
  and checks candidate root headers. This includes observation delay and actual
  storage contention. The journal timestamps give approximate initialization-to-lease
  and lease-to-terminal splits, not exact commit timing. The run CSV contains, in
  order: index, planned/sent/acknowledged/observed microseconds relative to the trial,
  both journal phase durations in milliseconds, and final journal sequence.
- **Scaling** uses both a fixed number of outstanding requests (closed loop) and
  fixed-rate arrivals that keep arriving while work is queued (open loop). Compare
  backlog at mid-offer and end-of-offer, completions/sec during the offer, and drain
  time. A short trial brackets saturation; establish a sustained service rate with
  longer repeated trials and a tail-latency/error budget. At very high rates, check
  `submit_lag_us` to detect a load generator falling behind its own schedule.
- **Correctness** is checked after the timed window: distinct run IDs, expected
  artifact values for every measured run, sampled bounded journal replay matching
  committed state, and idempotent start retries. The recovery case records lease
  wait, reopen-to-higher-fence, and reopen-to-success separately. It verifies that
  a fresh runtime accepts the same durable wait identity and publishes the expected
  reply. The existing crash campaigns qualify forced process failures.
- **Efficiency**: CPU and peak RSS cover the entire HTLK driver process, including
  warm-up, compile samples, observer, and verification. Operating-system child
  accounting can include reaped peer processes; it does not include the database
  server. SQLite growth is the net checkpointed file size.
  PostgreSQL schema bytes include tables, indexes, and bootstrap pages; WAL and
  transaction counters cover the whole isolated database process window. Watch
  both persisted size and WAL, since retained bytes and write traffic answer
  different questions. These are not allocator attribution or timed-phase CPU metrics.
- **Inspection** measures a pinned 32-header page and selected snapshot open after
  warm-up and at the end, and repeats the early pin after history grows. Replay is
  sampled separately; the mixed workload has independent discovery consumers.
  Compare pages with the same row counts when attributing historical-size effects.

Use repeated trials rather than one warm-up and one point estimate. Keep the
production durability settings, record storage/VM placement and PostgreSQL settings,
and vary observer frequency separately before assigning observed latency to the
engine. For a database profile, start the disposable PostgreSQL container with
`-c shared_preload_libraries=pg_stat_statements -c track_io_timing=on`, install the
`pg_stat_statements` extension in `htlk_perf`, then add `--postgres-statements`.
The runner resets statement counters before that trial. The server executor times
include lock waits during timed statements but do not capture all client/server
round trips. For runtime CPU attribution, attach a sampling profiler to the release
driver during a long trial and inspect the database process separately.

## Local baseline: `a3c751a`, 2026-11-19

Release profile, Rust 1.98.0, Apple M4 Max (14 cores, 36 GiB), macOS 26.6.2.
SQLite used a file on the local APFS volume with WAL and `synchronous=FULL`.
PostgreSQL 18.6 ran inside Docker Desktop (ARM64), reached over a localhost
published port, with `fsync`, `synchronous_commit`, and `full_page_writes` on and
128 MiB of shared buffers. These results characterize this deployment; repeat on
the intended PostgreSQL host/network before setting production limits. Latencies
are completion-observation times, rounded. The tabulated samples exclude warm-up.

### Durable one-node execution

| Backend | Load and scheduler | Runs | Runs/s | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| SQLite | 1 outstanding, 2 workers | 256 | 44.6 | 23 ms | 35 ms | 37 ms |
| SQLite | 16 outstanding, 2 workers | 512 | 134.2 | 122 ms | 179 ms | 187 ms |
| SQLite | 16 outstanding, 4 workers | 512 | 144.4 | 111 ms | 177 ms | 177 ms |
| SQLite | 16 outstanding, 2 coordinators | 512 | 123.5 | 99 ms | 334 ms | 411 ms |
| SQLite | 16 outstanding, 2 ms polling | 512 | 109.8 | 156 ms | 211 ms | 222 ms |
| PostgreSQL | 1 outstanding, 2 workers | 24 | 2.68 | 370 ms | 400 ms | 405 ms |
| PostgreSQL | 16 outstanding, 2 workers | 48 | 0.64 | 24.74 s | 25.62 s | 25.72 s |
| PostgreSQL | 16 outstanding, 2 ms polling | 48 | 0.58 | 27.68 s | 27.96 s | 28.30 s |
| PostgreSQL | 16 outstanding, 2 coordinators | 48 | 1.43 | 8.46 s | 20.46 s | 21.21 s |

In these trials, raising concurrency to 16 helped SQLite throughput and hurt
PostgreSQL throughput. Reducing the scheduler interval from 20 ms to 2 ms hurt
loaded goodput on both. With two PostgreSQL discovery readers added at 16 outstanding,
an execution deadline expired; repeat that result under controlled background load
before assigning the entire effect to the readers. The PostgreSQL eight-node chain
and eight-way fan-out also exceeded their 60-second graph deadline in trials with
up to 12 outstanding runs. Three-run follow-ups at two outstanding completed, with
observed maxima of 5.47 s for the chain and 8.91 s for the fan-out. Small PostgreSQL
samples make the reported p99 an observed maximum, not a robust estimate of
production tails.
Two follow-up SQLite trials gave 44.2–45.8 runs/s at one outstanding and
141.2–143.5 runs/s at 16 outstanding. A separate PostgreSQL serial profile measured
2.62 runs/s.

### Fixed-rate capacity and resource costs

| Backend | Offered runs/s | Completions/s during offer | Backlog at midpoint → end | p95 | Drain |
| --- | ---: | ---: | ---: | ---: | ---: |
| SQLite | 50 | ~50 | 1 → 1 | 25 ms | 0.02 s |
| SQLite | 75 | 74–75 | 3 → 2–4 | 38 ms | 0.03–0.04 s |
| SQLite | 100 | 98–99 | 3 → 5–9 | 41–70 ms | 0.04–0.06 s |
| SQLite | 125 | 100–102 | 5 → 115–127 | 1.27–1.46 s | 1.26–1.39 s |
| SQLite | 150 | ~96 | 54 → 272 | 4.55 s | 3.69 s |
| SQLite | 300 | ~71 | 475 → 1,145 | 26.27 s | 26.00 s |
| SQLite | 600 | Trial failed on an execution deadline | — | — | — |
| PostgreSQL | 2 | 2.0 | 1 → 1 | 386 ms | 0.35 s |
| PostgreSQL | 4 | 0.8 | 6 → 13 | 10.75 s | 9.96 s |

The SQLite 75–125 rates were each repeated twice over five-second offer windows.
Backlog stayed small around 75–100 runs/s and grew substantially at 125 runs/s.
The PostgreSQL rate points were 16-run trials: 2 runs/s stayed on pace, while
4 runs/s saturated this setup. Neither result establishes a sustained production
capacity or latency SLO.

The 512-run SQLite workload retained 24.0 MiB including eight warm-up runs
(about 47 KiB per total run) and the driver used 4.64 CPU seconds and peaked at
30.8 MiB RSS. The 48-run PostgreSQL workload (plus four warm-up runs) had a
4.52 MiB schema including bootstrap overhead, 8.31 MiB of WAL over the process
window, and 47,476 database commits (including observer, control, and verification
transactions). The driver used 7.86 CPU seconds and peaked at 24.5 MiB RSS;
that excludes PostgreSQL server CPU and memory. The serial PostgreSQL trial had
6,155 commits for 24 runs plus four warm-up runs. These counters are for the whole
process window, so the difference is evidence of transaction amplification rather
than an exact count for the execution step alone.

### Compiler, history, and workflow shape

Warm medians on the host were 0.33 ms for the one-node source, 0.83 ms for an
eight-node chain, and 1.14 ms for the eight-way/join source. The worked decision
brief compiled in 7.71 ms standalone and 7.82 ms from its equivalent bundle
(including bundle parsing). One-node canonical document encoding took 13 µs;
independent executable verification took 9.52 ms and first registration took
10.28 ms on SQLite and about 21 ms on PostgreSQL. These are separate operations.

At 16 outstanding, the SQLite eight-node chain achieved 29.9 runs/s (777 ms p95),
an eight-way fan-out plus join achieved 17.4 runs/s (1.53 s p95), and a three-step
guarded loop achieved 25.1 runs/s (903 ms p95). The SQLite 16 KiB artifact case
grew to 16.4 MiB for 264 total runs (about 64 KiB/run); it should be compared with
the 16-byte case by storage footprint, since the two short throughput trials had
different run counts and storage conditions. In the parallel one-node cases,
median 32-header historical discovery took 0.24 ms on SQLite and 10.46 ms on
PostgreSQL, opening one pinned header took 0.02 ms and 1.28 ms respectively,
and one-run journal read plus replay took 0.12 ms and 2.63 ms.

The SQLite native MCP trials completed and verified 24 measured calls per
transport and delay setting. Without added peer delay, HTTP JSON and SSE both
had about 69 ms p95 and 65–66 runs/s at four outstanding. The 20 ms peer delay
increased p95 to 115 ms for HTTP JSON and 124 ms for SSE. The local stdio case
was slower (231 ms p95 without added delay); it includes starting a Node process
for each fresh run session. These measurements cover a single call per run over
a local fixture, not provider latency or long-lived session reuse.

Across three restart samples per backend, the median reopen-to-higher-fence was
13 ms on SQLite and 59 ms on PostgreSQL. Reopen-to-success through the preserved
wait was 49 ms and 374 ms respectively. With the deliberately short two-second
lease, the median separate lease wait was 2.00 s and 1.74 s respectively.
Production restart downtime also depends on the previous owner's lease expiry.

### SQLite history scaling

The 4,096-run, 16-outstanding SQLite trial averaged 23.2 runs/s and 1.38 s p95.
In successive groups of 1,024 observed completions it sustained 86.6, 32.8,
19.2, and 12.8 runs/s. It added 194 MiB to the checkpointed database, while
driver peak RSS was 29.0 MiB. The median 32-header discovery page was 0.24 ms,
close to the 512-run result. Re-reading the original eight-header pin went from
70 to 75 µs median. Execution slowed materially while those bounded reads held up.

A five-second process sample late in the trial found the busiest SQLite store
stack in `PreparedTransition::apply → check_references → expired`. The expiry
check filters `records` by `family` and `record_id`, while the SQLite primary key
leads with `run_id`. An independent SQLite 3.50.4 probe copied 49,248 generated
records from the benchmark. For a missing tombstone, the median lookup was
2.62 ms with a table scan and under 1 µs with a partial index on
`record_id WHERE family='tombstone'`. This validates the query-plan change in
isolation; it is not an end-to-end speedup estimate for HTLK's bundled SQLite.

With the SQLite indexes and the shared first-page scheduling probe, a repeat of
the 4,096-run case on the same host achieved **229.3 runs/s** (87.9 ms p95).
The four completion groups sustained 238.6, 210.5, 230.7 and 239.9 runs/s. This
single follow-up compares the combined changes, not the isolated index cost.

### Bottleneck priorities

1. **Reduce PostgreSQL round trips per unit of durable work.** Each store operation
   starts a transaction, acquires the schema advisory lock, and closes the
   transaction. It does that for reads as well. A separate 18-second activity
   sample around the 2 ms polling trial observed 35 advisory-lock waits and one
   WAL sync wait among 362 backend observations. The sample confirms contention;
   it does not attribute every millisecond. Profile cursor setup and the scheduler's
   repeated header/scope/invocation reads before changing the scope of the lock.
   In a separate `pg_stat_statements` profile of eight runs (plus four warm-up),
   PostgreSQL recorded 33,415 statements in the whole process window. The
   advisory-lock query alone was called 5,463 times and took 1.61 of 1.83 summed
   server executor seconds. The same workload with at most four active runs
   completed at 1.94 vs. 1.14 runs/s and reduced p95 from 7.00 s to 4.12 s.
2. **Reduce polling work as active runs grow.** Coalesce supervisory and deadline
   reads and test a lower coordinated-run limit. Evaluate a deadline-aware wake-up
   and targeted indexed due-work reads, preserving commit-time deadline checks and
   fenced recovery. Faster polling alone was counterproductive under load.
   With zero runs and no observer, the engine issued about 250 advisory-lock
   queries over five seconds at 20 ms polling versus 2,061 at 2 ms.
3. **Index SQLite's expiry lookup.** Re-run the 4,096-run trial with the candidate
   partial index and qualify the private-layout change. Keep the transaction-local
   expiry and duplicate/corruption checks. Measure the remaining write cost before
   changing historical projections; the pinned discovery measurements do not
   explain the observed execution slowdown.

## Mixed-workload qualification

Reproduce the release trial with `--suite extended --cases mixed` on each
backend. The workload offers one-node runs interleaved with chains and fan-out
joins, alongside two inspection consumers and a population of runs that have
already durably parked. Before resuming twelve sampled waits, it verifies that
every parked run has the same header and fence as before the measured arrivals.
Sampled replies must publish the expected output under a higher fence.

| Backend | Measured runs | Nodes in chain / fan-out width | Parked runs | Offered → completed runs/s | Peak backlog | p95 / p99 | Parked reply p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SQLite | 512 | 24 | 256 | 40 → 39.93 | 7 | 75.7 / 96.6 ms | 14.0 ms |
| PostgreSQL 18 | 24 | 8 | 48 | 3 → 1.37 | 14 | 12.97 / 13.45 s | 227 ms |

Both trials completed and verified all measured runs. The local PostgreSQL
deployment accumulated 13 pending runs by the end of the offer and drained in
9.81 s. The SQLite trial checkpointed about 104 MiB of additional storage and
the driver peaked at 34.0 MiB RSS. The PostgreSQL trial recorded 27,405 commits
and 11.8 MiB of WAL over the entire process window (including setup, verification,
and readers), with 26.0 MiB driver RSS excluding the database server. These are
diagnostic trials at different offered loads and graph sizes, not a comparison
at equal load or a production SLO. PostgreSQL's small sample makes p99 the sample
maximum; reducing shared publication and database round trips remains important
for throughput on this deployment.
