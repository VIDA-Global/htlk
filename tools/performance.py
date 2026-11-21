#!/usr/bin/env python3
"""Run serial HTLK trials and retain raw measurements and per-trial summaries."""

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


def cases(backend, suite):
    sqlite = backend == "sqlite"
    common = {"warmup": 8 if sqlite else 4, "micro_samples": 0, "history_samples": 8}

    def case(name, runs, **options):
        return name, {**common, "runs": runs, **options}

    if suite == "smoke":
        return [case("smoke", 4, warmup=2, micro_samples=2, history_samples=2)]

    runs = 512 if sqlite else 48
    shape_load = {} if sqlite else {"concurrency": 2}
    result = [
        case("serial", runs // 2, concurrency=1, micro_samples=30),
        case("parallel", runs),
        case("poll_2ms", runs, poll_ms=2),
        case("single_worker", runs, workers=1),
        case("workers_4", runs, workers=4),
        case("coordinators_2", runs, coordinators=2),
        case("mixed_reads", runs, readers=2),
        case("chain_8", runs // 4, nodes=8, micro_samples=20, **shape_load),
        case("wide_8", runs // 4, shape="wide", nodes=8, micro_samples=20, **shape_load),
        case("artifact_16k", runs // 2, payload_bytes=16_384, **shape_load),
        case("wait", runs // 3, shape="wait", **shape_load),
        case("loop_3", runs // 8, shape="loop", nodes=3, **shape_load),
    ]
    for rate in ([50, 75, 100, 125, 150, 300, 600] if sqlite else [1, 2, 4, 8]):
        # Fixed-rate, not a closed-loop client. Use the same offer window at each rate.
        result.append(case(f"rate_{rate}", rate * (5 if sqlite else 12), rate=rate))
    for transport in ["stdio", "http", "sse"]:
        for delay in [0, 20]:
            result.append(case(f"{transport}_{delay}ms", 24 if sqlite else 8,
                               shape=transport, peer_delay_ms=delay, concurrency=4))
    if suite == "extended":
        result += [case("mixed", 512 if sqlite else 24, mix_nodes=24 if sqlite else 8,
                        parked_runs=256 if sqlite else 48, readers=2, max_runs=8,
                        rate=40 if sqlite else 3),
                   case("history_soak", 20_000 if sqlite else 1500, history_samples=50, timeout_s=3600, **shape_load),
                   case("history_growth", 4096 if sqlite else 128, history_samples=30, **shape_load),
                   case("recovery", 12 if sqlite else 4, shape="wait", recovery_samples=3, **shape_load),
                   case("idle_20ms", 0, idle_ms=5000, warmup=0),
                   case("idle_2ms", 0, idle_ms=5000, warmup=0, poll_ms=2),
                   case("chain_24", 80 if sqlite else 8, nodes=24, micro_samples=30, **shape_load),
                   case("wide_24", 80 if sqlite else 8, shape="wide", nodes=24, micro_samples=30, **shape_load),
                   case("cache_disabled", runs, cache=0),
                   case("observer_sensitivity", runs, observe_ms=25),
                   case("max_runs_4", runs, max_runs=4)]
    return result


def distribution(items):
    ordered = sorted(items)
    return {f"p{p}": ordered[math.ceil(p / 100 * len(ordered)) - 1] for p in (50, 95, 99)}


def read_csv(path):
    metadata, samples, runs = {}, defaultdict(list), []
    with path.open(newline="") as stream:
        for kind, *fields in csv.reader(stream):
            if kind == "metadata":
                key, value = fields
                metadata[key] = value if key == "postgres_schema" else int(value)
            elif kind == "sample":
                samples[fields[0]].append(int(fields[1]))
            elif kind == "run":
                runs.append(tuple(map(int, fields)))
            else:
                raise ValueError(f"unrecognized output row: {kind}")
    return metadata, samples, runs


def read_trial(path, spec):
    metadata, samples, runs = read_csv(path)
    if len(runs) != spec["runs"] or len({row[0] for row in runs}) != len(runs):
        raise ValueError("incorrect benchmark sample count")
    if not runs:
        return metadata, {}

    # For a fixed-rate trial, backlog includes planned arrivals, even when the
    # load generator or admissions fall behind. Tie-break arrivals before results.
    arrivals = sorted([(planned, 1) for _, planned, *_ in runs]
                      + [(done, -1) for _, _, _, _, done, *_ in runs],
                      key=lambda event: (event[0], -event[1]))
    peak = backlog = 0
    for _, change in arrivals:
        backlog += change
        peak = max(peak, backlog)
    # Integer microsecond rounding can put events from the next instant at the
    # same timestamp; rates and drain time are computed from raw timestamps.
    last_offer = max(row[1] for row in runs)
    last_done = max(row[4] for row in runs)
    half_offer = last_offer // 2
    during_offer = sum(row[4] <= last_offer for row in runs)
    during_first_half = sum(row[4] <= half_offer for row in runs)
    offers_first_half = sum(row[1] <= half_offer for row in runs)
    summary = {
        "goodput_runs_s": len(runs) * 1e6 / metadata["elapsed_us"],
        "peak_backlog": peak,
        "backlog_at_half_offer": offers_first_half - during_first_half,
        "backlog_at_end_offer": len(runs) - during_offer,
        "drain_us": last_done - last_offer,
        "during_offer_runs_s": during_offer * 1e6 / max(1, last_offer),
        "planned_to_observed_us": distribution(done - planned for _, planned, _, _, done, *_ in runs),
        "submit_lag_us": distribution(sent - planned for _, planned, sent, *_ in runs),
        "admit_us": distribution(ack - sent for _, _, sent, ack, *_ in runs),
        "journal_start_to_owner_ms": distribution(row[5] for row in runs),
        "journal_owner_to_terminal_ms": distribution(row[6] for row in runs),
        "journal_rows": distribution(row[7] for row in runs),
        "samples_us": {name: distribution(values) | {"n": len(values)} for name, values in samples.items()},
    }
    return metadata, summary


def psql(container, database, sql):
    return subprocess.check_output(["docker", "exec", container, "psql", "-XAt", "-v",
                                    "ON_ERROR_STOP=1", "-U", "postgres", "-d", database, "-c", sql], text=True).strip()


def postgres_stats(container, database):
    # These counters cover the entire disposable database; keep other clients idle.
    return list(map(int, psql(container, database, """
        SELECT xact_commit,xact_rollback,blks_read,blks_hit,temp_bytes,
               pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'0/0')::bigint
        FROM pg_stat_database WHERE datname=current_database()
    """).split("|")))


def run_trial(args, backend, name, spec, trial):
    directory = args.output / backend
    directory.mkdir(exist_ok=True)
    prefix = directory / f"{name}_{trial:02d}"
    raw = prefix.with_suffix(".csv")
    command = [str(args.binary), "--backend", backend, "--work-dir", str(args.output)]
    for key, value in spec.items():
        command += ["--" + key.replace("_", "-"), str(value)]
    pg = args.postgres_container if backend == "postgres" else None
    before = postgres_stats(pg, args.postgres_database) if pg else None
    if pg and args.postgres_statements:
        psql(pg, args.postgres_database, "SELECT pg_stat_statements_reset()")
    with raw.open("x") as out, prefix.with_suffix(".stderr").open("x") as err:
        process = subprocess.Popen(command, stdout=out, stderr=err)
        try:
            _, status, usage = os.wait4(process.pid, 0)
        except BaseException:
            process.kill()
            process.wait()
            raise
    process.returncode = os.waitstatus_to_exitcode(status)
    if process.returncode:
        payload = {"backend": backend, "case": name, "trial": trial, "settings": spec,
                   "failure": prefix.with_suffix(".stderr").read_text().strip(),
                   "exit_code": process.returncode}
        prefix.with_suffix(".json").write_text(json.dumps(payload, indent=2) + "\n")
        print(f"{backend:8} {name:18} trial {trial}: FAILED; see {prefix}.stderr", flush=True)
        return False
    metadata, summary = read_trial(raw, spec)
    if pg:
        after = postgres_stats(pg, args.postgres_database)
        keys = ("commits", "rollbacks", "blocks_read", "blocks_hit", "temp_bytes", "wal_bytes")
        summary["postgres_process_window"] = {key: end - start for key, start, end in zip(keys, before, after)}
        schema = metadata["postgres_schema"]
        if not schema.startswith("htlk_perf_") or not schema.replace("_", "").isalnum():
            raise ValueError("unexpected schema name")
        metadata["postgres_schema_bytes"] = int(psql(pg, args.postgres_database, f"""
            SELECT sum(pg_total_relation_size(c.oid))::bigint
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='{schema}' AND c.relkind IN ('r','m')
        """))
        if args.postgres_statements:
            statements = psql(pg, args.postgres_database, """
                SELECT json_build_object(
                    'total_calls',sum(calls), 'total_executor_ms',sum(total_exec_time),
                    'top', (SELECT json_agg(s) FROM (
                        SELECT left(query,220) AS statement,calls,total_exec_time AS executor_ms,
                               wal_bytes
                        FROM pg_stat_statements
                        WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
                        ORDER BY total_exec_time DESC LIMIT 15
                    ) s))
                FROM pg_stat_statements
                WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
            """)
            summary["postgres_statements"] = json.loads(statements)
    summary["process_cpu_s"] = usage.ru_utime + usage.ru_stime
    summary["process_peak_rss_bytes"] = usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024)
    payload = {"backend": backend, "case": name, "trial": trial, "settings": spec,
               "metadata": metadata, "summary": summary}
    prefix.with_suffix(".json").write_text(json.dumps(payload, indent=2) + "\n")
    if spec["runs"]:
        print(f"{backend:8} {name:18} trial {trial}: {summary['goodput_runs_s']:8.2f} runs/s  "
              f"p95 {summary['planned_to_observed_us']['p95'] / 1000:9.2f} ms", flush=True)
    else:
        print(f"{backend:8} {name:18} trial {trial}: idle for {metadata['elapsed_us'] / 1e6:.2f} s", flush=True)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", type=Path, default=Path("target/release/examples/performance"))
    parser.add_argument("--backend", choices=["sqlite", "postgres", "both"], default="both")
    parser.add_argument("--suite", choices=["smoke", "baseline", "extended"], default="baseline")
    parser.add_argument("--cases", help="optional comma-separated subset of cases")
    parser.add_argument("--runs", type=int, help="override measured run count per selected trial")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--pure-batch-steps", type=int, choices=range(1, 9))
    parser.add_argument("--postgres-container", help="dedicated local container for optional database/WAL statistics")
    parser.add_argument("--postgres-database", default="htlk_perf")
    parser.add_argument("--postgres-statements", action="store_true",
                        help="reset and collect pg_stat_statements on the disposable container per trial")
    args = parser.parse_args()
    if not (1 <= args.trials <= 20) or not args.binary.is_file():
        parser.error("build the release binary and select 1–20 trials")
    if args.runs is not None and not 1 <= args.runs <= 100_000:
        parser.error("--runs must be in 1–100000")
    if args.postgres_statements and not args.postgres_container:
        parser.error("statement profiling requires a disposable --postgres-container")
    backends = ["sqlite", "postgres"] if args.backend == "both" else [args.backend]
    if "postgres" in backends and not os.environ.get("HTLK_PERF_POSTGRES_URL"):
        parser.error("HTLK_PERF_POSTGRES_URL must point to a disposable database")
    selected = set(args.cases.split(",")) if args.cases else None
    grid = [(backend, name, spec) for backend in backends for name, spec in cases(backend, args.suite)
            if selected is None or name in selected]
    if not grid or (selected and selected - {entry[1] for entry in grid}):
        parser.error("select existing cases for the specified backend and suite")
    if args.runs is not None:
        grid = [(backend, name, spec | {"runs": args.runs} if spec["runs"] else spec)
                for backend, name, spec in grid]
    if args.pure_batch_steps is not None:
        grid = [(backend, name, spec | {"pure_batch_steps": args.pure_batch_steps})
                for backend, name, spec in grid]
    args.output.mkdir()  # Refuse to mix results with any previous campaign.
    with args.binary.open("rb") as binary:
        binary_sha = hashlib.file_digest(binary, "sha256").hexdigest()
    context = {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
               "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"])),
               "binary_sha256": binary_sha,
               "platform": platform.platform(), "cpu_count": os.cpu_count(),
               "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
               "suite": args.suite, "trials": args.trials, "cases": grid}
    if args.postgres_container and "postgres" in backends:
        context["postgres_settings"] = psql(args.postgres_container, args.postgres_database, """
            SELECT version() UNION ALL SELECT name||'='||setting FROM pg_settings
            WHERE name IN ('fsync','synchronous_commit','full_page_writes','shared_buffers','track_io_timing')
        """).splitlines()
    (args.output / "context.json").write_text(json.dumps(context, indent=2) + "\n")
    failed = False
    for trial in range(1, args.trials + 1):
        for backend, name, spec in grid:
            failed |= not run_trial(args, backend, name, spec, trial)
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
