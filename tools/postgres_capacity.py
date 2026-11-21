#!/usr/bin/env python3
"""Measure HTLK capacity with independent runtime processes and PostgreSQL stores."""

import argparse
import hashlib
import json
import math
import os
import platform
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

from performance import distribution, read_csv


def route_arrivals(count, partitions, hot_share):
    """Spread arrivals in time and keep the short/chain/wide mix on each store."""
    if partitions == 1:
        return [list(range(count))]
    weights = ([hot_share * (partitions - 1)] + [100 - hot_share] * (partitions - 1)
               if hot_share else [1] * partitions)
    total = sum(weights)
    credits = [[0] * partitions for _ in range(3)]
    routes = [[] for _ in weights]
    for arrival in range(count):
        credit = credits[{2: 1, 6: 2}.get(arrival % 8, 0)]
        for shard, weight in enumerate(weights):
            credit[shard] += weight
        shard = max(range(partitions), key=credit.__getitem__)
        credit[shard] -= total
        routes[shard].append(arrival)
    return routes


def split(total, partitions, index):
    return total // partitions + int(index < total % partitions)


def summarize(rows, offer_us, p95_us, max_backlog_growth, max_submit_lag_us):
    midpoint = offer_us // 2
    events = sorted((event for row in rows for event in ((row[1], -1), (row[4], 1))))
    pending = peak = half = end = during = 0
    for at, change in events:
        pending -= change  # Tie-break arrivals before completions.
        peak = max(peak, pending)
        if at < midpoint:
            half = pending
        if at < offer_us:
            end = pending
            during += change == 1
    observed = distribution(row[4] - row[1] for row in rows)
    submit_lag = distribution(row[2] - row[1] for row in rows)
    return {
        "runs": len(rows), "during_offer_runs_s": 1e6 * during / offer_us,
        "peak_backlog": peak, "backlog_at_midpoint": half, "backlog_at_end": end,
        "backlog_growth": end - half, "drain_us": max(0, max(row[4] for row in rows) - offer_us),
        "planned_to_observed_us": observed, "submit_lag_us": submit_lag,
        "admission_round_trip_us": distribution(row[3] - row[2] for row in rows),
        "passes": (observed["p95"] <= p95_us and end - half <= max_backlog_growth
                   and submit_lag["p95"] <= max_submit_lag_us),
    }


SNAPSHOT_SQL = """
SELECT json_build_object('commits',xact_commit,'rollbacks',xact_rollback,
    'blocks_read',blks_read,'blocks_hit',blks_hit,'read_ms',blk_read_time,'write_ms',blk_write_time,
    'wal_insert_bytes',pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'0/0')::bigint)
FROM pg_stat_database WHERE datname=current_database()
"""
ACTIVITY_SQL = """
SELECT json_build_object('active',count(*),
    'inspection_clock_lock',count(*) FILTER (WHERE wait_event_type='Lock'
        AND query ILIKE 'SELECT%FROM inspection_clock%FOR UPDATE%'),
    'other_lock',count(*) FILTER (WHERE wait_event_type='Lock'
        AND query NOT ILIKE 'SELECT%FROM inspection_clock%FOR UPDATE%'),
    'io',count(*) FILTER (WHERE wait_event_type='IO'),
    'sync_rep',count(*) FILTER (WHERE wait_event='SyncRep'))
FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()
    AND backend_type='client backend' AND state='active'
"""
SETTINGS_SQL = """
SELECT json_object_agg(name,setting) FROM pg_settings WHERE name IN
    ('server_version','fsync','synchronous_commit','synchronous_standby_names',
     'full_page_writes','shared_buffers','track_io_timing','block_size')
"""


def stats_query(args, primary, sql):
    env = dict(os.environ, PGCONNECT_TIMEOUT="5")
    cmd = ["psql"]
    if args.stats_container:
        cmd = ["docker", "exec", args.stats_container[primary], "psql", "-U", "postgres",
               "-d", args.stats_database]
    else:
        env["PGSERVICE"] = args.stats_service[primary]
    try:
        result = subprocess.run(cmd + ["-XAtw", "-v", "ON_ERROR_STOP=1", "-c", sql],
                                env=env, capture_output=True, text=True, timeout=8, check=True)
        return json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, ValueError):
        raise RuntimeError(f"PostgreSQL statistics query failed for primary {primary + 1}") from None


class Trial:
    def __init__(self, args, directory, url_envs):
        self.args = args
        self.directory = directory
        self.url_envs = url_envs
        self.children = {}

    def spawn(self, name, primary, schema, directory_option, options, arrivals=None):
        control = self.directory / name
        control.mkdir()
        if arrivals is not None:
            (control / "arrivals").write_text("".join(f"{arrival}\n" for arrival in arrivals))
        flags = {"backend": "postgres", "capacity_schema": schema, directory_option: control,
                 "workers": self.args.workers, "max_runs": self.args.max_runs,
                 "pure_batch_steps": self.args.pure_batch_steps, "mix_nodes": self.args.mix_nodes, **options}
        command = [str(self.args.binary)]
        for key, value in flags.items():
            command += ["--" + key.replace("_", "-"), str(value)]
        env = dict(os.environ, HTLK_PERF_POSTGRES_URL=os.environ[self.url_envs[primary]])
        with (control / "raw.csv").open("x") as raw, (control / "stderr").open("x") as err:
            self.children[name] = subprocess.Popen(command, env=env, stdout=raw, stderr=err)
        return control

    def wait(self, condition, deadline, phase, may_exit=(), tick=lambda: None):
        while True:
            for name, process in self.children.items():
                code = process.poll()
                if code is not None and (code or name not in may_exit):
                    raise RuntimeError(f"{name} exited with status {code} during {phase}; see {name}/stderr")
            if condition():
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(f"timed out during {phase}")
            tick()
            time.sleep(0.02)

    def cleanup(self):
        for process in self.children.values():
            if process.poll() is None:
                process.kill()
        for process in self.children.values():
            process.wait()

    def run(self, topology, size, routes, rate, payload):
        args = self.args
        partitions = len(routes)
        primary_for = [i % len(self.url_envs) if topology == "distributed" else 0
                       for i in range(partitions)]
        primaries = sorted(set(primary_for)) if args.stats_container or args.stats_service else []
        schemas = [f"htlk_perf_cap_{uuid.uuid4().hex}" for _ in routes]
        payload["partitions"] = [{"schema": schema, "primary": primary + 1, "scheduled": len(route)}
                                 for schema, primary, route in zip(schemas, primary_for, routes)]
        drivers = []
        for i, arrivals in enumerate(routes):
            options = {"runs": len(arrivals), "rate": rate, "warmup": args.warmup, "micro_samples": 0,
                       "history_samples": args.history_samples, "payload_bytes": args.payload_bytes,
                       "parked_runs": split(args.parked_runs, partitions, i),
                       "readers": split(args.readers, partitions, i), "observe_ms": args.observe_ms,
                       "timeout_s": args.seconds + args.drain_seconds}
            driver = self.spawn(f"partition-{i + 1}", primary_for[i], schemas[i],
                                "capacity_driver_dir", options, arrivals)
            drivers.append(driver)
        ready = time.monotonic() + args.setup_timeout
        self.wait(lambda: all((d / "ready").exists() for d in drivers), ready, "driver setup")
        engines = [self.spawn(f"replica-{i + 2}", 0, schemas[0], "capacity_engine_dir", {})
                   for i in range(size - 1)] if topology == "shared" else []
        self.wait(lambda: all((d / "ready").exists() for d in engines), ready, "replica setup")

        before = {primary: stats_query(args, primary, SNAPSHOT_SQL) for primary in primaries}
        diagnostics = {str(primary + 1): {"activity_samples": []} for primary in primaries}
        payload["postgres"] = diagnostics
        start = time.monotonic() + 1
        start_ns = time.time_ns() + 1_000_000_000
        for driver in drivers:
            temp = driver / "start.tmp"
            temp.write_text(str(start_ns))
            temp.rename(driver / "start")
        next_sample = start

        def sample():
            nonlocal next_sample
            now = time.monotonic()
            if now >= next_sample:
                for primary in primaries:
                    diagnostics[str(primary + 1)]["activity_samples"].append(
                        {"elapsed_s": time.monotonic() - start, **stats_query(args, primary, ACTIVITY_SQL)})
                next_sample = now + args.stats_interval

        self.wait(lambda: (time.monotonic() >= start + args.seconds
                          and all((d / "measured").exists() for d in drivers)),
                  start + args.seconds + args.drain_seconds, "offer and drain", tick=sample)
        for primary in primaries:
            after = stats_query(args, primary, SNAPSHOT_SQL)
            diagnostics[str(primary + 1)]["counter_delta"] = {
                key: value - before[primary][key] for key, value in after.items()}
        for engine in engines:
            (engine / "measure_done").touch()
        self.wait(lambda: all((d / "captured").exists() for d in engines),
                  time.monotonic() + 10, "publication metric collection")
        for driver in drivers:
            (driver / "verify").touch()
        names = {d.name for d in drivers}
        self.wait(lambda: all(self.children[name].returncode == 0 for name in names),
                  time.monotonic() + args.setup_timeout, "result verification", may_exit=names)
        for engine in engines:
            (engine / "stop").touch()
            names.add(engine.name)
        self.wait(lambda: all(p.returncode == 0 for p in self.children.values()),
                  time.monotonic() + 30, "replica shutdown", may_exit=names)

        def summary(rows):
            return summarize(rows, args.seconds * 1_000_000, args.p95_ms * 1_000,
                             args.max_backlog_growth, args.max_submit_lag_ms * 1_000)

        combined = []
        for driver, route, report in zip(drivers, routes, payload["partitions"]):
            metadata, _, rows = read_csv(driver / "raw.csv")
            # Compare exact identities and pacing to the plan, not just sample counts.
            expected = [(4096 + i, i * 1_000_000 // rate) for i in route]
            if ([(row[0], row[1]) for row in rows] != expected
                    or metadata.get("postgres_schema") != report["schema"]
                    or any(not row[1] <= row[2] <= row[3] or row[2] > row[4] for row in rows)):
                raise ValueError(f"invalid or incomplete samples in {driver.name}/raw.csv")
            report.update(metadata=metadata, summary=summary(rows))
            combined.extend(rows)
        payload["extra_replica_metadata"] = [read_csv(d / "raw.csv")[0] for d in engines]
        runtime_metrics = [p["metadata"] for p in payload["partitions"]] + payload["extra_replica_metadata"]
        publication = {key: sum(m[key] for m in runtime_metrics) for key in (
            "publication_commits", "publication_acquire_ns", "publication_hold_ns", "publication_statements")}
        if commits := publication["publication_commits"]:
            publication.update(
                mean_acquire_ms=publication["publication_acquire_ns"] / commits / 1e6,
                mean_hold_ms=publication["publication_hold_ns"] / commits / 1e6,
                statements_per_commit=publication["publication_statements"] / commits)
        payload["publication"] = publication
        payload["summary"] = summary(combined)
        payload["status"] = ("pass" if payload["summary"]["passes"]
                             and all(p["summary"]["passes"] for p in payload["partitions"]) else "over_slo")


def integer_list(value):
    try:
        values = sorted(set(map(int, value.split(","))))
    except ValueError:
        raise argparse.ArgumentTypeError("expected comma-separated positive integers") from None
    if not values or values[0] < 1:
        raise argparse.ArgumentTypeError("expected comma-separated positive integers")
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", type=Path, default=Path("target/release/examples/performance"))
    parser.add_argument("--postgres-url-env", action="append", help="name of a disposable primary's DSN variable; repeat for distributed")
    parser.add_argument("--topology", action="append", choices=["shared", "partitioned", "distributed"])
    parser.add_argument("--sizes", type=integer_list, default=[1, 2, 4], help="HTLK process counts")
    parser.add_argument("--rates", type=integer_list, required=True, help="aggregate offered runs/sec")
    parser.add_argument("--seconds", type=int, default=60, help="offer window per point")
    parser.add_argument("--drain-seconds", type=int, default=30)
    parser.add_argument("--setup-timeout", type=int, default=180)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--p95-ms", type=float, required=True)
    parser.add_argument("--max-backlog-growth", type=int, default=1, help="allowed midpoint-to-end increase in runs")
    parser.add_argument("--max-submit-lag-ms", type=float, default=25)
    parser.add_argument("--hot-share", type=int, default=0, help="percent of arrivals routed to partition 1 (0 balances)")
    parser.add_argument("--workers", type=int, default=2, choices=range(1, 65), metavar="1–64")
    parser.add_argument("--max-runs", type=int, default=8, help="active runs per process")
    parser.add_argument("--pure-batch-steps", type=int, default=1, choices=range(1, 9))
    parser.add_argument("--mix-nodes", type=int, default=8, help="chain depth and fan-out width; 0 uses only short runs")
    parser.add_argument("--payload-bytes", type=int, default=16)
    parser.add_argument("--warmup", type=int, default=4, help="warm-up runs per store")
    parser.add_argument("--history-samples", type=int, default=2, help="per-store history and replay samples")
    parser.add_argument("--parked-runs", type=int, default=0, help="total parked population, split across stores")
    parser.add_argument("--readers", type=int, default=4, help="total extra inspection readers, split across stores")
    parser.add_argument("--observe-ms", type=int, default=10)
    stats = parser.add_mutually_exclusive_group()
    stats.add_argument("--stats-container", action="append", help="local Docker container per primary, in DSN order")
    stats.add_argument("--stats-service", action="append", help="libpq service per primary in DSN order; requires psql")
    parser.add_argument("--stats-database", default="htlk_perf", help="database name for Docker statistics connections")
    parser.add_argument("--stats-interval", type=float, default=1.0, help="seconds between activity samples")
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    envs = args.postgres_url_env or ["HTLK_PERF_POSTGRES_URL"]
    topologies = args.topology or ["shared", "partitioned"]
    stats_endpoints = args.stats_container or args.stats_service
    if not args.binary.is_file() or not all(os.environ.get(name) for name in envs):
        parser.error("build the release example and set the selected disposable PostgreSQL DSN variables")
    if len({os.environ[name] for name in envs}) != len(envs) or (stats_endpoints and len(stats_endpoints) != len(envs)):
        parser.error("select distinct DSN variables and, if profiling, one stats endpoint per DSN")
    if "distributed" in topologies and (len(envs) < 2 or max(args.sizes) < 2):
        parser.error("distributed requires at least two DSN variables and a size of at least two")
    if (max(args.sizes) > 8 or not 1 <= args.trials <= 20 or not 1 <= args.setup_timeout <= 3600
            or args.seconds < 1 or args.drain_seconds < 1 or args.seconds + args.drain_seconds > 3600
            or args.rates[-1] * args.seconds > 100_000 or not 0 <= args.hot_share < 100
            or not 0 <= args.max_backlog_growth or not 1 <= args.max_runs <= 4096
            or not 0 <= args.mix_nodes <= 64 or not 8 <= args.payload_bytes <= 32_768
            or not 0 <= args.warmup <= 4096 or not 0 <= args.history_samples <= 1000
            or not 0 <= args.readers <= 8 or not 1 <= args.observe_ms <= 10_000
            or not 0 <= args.parked_runs <= 4096
            or (args.parked_runs and (not args.mix_nodes or args.seconds + args.drain_seconds > 180))
            or any(not math.isfinite(v) or v <= 0 for v in (args.p95_ms, args.max_submit_lag_ms, args.stats_interval))):
        parser.error("invalid capacity bounds; see the performance guide")

    grid = []
    for topology in dict.fromkeys(topologies):
        for size in args.sizes:
            if size == 1 and topology == "distributed":
                continue
            for rate in args.rates:
                partitions = 1 if topology == "shared" else size
                routes = route_arrivals(rate * args.seconds, partitions, args.hot_share)
                if not all(routes):
                    parser.error(f"{topology}/{size} at {rate}/s leaves an empty partition; extend the offer window")
                grid.append((topology, size, rate))
    args.output.mkdir()  # Keep this campaign's files separate from existing runs.
    with args.binary.open("rb") as binary:
        binary_sha = hashlib.file_digest(binary, "sha256").hexdigest()
    context = {"settings": vars(args), "binary_sha256": binary_sha, "platform": platform.platform(),
               "cpu_count": os.cpu_count(), "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
               "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"]))}
    if stats_endpoints:
        context["postgres_settings"] = [stats_query(args, primary, SETTINGS_SQL) for primary in range(len(envs))]
    (args.output / "context.json").write_text(json.dumps(context, indent=2, default=str) + "\n")
    outcomes = defaultdict(list)
    failed = False
    for repetition in range(1, args.trials + 1):
        for topology, size, rate in grid:
            directory = args.output / f"{topology}-{size}_rate-{rate}_trial-{repetition}"
            directory.mkdir()
            trial = Trial(args, directory, envs)
            payload = {"topology": topology, "processes": size, "rate": rate, "trial": repetition}
            try:
                routes = route_arrivals(rate * args.seconds, 1 if topology == "shared" else size, args.hot_share)
                trial.run(topology, size, routes, rate, payload)
                summary = payload["summary"]
                detail = (f"{summary['during_offer_runs_s']:.2f} runs/s, "
                          f"p95 {summary['planned_to_observed_us']['p95'] / 1000:.1f} ms, "
                          f"backlog {summary['backlog_at_midpoint']}→{summary['backlog_at_end']}")
            except (OSError, RuntimeError, ValueError) as exc:
                payload.update(status="failed", failure=str(exc))
                detail = str(exc)
                failed = True
            finally:
                trial.cleanup()
            (directory / "result.json").write_text(json.dumps(payload, indent=2) + "\n")
            outcomes[topology, size, rate].append(payload["status"])
            print(f"{directory.name}: {payload['status']}: {detail}", flush=True)
    capacity = defaultdict(lambda: None)
    for (topology, size, rate), statuses in sorted(outcomes.items()):
        label = f"{topology}-{size}"
        if all(status == "pass" for status in statuses):
            capacity[label] = rate
        elif label not in capacity:
            capacity[label] = None
    (args.output / "capacity.json").write_text(json.dumps(capacity, indent=2) + "\n")
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
