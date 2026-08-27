"""
regenerate_history.py
=====================
Rewrites the GitHub source files from the stateful engine in fleet_engine.py.

WHAT THIS PRODUCES
------------------
  historical/traditional_sample.ndjson   ping-level, 100 road trucks, 60s interval
  historical/autonomous_sample.ndjson    ping-level, 20 pods, 5s interval
  historical/shipments.ndjson            the shipment ledger (customer, promise, actual)
  historical/daily_rollup.ndjson         24 months x 120 vehicles, one row per day

THE SIZE BUDGET
---------------
GitHub warns above 50MB and hard-rejects above 100MB, and the project rule here is
to stay under 25MB per file. Rather than guessing a row count and hoping, each
writer streams rows and stops the moment it reaches its byte budget.

That works cleanly ONLY because rows are emitted in chronological order across the
whole fleet: stopping early therefore yields a SHORTER TIME WINDOW covering every
vehicle evenly, never a complete history for some trucks and none for others.

WHY THE DAILY ROLLUP EXISTS
---------------------------
Battery state of health is a months-to-years signal. A day of pings, however many
rows it contains, is dense but short -- there is no degradation to see in it. But
generating two years at ping resolution would be ~63 million rows. The rollup is
the standard answer: keep fine detail for a recent window, keep a coarse daily
summary for the long history. It is ~88k rows and carries the full SoH curve.
"""

import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fleet_engine import FleetEngine          # noqa: E402

_HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(_HERE, "..", "github_sources", "historical")

MAX_BYTES = 23_500_000          # ~22.4 MiB, comfortably inside the 25MB rule
PING_START = datetime(2026, 8, 24, 5, 30, 0, tzinfo=timezone.utc)


def _write_ndjson(path, row_iter, max_bytes=MAX_BYTES):
    """Stream rows to an NDJSON file, stopping at the byte budget.

    Returns (rows_written, bytes_written).

    Two deliberate choices here:
      * json.dumps is called ONCE per row and the encoded string is measured, so we
        never serialise a row twice just to find out how big it is.
      * separators=(",", ":") strips the spaces json.dumps adds by default. Across
        ~40k rows that alone recovers roughly 8% of the file size, which buys real
        extra time window inside the same budget.
    """
    written = rows = 0
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for row in row_iter:
            line = json.dumps(row, separators=(",", ":"), ensure_ascii=False)
            size = len(line.encode("utf-8")) + 1        # +1 for the newline
            if written + size > max_bytes:
                break
            fh.write(line)
            fh.write("\n")
            written += size
            rows += 1
    return rows, written


def _report(name, rows, written):
    print(f"  {name:28} {rows:>8,} rows  {written / 1024 / 1024:>6.1f} MB")


def generate_ping_level(kind, step_seconds, filename):
    """Ping-level telemetry for one vehicle family."""
    traditional = kind == "human_driven"
    engine = FleetEngine(
        start=PING_START,
        seed=42 if traditional else 4242,
        include_traditional=traditional,
        include_autonomous=not traditional,
    )
    # Snapshot the id list once. Re-reading dict keys every tick would be a second
    # pass over the fleet for no benefit, and the membership never changes.
    ids = list(engine.state.keys())

    def rows():
        while True:
            engine.advance(step_seconds)
            for vid in ids:
                yield engine.emit(vid, source="batch_archive")

    rows_out, bytes_out = _write_ndjson(os.path.join(OUT_DIR, filename), rows())
    _report(filename, rows_out, bytes_out)
    return engine


def generate_daily_rollup(months=24, filename="daily_rollup.ndjson"):
    """One summary row per vehicle per day, across the full history window."""
    start = PING_START.replace(year=PING_START.year - 2)
    engine = FleetEngine(start=start, seed=7, include_traditional=True,
                         include_autonomous=True)
    ids = list(engine.state.keys())
    days = int(months * 30.44)

    def rows():
        for _ in range(days):
            # Step in HOURS, not whole days: the engine's dispatch, charging and
            # rest logic all read the clock hour, so a single 24-hour jump would skip
            # the operating window entirely and no truck would ever move.
            #
            # step_hours trades fidelity for runtime. This loop runs
            # days x (24/step_hours) x vehicles times -- at 730 days and 120 vehicles
            # that is 2.1M state updates at 1h, but only 700k at 3h. Since the output
            # is a DAILY summary, sub-3-hour detail is discarded anyway, so 3h costs
            # nothing that survives into the file.
            engine.advance_day()
            for vid in ids:
                yield engine.daily_rollup(vid)

    rows_out, bytes_out = _write_ndjson(os.path.join(OUT_DIR, filename), rows())
    _report(filename, rows_out, bytes_out)


def write_shipments(engines, filename="shipments.ndjson"):
    """Flatten the shipment ledgers accumulated by the ping-level runs."""
    ledger = [rec for eng in engines for rec in eng.shipments]
    # Chronological order, matching the telemetry files, so a reader can follow both
    # side by side. Sorting on the ISO string is safe: fixed-width zero-padded UTC.
    ledger.sort(key=lambda r: r["dispatched_at"])
    rows_out, bytes_out = _write_ndjson(os.path.join(OUT_DIR, filename), iter(ledger))
    _report(filename, rows_out, bytes_out)


if __name__ == "__main__":
    # Each stage can be run on its own: "python regenerate_history.py rollup".
    # Useful because the rollup is by far the slowest stage and there is no reason
    # to rebuild the ping files every time you re-tune the degradation curve.
    stage = sys.argv[1] if len(sys.argv) > 1 else "all"
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"Regenerating history from the stateful engine [stage: {stage}]\n")

    if stage in ("all", "pings"):
        eng_road = generate_ping_level("human_driven", 60, "traditional_sample.ndjson")
        eng_pods = generate_ping_level("autonomous_pod", 5, "autonomous_sample.ndjson")
        write_shipments([eng_road, eng_pods])

    if stage in ("all", "rollup"):
        generate_daily_rollup()

    print("\nDone. Re-upload github_sources/ and re-run the ADF pipeline.")
