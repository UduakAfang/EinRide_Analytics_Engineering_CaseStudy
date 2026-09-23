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
import math
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fleet_engine import FleetEngine          # noqa: E402

_HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(_HERE, "..", "github_sources", "historical")

MAX_BYTES = 23_500_000          # ~22.4 MiB, comfortably inside the 25MB rule

# ---------------------------------------------------------------------------
# ONE CONTINUOUS TIMELINE
# ---------------------------------------------------------------------------
# The three datasets must butt up against each other with no gaps:
#
#   daily_rollup   two years of history, ending the day before the pings
#   ping files     yesterday, at full resolution
#   live stream    today onwards, from the web app
#
# Anchoring to "yesterday" rather than a hard-coded date means the archive always
# ends where the live stream begins, however long after this you run the app.
#
# Equally important, all three come from the SAME engine state, handed forward
# through files. Generate them independently and a truck's lifetime odometer
# jumps at every boundary -- the same defect the stateful engine exists to prevent,
# reappearing between datasets instead of between rows.
_TODAY = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
PING_START = (_TODAY - timedelta(days=1)).replace(hour=5, minute=30)

ROLLUP_DAYS = 660               # ~22 months, sized to land just inside MAX_BYTES
ROLLUP_START = PING_START - timedelta(days=ROLLUP_DAYS)

STATE_DIR = _HERE
AFTER_ROLLUP = os.path.join(STATE_DIR, "state_after_rollup.json")
HANDOFF      = os.path.join(STATE_DIR, "fleet_handoff.json")


def _save_state(engine, path, label):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(engine.export_state(), fh)
    print(f"  {label:28} saved at simulated {engine.clock:%Y-%m-%d %H:%M}")


def _load_state(engine, path, label):
    """Resume a previously saved fleet, or say plainly that we are cold-starting."""
    if not os.path.exists(path):
        print(f"  !! {os.path.basename(path)} not found -- starting a fresh fleet.")
        print(f"     Run the 'rollup' stage first so the history joins up.")
        return False
    with open(path, encoding="utf-8") as fh:
        engine.import_state(json.load(fh))
    print(f"  {label:28} resumed at simulated {engine.clock:%Y-%m-%d %H:%M}")
    return True


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


def generate_ping_level():
    """One day of full-resolution telemetry for the whole fleet.

    Both vehicle families come from a SINGLE engine and are routed to separate
    files by vehicle_type. Running two engines would give two unrelated fleets
    that happen to share a depot list.

    The tick is 5 seconds -- the finest cadence any vehicle uses. Each vehicle
    still reports on its own interval, because the engine gates that internally:
    pods every 5 seconds, road trucks every 60, plus event-triggered messages
    whenever something actually happens.
    """
    engine = FleetEngine(start=ROLLUP_START, seed=7,
                         include_traditional=True, include_autonomous=True)
    if _load_state(engine, AFTER_ROLLUP, "fleet from rollup"):
        engine.clock = PING_START          # jump to the ping window, keep the fleet
    ids = list(engine.state.keys())

    paths = {
        "human_driven":   os.path.join(OUT_DIR, "traditional_sample.ndjson"),
        "autonomous_pod": os.path.join(OUT_DIR, "autonomous_sample.ndjson"),
    }
    files  = {k: open(v, "w", encoding="utf-8", newline="\n") for k, v in paths.items()}
    # Each family gets its own byte budget and stops independently. Pods report
    # twelve times as often as trucks, so they fill their file far sooner; a shared
    # budget would cut the truck data short to make room for pods.
    written = {k: 0 for k in files}
    counts  = {k: 0 for k in files}

    try:
        while any(written[k] < MAX_BYTES for k in files):
            engine.advance(5)
            for vid in ids:
                for row in engine.drain(vid, source="batch_archive"):
                    kind = row["vehicle_type"]
                    if written[kind] >= MAX_BYTES:
                        continue
                    line = json.dumps(row, separators=(",", ":"), ensure_ascii=False)
                    size = len(line.encode("utf-8")) + 1
                    if written[kind] + size > MAX_BYTES:
                        written[kind] = MAX_BYTES      # mark this family finished
                        continue
                    files[kind].write(line)
                    files[kind].write("\n")
                    written[kind] += size
                    counts[kind] += 1
    finally:
        for fh in files.values():
            fh.close()

    for kind, path in paths.items():
        _report(os.path.basename(path), counts[kind], written[kind])

    # Hand the fleet to the live streamer so the web app continues this same fleet.
    _save_state(engine, HANDOFF, "handoff to live stream")
    return engine


def generate_daily_rollup(filename="daily_rollup.ndjson"):
    """Two years of daily summaries, ending the day before the ping files start.

    Runs the WHOLE fleet -- trucks and pods together -- through one engine, then
    saves its state. The ping stage picks that state up, so a truck's lifetime
    odometer carries straight across the boundary instead of resetting.
    """
    engine = FleetEngine(start=ROLLUP_START, seed=7,
                         include_traditional=True, include_autonomous=True)
    ids = list(engine.state.keys())

    def rows():
        for _ in range(ROLLUP_DAYS):
            engine.advance_day()
            for vid in ids:
                yield engine.daily_rollup(vid)

    rows_out, bytes_out = _write_ndjson(os.path.join(OUT_DIR, filename), rows())
    _report(filename, rows_out, bytes_out)
    _save_state(engine, AFTER_ROLLUP, "state after rollup")


def write_shipments(engines, filename="shipments.ndjson"):
    """Flatten the shipment ledgers accumulated by the ping-level runs."""
    ledger = [rec for eng in engines for rec in eng.shipments]
    # Chronological order, matching the telemetry files, so a reader can follow both
    # side by side. Sorting on the ISO string is safe: fixed-width zero-padded UTC.
    ledger.sort(key=lambda r: r["dispatched_at"])
    rows_out, bytes_out = _write_ndjson(os.path.join(OUT_DIR, filename), iter(ledger))
    _report(filename, rows_out, bytes_out)


def generate_weather(days=35, filename="weather_hourly.ndjson"):
    """A separate weather feed: one row per region per hour.

    WHY THIS IS ITS OWN FILE, not a field on every ping:

    A truck does not know the weather. Its sensor reports that the outside air is
    2 degrees; something else has to decide that meant snow. In a real pipeline
    weather comes from an API on a schedule -- hourly, for a handful of locations --
    and is joined to telemetry in the silver layer on region and hour.

    The volume argument settles it. Six regions x 24 hours is 144 rows a day. The
    same fact stamped onto every ping would be 120 vehicles x 60 pings x 24 hours,
    or about 173,000 copies of "it is raining in Skane" per day.

    This file is pulled by ADF on a schedule, exactly like grid_carbon_intensity.
    """
    engine = FleetEngine(start=PING_START - timedelta(days=2), seed=99)
    regions = sorted({d["region"] for d in engine.depots})
    rng = engine.rng
    rows = []

    for day in range(days):
        for hour in range(24):
            ts = (PING_START - timedelta(days=2)).replace(
                hour=0, minute=0, second=0) + timedelta(days=day, hours=hour)
            for region in regions:
                # Weather persists: reuse the engine's per-region daily condition so
                # the feed agrees with the consumption penalties already applied to
                # the telemetry. A pipeline whose weather contradicts its own physics
                # would show snow with no drop in efficiency.
                engine.clock = ts
                cond = engine._weather(region)
                wx = engine.weather_map[cond]
                # A plausible Swedish annual temperature curve, coldest in January.
                seasonal = 8.0 - 9.0 * math.cos(2 * math.pi * (ts.timetuple().tm_yday - 15) / 365)
                diurnal = 4.0 * math.sin(2 * math.pi * (hour - 9) / 24)
                rows.append({
                    "region": region,
                    "observed_at": ts.strftime("%Y-%m-%dT%H:00:00Z"),
                    "hour_of_day": hour,
                    "weather_condition": cond,
                    "air_temp_c": round(seasonal + diurnal + rng.uniform(-2, 2), 1),
                    "wind_speed_ms": round(abs(rng.gauss(4, 3)), 1),
                    "precipitation_mm": round(rng.uniform(0, 4), 1) if cond in ("rain", "snow") else 0.0,
                    "road_condition": {"snow": "snow_covered", "rain": "wet"}.get(cond, "dry"),
                    "consumption_factor": wx["consumption_factor"],
                    "ingested_by": "weather_api",
                })

    rows_out, bytes_out = _write_ndjson(os.path.join(OUT_DIR, filename), iter(rows))
    _report(filename, rows_out, bytes_out)


def sync_repo_folders():
    """Copy what was just generated into the two folders the repo serves from.

    The generator writes to github_sources/ because that is the staging area ADF
    reads. The repo publishes data/ and mapping/. Keeping the copy here means the
    two never drift, and there is nothing to arrange by hand before a commit.
    """
    import shutil

    pairs = [
        (OUT_DIR, os.path.join(_HERE, "..", "data"), ".ndjson"),
        (os.path.join(_HERE, "..", "github_sources", "mapping"),
         os.path.join(_HERE, "..", "mapping"), ".json"),
    ]
    for src, dst, ext in pairs:
        os.makedirs(dst, exist_ok=True)
        for name in sorted(os.listdir(src)):
            if not name.endswith(ext):
                continue                      # skips .bak and anything else
            shutil.copy2(os.path.join(src, name), os.path.join(dst, name))
            print(f"  synced {os.path.basename(dst)}/{name}")


if __name__ == "__main__":
    # Each stage can be run on its own: "python regenerate_history.py rollup".
    # Useful because the rollup is by far the slowest stage and there is no reason
    # to rebuild the ping files every time you re-tune the degradation curve.
    stage = sys.argv[1] if len(sys.argv) > 1 else "all"
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"Regenerating history from the stateful engine [stage: {stage}]\n")

    # Order matters: rollup builds the fleet's history and saves its state, and
    # pings resume from it. Iterating on pings alone against an existing state file
    # is fine; a full rebuild must run rollup -> pings -> weather.
    if stage in ("all", "rollup"):
        generate_daily_rollup()

    if stage in ("all", "pings"):
        eng = generate_ping_level()
        write_shipments([eng])

    if stage in ("all", "pings", "weather"):
        generate_weather()

    print(f"\nTimeline: rollup from {ROLLUP_START:%Y-%m-%d}"
          f" -> pings {PING_START:%Y-%m-%d}"
          f" -> live stream from {(PING_START + timedelta(days=1)):%Y-%m-%d}")
    print("\nSyncing into data/ and mapping/ so the repo matches:")
    sync_repo_folders()

    print("\nDone. Re-upload github_sources/ and re-run the ADF pipeline.")
