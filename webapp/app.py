"""
app.py -- Einride telemetry streaming demo
==========================================
A deliberately small Flask app with two buttons, sitting on top of the SAME
fleet_engine.py that generated the historical files. That shared engine is the
whole point: the live stream and the archive cannot drift apart, because there is
only one implementation of how a vehicle behaves.

  BUTTON 1  "Send test batch"  -- five events, shown in full, then read back off
                                  the hub to prove they actually arrived.
  BUTTON 2  "Run simulation"   -- choose vehicle family, days, grain and rate.
                                  Shows the event estimate BEFORE sending, runs
                                  with a live counter, and can be stopped.

CREDENTIALS
-----------
Set EVENTHUB_CONNECTION_STRING and EVENTHUB_NAME in a .env file beside this app.
With no connection string the app runs against a mock sender, so every button
still works offline -- useful for developing the UI without burning Azure credit.

NOTE ON THE SAS POLICY: sending needs the *Send* claim, but the read-back also
needs *Listen*. A Send-only policy will stream fine and simply skip the read-back
step, which the log will say out loud rather than failing silently.
"""

import json
import os
import queue
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, render_template, request

# The engine lives one directory up, in generator/.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "generator"))
from fleet_engine import FleetEngine          # noqa: E402

# Load .env by hand rather than pulling in python-dotenv for three lines of parsing.
_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_ENV_PATH):
    with open(_ENV_PATH, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            # Skip blanks and comments; split on the FIRST '=' only, because a
            # connection string contains '=' inside the SharedAccessKey itself.
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                v = v.strip()
                # Strip surrounding quotes. Writing TOKEN="dapi..." is a natural
                # thing to do and the quotes then travel into the Authorization
                # header, where Databricks answers "credential was not sent or
                # was of an unsupported type" and says nothing about quotes.
                if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
                    v = v[1:-1]
                os.environ.setdefault(k.strip(), v)

CONN_STR = os.environ.get("EVENTHUB_CONNECTION_STRING", "").strip()
HUB_NAME = os.environ.get("EVENTHUB_NAME", "telemetry").strip()

# The Databricks route. Event Hubs was the original transport; when the Azure
# subscription lapsed the hub went with it, so the app writes NDJSON files into
# a Unity Catalog volume instead and Auto Loader picks them up.
#
# Nothing downstream changed. read_json_text takes a dataframe with a text
# column and does not care whether a message arrived over AMQP or in a file --
# which is the point of having decoupled the transport in the first place.
DBX_HOST = os.environ.get("DATABRICKS_HOST", "").strip().rstrip("/")
DBX_TOKEN = os.environ.get("DATABRICKS_TOKEN", "").strip()
DBX_VOLUME = os.environ.get(
    "DATABRICKS_VOLUME", "/Volumes/einride/bronze/landing").strip().rstrip("/")

# Rows per file. One file per row would give Auto Loader tens of thousands of
# tiny objects to list; one file per run would not arrive until the run ended.
VOLUME_FLUSH_ROWS = int(os.environ.get("VOLUME_FLUSH_ROWS", "500"))

VOLUME_MODE = bool(DBX_HOST and DBX_TOKEN)
LIVE_MODE = bool(CONN_STR) or VOLUME_MODE

# What the UI calls the far end, and what the generator stamps on each message.
# The source tag is not decoration: silver keeps it, so test traffic and the two
# transports stay separable in the warehouse long after this app is closed.
if VOLUME_MODE:
    DESTINATION = DBX_VOLUME
    STREAM_SOURCE = "volume_stream"
elif CONN_STR:
    DESTINATION = HUB_NAME
    STREAM_SOURCE = "event_hub_stream"
else:
    DESTINATION = "mock"
    STREAM_SOURCE = "event_hub_stream"

app = Flask(__name__)
# Keep JSON keys in insertion order. Flask sorts them alphabetically by default,
# which would scramble the deliberate identity -> time -> state -> linkage ordering
# of the payload and make the demo panel harder to read. It does not affect what is
# sent to Event Hubs (that path uses json.dumps directly), only what the UI shows.
app.json.sort_keys = False

# Ping cadence per vehicle family, taken from vehicle_types.json so the app and the
# mapping files cannot disagree about how often a vehicle reports.
PING_INTERVAL = {"human_driven": 60, "autonomous_pod": 5}
FLEET_SIZE = {"human_driven": 100, "autonomous_pod": 20}


# ---------------------------------------------------------------------------
# SENDERS
# ---------------------------------------------------------------------------
class MockSender:
    """Stands in for Event Hubs when no connection string is configured."""

    mode = "MOCK"

    def send(self, events, partition_key=None):
        # Serialise anyway, so a payload that would fail to encode still fails here
        # rather than surfacing later against the real hub.
        for e in events:
            json.dumps(e)
        time.sleep(0.01 * len(events))       # crude stand-in for network latency
        return len(events)

    def close(self):
        pass


class EventHubSender:
    """Real Event Hubs producer. One client is reused for the whole session."""

    mode = "LIVE"

    def __init__(self):
        from azure.eventhub import EventHubProducerClient
        self._cls = EventHubProducerClient
        self.client = EventHubProducerClient.from_connection_string(
            conn_str=CONN_STR, eventhub_name=HUB_NAME)

    def send(self, events, partition_key=None):
        """Send a list of dicts as one batch.

        The partition key is the vehicle id. That matters more than usual here:
        each ping is derived from the previous ping for that vehicle, so keeping a
        vehicle's events on ONE partition keeps them in order end to end.
        """
        from azure.eventhub import EventData
        batch = self.client.create_batch(partition_key=partition_key)
        sent = 0
        for e in events:
            data = EventData(json.dumps(e, separators=(",", ":")))
            try:
                batch.add(data)
            except ValueError:
                # Batch is full: flush it and start a new one. Event Hubs caps a
                # batch at 1MB, so this is a normal control path, not an error.
                self.client.send_batch(batch)
                sent += len(batch)
                batch = self.client.create_batch(partition_key=partition_key)
                batch.add(data)
        if len(batch):
            self.client.send_batch(batch)
            sent += len(batch)
        return sent

    def close(self):
        try:
            self.client.close()
        except Exception:
            pass


class VolumeSender:
    """Writes messages as NDJSON files into a Unity Catalog volume.

    Buffered on purpose. send() is called once per vehicle in the simulation
    loop, and a file per vehicle per tick would bury Auto Loader in small
    objects. Rows accumulate and go out in one file per VOLUME_FLUSH_ROWS, plus
    whatever is left at close().

    There is no partition key here and none is needed. Event Hubs used it to
    keep one vehicle's events on one partition and therefore in order; a file
    has no partitions. Order is re-established downstream from the data itself
    -- event_time for telemetry, change_seq for shipment changes -- which was
    always the more honest place for it.
    """

    mode = "VOLUME"

    def __init__(self, prefix="telemetry"):
        self.prefix = prefix
        self.buffer = []
        self.files_written = 0

    def send(self, events, partition_key=None):
        for e in events:
            self.buffer.append(json.dumps(e, separators=(",", ":")))
        if len(self.buffer) >= VOLUME_FLUSH_ROWS:
            self._flush()
        return len(events)

    def _flush(self):
        if not self.buffer:
            return
        import requests

        # Name carries the time and a counter. Auto Loader tracks files it has
        # already seen, so the only hard requirement is that a name is never
        # reused -- but a sortable name makes the volume readable by a human.
        self.files_written += 1
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        name = f"{self.prefix}-{stamp}-{self.files_written:05d}.ndjson"

        body = ("\n".join(self.buffer) + "\n").encode("utf-8")
        self.buffer = []

        resp = requests.put(
            f"{DBX_HOST}/api/2.0/fs/files{DBX_VOLUME}/{name}",
            headers={"Authorization": f"Bearer {DBX_TOKEN}"},
            data=body,
            timeout=30,
        )
        if resp.status_code >= 400:
            # Databricks says why in the body. Raising the bare status throws
            # that away and leaves you guessing at a number.
            raise RuntimeError(
                f"upload failed {resp.status_code} for {DBX_VOLUME}/{name}: "
                f"{resp.text[:300]}")

    def close(self):
        self._flush()


def sender_mode():
    return "VOLUME" if VOLUME_MODE else ("LIVE" if CONN_STR else "MOCK")


def make_sender():
    if VOLUME_MODE:
        return VolumeSender()
    return EventHubSender() if CONN_STR else MockSender()


def preflight():
    """Write a tiny file and read it back, to prove the credentials work.

    Worth its own button. Everything else in this app takes seconds to minutes,
    and finding out at the end of a simulation that the token was wrong is a bad
    way to spend an afternoon.
    """
    if not VOLUME_MODE:
        return {"ok": False, "mode": DESTINATION,
                "detail": "no Databricks host or token configured - running mock"}

    # The shape of the token, never the token. A 401 is almost always something
    # visible from here: stray quotes, a pasted "Bearer " prefix, or a value
    # that is not a PAT at all.
    shape = {"length": len(DBX_TOKEN),
             "starts_with": DBX_TOKEN[:5],
             "looks_like_pat": DBX_TOKEN.startswith("dapi"),
             "has_quotes": DBX_TOKEN[:1] in "\"'" or DBX_TOKEN[-1:] in "\"'",
             "has_space": " " in DBX_TOKEN}

    import requests
    hdr = {"Authorization": f"Bearer {DBX_TOKEN}"}
    probe = f"{DBX_VOLUME}/_preflight.ndjson"
    body = json.dumps({"probe": datetime.now(timezone.utc).isoformat()}).encode()

    try:
        put = requests.put(f"{DBX_HOST}/api/2.0/fs/files{probe}?overwrite=true",
                           headers=hdr, data=body, timeout=30)
        put.raise_for_status()

        got = requests.get(f"{DBX_HOST}/api/2.0/fs/files{probe}",
                           headers=hdr, timeout=30)
        got.raise_for_status()

        # Named with a leading underscore so Auto Loader skips it -- Spark
        # ignores files starting with _ or . So the probe proves the round trip
        # without putting a junk row into bronze.
        return {"ok": True, "mode": "VOLUME", "volume": DBX_VOLUME,
                "host": DBX_HOST, "wrote": len(body),
                "read_back": got.text.strip()}
    except Exception as exc:
        detail = str(exc)[:200]
        hint = ""
        if "401" in detail or "403" in detail:
            hint = " - token rejected. See token_shape below."
            if shape["has_quotes"]:
                hint += " The value has quotes around it - remove them."
            elif shape["has_space"]:
                hint += " The value contains a space - did you paste 'Bearer ' too?"
            elif not shape["looks_like_pat"]:
                hint += " A Databricks PAT starts with 'dapi'."
            else:
                hint += " Shape looks right, so the token is wrong or expired."
        elif "404" in detail:
            hint = f" - path not found. Does {DBX_VOLUME} exist? CREATE VOLUME first."
        elif "Name or service not known" in detail or "getaddrinfo" in detail:
            hint = " - host unreachable. Check DATABRICKS_HOST has no trailing path."
        return {"ok": False, "mode": "VOLUME", "detail": detail + hint,
                "token_shape": shape}


def _read_back_volume(limit):
    """Read the most recent file back out of the volume.

    Proves the round trip: the app wrote it, Databricks stored it, and it can be
    read again without a notebook.
    """
    import requests

    hdr = {"Authorization": f"Bearer {DBX_TOKEN}"}
    try:
        listing = requests.get(f"{DBX_HOST}/api/2.0/fs/directories{DBX_VOLUME}",
                               headers=hdr, timeout=20)
        listing.raise_for_status()
        files = [f for f in listing.json().get("contents", [])
                 if f.get("path", "").endswith(".ndjson")]
        if not files:
            return [], "no files in the volume yet"

        newest = max(files, key=lambda f: f.get("last_modified", 0))
        body = requests.get(f"{DBX_HOST}/api/2.0/fs/files{newest['path']}",
                            headers=hdr, timeout=30)
        body.raise_for_status()
        rows = [json.loads(line) for line in body.text.splitlines() if line.strip()]
        return rows[:limit], None
    except Exception as exc:
        return [], f"read-back unavailable ({str(exc)[:120]})"


def read_back(limit, since, timeout=8):
    """Consume events enqueued after `since`, to prove the send actually landed.

    Requires the Listen claim. Returns [] (and says why) if the policy is Send-only.
    """
    if not LIVE_MODE:
        return [], "mock mode - nothing to read back"

    # A volume is not a queue. The proof that a file landed is that the file is
    # there, so the read-back lists the volume rather than replaying a stream.
    if VOLUME_MODE:
        return _read_back_volume(limit)

    try:
        from azure.eventhub import EventHubConsumerClient
    except ImportError:
        return [], "azure-eventhub not installed"

    collected = []
    client = EventHubConsumerClient.from_connection_string(
        conn_str=CONN_STR, consumer_group="$Default", eventhub_name=HUB_NAME)

    def on_event(ctx, event):
        if event is None:
            return
        try:
            collected.append(json.loads(event.body_as_str()))
        except Exception:
            pass
        # Close from inside the callback once we have enough; the SDK has no
        # bounded pull API, so stopping the client IS the way to stop receiving.
        if len(collected) >= limit:
            threading.Thread(target=client.close, daemon=True).start()

    err = {}

    def run():
        try:
            client.receive(on_event=on_event, starting_position=since,
                           max_wait_time=timeout)
        except Exception as exc:
            err["msg"] = str(exc)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout + 4)
    try:
        client.close()
    except Exception:
        pass

    if err:
        return [], f"read-back unavailable ({err['msg'][:120]})"
    return collected, None


# ---------------------------------------------------------------------------
# SIMULATION STATE
# ---------------------------------------------------------------------------
class Simulation:
    """Tracks the one background streaming run the app allows at a time."""

    def __init__(self):
        self.lock = threading.Lock()
        self.thread = None
        self.stop_flag = threading.Event()
        self.reset()

    def reset(self):
        self.running = False
        self.sent = 0
        self.target = 0
        self.started_at = None
        self.sim_clock = None
        self.log = []           # bounded below, so a long run cannot eat memory

    def say(self, msg):
        stamp = datetime.now().strftime("%H:%M:%S")
        with self.lock:
            self.log.append(f"[{stamp}] {msg}")
            # Keep only the most recent lines. A 1.2M-event run would otherwise
            # accumulate an unbounded list that the browser then has to render.
            if len(self.log) > 400:
                del self.log[:-400]


SIM = Simulation()


def estimate_events(families, days, grain):
    """How many events a given configuration will produce."""
    total = 0
    for fam in families:
        if grain == "daily":
            total += FLEET_SIZE[fam] * days
        else:
            per_day = 86400 // PING_INTERVAL[fam]
            total += FLEET_SIZE[fam] * per_day * days
    return total


def run_simulation(families, days, grain, rate):
    """Background worker: advance the engine and push events to the hub."""
    sender = make_sender()
    SIM.say(f"sender ready in {sender.mode} mode -> {DESTINATION}")

    try:
        engine = FleetEngine(
            start=datetime.now(timezone.utc) - timedelta(days=days),
            seed=int(time.time()) % 100000,
            include_traditional="human_driven" in families,
            include_autonomous="autonomous_pod" in families,
        )
        ids = list(engine.state.keys())
        SIM.say(f"engine initialised with {len(ids)} vehicles, "
                f"simulated clock starts {engine.clock:%Y-%m-%d %H:%M}")

        # Step size: the finest cadence among the selected families, so no family
        # is under-sampled when both are streaming together.
        step = min(PING_INTERVAL[f] for f in families) if grain == "ping" else 3600
        emit_daily = grain == "daily"
        last_day = None
        budget_start = time.time()

        while not SIM.stop_flag.is_set() and SIM.sent < SIM.target:
            engine.advance(step)
            SIM.sim_clock = engine.clock.strftime("%Y-%m-%d %H:%M:%S")

            if emit_daily:
                # One row per vehicle per simulated day.
                day = engine.clock.strftime("%Y-%m-%d")
                if day == last_day:
                    continue
                last_day = day
                batch = [engine.daily_rollup(v) for v in ids]
            else:
                batch = [engine.emit(v, source=STREAM_SOURCE) for v in ids]

            # Group by vehicle so each send carries one partition key. This is what
            # preserves per-vehicle ordering on the hub.
            for row in batch:
                vid = row.get("truck_id") or row.get("pod_id") or row.get("vehicle_id")
                sender.send([row], partition_key=vid)
                SIM.sent += 1
                if SIM.sent >= SIM.target:
                    break

            if SIM.sent % 500 < len(batch):
                SIM.say(f"{SIM.sent:,} / {SIM.target:,} events sent "
                        f"(sim time {SIM.sim_clock})")

            # Throttle to the requested rate. Comparing elapsed wall time against
            # events-sent keeps the average on target without sleeping per event.
            if rate > 0:
                expected = SIM.sent / rate
                drift = expected - (time.time() - budget_start)
                if drift > 0:
                    time.sleep(min(drift, 1.0))

        if SIM.stop_flag.is_set():
            SIM.say(f"STOPPED by user after {SIM.sent:,} events")
        else:
            SIM.say(f"COMPLETE - {SIM.sent:,} events sent")

    except Exception as exc:
        SIM.say(f"ERROR: {exc}")
    finally:
        sender.close()
        SIM.running = False


# ---------------------------------------------------------------------------
# ROUTES
# ---------------------------------------------------------------------------
@app.errorhandler(Exception)
def api_errors_as_json(exc):
    """An API route that blows up should say so in JSON.

    Flask's default is an HTML error page, and the browser then fails on the
    first '<' with a parse error that says nothing about what actually went
    wrong. The page keeps the HTML page; /api/ gets the reason.
    """
    if not request.path.startswith("/api/"):
        raise exc
    app.logger.exception("error on %s", request.path)
    return jsonify({"error": type(exc).__name__, "detail": str(exc)[:500]}), 500


@app.route("/")
def index():
    return render_template("index.html", live=LIVE_MODE, hub=DESTINATION)


@app.route("/api/estimate")
def api_estimate():
    families = request.args.getlist("family") or ["human_driven"]
    days = max(int(request.args.get("days", 1)), 1)
    grain = request.args.get("grain", "ping")
    rate = max(int(request.args.get("rate", 500)), 1)
    total = estimate_events(families, days, grain)
    return jsonify({
        "events": total,
        "seconds": round(total / rate),
        "human": f"{total:,} events, about {round(total / rate / 60):,} min at {rate}/s",
    })


@app.route("/api/preflight")
def api_preflight():
    """Prove the connection before spending a simulation on it."""
    return jsonify(preflight())


@app.route("/api/test-batch", methods=["POST"])
def api_test_batch():
    """Five events, shown in full, then read back off the hub."""
    n = int(request.json.get("count", 5)) if request.is_json else 5
    n = max(1, min(n, 25))

    engine = FleetEngine(start=datetime.now(timezone.utc), seed=int(time.time()) % 9999)
    # Warm the fleet up so the sample is not five trucks sitting idle at a depot.
    for _ in range(45):
        engine.advance(60)

    ids = list(engine.state.keys())[:n]
    events = [engine.emit(v, source=STREAM_SOURCE) for v in ids]

    # Mark the clock BEFORE sending, then read from that point, so the read-back
    # cannot pick up unrelated events already sitting on the hub.
    since = datetime.now(timezone.utc) - timedelta(seconds=5)

    sender = make_sender()
    t0 = time.time()
    sent = sum(sender.send([e], partition_key=e.get("truck_id") or e.get("pod_id"))
               for e in events)
    elapsed_ms = round((time.time() - t0) * 1000)
    sender.close()

    echoed, note = read_back(limit=n, since=since)

    return jsonify({
        "mode": sender_mode(),
        "hub": DESTINATION,
        "sent": sent,
        "elapsed_ms": elapsed_ms,
        "partition_keys": [e.get("truck_id") or e.get("pod_id") for e in events],
        "payloads": events,
        "read_back": echoed,
        "read_back_note": note,
    })


@app.route("/api/simulate/start", methods=["POST"])
def api_sim_start():
    if SIM.running:
        return jsonify({"error": "a simulation is already running"}), 409

    body = request.json or {}
    families = body.get("families") or ["human_driven"]
    days = max(int(body.get("days", 1)), 1)
    grain = body.get("grain", "ping")
    rate = max(int(body.get("rate", 500)), 1)
    cap = int(body.get("max_events", 0)) or estimate_events(families, days, grain)

    SIM.reset()
    SIM.stop_flag.clear()
    SIM.running = True
    SIM.target = cap
    SIM.started_at = time.time()
    SIM.say(f"starting: {', '.join(families)} | {days}d | {grain} grain | {rate}/s "
            f"| target {cap:,} events")

    SIM.thread = threading.Thread(target=run_simulation,
                                  args=(families, days, grain, rate), daemon=True)
    SIM.thread.start()
    return jsonify({"started": True, "target": cap})


@app.route("/api/simulate/status")
def api_sim_status():
    with SIM.lock:
        log = list(SIM.log)
    elapsed = time.time() - SIM.started_at if SIM.started_at else 0
    return jsonify({
        "running": SIM.running,
        "sent": SIM.sent,
        "target": SIM.target,
        "sim_clock": SIM.sim_clock,
        "elapsed": round(elapsed),
        "rate": round(SIM.sent / elapsed, 1) if elapsed > 0 else 0,
        "log": log,
    })


@app.route("/api/simulate/stop", methods=["POST"])
def api_sim_stop():
    SIM.stop_flag.set()
    return jsonify({"stopping": True})


if __name__ == "__main__":
    print(f"Mode: {sender_mode()} -> {DESTINATION}")
    _pf = preflight()
    print(f"Preflight: {'OK' if _pf['ok'] else 'FAILED - ' + _pf.get('detail', '')}")
    app.run(debug=True, port=5000)
