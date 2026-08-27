"""
fleet_engine.py
===============
The single source of truth for ALL synthetic telemetry in this project.

WHY THIS FILE EXISTS
--------------------
The first generator built every row independently: it picked a random truck,
a random waypoint and a random timestamp, then rolled random values. Row by row
that looks fine. But sort one truck's rows by time and it falls apart -- the
lifetime energy meter counts DOWN, state of charge jumps 81% -> 20% -> 93%,
and the truck teleports 900km in nine minutes.

That matters because every headline metric in the three dashboards is a WINDOW
FUNCTION over a per-vehicle time series:
    cycle count        = lifetime_energy / battery_capacity      (needs monotonic energy)
    state of health    = trend of capacity fade over time        (needs a real timeline)
    kWh per km         = delta(energy) / delta(distance)         (needs both to rise together)
    deadhead ratio     = empty_km / total_km                     (needs real distances)

Real telemetry has three kinds of field and they behave differently:
    1. INSTANT READINGS  (speed, lat/lon, battery temp) -- free to move around
    2. LIFETIME COUNTERS (odometer, lifetime energy)    -- only ever increase
    3. DRIFTING STATE    (state of charge, health)      -- change smoothly, never jump
Tesla literally names its field `LifetimeEnergyUsedDrive`. Meters do not run backwards.

So this engine keeps a state dictionary per vehicle and DERIVES each new ping from
the previous one. It is used by BOTH the historical file regeneration and the live
Event Hubs streamer, which is what keeps the two datasets consistent with each other.

DESIGN NOTES
------------
* The clock is SIMULATED, not wall-clock. `step_seconds` decides how much fleet time
  passes per tick, so 24 months of battery degradation can be produced in minutes.
* Vehicles are advanced in lockstep and pings are emitted in CHRONOLOGICAL order.
  That means a caller can stop early (e.g. on a file-size budget) and simply get a
  shorter time window covering every vehicle evenly, rather than a truncated fleet.
* Per-OEM field naming is applied at the very last step, in `_apply_oem_naming`, so
  the physics stays in one vocabulary and only the output is dialect-specific.
"""

from __future__ import annotations

import json
import math
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

# Resolve the mapping folder relative to THIS file, so the engine works no matter
# which directory the caller runs from (notebook, Flask app, or a plain script).
_HERE = os.path.dirname(os.path.abspath(__file__))
MAPPING_DIR = os.path.join(_HERE, "..", "github_sources", "mapping")

# ---------------------------------------------------------------------------
# PHYSICS CONSTANTS
# Tuned so the resulting numbers land in the range published for real electric
# heavy trucks (~1.0-1.4 kWh/km at 40t gross). They are deliberately simple:
# the goal is internally consistent data, not a simulation of vehicle dynamics.
# ---------------------------------------------------------------------------
BASE_KWH_PER_KM = 1.05      # consumption of an empty tractor unit on flat highway
LOADED_PENALTY = 0.45       # extra fraction of BASE consumed at 100% payload
HIGHWAY_SPEED_KMH = 78.0    # cruising speed on the E4/E6 long-haul routes
URBAN_SPEED_KMH = 42.0      # average speed on the Gothenburg distribution loop
SOC_CHARGE_FLOOR = 22.0     # below this the vehicle diverts to a depot to charge
SOC_CHARGE_TARGET = 90.0    # charging stops here (topping to 100% is slow and rare)
FAST_CHARGE_THRESHOLD_KW = 100.0   # above this counts as a "fast charge" for wear
CALENDAR_FADE_MULTIPLIER = 1.0     # scales battery_specs.degradation_rate (%/day)
CYCLE_FADE_TO_EOL_PCT = 20.0       # a battery is "end of life" after losing 20% SoH
FAST_CHARGE_WEAR_FACTOR = 1.6      # fast charging ages the pack 60% faster per cycle


def _load(name: str):
    """Read one mapping JSON file and return the parsed object.

    Kept as a helper so every load uses the same encoding and path handling --
    Windows defaults to cp1252, which would mangle the Swedish depot names.
    """
    with open(os.path.join(MAPPING_DIR, name), "r", encoding="utf-8") as fh:
        return json.load(fh)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two WGS84 points.

    Used once at startup to sanity-check route distances, not in the hot loop --
    inside the loop we advance along a precomputed route length instead, which
    avoids ~1M trigonometric calls per run.
    """
    r = 6371.0                                   # mean Earth radius in km
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1                                 # delta latitude in radians
    dl = math.radians(lon2 - lon1)               # delta longitude in radians
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class FleetEngine:
    """Holds the live state of every vehicle and advances it through simulated time."""

    def __init__(self, start: datetime, seed: int = 42, include_traditional: bool = True,
                 include_autonomous: bool = True):
        # A seeded Random INSTANCE (not the global module) so two engines running in
        # the same process -- e.g. the Flask app and a background regeneration -- cannot
        # disturb each other's sequence. Same seed always reproduces the same fleet.
        self.rng = random.Random(seed)

        # Simulated clock. Everything downstream reads this, never datetime.now().
        self.clock = start.replace(tzinfo=timezone.utc)

        # ---- Load reference data once, up front -------------------------------
        self.trucks = _load("trucks.json")
        self.pods = _load("autonomous_pods.json")
        self.oem_config = _load("oem_config.json")
        self.depots = _load("depots.json")
        self.routes = _load("routes.json")
        self.customers = _load("customers.json")
        self.drivers = _load("drivers.json")
        self.chemistries = _load("battery_specs.json")["battery_chemistries"]
        self.charger_types = _load("charger_types.json")["charger_types"]
        self.weather_map = _load("weather_mapping.json")["weather_conditions"]

        # ---- Build O(1) lookup tables ----------------------------------------
        # Every one of these replaces a list scan that would otherwise run inside
        # the per-tick loop. With ~1M ticks the difference is minutes, not milliseconds.
        self.depot_by_id = {d["depot_id"]: d for d in self.depots}
        self.drivers_by_depot: dict[str, list] = {}
        for drv in self.drivers:
            self.drivers_by_depot.setdefault(drv["home_depot_id"], []).append(drv)

        # Charger power options sorted ascending, so picking a depot-capped charger
        # is a filter over a small pre-sorted list rather than a repeated max().
        self.charger_powers = sorted(c["power_kw"] for c in self.charger_types.values())

        # Routes are directed in routes.json (STO->JKP) but a truck must be able to
        # come home again. Build an adjacency map that contains BOTH directions, and
        # tag the direction so the return leg can be marked as empty running.
        self.routes_from: dict[str, list] = {}
        for r in self.routes:
            self.routes_from.setdefault(r["origin"], []).append((r, "forward"))
            if r["origin"] != r["dest"]:                    # skip the GOT->GOT urban loop
                self.routes_from.setdefault(r["dest"], []).append((r, "reverse"))

        # Weather is decided once per region per simulated DAY, not per ping, so a
        # truck does not drive through snow and sunshine in the same minute.
        self.weather_by_day: dict[tuple[str, str], str] = {}

        # Monotonic counters for generated identifiers.
        self._shipment_seq = 0
        self._charge_seq = 0

        # Shipments are FACTS, not reference data: they are produced as the fleet
        # runs and written out beside the telemetry. This ledger is what gives the
        # Sustainability dashboard a customer to attribute emissions to, and the Ops
        # dashboard a promised-vs-actual arrival to measure OTIF against.
        self.shipments: list[dict] = []

        # ---- Materialise vehicle state ---------------------------------------
        self.state: dict[str, dict] = {}
        if include_traditional:
            for t in self.trucks:
                self.state[t["truck_id"]] = self._init_vehicle(t, "human_driven")
        if include_autonomous:
            for p in self.pods:
                self.state[p["pod_id"]] = self._init_vehicle(p, "autonomous_pod")

    # ------------------------------------------------------------------
    # SETUP
    # ------------------------------------------------------------------
    def _init_vehicle(self, spec: dict, kind: str) -> dict:
        """Create the starting state row for one vehicle.

        Vehicles do NOT all start pristine. Purchase/deployment date drives how much
        life the battery has already used, so the fleet shows a spread of health from
        day one -- which is what makes the Asset Health dashboard interesting.
        """
        vid = spec.get("truck_id") or spec["pod_id"]
        depot = self.depot_by_id[spec["home_depot_id"]]
        capacity = spec["battery_capacity_kwh"]

        # battery_chemistry is added to trucks.json by the mapping top-up script.
        # Fall back to NMC so the engine still runs against the un-topped-up files.
        chem_name = spec.get("battery_chemistry", "NMC")
        chem = self.chemistries.get(chem_name, self.chemistries["NMC"])

        # Age the vehicle to "now": work out how long it has been in service and
        # give it a plausible lifetime mileage, then derive energy from that. This is
        # why two trucks bought a year apart show visibly different health.
        bought = datetime.fromisoformat(
            (spec.get("purchase_date") or spec["deployment_date"]).replace("Z", "")
        ).replace(tzinfo=timezone.utc)
        days_in_service = max((self.clock - bought).days, 0)
        km_per_day = self.rng.uniform(180, 340)                    # regional haul duty cycle
        lifetime_km = days_in_service * km_per_day
        lifetime_kwh = lifetime_km * BASE_KWH_PER_KM * self.rng.uniform(1.05, 1.25)

        return {
            "vehicle_id": vid,
            "kind": kind,
            "oem": spec["oem"],
            "model": spec.get("model"),
            "capacity_kwh": capacity,
            "chemistry": chem_name,
            "cycle_life": chem["cycle_life"],
            "degradation_rate": chem["degradation_rate"],
            "max_charge_rate_c": chem["max_charge_rate_c"],
            "payload_capacity_kg": spec.get("payload_capacity_kg", 26000),
            "home_depot_id": spec["home_depot_id"],

            # --- drifting state ---
            "soc_pct": self.rng.uniform(55, 95),
            "initial_soh_pct": spec["initial_state_of_health_pct"],
            "soh_pct": spec["initial_state_of_health_pct"],
            "battery_temp_c": self.rng.uniform(15, 25),

            # --- lifetime counters: these may ONLY ever increase ---
            "lifetime_kwh": round(lifetime_kwh, 2),
            "lifetime_km": round(lifetime_km, 1),
            "fast_charge_kwh": 0.0,        # share of lifetime energy taken at high power
            "charge_sessions": 0,
            "fast_charge_sessions": 0,

            # --- position and mission ---
            "lat": depot["latitude"],
            "lon": depot["longitude"],
            "at_depot": spec["home_depot_id"],
            "route": None,
            "direction": None,
            "route_km_done": 0.0,
            "status": "idle",              # idle | driving | charging
            "cargo_weight_kg": 0,
            "shipment_id": None,
            "customer_id": None,
            "driver_id": None,
            "charge_session_id": None,
            "charge_power_kw": None,
            "idle_seconds": 0,
            "alerts": [],
            # Persistent lateral offset from the straight depot-to-depot line, so the
            # track reads as a road corridor. It is re-rolled per TRIP, never per ping:
            # fresh noise on every ping made consecutive points jump ~1.3km apart,
            # which at a 60-second cadence implies 80 km/h of phantom speed on top of
            # the real speed -- enough to trip any plausibility check on the data.
            "lat_off": self.rng.uniform(-0.03, 0.03),
            "lon_off": self.rng.uniform(-0.05, 0.05),
        }

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------
    def _uuid(self) -> str:
        """A version-4 UUID drawn from THIS engine's seeded RNG.

        uuid.uuid4() reads os.urandom, so it ignores the seed entirely. Two runs of
        the same script would then differ in event_id on every row -- and nothing
        else -- which makes the output impossible to diff and impossible to reproduce.
        Building the UUID from seeded bits keeps regeneration byte-for-byte stable
        while still producing a well-formed, collision-safe v4 identifier.
        """
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _weather(self, region: str) -> str:
        """Pick (and cache) the weather for one region on the current simulated day."""
        key = (region, self.clock.strftime("%Y-%m-%d"))
        cached = self.weather_by_day.get(key)
        if cached is None:
            # Sweden: mostly clear/cloudy, with snow only in the winter months.
            month = self.clock.month
            pool = ["clear", "cloudy", "cloudy", "rain"]
            if month in (11, 12, 1, 2, 3):
                pool += ["snow", "snow"]
            cached = self.rng.choice(pool)
            self.weather_by_day[key] = cached
        return cached

    def _start_trip(self, st: dict) -> None:
        """Dispatch an idle vehicle onto a route, loaded or empty."""
        options = self.routes_from.get(st["at_depot"])
        if not options:
            return                                   # depot has no outbound route
        route, direction = self.rng.choice(options)

        st["route"] = route
        st["direction"] = direction
        st["route_km_done"] = 0.0
        st["status"] = "driving"
        st["idle_seconds"] = 0
        # New trip, new corridor offset.
        st["lat_off"] = self.rng.uniform(-0.03, 0.03)
        st["lon_off"] = self.rng.uniform(-0.05, 0.05)

        # Forward legs carry freight for a customer. Return legs are empty ~55% of the
        # time -- that empty running IS the deadhead ratio on the Ops dashboard, so it
        # has to be produced here rather than assumed later.
        if direction == "forward" or self.rng.random() > 0.55:
            self._shipment_seq += 1
            customer = self.rng.choice(self.customers)
            st["shipment_id"] = f"SHP-{self._shipment_seq:07d}"
            st["customer_id"] = customer["customer_id"]
            # Loads cluster near capacity but rarely fill it exactly.
            st["cargo_weight_kg"] = int(
                st["payload_capacity_kg"] * self.rng.triangular(0.35, 0.95, 0.78)
            )
            # Promise an arrival time up front. Transit is the route length at the
            # corridor's nominal speed, plus a buffer -- so most runs land on time and
            # a minority slip, which is what an OTIF metric needs to be worth plotting.
            nominal_speed = URBAN_SPEED_KMH if route["origin"] == route["dest"] else HIGHWAY_SPEED_KMH
            transit_h = route["distance_km"] / nominal_speed
            # Buffer chosen so a minority of runs slip. Too generous and OTIF pins at
            # 100% and the metric tells you nothing; too tight and every customer is
            # in breach. This lands around 85-92%, the band real carriers operate in.
            promised = self.clock + timedelta(hours=transit_h * self.rng.uniform(1.02, 1.26))
            record = {
                "shipment_id": st["shipment_id"],
                "customer_id": customer["customer_id"],
                "vehicle_id": st["vehicle_id"],
                "vehicle_type": st["kind"],
                "route_id": route["route_id"],
                "origin_depot_id": route["origin"] if direction == "forward" else route["dest"],
                "dest_depot_id": route["dest"] if direction == "forward" else route["origin"],
                "planned_distance_km": route["distance_km"],
                "cargo_weight_kg": st["cargo_weight_kg"],
                "dispatched_at": self.clock.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "promised_arrival_at": promised.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "actual_arrival_at": None,      # filled in on arrival
                "delivered_on_time": None,
                "is_deadhead": False,
            }
            st["_shipment_rec"] = record
            self.shipments.append(record)
        else:
            st["shipment_id"] = None
            st["customer_id"] = None
            st["cargo_weight_kg"] = 0                # empty running = deadhead
            # Deadhead legs still get a ledger row (with no customer) so the Ops
            # dashboard can divide empty km by total km without guessing.
            record = {
                "shipment_id": None,
                "customer_id": None,
                "vehicle_id": st["vehicle_id"],
                "vehicle_type": st["kind"],
                "route_id": route["route_id"],
                "origin_depot_id": route["dest"],
                "dest_depot_id": route["origin"],
                "planned_distance_km": route["distance_km"],
                "cargo_weight_kg": 0,
                "dispatched_at": self.clock.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "promised_arrival_at": None,
                "actual_arrival_at": None,
                "delivered_on_time": None,
                "is_deadhead": True,
            }
            st["_shipment_rec"] = record
            self.shipments.append(record)

        # Human-driven trucks get a driver from the departure depot's pool.
        # Autonomous pods never do -- driver_id stays NULL, which is the point.
        if st["kind"] == "human_driven":
            pool = self.drivers_by_depot.get(st["at_depot"]) or self.drivers
            st["driver_id"] = self.rng.choice(pool)["driver_id"]
        else:
            st["driver_id"] = None

        st["at_depot"] = None

    def _begin_charge(self, st: dict, depot_id: str) -> None:
        """Plug the vehicle in, choosing a charger the depot can actually supply."""
        depot = self.depot_by_id[depot_id]
        self._charge_seq += 1

        # The charger cannot exceed what the depot supplies, nor what the pack accepts
        # (C-rate * capacity). Both limits are real and both shape the wear model.
        depot_cap = depot["max_charger_power_kw"]
        pack_cap = st["max_charge_rate_c"] * st["capacity_kwh"]
        usable = [p for p in self.charger_powers if p <= min(depot_cap, pack_cap)]
        power = usable[-1] if usable else self.charger_powers[0]

        st["status"] = "charging"
        st["charge_session_id"] = f"CHG-{self._charge_seq:07d}"
        st["charge_power_kw"] = float(power)
        st["charge_sessions"] += 1
        if power >= FAST_CHARGE_THRESHOLD_KW:
            st["fast_charge_sessions"] += 1

    def _age_battery(self, st: dict, kwh_used: float, days: float) -> None:
        """Apply cycle wear and calendar fade. State of health only ever falls."""
        # One "full cycle equivalent" is the pack's usable capacity moved through it.
        cycles = kwh_used / st["capacity_kwh"]
        # A pack is rated to lose CYCLE_FADE_TO_EOL_PCT over cycle_life cycles.
        fade = cycles * (CYCLE_FADE_TO_EOL_PCT / st["cycle_life"])
        # Energy taken at high power ages the pack faster.
        if st["charge_power_kw"] and st["charge_power_kw"] >= FAST_CHARGE_THRESHOLD_KW:
            fade *= FAST_CHARGE_WEAR_FACTOR
        # Calendar fade happens whether the truck moves or not.
        fade += st["degradation_rate"] * days * CALENDAR_FADE_MULTIPLIER
        st["soh_pct"] = max(60.0, st["soh_pct"] - fade)

    # ------------------------------------------------------------------
    # THE TICK
    # ------------------------------------------------------------------
    def advance(self, step_seconds: int) -> None:
        """Move the whole fleet forward by `step_seconds` of simulated time."""
        self.clock += timedelta(seconds=step_seconds)
        hours = step_seconds / 3600.0
        days = step_seconds / 86400.0

        for st in self.state.values():
            st["alerts"] = []                    # alerts are per-ping, not sticky

            if st["status"] == "charging":
                # Charging tapers above 80% SoC -- constant-current then constant-voltage.
                taper = 0.35 if st["soc_pct"] > 80 else 1.0
                kwh_in = st["charge_power_kw"] * hours * taper * 0.94   # 94% charge efficiency
                usable_capacity = st["capacity_kwh"] * st["soh_pct"] / 100.0
                st["soc_pct"] = min(100.0, st["soc_pct"] + kwh_in / usable_capacity * 100.0)
                st["battery_temp_c"] = min(48.0, st["battery_temp_c"] + 1.4 * hours * taper)
                self._age_battery(st, kwh_in, days)
                if st["soc_pct"] >= SOC_CHARGE_TARGET:
                    st["status"] = "idle"
                    st["charge_session_id"] = None
                    st["charge_power_kw"] = None
                continue

            if st["status"] == "idle":
                st["idle_seconds"] += step_seconds
                # Cool towards ambient while parked.
                st["battery_temp_c"] += (12.0 - st["battery_temp_c"]) * min(hours, 1.0) * 0.3
                self._age_battery(st, 0.0, days)
                # Below the floor, plug in rather than dispatch.
                if st["soc_pct"] < SOC_CHARGE_FLOOR and st["at_depot"]:
                    self._begin_charge(st, st["at_depot"])
                # Dispatch only inside the operating window. Without this a truck
                # would drive 24 hours a day and rack up ~1,900 km daily, which would
                # wreck both the utilisation metric and the degradation curve.
                elif (5 <= self.clock.hour < 21
                      and st["idle_seconds"] > self.rng.uniform(900, 5400)):
                    self._start_trip(st)
                continue

            # ---- driving ----
            route = st["route"]
            urban = route["origin"] == route["dest"]
            region = self.depot_by_id[route["origin"]]["region"]
            weather = self._weather(region)
            wx = self.weather_map[weather]

            speed = URBAN_SPEED_KMH if urban else HIGHWAY_SPEED_KMH
            speed *= self.rng.uniform(0.88, 1.12)          # traffic and terrain variance

            # Bad weather slows the corridor down as well as raising consumption.
            # Rolling resistance is the proxy already present in weather_mapping.json,
            # so snow costs both range AND time -- which is what makes "cold weather
            # delays deliveries" show up in the data instead of being asserted.
            speed /= wx.get("rolling_resistance", 1.0)

            km = speed * hours                             # distance covered this tick
            # Consumption rises with payload and with bad weather.
            load_frac = st["cargo_weight_kg"] / max(st["payload_capacity_kg"], 1)
            kwh_per_km = (BASE_KWH_PER_KM * (1 + LOADED_PENALTY * load_frac)
                          * wx["consumption_factor"])
            kwh = km * kwh_per_km

            # --- the three lifetime counters. Increment only. ---
            st["lifetime_km"] += km
            st["lifetime_kwh"] += kwh
            if st["charge_power_kw"]:
                st["fast_charge_kwh"] += 0.0               # only accrues while charging

            usable_capacity = st["capacity_kwh"] * st["soh_pct"] / 100.0
            st["soc_pct"] = max(0.0, st["soc_pct"] - kwh / usable_capacity * 100.0)
            st["battery_temp_c"] = min(52.0, st["battery_temp_c"] + kwh * 0.02)
            self._age_battery(st, kwh, days)

            st["route_km_done"] += km

            # Interpolate position along the straight line between the two depots.
            # Cheap, and accurate enough that the point tracks the real corridor.
            frac = min(st["route_km_done"] / route["distance_km"], 1.0)
            if st["direction"] == "reverse":
                frac = frac                                 # start/end swapped below
                a = self.depot_by_id[route["dest"]]
                b = self.depot_by_id[route["origin"]]
            else:
                a = self.depot_by_id[route["origin"]]
                b = self.depot_by_id[route["dest"]]
            st["lat"] = a["latitude"] + (b["latitude"] - a["latitude"]) * frac
            st["lon"] = a["longitude"] + (b["longitude"] - a["longitude"]) * frac
            # Bow the path away from the straight line using the trip's fixed offset,
            # scaled by sin(pi * frac) so the deviation is zero at both depots and
            # widest mid-route. Smooth in both position and its derivative, so the
            # implied speed between consecutive pings stays realistic.
            bow = math.sin(math.pi * frac)
            st["lat"] += st["lat_off"] * bow
            st["lon"] += st["lon_off"] * bow
            st["speed_kmh"] = round(speed, 1)
            st["weather"] = weather

            if st["soc_pct"] < 15:
                st["alerts"].append("low_state_of_charge")
            if self.rng.random() < 0.0025:
                st["alerts"].append(self.rng.choice(["harsh_braking", "harsh_acceleration",
                                                     "over_speed"]))

            # Arrived: park at the destination depot and drop the load.
            if frac >= 1.0:
                arrived = route["origin"] if st["direction"] == "reverse" else route["dest"]
                # Stamp the actual arrival and settle the on-time flag. Comparing the
                # two ISO strings works because both are zero-padded UTC of the same
                # format -- lexical order equals chronological order, no parsing needed.
                rec = st.pop("_shipment_rec", None)
                if rec is not None:
                    rec["actual_arrival_at"] = self.clock.strftime("%Y-%m-%dT%H:%M:%SZ")
                    if rec["promised_arrival_at"]:
                        rec["delivered_on_time"] = (
                            rec["actual_arrival_at"] <= rec["promised_arrival_at"])
                st["at_depot"] = arrived
                st["status"] = "idle"
                st["route"] = None
                st["route_km_done"] = 0.0
                st["cargo_weight_kg"] = 0
                st["idle_seconds"] = 0
                st["lat"] = self.depot_by_id[arrived]["latitude"]
                st["lon"] = self.depot_by_id[arrived]["longitude"]

    # ------------------------------------------------------------------
    # EMISSION
    # ------------------------------------------------------------------
    def _apply_oem_naming(self, st: dict, payload: dict) -> dict:
        """Rename the canonical soc/speed/energy keys to this OEM's own vocabulary.

        This is the whole point of the mixed fleet: Scania sends
        'EV Battery State Of Charge', Tesla sends 'soc_percentage', DAF sends
        'State of charge'. Reconciling them is the job of the dbt staging layer,
        so the raw data has to actually disagree.
        """
        fields = self.oem_config[st["oem"]]["telemetry_fields"]
        payload[fields["soc"]] = round(st["soc_pct"], 1)
        payload[fields["speed"]] = st.get("speed_kmh", 0.0)
        payload[fields["energy"]] = round(st["lifetime_kwh"], 2)
        return payload

    def emit(self, vehicle_id: str, source: str = "batch_archive") -> dict:
        """Build one telemetry payload from the vehicle's CURRENT state."""
        st = self.state[vehicle_id]
        ts = self.clock.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Fields shared by both vehicle families. Ordering is deliberate: identity,
        # then time, then state, then linkage -- it makes the NDJSON readable by eye.
        common = {
            "event_id": self._uuid(),
            "timestamp": ts,
            "ingested_by": source,
            "latitude": round(st["lat"], 6),
            "longitude": round(st["lon"], 6),
            "battery_temp_c": round(st["battery_temp_c"], 1),
            "cargo_weight_kg": st["cargo_weight_kg"],
            "is_charging": st["status"] == "charging",
            "charge_power_kw": st["charge_power_kw"],
            "charge_session_id": st["charge_session_id"],
            "cumulative_energy_kwh": round(st["lifetime_kwh"], 2),
            "cumulative_distance_km": round(st["lifetime_km"], 1),
            "state_of_health_pct": round(st["soh_pct"], 3),
            "shipment_id": st["shipment_id"],
            "customer_id": st["customer_id"],
            "route_id": st["route"]["route_id"] if st["route"] else None,
            "weather_condition": st.get("weather", "clear"),
            "alerts": st["alerts"],
        }

        if st["kind"] == "human_driven":
            payload = {
                "truck_id": vehicle_id,
                "oem": st["oem"],
                "vehicle_type": "human_driven",
                "driver_id": st["driver_id"],
                **common,
            }
            # Ambient temperature is only reported by the road trucks.
            payload["ambient_temp_c"] = round(
                self.weather_map[st.get("weather", "clear")]["consumption_factor"] * 8
                + self.rng.uniform(-6, 10), 1)
            return self._apply_oem_naming(st, payload)

        # Autonomous pods have no FMS standard to follow, so they emit a nested,
        # vendor-specific shape -- which is itself a useful dbt flattening exercise.
        return {
            "pod_id": vehicle_id,
            "oem": "Einride",
            "vehicle_type": "autonomous_pod",
            "driver_id": None,
            **common,
            "speed_kmh": st.get("speed_kmh", 0.0),
            "state_of_charge_pct": round(st["soc_pct"], 1),
            "perception": {
                "objects_detected": self.rng.randint(0, 14),
                "closest_object_distance_m": round(self.rng.uniform(4, 180), 1),
            },
            "path_planning": {
                "planned_steering_angle_deg": round(self.rng.uniform(-9, 9), 1),
                "planned_acceleration_ms2": round(self.rng.uniform(-2.2, 2.0), 1),
            },
            "safety": {
                "system_health": "normal" if self.rng.random() > 0.02 else "degraded",
                "fallback_mode_active": self.rng.random() < 0.01,
                "emergency_stop_triggered": self.rng.random() < 0.002,
                "disengagement_count": 1 if self.rng.random() < 0.004 else 0,
                "teleoperation_active": self.rng.random() < 0.015,
            },
            "mission_status": ("charging" if st["status"] == "charging"
                               else "en_route" if st["status"] == "driving" else "idle"),
            "remote_monitor_id": f"RM-{self.rng.randint(1, 60):04d}",
            "safety_confidence_score": round(self.rng.uniform(0.86, 0.99), 2),
        }

    def advance_day(self) -> None:
        """Advance the fleet by one whole day using a DAILY duty-cycle model.

        Why a separate path instead of calling advance() 24 times?

        The tick model exists to produce believable positions and speeds between
        pings. A daily summary throws all of that away -- it keeps only distance,
        energy, health and charge counts. Simulating minute-by-minute movement just
        to discard it costs ~2.1M state updates for a two-year history, which is the
        difference between a run that finishes in seconds and one that does not
        finish at all. So the daily path models the DAY as the unit directly.

        The physics is the same physics: distance x consumption -> energy, energy ->
        state of charge and cycle wear. Only the resolution changes.
        """
        self.clock += timedelta(days=1)
        region_weather = {}                       # one lookup per region, not per vehicle

        for st in self.state.values():
            depot = self.depot_by_id[st["home_depot_id"]]
            region = depot["region"]
            weather = region_weather.get(region)
            if weather is None:
                weather = region_weather[region] = self._weather(region)
            wx = self.weather_map[weather]

            # A vehicle works most days and rests on some. Roughly a six-day week,
            # which keeps annual mileage in the right band for regional haulage.
            if self.rng.random() < 0.14:
                self._age_battery(st, 0.0, 1.0)   # calendar fade still applies at rest
                continue

            km = self.rng.uniform(140, 380)       # a day's driving
            load_frac = self.rng.triangular(0.0, 0.95, 0.62)   # incl. empty return legs
            kwh_per_km = (BASE_KWH_PER_KM * (1 + LOADED_PENALTY * load_frac)
                          * wx["consumption_factor"])
            kwh = km * kwh_per_km

            # Lifetime counters: increment only, exactly as in the tick model.
            st["lifetime_km"] += km
            st["lifetime_kwh"] += kwh

            # A day's energy usually needs one recharge, sometimes two on long runs.
            depot_cap = depot["max_charger_power_kw"]
            pack_cap = st["max_charge_rate_c"] * st["capacity_kwh"]
            usable = [p for p in self.charger_powers if p <= min(depot_cap, pack_cap)]
            power = float(usable[-1] if usable else self.charger_powers[0])
            sessions = 1 + (1 if kwh > st["capacity_kwh"] * 0.75 else 0)
            st["charge_sessions"] += sessions
            st["charge_power_kw"] = power
            if power >= FAST_CHARGE_THRESHOLD_KW:
                st["fast_charge_sessions"] += sessions
                st["fast_charge_kwh"] += kwh

            # End-of-day charge state: topped up overnight, with day-to-day variation.
            st["soc_pct"] = self.rng.uniform(62, 95)
            self._age_battery(st, kwh, 1.0)
            st["charge_power_kw"] = None

    def daily_rollup(self, vehicle_id: str) -> dict:
        """One end-of-day summary row per vehicle.

        This is the cheap way to get 24 months of battery degradation without
        generating 60 million pings: same engine, coarser grain.
        """
        st = self.state[vehicle_id]
        # NOTE ON THE FIELD LIST: oem, vehicle_type, battery_chemistry and
        # home_depot_id are deliberately NOT here. They are attributes of the
        # vehicle, not of the day, and they already live in trucks.json /
        # autonomous_pods.json -- dbt joins them on vehicle_id. Copying them onto
        # every one of ~88,000 fact rows would denormalise the model AND roughly
        # double the file size, pushing it past the 25MB limit for no new information.
        # There is no event_id either: vehicle_id + date is the natural key.
        return {
            "vehicle_id": vehicle_id,
            "date": self.clock.strftime("%Y-%m-%d"),
            "ingested_by": "daily_rollup",
            "end_of_day_soc_pct": round(st["soc_pct"], 1),
            "state_of_health_pct": round(st["soh_pct"], 3),
            "cumulative_energy_kwh": round(st["lifetime_kwh"], 2),
            "cumulative_distance_km": round(st["lifetime_km"], 1),
            "full_cycle_equivalents": round(st["lifetime_kwh"] / st["capacity_kwh"], 2),
            "charge_sessions_total": st["charge_sessions"],
            "fast_charge_sessions_total": st["fast_charge_sessions"],
        }
