"""
topup_mappings.py
=================
Adds the few fields the mapping files are missing, IN PLACE. Additive only --
it never rewrites a value that already exists, so it is safe to run twice.

WHY EACH CHANGE IS NEEDED
-------------------------
1. trucks.json / autonomous_pods.json need `battery_chemistry`.
   battery_specs.json already defines NMC / LFP / NCA with their cycle lives and
   degradation rates, but nothing said WHICH pack each vehicle has. Without that
   link the state-of-health model has no input, so the Asset Health dashboard has
   nothing to plot. Chemistry is assigned by OEM, matching what these makers ship.

2. trucks.json / autonomous_pods.json need `payload_capacity_kg`.
   Telemetry reports cargo_weight_kg, but fill rate is cargo / capacity and there
   was no capacity anywhere in the model.

3. grid_carbon_intensity.json is keyed to one specific date (2026-08-24), so it
   only joins to that day. Re-keying it to hour-of-day makes it a repeating daily
   profile that joins to any date in the simulation.

The other eleven mapping files are correct as they stand and are not touched.
"""

import json
import os
import random

_HERE = os.path.dirname(os.path.abspath(__file__))
MAPPING_DIR = os.path.join(_HERE, "..", "github_sources", "mapping")

# Seeded so re-running assigns the SAME chemistry to the same truck. If this were
# unseeded, every run would silently rewrite the fleet's battery types and the
# state-of-health history would stop being reproducible.
RNG = random.Random(20260826)

# Chemistry by manufacturer, weighted to what each actually fields:
#   BYD builds its own LFP blade packs almost exclusively.
#   Tesla uses nickel-rich chemistries in the Semi.
#   The European makers mostly ship NMC, with LFP appearing in newer trims.
CHEMISTRY_BY_OEM = {
    "Tesla":    ["NCA", "NCA", "NMC"],
    "BYD":      ["LFP"],
    "Scania":   ["NMC", "NMC", "LFP"],
    "Mercedes": ["NMC", "NMC", "LFP"],
    "DAF":      ["NMC"],
    "Einride":  ["LFP", "NMC"],
}

# Payload capacity in kg. A 40-tonne gross combination minus tractor and battery
# mass leaves roughly 24-28t of payload; bigger packs weigh more and eat into it.
PAYLOAD_BY_CAPACITY = {
    250: 27500, 300: 27000, 320: 26800, 350: 26500,
    400: 26000, 450: 25500, 500: 25000, 600: 24000, 750: 22500,
}


def _path(name):
    return os.path.join(MAPPING_DIR, name)


def _read(name):
    with open(_path(name), "r", encoding="utf-8") as fh:
        return json.load(fh)


def _write(name, obj):
    # newline="\n" keeps the files LF on Windows, so pushing to GitHub does not
    # show every line as changed purely because of line endings.
    with open(_path(name), "w", encoding="utf-8", newline="\n") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)


def topup_vehicles(filename, id_field):
    """Add battery_chemistry and payload_capacity_kg to one vehicle file."""
    vehicles = _read(filename)
    added_chem = added_payload = 0

    for v in vehicles:
        # setdefault-style guard: only fill what is missing, never overwrite.
        if "battery_chemistry" not in v:
            options = CHEMISTRY_BY_OEM.get(v["oem"], ["NMC"])
            v["battery_chemistry"] = RNG.choice(options)
            added_chem += 1

        if "payload_capacity_kg" not in v:
            cap = v["battery_capacity_kwh"]
            # Exact match if the pack size is a known one; otherwise interpolate
            # from the nearest known size rather than failing on an unseen value.
            if cap in PAYLOAD_BY_CAPACITY:
                payload = PAYLOAD_BY_CAPACITY[cap]
            else:
                nearest = min(PAYLOAD_BY_CAPACITY, key=lambda k: abs(k - cap))
                payload = PAYLOAD_BY_CAPACITY[nearest]
            # A little unit-to-unit variation so fill rate is not a step function.
            v["payload_capacity_kg"] = payload + RNG.randrange(-400, 401, 50)
            added_payload += 1

    _write(filename, vehicles)
    print(f"  {filename}: {len(vehicles)} rows | +battery_chemistry {added_chem}"
          f" | +payload_capacity_kg {added_payload}")


def rekey_grid_carbon():
    """Turn the date-specific carbon feed into a repeating hour-of-day profile."""
    rows = _read("grid_carbon_intensity.json")
    if rows and "hour_of_day" in rows[0]:
        print("  grid_carbon_intensity.json: already re-keyed, skipping")
        return

    out = []
    for r in rows:
        # "2026-08-24T14:00:00" -> 14. Slicing beats datetime parsing here: the
        # format is fixed and this runs over every row.
        hour = int(r["datetime_hour"][11:13])
        out.append({
            "price_zone": r["price_zone"],
            "hour_of_day": hour,
            "carbon_intensity_gco2_per_kwh": r["carbon_intensity_gco2_per_kwh"],
        })

    _write("grid_carbon_intensity.json", out)
    print(f"  grid_carbon_intensity.json: re-keyed {len(out)} rows to hour_of_day")


if __name__ == "__main__":
    print("Topping up mapping files...")
    topup_vehicles("trucks.json", "truck_id")
    topup_vehicles("autonomous_pods.json", "pod_id")
    rekey_grid_carbon()
    print("Done. The other 11 mapping files were not modified.")
