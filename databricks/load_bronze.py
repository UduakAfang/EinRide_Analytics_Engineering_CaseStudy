# Databricks notebook source
# Bronze Initial Load — GitHub -> einride.bronze
#
# WHY NOT ADLS ANY MORE
# ---------------------
# The Azure subscription is out of credit, so the storage account is read-only
# and the SAS token buys nothing. The files it served are the same files that
# live in this repo, so bronze reads them straight from GitHub instead.
#
# This is not a downgrade. The archive was always a batch backfill of static
# files, and GitHub serves static files over HTTPS with no credential, no
# egress cost and no expiry. What it does NOT replace is the live stream:
# Event Hubs is Azure, so the streaming half of the pipeline is parked until
# billing is sorted. The archive alone is enough to build every silver and
# gold model.

import pandas as pd
import requests

# No token. The repo is public, so raw.githubusercontent.com serves these
# directly. Pin to a branch, not a commit, so a regeneration is picked up by
# re-running this notebook rather than editing it.
REPO = "UduakAfang/EinRide_Analytics_Engineering_CaseStudy"
BRANCH = "main"
RAW = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}"

# folder is part of the entry now, because the repo keeps generated data and
# hand-maintained mapping in two places. shipments is still the exception:
# SDP owns einride.bronze.shipments, so the archive lands beside it.
files = [
    {"folder": "data", "file": "traditional_sample.ndjson"},
    {"folder": "data", "file": "autonomous_sample.ndjson"},
    {"folder": "data", "file": "shipments.ndjson", "table": "shipments_archive"},
    {"folder": "data", "file": "daily_rollup.ndjson"},
    {"folder": "data", "file": "weather_hourly.ndjson"},

    {"folder": "mapping", "file": "trucks.json"},
    {"folder": "mapping", "file": "customers.json"},
    {"folder": "mapping", "file": "routes.json"},
    {"folder": "mapping", "file": "depots.json"},
    {"folder": "mapping", "file": "drivers.json"},
    {"folder": "mapping", "file": "tariffs.json"},
    {"folder": "mapping", "file": "grid_carbon_intensity.json"},
    {"folder": "mapping", "file": "battery_specs.json"},
    {"folder": "mapping", "file": "charger_types.json"},
    {"folder": "mapping", "file": "vehicle_types.json"},
    {"folder": "mapping", "file": "weather_mapping.json"},
    {"folder": "mapping", "file": "sla_tiers.json"},
    {"folder": "mapping", "file": "autonomous_pods.json"},
    {"folder": "mapping", "file": "oem_config.json"},
]

# Set True to rebuild a table that already exists. Left False the notebook is
# safe to re-run: it skips what is loaded and only fills the gaps.
RELOAD = False


for file in files:
    filename = file["file"]

    default_name = filename.replace('.ndjson', '').replace('.json', '')
    table_name = file.get("table", default_name)
    full_table = f"einride.bronze.{table_name}"

    # The stream reads these tables. Overwriting one mid-query pulls the ground
    # out from under it, so an existing table is left alone unless asked.
    if spark.catalog.tableExists(full_table) and not RELOAD:
        print(f"Skipping {full_table}, already loaded")
        continue

    url = f"{RAW}/{file['folder']}/{filename}"

    try:
        if filename.endswith('.ndjson'):
            df_pandas = pd.read_json(url, lines=True)
        else:
            df_pandas = pd.read_json(url)

        # Files keyed by name, like {"NMC": {...}, "LFP": {...}}, put those names
        # in the pandas index, and Spark drops the index. Keep them as a column.
        if not isinstance(df_pandas.index, pd.RangeIndex):
            df_pandas = df_pandas.rename_axis("key").reset_index()

        df_spark = spark.createDataFrame(df_pandas)

        df_spark.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table)

        print(f"Loaded {full_table} ({df_spark.count()} rows)")

    except Exception as e:
        # The five OEMs name the same field differently — "Vehicle speed" and
        # "Vehicle Speed" collide because Spark SQL is case-insensitive. Keep the
        # text as it came and let silver parse it with an explicit schema.
        print(f"Could not be typed: {str(e)[:200]}")

        response = requests.get(url)
        response.raise_for_status()
        raw_content = response.text

        # One message per row for NDJSON, matching how Event Hubs delivers, so
        # the archive and the stream can share one silver reader.
        if filename.endswith('.ndjson'):
            rows = [{"value": line} for line in raw_content.splitlines() if line.strip()]
        else:
            rows = [{"value": raw_content}]

        df_spark = spark.createDataFrame(rows)

        df_spark.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table)

        print(f"Loaded {full_table} as raw text ({df_spark.count()} rows)")
