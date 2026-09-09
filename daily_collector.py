# ============================================================
# 🏇 UK HORSE RACING PREDICTION PIPELINE
# DAILY COLLECTOR — headless version for GitHub Actions
# Same logic as the Colab daily collector, adapted to run
# unattended: credentials from environment variables (GitHub
# Secrets), no Drive mount (persistence is via git commit,
# handled by the workflow after this script runs), fails
# loudly instead of prompting if anything's missing.
# ============================================================

import os
import re
import sys
import json
import duckdb
import pandas as pd
import requests

from pathlib import Path
from datetime import datetime, timezone

print("=" * 70)
print("🏇 UK HORSE RACING PREDICTION PIPELINE")
print("DAILY COLLECTOR (headless / GitHub Actions)")
print("=" * 70)


# ============================================================
# 1. PATHS — relative to repo root (Actions checks out the repo
#    into the working directory, so this is just a plain folder
#    in the repo that gets committed back each run)
# ============================================================

BASE_DIR = Path("horse_racing_pipeline")
SNAPSHOT_DIR = BASE_DIR / "data" / "snapshots"
RAW_DIR = BASE_DIR / "data" / "raw"
DATABASE_DIR = BASE_DIR / "database"
DUCKDB_PATH = DATABASE_DIR / "racing.duckdb"

for d in [SNAPSHOT_DIR, RAW_DIR, DATABASE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(DUCKDB_PATH))
con.execute("CREATE SCHEMA IF NOT EXISTS raw")
con.execute("CREATE SCHEMA IF NOT EXISTS clean")

ALLOWED_REGIONS = ["GB"]


# ============================================================
# 2. CREDENTIALS — from environment (set by the workflow from
#    GitHub Secrets). No interactive fallback: an unattended run
#    that's missing a secret should fail loudly, not hang.
# ============================================================

username = os.environ.get("RACING_API_USERNAME")
password = os.environ.get("RACING_API_PASSWORD")

if not username or not password:
    print("❌ RACING_API_USERNAME / RACING_API_PASSWORD not set in environment.")
    print("   Check the repo's Settings > Secrets and variables > Actions,")
    print("   and that the workflow file passes them through to this step.")
    sys.exit(1)

AUTH = requests.auth.HTTPBasicAuth(username, password)
BASE_URL = "https://api.theracingapi.com/v1"


# ============================================================
# 3. PULL RACECARDS + RESULTS (TODAY)
# ============================================================

now_utc = datetime.now(timezone.utc)
timestamp = now_utc.strftime("%Y%m%d_%H%M%S")

print("\n📡 Pulling racecards...")
racecard_resp = requests.get(
    f"{BASE_URL}/racecards/free", auth=AUTH, params={"day": "today"}, timeout=30
)
if racecard_resp.status_code != 200:
    print(f"❌ Racecards pull failed: HTTP {racecard_resp.status_code} — {racecard_resp.text[:300]}")
    sys.exit(1)
racecard_data = racecard_resp.json()
print(f"   ✅ HTTP 200 — {len(racecard_data.get('racecards', []))} races (all regions)")

racecard_snapshot_path = SNAPSHOT_DIR / f"racecards_free_{timestamp}.json"
with open(racecard_snapshot_path, "w", encoding="utf-8") as f:
    json.dump({
        "metadata": {"source": "The Racing API", "endpoint": "/v1/racecards/free", "day": "today", "retrieved_at_utc": now_utc.isoformat()},
        "data": racecard_data,
    }, f, indent=2, default=str)
print(f"   💾 {racecard_snapshot_path.name}")

print("\n📡 Pulling results...")
results_resp = requests.get(f"{BASE_URL}/results/today/free", auth=AUTH, timeout=30)
if results_resp.status_code == 200:
    results_data = results_resp.json()
    print(f"   ✅ HTTP 200 — {len(results_data.get('results', []))} races (all regions)")
    results_snapshot_path = SNAPSHOT_DIR / f"results_free_{timestamp}.json"
    with open(results_snapshot_path, "w", encoding="utf-8") as f:
        json.dump({
            "metadata": {"source": "The Racing API", "endpoint": "/v1/results/today/free", "retrieved_at_utc": now_utc.isoformat()},
            "data": results_data,
        }, f, indent=2, default=str)
    print(f"   💾 {results_snapshot_path.name}")
else:
    print(f"   ⚠️ Results pull failed: HTTP {results_resp.status_code} — {results_resp.text[:300]}")
    results_data = None


# ============================================================
# 4. HELPERS
# ============================================================

def find_race_list(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                sample_keys = value[0].keys()
                if "runners" in sample_keys or "off_time" in sample_keys or "off" in sample_keys or "race_id" in sample_keys:
                    return value
            found = find_race_list(value)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_race_list(item)
            if found is not None:
                return found
    return None


def ensure_table(con, table, parquet_path):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} AS
        SELECT * FROM read_parquet('{parquet_path}') WHERE 1=0
    """)


def sync_columns(con, table, parquet_path):
    existing_cols = set(con.execute(f"DESCRIBE {table}").fetchdf()["column_name"])
    incoming_cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").fetchdf()["column_name"]
    for col in incoming_cols:
        if col not in existing_cols:
            con.execute(f'ALTER TABLE {table} ADD COLUMN "{col}" VARCHAR')


def ingest_snapshot(con, payload, as_of_utc, region_filter, races_table, runners_table, raw_dir, tag):
    races_list = find_race_list(payload)
    if races_list is None:
        print(f"   ⚠️ Could not locate race list for {tag} — skipping ingest")
        return 0, 0

    races_list = [r for r in races_list if r.get("region") in region_filter]
    if not races_list:
        print(f"   ⚠️ No {region_filter} races for {tag} — nothing to ingest")
        return 0, 0

    races_rows, runners_rows = [], []
    for race in races_list:
        race_copy = dict(race)
        runners = race_copy.pop("runners", [])
        race_id = race_copy.get("race_id") or race_copy.get("id")
        race_row = {"as_of_utc": as_of_utc, "race_id": race_id}
        race_row.update(race_copy)
        races_rows.append(race_row)
        for runner in runners:
            runner_row = {"as_of_utc": as_of_utc, "race_id": race_id}
            runner_row.update(runner)
            runners_rows.append(runner_row)

    races_df = pd.json_normalize(races_rows).astype("string")
    runners_df = pd.json_normalize(runners_rows).astype("string")

    races_pq = raw_dir / f"{tag}_races_{timestamp}.parquet"
    runners_pq = raw_dir / f"{tag}_runners_{timestamp}.parquet"
    races_df.to_parquet(races_pq, index=False)
    runners_df.to_parquet(runners_pq, index=False)

    ensure_table(con, races_table, races_pq)
    ensure_table(con, runners_table, runners_pq)
    sync_columns(con, races_table, races_pq)
    sync_columns(con, runners_table, runners_pq)

    existing = con.execute(f"SELECT COUNT(*) FROM {races_table} WHERE as_of_utc = '{as_of_utc}'").fetchone()[0]
    if existing > 0:
        con.execute(f"DELETE FROM {races_table} WHERE as_of_utc = '{as_of_utc}'")
        con.execute(f"DELETE FROM {runners_table} WHERE as_of_utc = '{as_of_utc}'")

    con.execute(f"INSERT INTO {races_table} BY NAME SELECT * FROM read_parquet('{races_pq}')")
    con.execute(f"INSERT INTO {runners_table} BY NAME SELECT * FROM read_parquet('{runners_pq}')")

    return len(races_df), len(runners_df)


# ============================================================
# 5. INGEST INTO raw SCHEMA
# ============================================================

as_of_iso = now_utc.isoformat()

print("\n🗄️ Ingesting racecards into raw...")
rc_races_n, rc_runners_n = ingest_snapshot(
    con, racecard_data, as_of_iso, ALLOWED_REGIONS,
    "raw.racecards_races", "raw.racecards_runners", RAW_DIR, "racecards"
)
print(f"   {rc_races_n} races / {rc_runners_n} runners (GB only)")

if results_data is not None:
    print("\n🗄️ Ingesting results into raw...")
    res_races_n, res_runners_n = ingest_snapshot(
        con, results_data, as_of_iso, ALLOWED_REGIONS,
        "raw.results_races", "raw.results_runners", RAW_DIR, "results"
    )
    print(f"   {res_races_n} races / {res_runners_n} runners (GB only)")
else:
    print("\n🗄️ Skipping results ingest (pull failed earlier)")


# ============================================================
# 6. REBUILD clean SCHEMA
# ============================================================

RACE_COLUMN_MAP = {
    "distance_f": "distance_furlongs", "dist_f": "distance_furlongs",
    "off_time": "scheduled_off", "off": "scheduled_off",
    "class": "race_class",
    "sex_rest": "sex_restriction",
}
RUNNER_COLUMN_MAP = {"ofr": "official_rating", "or": "official_rating", "lbs": "weight_lbs"}
COUNTRY_SUFFIX_PATTERN = re.compile(r"\s*\([A-Z]{2,4}\)\s*$")


def clean_numeric(series):
    cleaned = series.astype("string").str.replace("£", "", regex=False).str.replace(",", "", regex=False).str.strip()
    cleaned = cleaned.replace({"-": pd.NA, "": pd.NA})
    return pd.to_numeric(cleaned, errors="coerce")


def normalise_horse_name(series):
    cleaned = series.astype("string").str.strip()
    cleaned = cleaned.str.replace(COUNTRY_SUFFIX_PATTERN, "", regex=True)
    return cleaned.str.strip().str.upper()


def rename_known(df, colmap):
    return df.rename(columns={k: v for k, v in colmap.items() if k in df.columns})


def has_rows(table):
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] > 0


print("\n🧹 Rebuilding clean schema...")

if has_rows("raw.racecards_races"):
    rc_races = rename_known(con.execute("SELECT * FROM raw.racecards_races").fetchdf(), RACE_COLUMN_MAP)
    rc_races["distance_furlongs"] = clean_numeric(rc_races["distance_furlongs"])
    rc_races["field_size"] = clean_numeric(rc_races["field_size"]).astype("Int64")
    rc_races["prize"] = clean_numeric(rc_races["prize"])
    rc_races["date"] = pd.to_datetime(rc_races["date"], errors="coerce")
    rc_races["off_dt"] = pd.to_datetime(rc_races["off_dt"], errors="coerce", utc=True)
    rc_races["as_of_utc"] = pd.to_datetime(rc_races["as_of_utc"], errors="coerce", utc=True)
    con.register("v1", rc_races)
    con.execute("CREATE OR REPLACE TABLE clean.racecard_races AS SELECT * FROM v1")

    rc_runners = rename_known(con.execute("SELECT * FROM raw.racecards_runners").fetchdf(), RUNNER_COLUMN_MAP)
    rc_runners["age"] = clean_numeric(rc_runners["age"]).astype("Int64")
    rc_runners["number"] = clean_numeric(rc_runners["number"]).astype("Int64")
    rc_runners["draw"] = clean_numeric(rc_runners["draw"]).astype("Int64")
    rc_runners["weight_lbs"] = clean_numeric(rc_runners["weight_lbs"]).astype("Int64")
    rc_runners["official_rating"] = clean_numeric(rc_runners["official_rating"]).astype("Int64")
    rc_runners["last_run"] = clean_numeric(rc_runners["last_run"]).astype("Int64")
    rc_runners["as_of_utc"] = pd.to_datetime(rc_runners["as_of_utc"], errors="coerce", utc=True)
    rc_runners["horse_name_normalised"] = normalise_horse_name(rc_runners["horse"])
    con.register("v2", rc_runners)
    con.execute("CREATE OR REPLACE TABLE clean.racecard_runners AS SELECT * FROM v2")

if has_rows("raw.results_races"):
    res_races = rename_known(con.execute("SELECT * FROM raw.results_races").fetchdf(), RACE_COLUMN_MAP)
    res_races["distance_furlongs"] = clean_numeric(res_races["distance_furlongs"])
    res_races["date"] = pd.to_datetime(res_races["date"], errors="coerce")
    res_races["off_dt"] = pd.to_datetime(res_races["off_dt"], errors="coerce", utc=True)
    res_races["as_of_utc"] = pd.to_datetime(res_races["as_of_utc"], errors="coerce", utc=True)
    con.register("v3", res_races)
    con.execute("CREATE OR REPLACE TABLE clean.results_races AS SELECT * FROM v3")

    res_runners = rename_known(con.execute("SELECT * FROM raw.results_runners").fetchdf(), RUNNER_COLUMN_MAP)
    res_runners["age"] = clean_numeric(res_runners["age"]).astype("Int64")
    res_runners["number"] = clean_numeric(res_runners["number"]).astype("Int64")
    res_runners["draw"] = clean_numeric(res_runners["draw"]).astype("Int64")
    res_runners["weight_lbs"] = clean_numeric(res_runners["weight_lbs"]).astype("Int64")
    res_runners["official_rating"] = clean_numeric(res_runners["official_rating"]).astype("Int64")
    res_runners["as_of_utc"] = pd.to_datetime(res_runners["as_of_utc"], errors="coerce", utc=True)
    res_runners["horse_name_normalised"] = normalise_horse_name(res_runners["horse"])
    if "position" in res_runners.columns:
        res_runners["finishing_position_raw"] = res_runners["position"].astype("string")
        res_runners["finishing_position"] = pd.to_numeric(res_runners["position"], errors="coerce").astype("Int64")
        res_runners["completed_race"] = res_runners["finishing_position"].notna()
    con.register("v4", res_runners)
    con.execute("CREATE OR REPLACE TABLE clean.results_runners AS SELECT * FROM v4")

if has_rows("clean.racecard_runners") or has_rows("clean.results_runners"):
    parts = []
    if has_rows("clean.racecard_runners"):
        parts.append("SELECT horse_id, horse, horse_name_normalised, trainer_id, trainer, jockey_id, jockey FROM clean.racecard_runners")
    if has_rows("clean.results_runners"):
        parts.append("SELECT horse_id, horse, horse_name_normalised, trainer_id, trainer, jockey_id, jockey FROM clean.results_runners")
    union_sql = " UNION ALL ".join(parts)

    con.execute(f"CREATE OR REPLACE TABLE clean.dim_horses AS SELECT DISTINCT horse_id, horse, horse_name_normalised FROM ({union_sql}) WHERE horse_id IS NOT NULL")
    con.execute(f"CREATE OR REPLACE TABLE clean.dim_trainers AS SELECT DISTINCT trainer_id, trainer FROM ({union_sql}) WHERE trainer_id IS NOT NULL")
    con.execute(f"CREATE OR REPLACE TABLE clean.dim_jockeys AS SELECT DISTINCT jockey_id, jockey FROM ({union_sql}) WHERE jockey_id IS NOT NULL")

print("   ✅ clean schema rebuilt")

con.close()  # flush WAL into the main .duckdb file before git commits it


# ============================================================
# 7. SUMMARY
# ============================================================

con2 = duckdb.connect(str(DUCKDB_PATH))

def count(table):
    try:
        return con2.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        return 0

n_days = con2.execute(
    "SELECT COUNT(DISTINCT CAST(as_of_utc AS DATE)) FROM raw.racecards_races"
).fetchone()[0] if count("raw.racecards_races") else 0

print("\n" + "=" * 70)
print("🏁 DAILY COLLECTOR COMPLETE")
print("=" * 70)
print(f"""
raw.racecards_races   : {count('raw.racecards_races')}
raw.racecards_runners : {count('raw.racecards_runners')}
raw.results_races     : {count('raw.results_races')}
raw.results_runners   : {count('raw.results_runners')}

Distinct collection days so far: {n_days}
""")
print("=" * 70)

con2.close()
