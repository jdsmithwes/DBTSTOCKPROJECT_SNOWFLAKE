"""
fetch_fred_macro_data.py
────────────────────────
Fetches macro/risk signals from the FRED API and loads them into Snowflake.

Series collected
────────────────
  VIXCLS        CBOE Volatility Index (market fear gauge)
  DCOILWTICO    WTI Crude Oil Spot Price (USD/barrel)
  DCOILBRENTEU  Brent Crude Oil Spot Price (USD/barrel)
  T10YIE        10-Year Breakeven Inflation Rate
  BAMLH0A0HYM2  ICE BofA US High Yield Option-Adjusted Spread

Ingestion path
──────────────
  FRED API → Snowflake direct write (no S3 staging; volume is small)

Phases
──────
  1. SETUP   — CREATE TABLE IF NOT EXISTS RAW_FRED_MACRO
  2. DETECT  — Find the latest loaded date per series
  3. FETCH   — Pull new observations from FRED API
  4. LOAD    — Write new rows into RAW_FRED_MACRO via write_pandas
  5. REFRESH — Run dbt run to rebuild downstream models

Required environment variables
───────────────────────────────
  SNOWFLAKE_ACCOUNT
  SNOWFLAKE_USER
  SNOWFLAKE_PASSWORD
  SNOWFLAKE_ROLE
  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_DATABASE
  FRED_API_KEY

Optional overrides
──────────────────
  FRED_START_DATE      default: 2015-01-01
  DBT_EXECUTABLE       default: /Users/jamaalsmith/dbtenv/bin/dbt
  DBT_PROJECT_DIR      default: <script parent>/DBTSTOCKPROJECT
  DBT_PROFILES_DIR     default: directory of this script's parent/DBTSTOCKPROJECT
"""

import logging
import os
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ROLE      = os.environ["SNOWFLAKE_ROLE"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE  = os.environ["SNOWFLAKE_DATABASE"]

FRED_API_KEY    = os.environ["FRED_API_KEY"]
FRED_BASE_URL   = "https://api.stlouisfed.org/fred/series/observations"
FRED_START_DATE = os.environ.get("FRED_START_DATE", "2015-01-01")

_SCRIPT_DIR      = Path(__file__).resolve().parent
DBT_EXECUTABLE   = os.environ.get("DBT_EXECUTABLE",   "/Users/jamaalsmith/dbtenv/bin/dbt")
DBT_PROJECT_DIR  = os.environ.get("DBT_PROJECT_DIR",  str(_SCRIPT_DIR.parent / "DBTSTOCKPROJECT"))
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)

TARGET_TABLE = "RAW_FRED_MACRO"

# Series to collect — id → human-readable name
FRED_SERIES: dict[str, str] = {
    "VIXCLS":       "CBOE Volatility Index",
    "DCOILWTICO":   "WTI Crude Oil Price (USD/barrel)",
    "DCOILBRENTEU": "Brent Crude Oil Price (USD/barrel)",
    "T10YIE":       "10-Year Breakeven Inflation Rate",
    "BAMLH0A0HYM2": "ICE BofA US High Yield Option-Adjusted Spread",
}

# ──────────────────────────────────────────────
# SNOWFLAKE HELPERS
# ──────────────────────────────────────────────
def _snowflake_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema="PUBLIC",
    )


# ──────────────────────────────────────────────
# PHASE 1 — SETUP
# ──────────────────────────────────────────────
def setup_table() -> None:
    """Creates RAW_FRED_MACRO if it does not already exist."""
    log.info("Phase 1: ensuring RAW_FRED_MACRO exists …")
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.PUBLIC.{TARGET_TABLE} (
            series_id      VARCHAR(50)    NOT NULL,
            series_name    VARCHAR(200),
            date           DATE           NOT NULL,
            value          FLOAT,
            load_timestamp TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
        )
    """
    conn = _snowflake_conn()
    try:
        conn.cursor().execute(ddl)
        log.info("✅ Table ready.")
    finally:
        conn.close()


# ──────────────────────────────────────────────
# PHASE 2 — DETECT LATEST LOADED DATE PER SERIES
# ──────────────────────────────────────────────
def detect_latest_dates() -> dict[str, str]:
    """
    Returns the latest loaded date per series_id as ISO strings.
    Falls back to FRED_START_DATE for series not yet in the table.
    """
    log.info("Phase 2: checking latest loaded dates …")
    conn = _snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT series_id, MAX(date)::date::varchar
            FROM {SNOWFLAKE_DATABASE}.PUBLIC.{TARGET_TABLE}
            GROUP BY series_id
        """)
        loaded = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()

    latest: dict[str, str] = {}
    for series_id in FRED_SERIES:
        if series_id in loaded:
            # Fetch from day after the last loaded date
            last = date.fromisoformat(loaded[series_id])
            latest[series_id] = (last + timedelta(days=1)).isoformat()
            log.info(f"  {series_id}: resuming from {latest[series_id]}")
        else:
            latest[series_id] = FRED_START_DATE
            log.info(f"  {series_id}: first load, starting from {FRED_START_DATE}")

    return latest


# ──────────────────────────────────────────────
# PHASE 3 — FETCH FROM FRED API
# ──────────────────────────────────────────────
def fetch_series(series_id: str, observation_start: str) -> pd.DataFrame:
    """Fetches observations for one FRED series from observation_start to today."""
    params = {
        "series_id":         series_id,
        "api_key":           FRED_API_KEY,
        "file_type":         "json",
        "observation_start": observation_start,
        "observation_end":   date.today().isoformat(),
    }
    resp = requests.get(FRED_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()

    payload = resp.json()
    observations = payload.get("observations", [])

    if not observations:
        log.info(f"  {series_id}: no new observations since {observation_start}")
        return pd.DataFrame()

    records = []
    for obs in observations:
        raw_value = obs.get("value", ".")
        if raw_value == ".":
            continue  # FRED uses "." for missing; skip rather than store null
        records.append({
            "SERIES_ID":   series_id,
            "SERIES_NAME": FRED_SERIES[series_id],
            "DATE":        obs["date"],
            "VALUE":       float(raw_value),
        })

    if not records:
        log.info(f"  {series_id}: all observations were missing ('.') — nothing to load")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    log.info(f"  {series_id}: fetched {len(df)} new observation(s)")
    return df


def fetch_all(latest_dates: dict[str, str]) -> pd.DataFrame:
    """Fetches all configured FRED series and returns a combined DataFrame."""
    log.info("Phase 3: fetching from FRED API …")
    frames = []
    for series_id, start_date in latest_dates.items():
        try:
            df = fetch_series(series_id, start_date)
            if not df.empty:
                frames.append(df)
        except requests.RequestException as exc:
            log.error(f"  {series_id}: fetch failed — {exc}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    log.info(f"📊 Total new rows to load: {len(combined)}")
    return combined


# ──────────────────────────────────────────────
# PHASE 4 — LOAD INTO SNOWFLAKE
# ──────────────────────────────────────────────
def load_to_snowflake(df: pd.DataFrame) -> int:
    """Writes the DataFrame to RAW_FRED_MACRO using write_pandas."""
    log.info(f"Phase 4: writing {len(df)} rows to {TARGET_TABLE} …")
    conn = _snowflake_conn()
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=TARGET_TABLE,
            database=SNOWFLAKE_DATABASE,
            schema="PUBLIC",
            auto_create_table=False,
            overwrite=False,
        )
        if success:
            log.info(f"✅ Loaded {nrows} rows in {nchunks} chunk(s).")
        else:
            log.error("❌ write_pandas reported failure.")
        return nrows
    finally:
        conn.close()


# ──────────────────────────────────────────────
# PHASE 5 — REFRESH DBT MODELS
# ──────────────────────────────────────────────
def run_dbt() -> None:
    """Runs dbt run scoped to the FRED-dependent models."""
    log.info("Phase 5: refreshing dbt models …")
    cmd = [
        DBT_EXECUTABLE,
        "run",
        "--select", "stg_fred_macro int_macro_signals mart_ml_features",
        "--profiles-dir", DBT_PROFILES_DIR,
        "--project-dir",  DBT_PROJECT_DIR,
    ]
    log.info(f"   Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False, text=True)
    if result.returncode != 0:
        log.error(f"❌ dbt run failed with exit code {result.returncode}.")
        raise SystemExit(result.returncode)
    log.info("✅ dbt run complete.")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main() -> None:
    log.info("🚀 Starting FRED macro data pipeline")

    setup_table()
    latest_dates = detect_latest_dates()

    df = fetch_all(latest_dates)
    if df.empty:
        log.info("Nothing new to load. Exiting.")
        return

    load_to_snowflake(df)
    run_dbt()

    log.info("🏁 FRED macro pipeline complete.")


if __name__ == "__main__":
    main()
