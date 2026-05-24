"""
fetch_alphavantage_fixed_income.py
───────────────────────────────────
Fetches fixed income data from AlphaVantage and loads it into Snowflake.

Series collected
────────────────
  TREASURY_YIELD      3month, 2year, 5year, 7year, 10year, 30year (daily)
  FEDERAL_FUNDS_RATE  Effective federal funds rate (daily)
  CPI                 Consumer Price Index, all urban consumers (monthly)

Ingestion path
──────────────
  AlphaVantage API → Snowflake direct write (no S3 needed; volume is small)

Snowflake tables created
────────────────────────
  DBT_STOCKPROJECT.PUBLIC.RAW_TREASURY_YIELDS   (date, maturity, yield_pct)
  DBT_STOCKPROJECT.PUBLIC.RAW_FED_FUNDS_RATE    (date, rate_pct)
  DBT_STOCKPROJECT.PUBLIC.RAW_CPI               (date, cpi_value)

Phases
──────
  1. SETUP   — CREATE TABLE IF NOT EXISTS for each raw table
  2. DETECT  — Find the latest loaded date per series / maturity
  3. FETCH   — Pull new observations from AlphaVantage
  4. LOAD    — Write new rows into Snowflake via write_pandas
  5. REFRESH — Run dbt run scoped to fixed income models

Required environment variables
───────────────────────────────
  SNOWFLAKE_ACCOUNT
  SNOWFLAKE_USER
  SNOWFLAKE_PASSWORD
  SNOWFLAKE_ROLE
  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_DATABASE
  ALPHAVANTAGE_API_KEY

Optional overrides
──────────────────
  FI_START_DATE       last-resort fallback if RAW_STOCK_DATA is also empty (default: 2015-01-01)
                      On first load the script uses MIN(date) from RAW_STOCK_DATA so fixed
                      income history automatically matches the equity date range.
  DBT_EXECUTABLE      default: /Users/jamaalsmith/dbtenv/bin/dbt
  DBT_PROJECT_DIR     default: <script parent>/DBTSTOCKPROJECT
  DBT_PROFILES_DIR    default: <DBT_PROJECT_DIR>
"""

import logging
import os
import subprocess
import time
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

ALPHAVANTAGE_API_KEY = os.environ["ALPHAVANTAGE_API_KEY"]
ALPHAVANTAGE_URL     = "https://www.alphavantage.co/query"

FI_START_DATE = os.environ.get("FI_START_DATE", "2015-01-01")

_SCRIPT_DIR      = Path(__file__).resolve().parent
DBT_EXECUTABLE   = os.environ.get("DBT_EXECUTABLE",   "/Users/jamaalsmith/dbtenv/bin/dbt")
DBT_PROJECT_DIR  = os.environ.get("DBT_PROJECT_DIR",  str(_SCRIPT_DIR.parent / "DBTSTOCKPROJECT"))
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)

# AlphaVantage free tier: 25 req/day, premium: 75/min
# Fixed income is 8 calls total — add a small delay to be safe
AV_REQUEST_DELAY = float(os.environ.get("AV_REQUEST_DELAY", "2.0"))

TREASURY_MATURITIES = ["3month", "2year", "5year", "7year", "10year", "30year"]

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
_DDL = {
    "RAW_TREASURY_YIELDS": f"""
        CREATE TABLE IF NOT EXISTS {{}}.PUBLIC.RAW_TREASURY_YIELDS (
            date           DATE          NOT NULL,
            maturity       VARCHAR(10)   NOT NULL,
            yield_pct      FLOAT,
            load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "RAW_FED_FUNDS_RATE": f"""
        CREATE TABLE IF NOT EXISTS {{}}.PUBLIC.RAW_FED_FUNDS_RATE (
            date           DATE          NOT NULL,
            rate_pct       FLOAT,
            load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "RAW_CPI": f"""
        CREATE TABLE IF NOT EXISTS {{}}.PUBLIC.RAW_CPI (
            date           DATE          NOT NULL,
            cpi_value      FLOAT,
            load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """,
}


def setup_tables() -> None:
    log.info("Phase 1: ensuring raw fixed income tables exist …")
    conn = _snowflake_conn()
    try:
        cur = conn.cursor()
        for table, ddl in _DDL.items():
            cur.execute(ddl.format(SNOWFLAKE_DATABASE))
            log.info(f"  ✅ {table} ready")
    finally:
        conn.close()


# ──────────────────────────────────────────────
# PHASE 2 — DETECT LATEST LOADED DATES
# ──────────────────────────────────────────────
def _equity_start_date(cur) -> str:
    """
    Returns MIN(date) from RAW_STOCK_DATA so fixed income history matches the equity spine.
    Falls back to FI_START_DATE if RAW_STOCK_DATA is empty or missing.
    """
    try:
        cur.execute(f"SELECT MIN(date)::date::varchar FROM {SNOWFLAKE_DATABASE}.PUBLIC.RAW_STOCK_DATA")
        row = cur.fetchone()
        if row and row[0]:
            log.info(f"  Equity start date detected: {row[0]} — using as fallback for new FI series")
            return row[0]
    except Exception as exc:
        log.warning(f"  Could not query RAW_STOCK_DATA for equity start date ({exc}); using FI_START_DATE")
    log.info(f"  No equity data found; defaulting to FI_START_DATE={FI_START_DATE}")
    return FI_START_DATE


def detect_latest_dates() -> dict[str, str]:
    """
    Returns start dates for each fetch call:
      - treasury_<maturity>: latest date in RAW_TREASURY_YIELDS for that maturity
      - fed_funds:           latest date in RAW_FED_FUNDS_RATE
      - cpi:                 latest date in RAW_CPI

    For series with no existing data (first load), falls back to MIN(date) from
    RAW_STOCK_DATA so fixed income coverage matches the equity date range.
    FI_START_DATE is only used when both FI tables and RAW_STOCK_DATA are empty.
    """
    log.info("Phase 2: detecting latest loaded dates …")
    conn = _snowflake_conn()
    try:
        cur = conn.cursor()

        # Anchor: match equity history length for first-time loads
        fallback_start = _equity_start_date(cur)

        # Treasury yields — per maturity
        cur.execute(f"""
            SELECT maturity, MAX(date)::date::varchar
            FROM {SNOWFLAKE_DATABASE}.PUBLIC.RAW_TREASURY_YIELDS
            GROUP BY maturity
        """)
        treasury_latest = {row[0]: row[1] for row in cur.fetchall()}

        # Fed funds
        cur.execute(f"SELECT MAX(date)::date::varchar FROM {SNOWFLAKE_DATABASE}.PUBLIC.RAW_FED_FUNDS_RATE")
        fed_row = cur.fetchone()
        fed_latest = fed_row[0] if fed_row and fed_row[0] else None

        # CPI
        cur.execute(f"SELECT MAX(date)::date::varchar FROM {SNOWFLAKE_DATABASE}.PUBLIC.RAW_CPI")
        cpi_row = cur.fetchone()
        cpi_latest = cpi_row[0] if cpi_row and cpi_row[0] else None

    finally:
        conn.close()

    dates: dict[str, str] = {}

    for maturity in TREASURY_MATURITIES:
        if maturity in treasury_latest:
            start = (date.fromisoformat(treasury_latest[maturity]) + timedelta(days=1)).isoformat()
        else:
            start = fallback_start
        dates[f"treasury_{maturity}"] = start
        log.info(f"  treasury {maturity}: fetching from {start}")

    dates["fed_funds"] = (
        (date.fromisoformat(fed_latest) + timedelta(days=1)).isoformat()
        if fed_latest else fallback_start
    )
    log.info(f"  fed_funds: fetching from {dates['fed_funds']}")

    dates["cpi"] = (
        (date.fromisoformat(cpi_latest) + timedelta(days=1)).isoformat()
        if cpi_latest else fallback_start
    )
    log.info(f"  cpi: fetching from {dates['cpi']}")

    return dates


# ──────────────────────────────────────────────
# PHASE 3 — FETCH FROM ALPHAVANTAGE
# ──────────────────────────────────────────────
def _av_get(params: dict) -> list[dict]:
    """Makes one AlphaVantage API call and returns the 'data' array."""
    resp = requests.get(ALPHAVANTAGE_URL, params={**params, "apikey": ALPHAVANTAGE_API_KEY}, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    if "data" not in payload:
        note = payload.get("Note") or payload.get("Information") or payload.get("Error Message", "unknown")
        log.warning(f"  No 'data' key in response: {note[:120]}")
        return []

    time.sleep(AV_REQUEST_DELAY)
    return payload["data"]


def fetch_treasury_yields(latest_dates: dict[str, str]) -> pd.DataFrame:
    """Fetches all treasury yield maturities and returns a combined long-format DataFrame."""
    log.info("  Fetching treasury yields (6 maturities) …")
    frames = []
    for maturity in TREASURY_MATURITIES:
        start = latest_dates[f"treasury_{maturity}"]
        observations = _av_get({"function": "TREASURY_YIELD", "interval": "daily", "maturity": maturity})
        records = [
            {
                "DATE":    row["date"],
                "MATURITY": maturity,
                "YIELD_PCT": float(row["value"]) if row.get("value") not in (".", "", None) else None,
            }
            for row in observations
            if row.get("date", "") >= start and row.get("value") not in (".", "", None)
        ]
        if records:
            frames.append(pd.DataFrame(records))
            log.info(f"    {maturity}: {len(records)} new row(s)")
        else:
            log.info(f"    {maturity}: nothing new since {start}")

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    return df


def fetch_fed_funds(start: str) -> pd.DataFrame:
    log.info("  Fetching federal funds rate …")
    observations = _av_get({"function": "FEDERAL_FUNDS_RATE", "interval": "daily"})
    records = [
        {"DATE": row["date"], "RATE_PCT": float(row["value"])}
        for row in observations
        if row.get("date", "") >= start and row.get("value") not in (".", "", None)
    ]
    if not records:
        log.info(f"    Nothing new since {start}")
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    log.info(f"    {len(df)} new row(s)")
    return df


def fetch_cpi(start: str) -> pd.DataFrame:
    log.info("  Fetching CPI …")
    observations = _av_get({"function": "CPI", "interval": "monthly"})
    records = [
        {"DATE": row["date"], "CPI_VALUE": float(row["value"])}
        for row in observations
        if row.get("date", "") >= start and row.get("value") not in (".", "", None)
    ]
    if not records:
        log.info(f"    Nothing new since {start}")
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    log.info(f"    {len(df)} new row(s)")
    return df


def fetch_all(latest_dates: dict[str, str]) -> dict[str, pd.DataFrame]:
    log.info("Phase 3: fetching from AlphaVantage …")
    return {
        "RAW_TREASURY_YIELDS": fetch_treasury_yields(latest_dates),
        "RAW_FED_FUNDS_RATE":  fetch_fed_funds(latest_dates["fed_funds"]),
        "RAW_CPI":             fetch_cpi(latest_dates["cpi"]),
    }


# ──────────────────────────────────────────────
# PHASE 4 — LOAD INTO SNOWFLAKE
# ──────────────────────────────────────────────
def load_to_snowflake(datasets: dict[str, pd.DataFrame]) -> None:
    log.info("Phase 4: loading into Snowflake …")
    conn = _snowflake_conn()
    try:
        for table, df in datasets.items():
            if df.empty:
                log.info(f"  {table}: nothing to load")
                continue
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=table,
                database=SNOWFLAKE_DATABASE,
                schema="PUBLIC",
                auto_create_table=False,
                overwrite=False,
            )
            if success:
                log.info(f"  ✅ {table}: {nrows} rows loaded in {nchunks} chunk(s)")
            else:
                log.error(f"  ❌ {table}: write_pandas reported failure")
    finally:
        conn.close()


# ──────────────────────────────────────────────
# PHASE 5 — REFRESH DBT
# ──────────────────────────────────────────────
def run_dbt() -> None:
    log.info("Phase 5: refreshing dbt fixed income models …")
    cmd = [
        DBT_EXECUTABLE, "run",
        "--select", "stg_treasury_yields stg_fed_funds_rate stg_cpi int_yield_curve mart_fixed_income mart_ml_features",
        "--profiles-dir", DBT_PROFILES_DIR,
        "--project-dir",  DBT_PROJECT_DIR,
    ]
    log.info(f"   Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False, text=True)
    if result.returncode != 0:
        log.error(f"❌ dbt run failed (exit {result.returncode})")
        raise SystemExit(result.returncode)
    log.info("✅ dbt run complete")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main() -> None:
    log.info("🚀 Starting AlphaVantage fixed income pipeline")

    setup_tables()
    latest_dates = detect_latest_dates()
    datasets     = fetch_all(latest_dates)

    any_data = any(not df.empty for df in datasets.values())
    if not any_data:
        log.info("Nothing new to load. Exiting.")
        return

    load_to_snowflake(datasets)
    run_dbt()

    log.info("🏁 Fixed income pipeline complete.")


if __name__ == "__main__":
    main()
