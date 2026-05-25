"""
fetch_av_company_overview.py
─────────────────────────────
Refreshes AlphaVantage OVERVIEW fundamentals for every ticker in
RAW_STOCK_DATA and loads the results into RAW_COMPANY_OVERVIEW.

Signals refreshed (all point-in-time)
──────────────────────────────────────
  Valuation       P/E, forward P/E, P/B, EV/EBITDA, EV/Revenue, beta
  Profitability   profit margin, operating margin, ROE, ROA, EPS
  Size/structure  market cap, shares outstanding, shares float
  Analyst         ratings (strong buy → strong sell), target price
  Descriptive     sector, industry, exchange, country, fiscal year end

Snowflake table written
───────────────────────
  DBT_STOCKPROJECT.PUBLIC.RAW_COMPANY_OVERVIEW

  New rows are appended; stg_companyoverview deduplicates by keeping
  the latest load_timestamp per ticker via QUALIFY ROW_NUMBER().

Phases
──────
  1. SETUP   — verify RAW_COMPANY_OVERVIEW exists
  2. DETECT  — get distinct tickers from RAW_STOCK_DATA
  3. FETCH   — call AlphaVantage OVERVIEW for each ticker
  4. LOAD    — insert rows into Snowflake via executemany + PARSE_JSON
  5. REFRESH — dbt run stg_companyoverview and all downstream models

Required environment variables
────────────────────────────────
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
  SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
  ALPHAVANTAGE_API_KEY

Optional overrides
──────────────────
  AV_REQUEST_DELAY   seconds between API calls (default: 1.0)
  DBT_EXECUTABLE     default: /Users/jamaalsmith/dbtenv/bin/dbt
  DBT_PROJECT_DIR    default: <script parent>/DBTSTOCKPROJECT
  DBT_PROFILES_DIR   default: <DBT_PROJECT_DIR>
"""

import json
import logging
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import snowflake.connector

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
SNOWFLAKE_ACCOUNT    = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER       = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD   = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ROLE       = os.environ["SNOWFLAKE_ROLE"]
SNOWFLAKE_WAREHOUSE  = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE   = os.environ["SNOWFLAKE_DATABASE"]
ALPHAVANTAGE_API_KEY = os.environ["ALPHAVANTAGE_API_KEY"]

AV_REQUEST_DELAY = float(os.environ.get("AV_REQUEST_DELAY", "1.0"))

_SCRIPT_DIR      = Path(__file__).resolve().parent
DBT_EXECUTABLE   = os.environ.get("DBT_EXECUTABLE",   "/Users/jamaalsmith/dbtenv/bin/dbt")
DBT_PROJECT_DIR  = os.environ.get("DBT_PROJECT_DIR",  str(_SCRIPT_DIR.parent / "DBTSTOCKPROJECT"))
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)

ALPHAVANTAGE_URL = "https://www.alphavantage.co/query"
TARGET_TABLE     = "RAW_COMPANY_OVERVIEW"


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
# PHASE 1 — VERIFY TABLE
# ──────────────────────────────────────────────
def verify_table() -> None:
    log.info("Phase 1: verifying RAW_COMPANY_OVERVIEW exists …")
    conn = _snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.PUBLIC.{TARGET_TABLE}")
        count = cur.fetchone()[0]
        log.info(f"  ✅ {TARGET_TABLE} ready ({count:,} existing rows)")
    finally:
        conn.close()


# ──────────────────────────────────────────────
# PHASE 2 — DETECT TICKERS
# ──────────────────────────────────────────────
def detect_tickers() -> list[str]:
    log.info("Phase 2: detecting tickers from RAW_STOCK_DATA …")
    conn = _snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT DISTINCT ticker FROM {SNOWFLAKE_DATABASE}.PUBLIC.RAW_STOCK_DATA ORDER BY ticker"
        )
        tickers = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()
    log.info(f"  {len(tickers):,} tickers to refresh")
    return tickers


# ──────────────────────────────────────────────
# PHASE 3 — FETCH FROM ALPHAVANTAGE
# ──────────────────────────────────────────────
def _fetch_overview(ticker: str) -> dict | None:
    params = {
        "function": "OVERVIEW",
        "symbol":   ticker,
        "apikey":   ALPHAVANTAGE_API_KEY,
    }
    resp = requests.get(ALPHAVANTAGE_URL, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    if not payload or "Symbol" not in payload:
        note = (
            payload.get("Note")
            or payload.get("Information")
            or payload.get("Error Message", "empty response or missing Symbol key")
        )
        log.warning(f"  [{ticker}] skipped: {str(note)[:120]}")
        return None

    payload["ticker"]           = ticker
    payload["ingest_timestamp"] = datetime.now(timezone.utc).isoformat()
    return payload


def fetch_all(tickers: list[str]) -> list[dict]:
    log.info(f"Phase 3: fetching OVERVIEW for {len(tickers):,} tickers …")
    results: list[dict] = []

    for i, ticker in enumerate(tickers, 1):
        try:
            data = _fetch_overview(ticker)
        except requests.RequestException as exc:
            log.error(f"  [{ticker}] request failed — {exc}")
            data = None

        if data:
            results.append(data)

        if i % 50 == 0:
            log.info(f"  {i}/{len(tickers)} tickers processed ({len(results)} successful so far)")

        time.sleep(AV_REQUEST_DELAY)

    log.info(f"  ✅ {len(results):,} overviews fetched ({len(tickers) - len(results)} skipped)")
    return results


# ──────────────────────────────────────────────
# PHASE 4 — LOAD INTO SNOWFLAKE
# ──────────────────────────────────────────────
def load_to_snowflake(payloads: list[dict]) -> None:
    log.info(f"Phase 4: loading {len(payloads):,} rows into {TARGET_TABLE} …")

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    filename = f"company_overview_api/overview_{ts}.json"

    insert_sql = f"""
        INSERT INTO {SNOWFLAKE_DATABASE}.PUBLIC.{TARGET_TABLE}
            (RAW_PAYLOAD, METADATA_FILENAME, METADATA_FILE_ROW_NUMBER, LOAD_TIMESTAMP)
        SELECT PARSE_JSON(%s), %s, %s, CURRENT_TIMESTAMP()
    """

    conn = _snowflake_conn()
    try:
        cur = conn.cursor()
        for idx, payload in enumerate(payloads):
            cur.execute(insert_sql, (json.dumps(payload), filename, idx + 1))
        log.info(f"  ✅ {len(payloads):,} rows loaded")
    finally:
        conn.close()


# ──────────────────────────────────────────────
# PHASE 5 — REFRESH DBT
# ──────────────────────────────────────────────
def run_dbt() -> None:
    log.info("Phase 5: refreshing dbt company-overview-dependent models …")
    cmd = [
        DBT_EXECUTABLE, "run",
        "--select", "stg_companyoverview+",
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
    log.info("🚀 Starting AlphaVantage company overview refresh")

    verify_table()
    tickers = detect_tickers()

    if not tickers:
        log.info("No tickers found in RAW_STOCK_DATA. Exiting.")
        return

    payloads = fetch_all(tickers)
    if not payloads:
        log.info("No overviews fetched. Exiting.")
        return

    load_to_snowflake(payloads)
    run_dbt()

    log.info("🏁 Company overview refresh complete.")


if __name__ == "__main__":
    main()
