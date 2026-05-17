"""
backfill_missing_dates.py
─────────────────────────
Fully autonomous backfill pipeline. No manual dbt pre-run required.

Phases
──────
  1. DETECT  — Query RAW_STOCK_DATA for each ticker's last loaded date.
               Build expected NYSE trading days in Python (weekdays minus
               US market holidays). Compute gaps natively.
  2. FETCH   — Async AlphaVantage calls for every ticker × missing dates.
  3. UPLOAD  — Push backfilled rows as CSV to S3 for Snowpipe ingestion.
  4. WAIT    — Poll RAW_STOCK_DATA until Snowpipe loads the new rows
               (or 5-minute timeout).
  5. REFRESH — Run `dbt run` to rebuild all downstream models.

Required environment variables
───────────────────────────────
  SNOWFLAKE_ACCOUNT       e.g. TPRFGUJ-JNC76647
  SNOWFLAKE_USER          e.g. jdsmithwes
  SNOWFLAKE_PASSWORD      Snowflake password or PAT
  SNOWFLAKE_ROLE          e.g. DBT_ROLE
  SNOWFLAKE_WAREHOUSE     e.g. DBT_STOCKPROJECT
  SNOWFLAKE_DATABASE      e.g. DBT_STOCKPROJECT
  ALPHAVANTAGE_API_KEY    your AlphaVantage API key
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_REGION              default: us-east-1
  S3_BUCKET_NAME          the bucket Snowpipe watches
  S3_PREFIX               default: stock_prices/

Optional overrides
──────────────────
  DBT_EXECUTABLE          default: /Users/jamaalsmith/dbtenv/bin/dbt
  DBT_PROJECT_DIR         default: directory of this script's parent/DBTSTOCKPROJECT
  DBT_PROFILES_DIR        default: ~/.dbt
  SNOWPIPE_POLL_INTERVAL  seconds between Snowpipe checks (default: 30)
  SNOWPIPE_POLL_TIMEOUT   max seconds to wait for Snowpipe (default: 300)
"""

import asyncio
import logging
import os
import subprocess
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import aiohttp
import boto3
import pandas as pd
import snowflake.connector
from tenacity import retry, stop_after_attempt, wait_fixed

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# ENVIRONMENT
# ──────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ROLE      = os.environ["SNOWFLAKE_ROLE"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE  = os.environ["SNOWFLAKE_DATABASE"]

ALPHAVANTAGE_API_KEY = os.environ["ALPHAVANTAGE_API_KEY"]
ALPHAVANTAGE_URL     = "https://www.alphavantage.co/query"

AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET_NAME        = os.environ["S3_BUCKET_NAME"]
S3_PREFIX             = os.environ.get("S3_PREFIX", "stock_prices/")

_SCRIPT_DIR = Path(__file__).resolve().parent
DBT_EXECUTABLE   = os.environ.get(
    "DBT_EXECUTABLE", "/Users/jamaalsmith/dbtenv/bin/dbt"
)
DBT_PROJECT_DIR  = os.environ.get(
    "DBT_PROJECT_DIR", str(_SCRIPT_DIR.parent / "DBTSTOCKPROJECT")
)
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", str(Path.home() / ".dbt"))

SNOWPIPE_POLL_INTERVAL = int(os.environ.get("SNOWPIPE_POLL_INTERVAL", "30"))
SNOWPIPE_POLL_TIMEOUT  = int(os.environ.get("SNOWPIPE_POLL_TIMEOUT", "300"))

MAX_CONCURRENT_REQUESTS = 5
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=60)

# ──────────────────────────────────────────────
# NYSE MARKET HOLIDAYS  (mirrors seeds/us_market_holidays.csv)
# ──────────────────────────────────────────────
_NYSE_HOLIDAYS: set[date] = {
    date.fromisoformat(d) for d in [
        # 2020
        "2020-01-01", "2020-01-20", "2020-02-17", "2020-04-10",
        "2020-05-25", "2020-07-03", "2020-09-07", "2020-11-26", "2020-12-25",
        # 2021
        "2021-01-01", "2021-01-18", "2021-02-15", "2021-04-02",
        "2021-05-31", "2021-07-05", "2021-09-06", "2021-11-25",
        "2021-12-24", "2021-12-31",
        # 2022
        "2022-01-17", "2022-02-21", "2022-04-15", "2022-05-30",
        "2022-06-20", "2022-07-04", "2022-09-05", "2022-11-24", "2022-12-26",
        # 2023
        "2023-01-02", "2023-01-16", "2023-02-20", "2023-04-07",
        "2023-05-29", "2023-06-19", "2023-07-04", "2023-09-04",
        "2023-11-23", "2023-12-25",
        # 2024
        "2024-01-01", "2024-01-15", "2024-02-19", "2024-03-29",
        "2024-05-27", "2024-06-19", "2024-07-04", "2024-09-02",
        "2024-11-28", "2024-12-25",
        # 2025
        "2025-01-01", "2025-01-09", "2025-01-20", "2025-02-17",
        "2025-04-18", "2025-05-26", "2025-06-19", "2025-07-04",
        "2025-09-01", "2025-11-27", "2025-12-25",
        # 2026
        "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
        "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
        "2026-11-26", "2026-12-25",
    ]
}


def _last_completed_trading_day() -> date:
    """
    Returns the most recent trading day that has fully closed —
    yesterday (or last Friday if today is Monday, adjusting for holidays).
    """
    candidate = date.today() - timedelta(days=1)
    while candidate.weekday() >= 5 or candidate in _NYSE_HOLIDAYS:
        candidate -= timedelta(days=1)
    return candidate


def _trading_days_between(start: date, end: date) -> list[date]:
    """All NYSE trading days in (start, end] — exclusive start, inclusive end."""
    result = []
    current = start + timedelta(days=1)
    while current <= end:
        if current.weekday() < 5 and current not in _NYSE_HOLIDAYS:
            result.append(current)
        current += timedelta(days=1)
    return result


# ──────────────────────────────────────────────
# SNOWFLAKE CONNECTION HELPER
# ──────────────────────────────────────────────
def _snowflake_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
    )


# ──────────────────────────────────────────────
# PHASE 1 — DETECT GAPS NATIVELY
# ──────────────────────────────────────────────
def detect_gaps() -> dict[date, list[str]]:
    """
    Queries RAW_STOCK_DATA directly for the latest loaded date per ticker.
    Computes missing NYSE trading days in Python — no dbt pre-run required.

    Returns dict mapping each missing trading date → list of tickers that
    need data on that date.
    """
    log.info("Phase 1: detecting gaps in RAW_STOCK_DATA …")

    conn = _snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT ticker, MAX(date)::date AS last_date
            FROM {SNOWFLAKE_DATABASE}.PUBLIC.RAW_STOCK_DATA
            GROUP BY ticker
        """)
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        log.warning("RAW_STOCK_DATA is empty — nothing to backfill against.")
        return {}

    last_trading_day = _last_completed_trading_day()
    log.info(f"Last completed trading day: {last_trading_day}")

    gaps: dict[date, list[str]] = defaultdict(list)
    for ticker, last_date in rows:
        if isinstance(last_date, str):
            last_date = date.fromisoformat(last_date)
        if last_date >= last_trading_day:
            continue
        for missing in _trading_days_between(last_date, last_trading_day):
            gaps[missing].append(ticker)

    if not gaps:
        log.info("✅ No missing dates — all tickers are up to date.")
        return {}

    unique_dates   = len(gaps)
    unique_tickers = len({t for tickers in gaps.values() for t in tickers})
    total_pairs    = sum(len(v) for v in gaps.values())
    log.info(
        f"📋 Found {total_pairs} missing (ticker, date) pairs "
        f"across {unique_dates} dates and {unique_tickers} tickers."
    )
    log.info(f"📅 Gap window: {min(gaps)} → {max(gaps)}")

    return dict(gaps)


# ──────────────────────────────────────────────
# PHASE 2 — FETCH FROM ALPHAVANTAGE
# ──────────────────────────────────────────────
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def _fetch_ticker(
    session: aiohttp.ClientSession,
    ticker: str,
    missing_dates: set[date],
) -> pd.DataFrame | None:
    days_gap  = (max(missing_dates) - min(missing_dates)).days
    outputsize = "compact" if days_gap <= 90 else "full"

    params = {
        "function":   "TIME_SERIES_DAILY_ADJUSTED",
        "symbol":     ticker,
        "outputsize": outputsize,
        "apikey":     ALPHAVANTAGE_API_KEY,
    }

    async with session.get(ALPHAVANTAGE_URL, params=params) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {ticker}")

        payload = await resp.json(content_type=None)

        if "Time Series (Daily)" not in payload:
            log.warning(f"⚠️  No daily data returned for {ticker} — skipping")
            return None

        records = []
        for date_str, values in payload["Time Series (Daily)"].items():
            trading_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            if trading_date not in missing_dates:
                continue
            records.append({
                "ticker":            ticker,
                "date":              date_str,
                "open":              values.get("1. open"),
                "high":              values.get("2. high"),
                "low":               values.get("3. low"),
                "close":             values.get("4. close"),
                "adjusted_close":    values.get("5. adjusted close"),
                "volume":            values.get("6. volume"),
                "dividend_amount":   values.get("7. dividend amount"),
                "split_coefficient": values.get("8. split coefficient"),
                "load_time":         datetime.utcnow().isoformat(),
            })

        if not records:
            log.warning(f"⚠️  {ticker}: AlphaVantage had no rows for the missing dates")
            return None

        log.info(f"✅ {ticker}: fetched {len(records)} missing row(s)")
        return pd.DataFrame(records)


async def _fetch_all(gaps: dict[date, list[str]]) -> pd.DataFrame:
    # Invert: ticker → set of dates it needs
    ticker_to_dates: dict[str, set[date]] = defaultdict(set)
    for trading_date, tickers in gaps.items():
        for ticker in tickers:
            ticker_to_dates[ticker].add(trading_date)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    frames    = []

    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:

        async def bounded(ticker: str, dates: set[date]):
            async with semaphore:
                return await _fetch_ticker(session, ticker, dates)

        tasks = [bounded(t, d) for t, d in ticker_to_dates.items()]

        success, skipped = 0, 0
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result is None:
                skipped += 1
            else:
                frames.append(result)
                success += 1

    log.info(f"📊 Fetch summary — success: {success}, skipped: {skipped}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ──────────────────────────────────────────────
# PHASE 3 — UPLOAD TO S3
# ──────────────────────────────────────────────
def upload_to_s3(df: pd.DataFrame) -> datetime:
    """
    Uploads backfilled rows as CSV to S3 under S3_PREFIX.
    Returns the UTC timestamp just before upload (used for Snowpipe polling).
    """
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    upload_time   = datetime.utcnow()
    run_timestamp = upload_time.strftime("%Y%m%d_%H%M%S")
    s3_key        = f"{S3_PREFIX}backfill_{run_timestamp}.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=csv_bytes)

    log.info(
        f"📤 Uploaded {len(df)} rows "
        f"({df['ticker'].nunique()} tickers, {df['date'].nunique()} dates) "
        f"→ s3://{S3_BUCKET_NAME}/{s3_key}"
    )
    return upload_time


# ──────────────────────────────────────────────
# PHASE 4 — WAIT FOR SNOWPIPE
# ──────────────────────────────────────────────
def wait_for_snowpipe(upload_time: datetime) -> bool:
    """
    Polls RAW_STOCK_DATA every SNOWPIPE_POLL_INTERVAL seconds until new rows
    with LOAD_TIME > upload_time appear, or SNOWPIPE_POLL_TIMEOUT seconds elapse.

    Returns True if rows were detected, False on timeout.
    """
    log.info(
        f"Phase 4: waiting for Snowpipe (checking every {SNOWPIPE_POLL_INTERVAL}s, "
        f"timeout {SNOWPIPE_POLL_TIMEOUT}s) …"
    )
    upload_ts = upload_time.strftime("%Y-%m-%d %H:%M:%S")
    deadline  = time.monotonic() + SNOWPIPE_POLL_TIMEOUT

    conn = _snowflake_conn()
    try:
        cur = conn.cursor()
        while time.monotonic() < deadline:
            cur.execute(f"""
                SELECT COUNT(*)
                FROM {SNOWFLAKE_DATABASE}.PUBLIC.RAW_STOCK_DATA
                WHERE load_time > '{upload_ts}'
            """)
            (count,) = cur.fetchone()
            if count and count > 0:
                log.info(f"✅ Snowpipe loaded {count} new row(s) — proceeding.")
                return True

            remaining = int(deadline - time.monotonic())
            log.info(
                f"   Still waiting … 0 new rows so far "
                f"({remaining}s remaining)"
            )
            time.sleep(SNOWPIPE_POLL_INTERVAL)
    finally:
        conn.close()

    log.warning(
        f"⏱️  Snowpipe timeout after {SNOWPIPE_POLL_TIMEOUT}s. "
        "Proceeding with dbt run anyway — data may not be complete."
    )
    return False


# ──────────────────────────────────────────────
# PHASE 5 — REFRESH DBT MODELS
# ──────────────────────────────────────────────
def run_dbt() -> None:
    """Runs `dbt run` using the project's virtual-env dbt binary."""
    log.info("Phase 5: running dbt run …")
    cmd = [
        DBT_EXECUTABLE,
        "run",
        "--profiles-dir", DBT_PROFILES_DIR,
        "--project-dir",  DBT_PROJECT_DIR,
    ]
    log.info(f"   Command: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=False, text=True)

    if result.returncode == 0:
        log.info("✅ dbt run completed successfully.")
    else:
        log.error(
            f"❌ dbt run failed with exit code {result.returncode}. "
            "Check the output above for model errors."
        )
        raise SystemExit(result.returncode)


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
async def main() -> None:
    log.info("🚀 Starting autonomous backfill pipeline")

    # Phase 1 — detect
    gaps = detect_gaps()
    if not gaps:
        log.info("Nothing to backfill. Exiting.")
        return

    # Phase 2 — fetch
    log.info("Phase 2: fetching missing data from AlphaVantage …")
    df = await _fetch_all(gaps)
    if df.empty:
        log.warning("⚠️  No data fetched from AlphaVantage — nothing to upload.")
        return

    # Phase 3 — upload
    log.info("Phase 3: uploading to S3 …")
    upload_time = upload_to_s3(df)

    # Phase 4 — wait
    wait_for_snowpipe(upload_time)

    # Phase 5 — refresh
    run_dbt()

    log.info("🏁 Autonomous backfill pipeline complete.")


if __name__ == "__main__":
    asyncio.run(main())
