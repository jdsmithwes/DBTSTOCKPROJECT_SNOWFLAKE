"""
backfill_missing_dates.py
─────────────────────────
Reads mart_data_gaps from Snowflake to find every (ticker, date) pair that is
missing from the price database, fetches the data from AlphaVantage, and
uploads it to S3 so Snowpipe can load it into RAW_STOCK_DATA.

Run order:
  1. dbt run --select mart_data_gaps   (refresh the gap table first)
  2. python Python_Scripts/backfill_missing_dates.py
  3. Wait ~2-5 min for Snowpipe to ingest
  4. dbt run                            (refresh all downstream models)

Required environment variables:
  SNOWFLAKE_ACCOUNT       e.g. TPRFGUJ-JNC76647
  SNOWFLAKE_USER          e.g. jdsmithwes
  SNOWFLAKE_PASSWORD      your Snowflake password or PAT
  SNOWFLAKE_ROLE          e.g. DBT_ROLE
  SNOWFLAKE_WAREHOUSE     e.g. DBT_STOCKPROJECT
  SNOWFLAKE_DATABASE      e.g. DBT_STOCKPROJECT
  SNOWFLAKE_SCHEMA        e.g. JDS_MARTS
  ALPHAVANTAGE_API_KEY    your AlphaVantage API key
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_REGION              default: us-east-1
  S3_BUCKET_NAME          the bucket Snowpipe watches
  S3_PREFIX               default: stock_prices/
"""

import os
import asyncio
import logging
from collections import defaultdict
from datetime import date, datetime

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
SNOWFLAKE_SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA", "JDS_MARTS")

ALPHAVANTAGE_API_KEY = os.environ["ALPHAVANTAGE_API_KEY"]
ALPHAVANTAGE_URL     = "https://www.alphavantage.co/query"

AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET_NAME        = os.environ["S3_BUCKET_NAME"]
S3_PREFIX             = os.environ.get("S3_PREFIX", "stock_prices/")

MAX_CONCURRENT_REQUESTS = 5
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=60)

# ──────────────────────────────────────────────
# STEP 1 — QUERY SNOWFLAKE FOR MISSING DATES
# ──────────────────────────────────────────────
def fetch_missing_dates() -> dict[date, list[str]]:
    """
    Returns a dict mapping each missing trading date to the list of tickers
    that need data on that date.

    Reads from MART_DATA_GAPS — run `dbt run --select mart_data_gaps` first.
    """
    log.info("Connecting to Snowflake to read mart_data_gaps …")

    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT ticker, missing_date
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.JDS_MART_mart_data_gaps
            ORDER BY missing_date, ticker
        """)
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        log.info("✅ No missing dates found — database is up to date.")
        return {}

    # Group tickers by missing date
    gaps: dict[date, list[str]] = defaultdict(list)
    for ticker, missing_date in rows:
        gaps[missing_date].append(ticker)

    unique_dates   = len(gaps)
    unique_tickers = len({t for tickers in gaps.values() for t in tickers})
    log.info(
        f"📋 Found {len(rows)} missing (ticker, date) pairs "
        f"across {unique_dates} dates and {unique_tickers} tickers."
    )

    return dict(gaps)


# ──────────────────────────────────────────────
# STEP 2 — FETCH FROM ALPHAVANTAGE
# ──────────────────────────────────────────────
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def fetch_ticker(session: aiohttp.ClientSession, ticker: str, missing_dates: set[date]):
    """
    Calls AlphaVantage TIME_SERIES_DAILY_ADJUSTED for one ticker and returns
    only the rows whose dates are in missing_dates.
    Uses compact (100-day) output when the gap is ≤90 days, full otherwise.
    """
    days_gap = (max(missing_dates) - min(missing_dates)).days
    outputsize = "compact" if days_gap <= 90 else "full"

    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol":   ticker,
        "outputsize": outputsize,
        "apikey":   ALPHAVANTAGE_API_KEY,
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
                "ticker":           ticker,
                "date":             date_str,
                "open":             values.get("1. open"),
                "high":             values.get("2. high"),
                "low":              values.get("3. low"),
                "close":            values.get("4. close"),
                "adjusted_close":   values.get("5. adjusted close"),
                "volume":           values.get("6. volume"),
                "dividend_amount":  values.get("7. dividend amount"),
                "split_coefficient":values.get("8. split coefficient"),
                "load_time":        datetime.utcnow().isoformat(),
            })

        if not records:
            log.warning(f"⚠️  {ticker}: AlphaVantage had no rows for the missing dates")
            return None

        log.info(f"✅ {ticker}: fetched {len(records)} missing row(s)")
        return pd.DataFrame(records)


async def fetch_all_tickers(gaps: dict[date, list[str]]) -> pd.DataFrame:
    """
    Fetches data for every ticker that has at least one missing date.
    Passes each ticker only the set of dates it is missing.
    """
    # Invert: ticker → set of dates it needs
    ticker_to_dates: dict[str, set[date]] = defaultdict(set)
    for trading_date, tickers in gaps.items():
        for ticker in tickers:
            ticker_to_dates[ticker].add(trading_date)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    frames = []

    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:

        async def bounded_fetch(ticker: str, dates: set[date]):
            async with semaphore:
                return await fetch_ticker(session, ticker, dates)

        tasks = [
            bounded_fetch(ticker, dates)
            for ticker, dates in ticker_to_dates.items()
        ]

        success, skipped = 0, 0
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result is None:
                skipped += 1
            else:
                frames.append(result)
                success += 1

    log.info(f"📊 Fetch summary — success: {success}, skipped: {skipped}")

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


# ──────────────────────────────────────────────
# STEP 3 — UPLOAD TO S3
# ──────────────────────────────────────────────
def upload_to_s3(df: pd.DataFrame) -> None:
    """
    Uploads the backfilled rows as a CSV to S3 under the same prefix that
    Snowpipe watches (S3_PREFIX). Snowpipe will auto-ingest into RAW_STOCK_DATA.
    """
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    run_timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{S3_PREFIX}backfill_{run_timestamp}.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=csv_bytes)

    log.info(
        f"📤 Uploaded {len(df)} rows ({df['ticker'].nunique()} tickers, "
        f"{df['date'].nunique()} dates) → s3://{S3_BUCKET_NAME}/{s3_key}"
    )


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
async def main():
    log.info("🚀 Starting missing-date backfill")

    # 1. Find gaps
    gaps = fetch_missing_dates()
    if not gaps:
        return

    date_range_str = f"{min(gaps)} → {max(gaps)}"
    log.info(f"📅 Gap window: {date_range_str}")

    # 2. Fetch from AlphaVantage
    df = await fetch_all_tickers(gaps)
    if df.empty:
        log.warning("⚠️  No data fetched — nothing to upload.")
        return

    # 3. Upload to S3
    upload_to_s3(df)

    log.info(
        "🏁 Backfill complete. "
        "Wait ~2-5 min for Snowpipe, then run: dbt run"
    )


if __name__ == "__main__":
    asyncio.run(main())
