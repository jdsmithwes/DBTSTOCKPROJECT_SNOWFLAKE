"""
fetch_av_news_sentiment.py
──────────────────────────
Fetches market news sentiment from the AlphaVantage NEWS_SENTIMENT endpoint,
aggregates to (date, topic) grain, and loads into Snowflake.

Topics fetched (6 calls/day — stays within the 25 req/day free-tier budget
when combined with the fixed income script's 8 calls)
─────────────────────────────────────────────────────
  financial_markets      Broad equity and market news
  economy_macro          GDP, employment, consumer spending
  economy_monetary       Fed policy, interest rates, QE/QT
  economy_fiscal         Government spending, tax policy
  earnings               Corporate earnings reports and guidance
  energy_transportation  Oil, gas, energy sector news

Signals in RAW_AV_NEWS_SENTIMENT (grain: date, topic)
───────────────────────────────────────────────────────
  avg_sentiment_score    -1.0 (very bearish) to +1.0 (very bullish)
  article_count          Article volume; spikes signal elevated attention
  bullish_pct            Share of articles labeled Bullish or Somewhat-Bullish
  bearish_pct            Share of articles labeled Bearish or Somewhat-Bearish

Note on history depth
──────────────────────
  AlphaVantage NEWS_SENTIMENT has reliable coverage from ~2022 onward.
  The AV_NEWS_START_DATE default is 2022-01-01. Rows before that date may be
  sparse; the int_news_sentiment model handles nulls with LEFT JOINs.

Snowflake table created
───────────────────────
  DBT_STOCKPROJECT.PUBLIC.RAW_AV_NEWS_SENTIMENT

Phases
──────
  1. SETUP   — CREATE TABLE IF NOT EXISTS
  2. DETECT  — MAX(date) per topic; fall back to AV_NEWS_START_DATE
  3. FETCH   — Pull articles from AlphaVantage and aggregate to daily rows
  4. LOAD    — Write via write_pandas
  5. REFRESH — Run dbt scoped to news-dependent models

Required environment variables
────────────────────────────────
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
  SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
  ALPHAVANTAGE_API_KEY

Optional overrides
──────────────────
  AV_NEWS_START_DATE   earliest date to fetch (default: 2022-01-01; AV history limit)
  AV_REQUEST_DELAY     seconds between API calls (default: 2.0)
  DBT_EXECUTABLE       default: /Users/jamaalsmith/dbtenv/bin/dbt
  DBT_PROJECT_DIR      default: <script parent>/DBTSTOCKPROJECT
  DBT_PROFILES_DIR     default: <DBT_PROJECT_DIR>
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

# AV news history is sparse before 2022; keep this floor even when equity data goes earlier
AV_NEWS_START_DATE   = os.environ.get("AV_NEWS_START_DATE",   "2022-01-01")
AV_FORCE_START_DATE  = os.environ.get("AV_FORCE_START_DATE",  "")  # overrides watermark when set
AV_REQUEST_DELAY   = float(os.environ.get("AV_REQUEST_DELAY", "2.0"))
# Days per API window. Premium keys support up to 1000 articles per call;
# 30-day windows keep each call well within that limit for all topic volumes.
AV_WINDOW_DAYS     = int(os.environ.get("AV_WINDOW_DAYS", "30"))

ALPHAVANTAGE_URL = "https://www.alphavantage.co/query"

_SCRIPT_DIR      = Path(__file__).resolve().parent
DBT_EXECUTABLE   = os.environ.get("DBT_EXECUTABLE",   "/Users/jamaalsmith/dbtenv/bin/dbt")
DBT_PROJECT_DIR  = os.environ.get("DBT_PROJECT_DIR",  str(_SCRIPT_DIR.parent / "DBTSTOCKPROJECT"))
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)

TARGET_TABLE = "RAW_AV_NEWS_SENTIMENT"

AV_TOPICS = [
    "financial_markets",
    "economy_macro",
    "economy_monetary",
    "economy_fiscal",
    "earnings",
    "energy_transportation",
]

_BULLISH_LABELS = {"Bullish", "Somewhat-Bullish"}
_BEARISH_LABELS = {"Bearish", "Somewhat-Bearish"}


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
_DDL = """
    CREATE TABLE IF NOT EXISTS {db}.PUBLIC.RAW_AV_NEWS_SENTIMENT (
        date                  DATE          NOT NULL,
        topic                 VARCHAR(50)   NOT NULL,
        avg_sentiment_score   FLOAT,
        article_count         INTEGER,
        bullish_pct           FLOAT,
        bearish_pct           FLOAT,
        load_timestamp        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
"""


def setup_table() -> None:
    log.info("Phase 1: ensuring RAW_AV_NEWS_SENTIMENT exists …")
    conn = _snowflake_conn()
    try:
        conn.cursor().execute(_DDL.format(db=SNOWFLAKE_DATABASE))
        log.info("  ✅ RAW_AV_NEWS_SENTIMENT ready")
    finally:
        conn.close()


# ──────────────────────────────────────────────
# PHASE 2 — DETECT LATEST DATES PER TOPIC
# ──────────────────────────────────────────────
def detect_latest_dates() -> dict[str, str]:
    """
    Returns fetch-from dates per topic. Incremental (MAX(date) + 1d) when data exists;
    falls back to AV_NEWS_START_DATE for topics with no rows yet.
    Does not query RAW_STOCK_DATA because AV news history only goes back to ~2022.
    """
    log.info("Phase 2: detecting latest AV news dates …")
    conn = _snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT topic, MAX(date)::date::varchar
            FROM {SNOWFLAKE_DATABASE}.PUBLIC.{TARGET_TABLE}
            GROUP BY topic
        """)
        loaded = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()

    dates: dict[str, str] = {}
    for topic in AV_TOPICS:
        if AV_FORCE_START_DATE:
            start = AV_FORCE_START_DATE
        elif topic in loaded:
            start = (date.fromisoformat(loaded[topic]) + timedelta(days=1)).isoformat()
        else:
            start = AV_NEWS_START_DATE
        dates[topic] = start
        log.info(f"  {topic}: fetching from {start}")

    return dates


# ──────────────────────────────────────────────
# PHASE 3 — FETCH FROM ALPHAVANTAGE
# ──────────────────────────────────────────────
def _fetch_topic_articles(topic: str, time_from: str, time_to: str) -> list[dict]:
    """Fetches up to 1000 articles for a topic within the given time window."""
    params = {
        "function":  "NEWS_SENTIMENT",
        "topics":    topic,
        "time_from": time_from,
        "time_to":   time_to,
        "sort":      "LATEST",
        "limit":     "1000",
        "apikey":    ALPHAVANTAGE_API_KEY,
    }
    resp = requests.get(ALPHAVANTAGE_URL, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    if "feed" not in payload:
        note = (
            payload.get("Note")
            or payload.get("Information")
            or payload.get("Error Message", "unknown")
        )
        log.warning(f"  [{topic}] No 'feed' key: {note[:120]}")
        return []

    return payload["feed"]


def _aggregate_articles(articles: list[dict], topic: str) -> pd.DataFrame:
    """Aggregates article-level sentiment to (date, topic) daily rows."""
    records = []
    for article in articles:
        raw_ts = article.get("time_published", "")
        if len(raw_ts) < 8:
            continue
        try:
            article_date = datetime.strptime(raw_ts[:8], "%Y%m%d").date()
        except ValueError:
            continue
        score = article.get("overall_sentiment_score")
        label = article.get("overall_sentiment_label", "")
        if score is None:
            continue
        records.append({"date": article_date, "score": float(score), "label": label})

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    agg = (
        df.groupby("date")
        .agg(
            avg_sentiment_score=("score", "mean"),
            article_count=("score", "count"),
            bullish_count=("label", lambda x: x.isin(_BULLISH_LABELS).sum()),
            bearish_count=("label", lambda x: x.isin(_BEARISH_LABELS).sum()),
        )
        .reset_index()
    )
    agg["topic"]       = topic
    agg["bullish_pct"] = agg["bullish_count"] / agg["article_count"]
    agg["bearish_pct"] = agg["bearish_count"] / agg["article_count"]
    return agg[["date", "topic", "avg_sentiment_score", "article_count", "bullish_pct", "bearish_pct"]]


def fetch_all(latest_dates: dict[str, str]) -> pd.DataFrame:
    log.info("Phase 3: fetching AlphaVantage news sentiment …")
    today  = date.today()
    frames = []

    for topic, start_str in latest_dates.items():
        start = date.fromisoformat(start_str)
        if start > today:
            log.info(f"  {topic}: already up to date")
            continue

        all_articles: list[dict] = []
        window_start = start

        while window_start <= today:
            window_end = min(window_start + timedelta(days=AV_WINDOW_DAYS - 1), today)
            time_from  = window_start.strftime("%Y%m%dT0000")
            time_to    = window_end.strftime("%Y%m%dT2359")

            try:
                batch = _fetch_topic_articles(topic, time_from, time_to)
                all_articles.extend(batch)
                log.info(f"  {topic} {window_start}→{window_end}: {len(batch)} articles")
            except requests.RequestException as exc:
                log.error(f"  {topic} {window_start}: request failed — {exc}")

            time.sleep(AV_REQUEST_DELAY)
            window_start = window_end + timedelta(days=1)

        if all_articles:
            df_topic = _aggregate_articles(all_articles, topic)
            if not df_topic.empty:
                frames.append(df_topic)
                log.info(f"  {topic}: {len(all_articles)} total articles → {len(df_topic)} daily rows")
        else:
            log.info(f"  {topic}: no articles returned")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    log.info(f"📊 Total: {len(combined)} daily sentiment rows")
    return combined


# ──────────────────────────────────────────────
# PHASE 4 — LOAD INTO SNOWFLAKE
# ──────────────────────────────────────────────
def load_to_snowflake(df: pd.DataFrame) -> None:
    log.info(f"Phase 4: loading {len(df)} rows into {TARGET_TABLE} …")
    df_load = df.rename(columns={
        "date":                "DATE",
        "topic":               "TOPIC",
        "avg_sentiment_score": "AVG_SENTIMENT_SCORE",
        "article_count":       "ARTICLE_COUNT",
        "bullish_pct":         "BULLISH_PCT",
        "bearish_pct":         "BEARISH_PCT",
    })
    conn = _snowflake_conn()
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df_load,
            table_name=TARGET_TABLE,
            database=SNOWFLAKE_DATABASE,
            schema="PUBLIC",
            auto_create_table=False,
            overwrite=False,
        )
        if success:
            log.info(f"  ✅ {nrows} rows loaded in {nchunks} chunk(s)")
        else:
            log.error("  ❌ write_pandas reported failure")
    finally:
        conn.close()


# ──────────────────────────────────────────────
# PHASE 5 — REFRESH DBT
# ──────────────────────────────────────────────
def run_dbt() -> None:
    log.info("Phase 5: refreshing dbt news-dependent models …")
    cmd = [
        DBT_EXECUTABLE, "run",
        "--select", "stg_av_news_sentiment int_news_sentiment mart_ml_features",
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
    log.info("🚀 Starting AlphaVantage news sentiment pipeline")

    setup_table()
    latest_dates = detect_latest_dates()

    df = fetch_all(latest_dates)
    if df.empty:
        log.info("Nothing new to load. Exiting.")
        return

    load_to_snowflake(df)
    run_dbt()

    log.info("🏁 AV news sentiment pipeline complete.")


if __name__ == "__main__":
    main()
