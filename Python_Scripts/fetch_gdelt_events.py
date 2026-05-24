"""
fetch_gdelt_events.py
─────────────────────
Downloads GDELT 1.0 daily event files, filters for US-relevant geopolitical
events, aggregates to one row per date, and loads into Snowflake.

GDELT (Global Database of Events, Language, and Tone) monitors news worldwide
and classifies every event with CAMEO codes, Goldstein conflict-intensity scores,
and average tone. We aggregate per trading day to produce geopolitical risk
features for the ML feature mart.

Signals produced
────────────────
  avg_tone              Weighted mean article tone for US-relevant events (-100 to +100)
  conflict_intensity    Weighted mean Goldstein scale (-10=max conflict, +10=max coop)
  total_articles        Article volume (attention / volatility proxy)
  total_events          Raw event count
  pct_conflict          Share of events with conflictual QuadClass (3 or 4)
  us_china_tone         Bilateral US–China event tone (weighted)
  us_iran_tone          Bilateral US–Iran event tone (weighted)
  us_russia_tone        Bilateral US–Russia event tone (weighted)

Snowflake table created
───────────────────────
  DBT_STOCKPROJECT.PUBLIC.RAW_GDELT_DAILY

Phases
──────
  1. SETUP   — CREATE TABLE IF NOT EXISTS
  2. DETECT  — MAX(date) from RAW_GDELT_DAILY; fall back to MIN(date) of equity data
  3. FETCH   — Download + filter GDELT 1.0 daily CSV.zip files
  4. LOAD    — Write aggregated rows to Snowflake via write_pandas
  5. REFRESH — Run dbt scoped to news-dependent models

Required environment variables
────────────────────────────────
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
  SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE

Optional overrides
──────────────────
  GDELT_START_DATE   last-resort fallback if RAW_STOCK_DATA is also empty (default: 2015-01-01)
  GDELT_MAX_DAYS     max calendar days to backfill per run (default: 30); run again to continue
  DBT_EXECUTABLE     default: /Users/jamaalsmith/dbtenv/bin/dbt
  DBT_PROJECT_DIR    default: <script parent>/DBTSTOCKPROJECT
  DBT_PROFILES_DIR   default: <DBT_PROJECT_DIR>
"""

import io
import logging
import os
import subprocess
import time
import zipfile
from datetime import date, timedelta
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

GDELT_START_DATE = os.environ.get("GDELT_START_DATE", "2015-01-01")
GDELT_MAX_DAYS   = int(os.environ.get("GDELT_MAX_DAYS", "30"))

_SCRIPT_DIR      = Path(__file__).resolve().parent
DBT_EXECUTABLE   = os.environ.get("DBT_EXECUTABLE",   "/Users/jamaalsmith/dbtenv/bin/dbt")
DBT_PROJECT_DIR  = os.environ.get("DBT_PROJECT_DIR",  str(_SCRIPT_DIR.parent / "DBTSTOCKPROJECT"))
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)

TARGET_TABLE = "RAW_GDELT_DAILY"

# GDELT 1.0 daily export — no API key required
GDELT_URL = "http://data.gdeltproject.org/events/{date}.export.CSV.zip"

# GDELT 1.0 column indices we need (0-based, tab-separated, no header)
# Full schema: https://www.gdeltproject.org/data/documentation/GDELT-Event_Codebook-V2.0.pdf
_USECOLS = [7, 17, 29, 30, 33, 34, 37, 44, 51]
_COL_NAMES = [
    "actor1_country",     # col 7
    "actor2_country",     # col 17
    "quad_class",         # col 29: 1=VerbalCoop 2=MatCoop 3=VerbalConflict 4=MatConflict
    "goldstein_scale",    # col 30: -10 (conflict) to +10 (cooperation)
    "num_articles",       # col 33
    "avg_tone",           # col 34: -100 to +100
    "actor1_geo_country", # col 37: FIPS geo code for actor 1
    "actor2_geo_country", # col 44: FIPS geo code for actor 2
    "action_geo_country", # col 51: FIPS geo code for the action location
]

# Country code sets — GDELT uses both FIPS-10/4 and ISO-3166 variants
_US  = {"US", "USA"}
_CHN = {"CH", "CHN"}
_IRN = {"IR", "IRN"}
_RUS = {"RS", "RUS"}

# Regional groupings for aggregated bilateral tones
# FIPS-10/4 + ISO-3166-alpha-3 variants both included
_EU = {
    # Major Western Europe
    "UK", "GBR", "GM", "DEU", "FR", "FRA", "IT", "ITA", "SP", "ESP",
    # Nordics
    "NO", "NOR", "SE", "SWE", "DA", "DNK", "FI", "FIN",
    # Benelux + smaller
    "NL", "NLD", "BE", "BEL", "PO", "PRT", "EI", "IRL", "AU", "AUT", "SZ", "CHE",
    # Eastern Europe
    "PL", "POL", "GR", "GRC", "HU", "HUN", "RO", "ROU", "CZ", "CZE",
    # EU bloc code
    "EUN",
}
_MIDEAST = {
    "IR", "IRN",    # Iran
    "SA", "SAU",    # Saudi Arabia
    "IS", "ISR",    # Israel
    "IQ", "IRQ",    # Iraq
    "SY", "SYR",    # Syria
    "JO", "JOR",    # Jordan
    "LB", "LBN",    # Lebanon
    "AE", "ARE",    # UAE
    "QA", "QAT",    # Qatar
    "KU", "KWT",    # Kuwait
    "BH", "BHR",    # Bahrain
    "OM", "OMN",    # Oman
    "YM", "YEM",    # Yemen
    "TU", "TUR",    # Turkey
    "EG", "EGY",    # Egypt
}
_APAC = {
    "CH", "CHN",    # China
    "JA", "JPN",    # Japan
    "KS", "KOR",    # South Korea
    "IN", "IND",    # India
    "AS", "AUS",    # Australia
    "NZ", "NZL",    # New Zealand
    "VM", "VNM",    # Vietnam
    "TH", "THA",    # Thailand
    "MY", "MYS",    # Malaysia
    "SN", "SGP",    # Singapore
    "RP", "PHL",    # Philippines
    "ID", "IDN",    # Indonesia
    "KN", "PRK",    # North Korea
    "TW", "TWN",    # Taiwan
    "PK", "PAK",    # Pakistan
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
_DDL = """
    CREATE TABLE IF NOT EXISTS {db}.PUBLIC.RAW_GDELT_DAILY (
        date                DATE          NOT NULL,
        avg_tone            FLOAT,
        conflict_intensity  FLOAT,
        total_articles      INTEGER,
        total_events        INTEGER,
        pct_conflict        FLOAT,
        -- Bilateral country-pair tones
        us_china_tone       FLOAT,
        us_iran_tone        FLOAT,
        us_russia_tone      FLOAT,
        -- Regional aggregated tones
        us_europe_tone      FLOAT,
        us_mideast_tone     FLOAT,
        us_apac_tone        FLOAT,
        load_timestamp      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
"""


def setup_table() -> None:
    log.info("Phase 1: ensuring RAW_GDELT_DAILY exists …")
    conn = _snowflake_conn()
    try:
        conn.cursor().execute(_DDL.format(db=SNOWFLAKE_DATABASE))
        log.info("  ✅ RAW_GDELT_DAILY ready")
    finally:
        conn.close()


# ──────────────────────────────────────────────
# PHASE 2 — DETECT START DATE
# ──────────────────────────────────────────────
def _equity_start_date(cur) -> str:
    """MIN(date) from RAW_STOCK_DATA; falls back to GDELT_START_DATE."""
    try:
        cur.execute(
            f"SELECT MIN(date)::date::varchar FROM {SNOWFLAKE_DATABASE}.PUBLIC.RAW_STOCK_DATA"
        )
        row = cur.fetchone()
        if row and row[0]:
            log.info(f"  Equity start date: {row[0]} — using as GDELT fallback")
            return row[0]
    except Exception as exc:
        log.warning(f"  Could not query RAW_STOCK_DATA ({exc})")
    log.info(f"  Defaulting to GDELT_START_DATE={GDELT_START_DATE}")
    return GDELT_START_DATE


def detect_start_date() -> date:
    """
    Returns the first date to fetch. Resumes from MAX(date) + 1 when data exists;
    on first load anchors to the equity data start date so coverage matches the S&P 500.
    """
    log.info("Phase 2: detecting GDELT start date …")
    conn = _snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT MAX(date)::date::varchar FROM {SNOWFLAKE_DATABASE}.PUBLIC.{TARGET_TABLE}"
        )
        row = cur.fetchone()
        if row and row[0]:
            resume = date.fromisoformat(row[0]) + timedelta(days=1)
            log.info(f"  Resuming from {resume}")
            return resume
        fallback = _equity_start_date(cur)
    finally:
        conn.close()

    start = date.fromisoformat(fallback)
    log.info(f"  First load — starting from {start}")
    return start


# ──────────────────────────────────────────────
# PHASE 3 — FETCH & AGGREGATE
# ──────────────────────────────────────────────
def _bilateral_tone(df: pd.DataFrame, codes_a: set, codes_b: set) -> float | None:
    """Article-weighted mean tone for events where one actor is in codes_a, the other in codes_b."""
    mask = (
        (df["actor1_country"].isin(codes_a) & df["actor2_country"].isin(codes_b)) |
        (df["actor1_country"].isin(codes_b) & df["actor2_country"].isin(codes_a))
    )
    subset = df.loc[mask]
    if subset.empty or subset["num_articles"].sum() == 0:
        return None
    return float(
        (subset["avg_tone"] * subset["num_articles"]).sum() / subset["num_articles"].sum()
    )


def _fetch_and_aggregate(target_date: date) -> dict | None:
    """
    Downloads the GDELT 1.0 export for target_date, filters to US-relevant events,
    and returns a dict of aggregated daily signals. Returns None on 404 or parse error.
    """
    url = GDELT_URL.format(date=target_date.strftime("%Y%m%d"))
    try:
        resp = requests.get(url, timeout=120)
        if resp.status_code == 404:
            log.info(f"  {target_date}: file not yet published (404) — skipping")
            return None
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning(f"  {target_date}: download failed — {exc}")
        return None

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                df = pd.read_csv(
                    f,
                    sep="\t",
                    header=None,
                    usecols=_USECOLS,
                    on_bad_lines="skip",
                    dtype=str,
                    low_memory=False,
                )
    except Exception as exc:
        log.warning(f"  {target_date}: parse error — {exc}")
        return None

    df.columns = _COL_NAMES

    # Normalize country codes to uppercase; fill NaN
    for col in ("actor1_country", "actor2_country", "actor1_geo_country",
                "actor2_geo_country", "action_geo_country"):
        df[col] = df[col].fillna("").str.upper().str.strip()

    # Convert numerics
    df["num_articles"]    = pd.to_numeric(df["num_articles"],    errors="coerce").fillna(1)
    df["avg_tone"]        = pd.to_numeric(df["avg_tone"],        errors="coerce")
    df["goldstein_scale"] = pd.to_numeric(df["goldstein_scale"], errors="coerce")
    df["quad_class"]      = pd.to_numeric(df["quad_class"],      errors="coerce")

    # Filter: keep only events with some US involvement
    us_mask = (
        df["actor1_country"].isin(_US) |
        df["actor2_country"].isin(_US) |
        df["actor1_geo_country"].isin(_US) |
        df["actor2_geo_country"].isin(_US) |
        df["action_geo_country"].isin(_US)
    )
    df = df.loc[us_mask].dropna(subset=["avg_tone"]).copy()

    if df.empty:
        log.info(f"  {target_date}: no US-relevant events found")
        return None

    total_articles = int(df["num_articles"].sum())
    total_events   = len(df)
    weight         = df["num_articles"]

    weighted_tone      = float((df["avg_tone"] * weight).sum() / total_articles)
    weighted_goldstein = float(
        (df["goldstein_scale"].fillna(0) * weight).sum() / total_articles
    )
    pct_conflict = float(df["quad_class"].isin([3, 4]).sum() / total_events)

    return {
        "DATE":               target_date,
        "AVG_TONE":           round(weighted_tone, 4),
        "CONFLICT_INTENSITY": round(weighted_goldstein, 4),
        "TOTAL_ARTICLES":     total_articles,
        "TOTAL_EVENTS":       total_events,
        "PCT_CONFLICT":       round(pct_conflict, 4),
        # Bilateral country-pair tones
        "US_CHINA_TONE":      _bilateral_tone(df, _US, _CHN),
        "US_IRAN_TONE":       _bilateral_tone(df, _US, _IRN),
        "US_RUSSIA_TONE":     _bilateral_tone(df, _US, _RUS),
        # Regional aggregated tones (weighted mean across all events with any country in region)
        "US_EUROPE_TONE":     _bilateral_tone(df, _US, _EU),
        "US_MIDEAST_TONE":    _bilateral_tone(df, _US, _MIDEAST),
        "US_APAC_TONE":       _bilateral_tone(df, _US, _APAC),
    }


def fetch_date_range(start: date, end: date) -> pd.DataFrame:
    log.info(f"Phase 3: fetching GDELT {start} → {end} ({(end - start).days + 1} days) …")
    records = []
    current = start
    while current <= end:
        result = _fetch_and_aggregate(current)
        if result:
            records.append(result)
            log.info(
                f"  ✅ {current}: {result['TOTAL_EVENTS']:,} events, "
                f"tone={result['AVG_TONE']:.2f}, conflict={result['CONFLICT_INTENSITY']:.2f}"
            )
        time.sleep(0.5)  # polite pause for GDELT servers
        current += timedelta(days=1)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    log.info(f"📊 {len(df)} days aggregated")
    return df


# ──────────────────────────────────────────────
# PHASE 4 — LOAD INTO SNOWFLAKE
# ──────────────────────────────────────────────
def load_to_snowflake(df: pd.DataFrame) -> None:
    log.info(f"Phase 4: loading {len(df)} rows into {TARGET_TABLE} …")
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
        "--select", "stg_gdelt_events int_news_sentiment mart_ml_features",
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
    log.info("🚀 Starting GDELT geopolitical events pipeline")

    setup_table()

    start = detect_start_date()
    # GDELT publishes yesterday's file; today's is not available yet
    yesterday = date.today() - timedelta(days=1)
    end = min(yesterday, start + timedelta(days=GDELT_MAX_DAYS - 1))

    if start > end:
        log.info("GDELT is already up to date. Exiting.")
        return

    df = fetch_date_range(start, end)
    if df.empty:
        log.info("No new GDELT data to load. Exiting.")
        return

    load_to_snowflake(df)
    run_dbt()

    if end < yesterday:
        log.info(
            f"⚠️  Backfilled through {end} only. "
            f"Run again to continue (GDELT_MAX_DAYS={GDELT_MAX_DAYS})."
        )

    log.info("🏁 GDELT pipeline complete.")


if __name__ == "__main__":
    main()
