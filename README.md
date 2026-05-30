# DBT Stock Market Project — ML Feature Pipeline

A dbt + Snowflake pipeline that ingests S&P 500 market data from five sources, engineers institutional-grade features, and produces a single ML-ready table for predicting stock returns at 1, 3, 6, 9, and 12-month horizons. The end consumer is a family-friendly retirement tool — interpretable, moderately accurate models over black-box complexity.

---

## Table of Contents

1. [Project Purpose](#project-purpose)
2. [Architecture Overview](#architecture-overview)
3. [Getting Started](#getting-started)
4. [Data Sources & Ingestion](#data-sources--ingestion)
5. [Model Layers](#model-layers)
   - [Staging](#staging-layer)
   - [Intermediate](#intermediate-layer)
   - [Marts](#marts-layer)
6. [ML Training Pipeline](#ml-training-pipeline)
7. [Data Quality Tests](#data-quality-tests)
8. [Automation & Nightly Pipeline](#automation--nightly-pipeline)
9. [dbt Patterns & Best Practices](#dbt-patterns--best-practices)
10. [Daily Operations](#daily-operations)

---

## Project Purpose

The goal is to determine how well a machine learning model can predict S&P 500 stock performance for retirement investors. The pipeline feeds a nightly ML feature mart with five prediction horizons:

| Horizon | Column | Why it matters to a retirement saver |
|---|---|---|
| 1 month | `forward_return_1m` | Short-term rebalancing signal |
| 3 months | `forward_return_3m` | **Primary ML target** — one quarter ahead |
| 6 months | `forward_return_6m` | Intermediate-term conviction |
| 9 months | `forward_return_9m` | Portfolio review cycle |
| 12 months | `forward_return_12m` | Annual performance benchmark |

The pipeline is designed with institutional rigor: no data leakage, tested at every layer, schema change protection on the ML table, and a clear separation between features that come from the past (safe to use) and targets that come from the future (only available in training data).

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                      │
├──────────────────┬──────────────────┬──────────────────┬────────────────────┤
│  AlphaVantage    │  AlphaVantage    │    FRED API      │  Snowflake         │
│  Market Data     │  Fixed Income    │  (St. Louis Fed) │  Marketplace       │
│                  │                  │                  │                    │
│ · Stock prices   │ · Treasury yields│ · VIX            │ · Industry leading │
│ · Company        │   (6 maturities) │ · WTI/Brent oil  │   indicators       │
│   fundamentals   │ · Fed Funds Rate │ · HY credit      │ · Coincident &     │
│ · Analyst        │ · CPI            │   spread         │   lagging signals  │
│   ratings        │                  │ · Inflation      │                    │
│ · News sentiment │                  │   breakeven      │                    │
└────────┬─────────┴────────┬─────────┴────────┬─────────┴──────────┬─────────┘
         │                  │                  │                     │
         ▼                  ▼                  ▼                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     RAW TABLES (Snowflake PUBLIC schema)                    │
│                                                                             │
│  RAW_STOCK_DATA    RAW_TREASURY_YIELDS    RAW_FRED_MACRO    INDUSTRY_       │
│  RAW_COMPANY_      RAW_FED_FUNDS_RATE     RAW_GDELT_DAILY   LEADING_        │
│  OVERVIEW          RAW_CPI               RAW_AV_NEWS_SENT  INDICATORS_*    │
│                                          RAW_ML_PREDICTIONS                │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     STAGING LAYER (ephemeral — no storage cost)             │
│                                                                             │
│  Type-cast, rename, clean, and deduplicate raw data. One model per source. │
│  No business logic. No joins.                                               │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                 INTERMEDIATE LAYER (tables — cached computations)           │
│                                                                             │
│  Feature engineering: moving averages, RSI, Bollinger Bands, forward       │
│  returns, yield curve signals, economic aggregates, news sentiment,         │
│  ticker coverage classification.                                            │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                MARTS LAYER (Snowflake-managed Iceberg tables)               │
│                                                                             │
│  mart_ml_features     mart_fixed_income      mart_data_gaps                │
│  (95 columns,         (long format:          (operational: missing          │
│   grain: ticker +      grain: date +          trading days per ticker)      │
│   date)                security_name)                                       │
│                                                                             │
│  mart_ml_predictions  mart_model_evaluation  mart_macro_health             │
│  (incremental;        (IC, ICIR, MAE/RMSE,   (daily policy-inflation        │
│   grain: ticker +      directional accuracy   regime: plain-English         │
│   date + horizon +     by horizon, model,     labels for retirement         │
│   model_version)       and sector)            savers)                       │
│                                                                             │
│  mart_delisted_tickers                                                      │
│  (operational: one row per delisted ticker with inferred exit reason)       │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │ predictions written by Python
                          ┌─────────┴──────────┐
                          │  train_ml_model.py  │
                          │  (XGBoost; 4 models │
                          │   per nightly run)  │
                          └────────────────────┘
```

---

## Getting Started

### Prerequisites

- dbt Core 1.11+ and dbt-snowflake adapter installed
- Python 3.11 with `snowflake-connector-python`, `pandas`, `pyarrow`, `xgboost`, `scikit-learn`, and `requests`
- A `.env` file at `DBTSTOCKPROJECT/.env` (copy from `.env.example` and fill in credentials)

### Install dbt packages

```bash
cd DBTSTOCKPROJECT
dbt deps --profiles-dir .
```

### Verify your Snowflake connection

All dbt commands must be run from the `DBTSTOCKPROJECT/` directory with the `.env` loaded. The `--profiles-dir .` flag tells dbt to read `profiles.yml` from the current folder instead of `~/.dbt/`. This keeps credentials local to the project.

```bash
cd DBTSTOCKPROJECT
set -a && source .env && set +a   # load env vars into the shell
dbt debug --profiles-dir .        # should print: All checks passed!
```

> **If you see a network policy error:** Go to your Snowflake console as `ACCOUNTADMIN` and run:
> `ALTER USER jdsmithwes SET NETWORK_POLICY = NULL;`

### Run the full pipeline

```bash
dbt run --profiles-dir .    # build all models
dbt test --profiles-dir .   # validate data quality
```

---

## Data Sources & Ingestion

The pipeline draws from five sources. Four require Python scripts to populate raw Snowflake tables; the fifth arrives via Snowflake Marketplace.

### 1 — AlphaVantage: Stock Prices & Fundamentals

The primary equity data source. Three scripts handle separate parts of this data:

| Script | What it fetches | Destination |
|---|---|---|
| `Python_Scripts/backfill_missing_dates.py` | Daily OHLCV prices for all tickers; fills gaps since last load | `PUBLIC.RAW_STOCK_DATA` |
| `Python_Scripts/fetch_av_company_overview.py` | Point-in-time fundamentals: P/E, P/B, EV/EBITDA, margins, analyst ratings, insider ownership, sector, industry | `PUBLIC.RAW_COMPANY_OVERVIEW` |
| `Python_Scripts/fetch_av_news_sentiment.py` | Topic-level sentiment scores from AlphaVantage's NLP-tagged news feed | `PUBLIC.RAW_AV_NEWS_SENTIMENT` |

**What it collects:** daily OHLCV prices (Open, High, Low, Close, adjusted Close, Volume), split coefficients, dividends, and 50+ company fundamental fields.

**Why adjusted close matters:** When a stock splits 2-for-1, the share price halves overnight even though nothing fundamentally changed. AlphaVantage adjusts historical prices retroactively so that all return calculations are apples-to-apples. Every feature and target in the ML table uses `adjusted_close`.

**Why the company overview is append-only:** `RAW_COMPANY_OVERVIEW` grows a new row on each load rather than updating in place. This is an intentional design choice — you keep a history of what the fundamentals looked like at different points in time. The staging model deduplicates by keeping the latest row per ticker via `QUALIFY ROW_NUMBER()`. If you ever want point-in-time snapshots for backtesting, the history is there.

For missing stock price dates, run:
```bash
dbt run --select mart_data_gaps --profiles-dir .
python3 Python_Scripts/backfill_missing_dates.py
```

### 2 — AlphaVantage: Fixed Income Data

Treasury yields, the Fed Funds Rate, and CPI loaded directly into Snowflake (no S3 required; data volume is small).

```bash
set -a && source DBTSTOCKPROJECT/.env && set +a
python3 Python_Scripts/fetch_alphavantage_fixed_income.py
```

**Tables created:**

| Table | Content |
|---|---|
| `PUBLIC.RAW_TREASURY_YIELDS` | Daily yields for 3m, 2y, 5y, 7y, 10y, 30y maturities |
| `PUBLIC.RAW_FED_FUNDS_RATE` | Effective federal funds rate (daily) |
| `PUBLIC.RAW_CPI` | Consumer Price Index, all urban consumers (monthly) |

**Why fixed income matters for equity prediction:** Interest rates are the discount rate for every future cash flow. When the 10-year yield rises, the present value of future earnings falls — which mechanically pressures stock prices. The yield curve shape (`spread_2s10s`, `spread_3m10y`) has historically been one of the most reliable leading indicators of recessions. An inverted curve (short rates higher than long rates) preceded every recession in modern history.

The script is idempotent — running it again only fetches rows newer than the last loaded date.

### 3 — FRED API: Macro & Risk Signals

Five daily macro series from the Federal Reserve Bank of St. Louis.

```bash
python3 Python_Scripts/fetch_fred_macro_data.py
```

**Table created:** `PUBLIC.RAW_FRED_MACRO`

| Series | What it measures | Why it matters |
|---|---|---|
| VIX | CBOE Volatility Index | Market fear gauge; >30 = elevated stress |
| WTI / Brent oil | Crude oil spot price | Energy cost shock proxy; correlated with inflation |
| T10YIE | 10-year breakeven inflation | Market's implied inflation expectation |
| BAMLH0A0HYM2 | High yield credit spread | Corporate credit stress; spikes before equity drawdowns |

**FRED_API_KEY** is required in `.env`. Get a free key at https://fred.stlouisfed.org.

### 4 — GDELT: Geopolitical & News Sentiment

The GDELT project monitors global news in real time and scores every event for conflict intensity and sentiment. No API key required — data is freely available.

```bash
python3 Python_Scripts/fetch_gdelt_events.py          # loads 30 days by default
GDELT_MAX_DAYS=3650 python3 Python_Scripts/fetch_gdelt_events.py  # full backfill
```

**Table created:** `PUBLIC.RAW_GDELT_DAILY`

**What it captures:**
- `avg_tone` — article-weighted news sentiment for US-relevant events (-100 to +100)
- `conflict_intensity` — Goldstein scale (-10 = maximum conflict, +10 = maximum cooperation)
- Bilateral tones: `us_china_tone`, `us_iran_tone`, `us_russia_tone`
- Regional tones: `us_europe_tone`, `us_mideast_tone`, `us_apac_tone`

**Why geopolitical sentiment matters:** Equity risk premiums expand when geopolitical uncertainty rises. By quantifying bilateral tension (e.g., US–China trade war, US–Iran flare-ups), the model can adjust for event-driven risk that fundamental ratios miss.

> **Note on backfill time:** Each day requires downloading a ~40MB CSV from the GDELT servers. A full 5-year backfill takes approximately 60–90 minutes. The script resumes from where it stopped, so you can re-run it after interruption.

### 5 — Snowflake Marketplace: Industry Economic Indicators

Pre-loaded — no script needed. The Marketplace dataset contains monthly leading, coincident, and lagging indicators indexed by industry. Access it in Snowflake via:

```
ECONOMIC_INDICATORS.INDUSTRY_LEADING_INDICATORS_METADATA
ECONOMIC_INDICATORS.INDUSTRY_LEADING_INDICATORS_TIMESERIES
```

**Why leading/coincident/lagging matters:** Economic cycles don't affect all industries equally or simultaneously. Tech often leads consumer staples out of recessions. By joining each ticker's industry to its specific economic indicators, the model picks up sector-level business cycle position that aggregate GDP numbers miss.

---

## Model Layers

### Staging Layer

**Materialization: `ephemeral`**

Ephemeral models are the dbt equivalent of a SQL CTE — they get inlined into downstream queries at compile time and never create a Snowflake object. This means **zero storage cost** for the staging layer.

> **Why ephemeral for staging?** Staging models do one job: clean and type-cast raw data. There is no reason to pay for a table or even a view when the data lives in the raw tables just one step away. The savings add up when running hundreds of tickers daily.

**Staging rule:** No business logic, no joins, no aggregations. If you find yourself doing a `GROUP BY` in staging, move it to intermediate.

| Model | Source | What it does |
|---|---|---|
| `stg_stockprice` | `RAW_STOCK_DATA` | Casts prices to `FLOAT`, dates to `DATE`, renames to snake_case; deduplicates via `QUALIFY ROW_NUMBER()` — critical because the append-only load creates ~10x duplicates |
| `stg_companyoverview` | `RAW_COMPANY_OVERVIEW` | Parses 50+ fields from fundamentals; deduplicates by latest `load_timestamp` per ticker |
| `stg_indicator_metadata` | Marketplace metadata table | Cleans indicator names, types, and descriptions |
| `stg_indicator_timeseries` | Marketplace timeseries table | Casts indicator values and dates |
| `stg_fred_macro` | `RAW_FRED_MACRO` | Pivots long-format series rows into wide-format date-keyed rows |
| `stg_treasury_yields` | `RAW_TREASURY_YIELDS` | Pivots maturity rows to one row per date with yield columns |
| `stg_fed_funds_rate` | `RAW_FED_FUNDS_RATE` | Simple clean and cast |
| `stg_cpi` | `RAW_CPI` | Simple clean and cast |
| `stg_gdelt_events` | `RAW_GDELT_DAILY` | Renames columns to `gdelt_` prefix for clarity in downstream models |
| `stg_av_news_sentiment` | `RAW_AV_NEWS_SENTIMENT` | Aggregates topic-level sentiment to one row per date |
| `stg_ml_predictions` | `RAW_ML_PREDICTIONS` | Deduplicates ML predictions by `(ticker, date, horizon, model_version)` keeping the latest `run_timestamp`; casts `run_timestamp` to `TIMESTAMP_NTZ(6)` |

> **Why deduplicate ML predictions in staging?** The Python training script can be re-run (by CI, by the analyst, manually) and will write new prediction rows without deleting old ones. If you don't deduplicate in staging, the downstream `mart_ml_predictions` would double-count rows from multiple runs. The `ROW_NUMBER()` dedup on `run_timestamp DESC` keeps only the most recent prediction per `(ticker, date, horizon, model_version)` key — everything downstream can trust there is exactly one row per that grain.

---

### Intermediate Layer

**Materialization: `table` (default) or `view` for lightweight pass-throughs**

Intermediate models contain all the real feature engineering. They are materialized as tables because window functions (the `OVER (PARTITION BY ... ORDER BY ...)` syntax used for moving averages, RSI, and forward returns) are expensive to compute repeatedly. By persisting the results as tables, downstream models read pre-computed values.

> **Why table (not view) for intermediate?** A view re-executes its SQL every time it is queried. For a model like `int_stock_price_features` that computes 18 window functions across 500+ tickers over 5 years of daily data, re-running that SQL every time `mart_ml_features` is built would be extremely slow and expensive in Snowflake credits. A table stores the result once per dbt run.

#### Price Features — `int_stock_price_features`

Computes all price-derived technical indicators using SQL window functions:

| Feature | How it's computed | What it signals |
|---|---|---|
| `daily_return` | `(adjusted_close / LAG(adjusted_close, 1)) - 1` | Day-over-day price change |
| `return_1m / 3m / 6m` | `LAG(adjusted_close, 21/63/126)` — backward looking | Price momentum at multiple horizons |
| `ma_20d / 50d / 200d` | `AVG(adjusted_close) OVER (...ROWS BETWEEN N PRECEDING AND CURRENT ROW)` | Trend direction and support levels |
| `price_to_ma_20d/50d/200d` | `adjusted_close / ma_Nd` | How extended price is relative to trend |
| `bollinger_pct_b` | `(price - lower_band) / (upper_band - lower_band)` | Position within the volatility band (0=oversold, 1=overbought) |
| `volatility_20d / 60d` | Rolling standard deviation of daily returns | Risk level; high vol = wider prediction uncertainty |
| `rsi_14` | 14-period Relative Strength Index | Momentum oscillator; <30=oversold, >70=overbought |
| `volume_ratio_20d` | `volume / AVG(volume) OVER 20 days` | Unusual volume spike = institutional activity |

> **Data leakage warning:** Every backward-looking feature uses `LAG()` (looking into the past). The forward-return targets use `LEAD()` (looking into the future) — but only on the target columns, never on the features. Mixing future information into a feature is the most common cause of falsely optimistic ML backtests.

#### Forward Returns — `int_forward_returns`

Computes the five prediction targets using `LEAD()`:

```sql
LEAD(adjusted_close, 63) OVER (PARTITION BY ticker ORDER BY date)
```

This gives the adjusted close 63 trading days in the future. Dividing today's price by that future price gives the forward return. The model then sets `has_3m_target = true` only when the future price actually exists (i.e., the date is far enough in the past). The most recent ~63 trading days will have `NULL` targets — these become the `inference` rows.

#### Ticker Coverage — `int_ticker_coverage`

Grain: one row per ticker. Classifies each ticker as `COMPLETE`, `DELISTED`, or `INCOMPLETE` based on data completeness and recency.

| Status | Criteria |
|---|---|
| `COMPLETE` | Last trade within 14 calendar days of the dataset end AND coverage ≥ 95% of expected trading days |
| `DELISTED` | Last trade more than 63 calendar days behind the dataset end — clearly inactive |
| `INCOMPLETE` | Everything else: new entrants with short history, data gaps, gray zone tickers |

> **Why gate the ML mart on ticker status?** Without this filter, delisted tickers contaminate the ML feature matrix in two ways. First, their most recent rows have stale features (prices, fundamentals) that don't represent a tradeable stock. Second, including them in training without marking them as non-investable would teach the model patterns from stocks you can't actually buy. `mart_ml_features` hard-excludes any ticker that is not `COMPLETE` — only actively trading stocks with sufficient history get into the training and inference data.

> **Why 14 days and 63 days specifically?** 14 calendar days covers about 10 NYSE trading days — enough buffer for holidays or delayed data loads. 63 trading days is exactly one quarter, the point at which a stock has clearly stopped trading and is no longer a viable investment candidate.

#### Company Overview Models

| Model | Content |
|---|---|
| `company_overview_nonfinancial` | Ticker, name, sector, GICS industry, country, fiscal year end |
| `company_overview_valuation` | P/E, P/B, EV/EBITDA, beta, market cap, revenue, EBITDA snapshot |
| `company_overview_analystrating` | Buy/hold/sell counts, consensus target price |

> **Why snapshot fundamentals?** Company fundamentals (P/E ratio, market cap) are refreshed monthly from the AlphaVantage overview endpoint. They don't change daily. Joining them as a snapshot per ticker (not per date) keeps the mart lean. Before training an ML model, be aware that these values represent the current state — in a production backtesting environment you'd want point-in-time fundamentals to avoid look-ahead bias.

#### Analyst Signals — `int_analyst_signals`

Aggregates analyst rating counts into a single `net_bullish_score` on a [-2, +2] scale:
- Strong Buy = +2, Buy = +1, Hold = 0, Sell = -1, Strong Sell = -2

Also computes `analyst_upside = (consensus_target / latest_close) - 1` and `total_aligned_ownership_pct = percent_insiders + percent_institutions`.

#### Yield Curve — `int_yield_curve`

Builds yield spread signals from treasury yields and the Fed Funds Rate:

| Signal | Formula | Interpretation |
|---|---|---|
| `spread_2s10s` | `yield_10y - yield_2y` | Negative = inverted curve = recession warning |
| `spread_3m10y` | `yield_10y - yield_3m` | Fed's own preferred recession indicator |
| `is_2s10s_inverted` | `CASE WHEN spread_2s10s < 0` | Boolean flag for easy filtering |
| `term_premium` | `yield_10y - fed_funds_rate` | Compensation for holding duration risk |
| `yield_10y_1d_chg_bps` | `(yield_10y - LAG(yield_10y)) * 100` | Rate shock signal; large moves predict equity volatility |
| `fed_regime` | 90-day Fed Funds trend | `'HIKING'`, `'CUTTING'`, or `'NEUTRAL'` |

`int_yield_curve` is a shared dependency: it feeds `mart_fixed_income`, `mart_macro_health`, and (indirectly via `int_macro_signals`) `mart_ml_features`. Keep it as a table, not a view.

#### Macro Signals — `int_macro_signals`

Pivots the FRED series from long format (one row per series per date) to wide format (one row per date with columns for VIX, WTI, Brent, inflation breakeven, and HY spread).

#### Economic Signals — `int_economic_signals`

Joins the Snowflake Marketplace timeseries to the industry mapping bridge table, then aggregates to one row per (`indicator_industry`, `date`) with averages for leading, coincident, and lagging indicators. This is joined to the stock price spine via each ticker's GICS industry.

#### News Sentiment — `int_news_sentiment`

Combines GDELT geopolitical signals with AlphaVantage topic-level sentiment into one row per trading date. Columns are prefixed (`gdelt_*`, `av_*`) to make provenance clear.

#### Trading Calendar — `int_trading_calendar`

A reference table of valid NYSE trading days from 2020 to present, built by excluding weekends and US market holidays. Used to validate that price data doesn't contain weekend dates, to calculate trading-day–accurate forward windows, and as the denominator in `int_ticker_coverage` when computing `coverage_pct`.

---

### Marts Layer

**Materialization: Snowflake-managed Iceberg tables (most marts)**

All analytics marts use `table_format='iceberg'` — a cloud-native open table format managed by Snowflake's catalog. This is required for the project's data governance standards and ensures the output can be read by any Iceberg-compatible engine (Spark, DuckDB, etc.) without vendor lock-in.

Operational marts (`mart_data_gaps`, `mart_delisted_tickers`) use standard `table` materialization — they don't need Iceberg because they're internal tooling, not analytics outputs.

Critical marts use `on_schema_change='fail'`. If a dbt run would add, remove, or rename a column, it will fail immediately rather than silently altering the table. This is intentional: ML pipelines reading these tables by column name will break silently at runtime if a schema change slips through — failing loudly at the dbt layer is far easier to debug.

---

#### `mart_ml_features`

The primary output. Grain: one row per (`ticker`, `date`). **95 columns** organized into seven feature groups plus five targets.

Clustered on `[ticker, date]` — Snowflake uses micro-partition pruning to skip entire blocks of data that don't match query filters. Queries like `WHERE ticker = 'AAPL' AND date >= '2024-01-01'` scan a tiny fraction of the table.

| Feature group | Columns | Source |
|---|---|---|
| Identity | ticker, date, company_name, sector, industry, country | `company_overview_nonfinancial` |
| Price features | 18 columns: returns, MAs, Bollinger, RSI, volatility, volume ratio | `int_stock_price_features` |
| Fundamental features | 15 columns: P/E ratios, market cap, revenue, EBITDA, beta | `company_overview_valuation` |
| Analyst features | 7 columns: target price, net bullish score, insider/institution ownership | `int_analyst_signals` |
| Economic features | 5 columns: industry-level leading/coincident/lagging indicator averages | `int_economic_signals` |
| Macro / risk | 13 columns: VIX, oil, HY spread, breakeven inflation, yield curve, rate changes | `int_macro_signals` + `int_yield_curve` |
| News sentiment | 19 columns: GDELT bilateral/regional tones, AlphaVantage topic sentiments | `int_news_sentiment` |
| Targets | 5 forward returns (1m/3m/6m/9m/12m) + 5 availability flags + `dataset_split` | `int_forward_returns` |

**Only `COMPLETE` tickers are included.** The mart filters via `int_ticker_coverage` — delisted and incomplete tickers are excluded before this table is built.

**Using the table:**

```sql
-- Training data (targets exist; use to fit the model)
SELECT * FROM JDS_MARTS.JDS_MART_MART_ML_FEATURES
WHERE dataset_split = 'training';

-- Inference data (most recent ~63 trading days; no target yet — run the model here)
SELECT * FROM JDS_MARTS.JDS_MART_MART_ML_FEATURES
WHERE dataset_split = 'inference';

-- Train only on rows where the 6-month target is also available
SELECT * FROM JDS_MARTS.JDS_MART_MART_ML_FEATURES
WHERE has_6m_target = TRUE;
```

> **Important preprocessing note:** Economic signal columns (`avg_leading_value`, etc.) are monthly — most trading-day rows will be `NULL` on non-reporting dates. Forward-fill these columns in your Python preprocessing pipeline (e.g., `df.ffill()`) before training. Do not drop rows that have nulls in these columns.

---

#### `mart_fixed_income`

Grain: one row per **(NYSE trading date, security)**. A long-format fixed income table where each instrument (e.g., "US Treasury 10Y", "Federal Funds Rate", "CPI") gets its own row per date.

> **Why long format instead of one wide row per date?** When the mart only contained yield spreads, a wide row per date made sense. Once we added CPI and Fed Funds Rate, the semantics diverged — a `rate_value` for a 3-month Treasury means something completely different from a `rate_value` for CPI. Long format with a `security_name` column and `instrument_type` grouping makes filtering natural: `WHERE instrument_type = 'TREASURY_YIELD'` gives you the entire yield curve; `WHERE security_name = 'US Treasury 10Y'` gives you just that maturity's history.

Key columns:
- `security_name` — "US Treasury 3M", "US Treasury 10Y", "Federal Funds Rate", "CPI", etc.
- `instrument_type` — `'TREASURY_YIELD'`, `'FEDERAL_FUNDS_RATE'`, `'CPI'`
- `rate_value` — the yield / rate / index value for that security on that date
- Spread signals (`spread_2s10s`, `spread_3m10y`, `spread_2s30s`) — repeated on every row for the date so any single-security time series retains full market context without a join
- `fed_regime` — `'HIKING'`, `'CUTTING'`, or `'NEUTRAL'` based on 90-day Fed Funds trend
- `inversion_days_trailing_1y` — how many of the past 252 sessions had an inverted 2s10s spread
- `fi_attractive_flag` — `TRUE` when 10-year yield ≥ 4%, the point at which fixed income meaningfully competes with equities for a retirement portfolio
- `approx_real_yield_10y` — nominal 10Y minus CPI YoY; a quick read on whether bond investors are earning a real return

---

#### `mart_macro_health`

Grain: one row per NYSE trading date. A daily macro policy-inflation regime table that translates raw Fed and CPI data into plain-English signals.

> **Why a separate macro health mart instead of embedding this in `mart_ml_features`?** The audience for macro health analysis is broader than just ML training. A family member reviewing their retirement portfolio can ask "what's the economic regime right now?" and get a single-row answer from `mart_macro_health` without needing to understand a 95-column ML feature matrix. Separation of concerns — the ML mart is for models, this mart is for humans.

Key columns:
- `fed_funds_rate`, `cpi_yoy_pct`, `cpi_mom_pct` — the raw inputs
- `real_fed_funds_rate` — Fed Funds Rate minus CPI YoY; positive means restrictive policy, negative means the Fed is "behind the curve" (subsidizing borrowing while prices rise)
- `inflation_vs_target` — how far CPI is above or below the Fed's 2% mandate
- `fed_regime` — direction of policy change over the past 90 days
- `inflation_regime` — `'DEFLATIONARY'`, `'LOW'`, `'TARGET'`, `'ELEVATED'`, `'HIGH'`
- `policy_stance` — `'RESTRICTIVE'`, `'NEUTRAL'`, or `'ACCOMMODATIVE'` using a ±0.5pp real-rate buffer to avoid excessive regime flipping
- `fed_behind_curve` — `TRUE` when the real rate is negative AND inflation is above 2%
- `economic_health_label` — plain-English label combining inflation and policy: `'STABLE'`, `'OVERHEATING — Fed hiking'`, `'NORMALIZING — restrictive policy working'`, `'STAGFLATION RISK'`, `'DEFLATION RISK — Fed cutting'`, `'TRANSITIONING'`

---

#### `mart_ml_predictions`

Grain: one row per (`ticker`, `date`, `horizon`, `model_version`). An incremental Iceberg table that holds every model prediction alongside the actual forward return once it materializes.

> **Why incremental?** Each nightly training run produces predictions for hundreds of tickers across 4 horizons. We never need to rebuild historical predictions — they're fixed once written. Incremental materialization inserts only model versions not yet present in the table, so nightly runs add a few thousand rows instead of rebuilding tens of millions.

Key columns:
- `predicted_return` — the model's predicted forward return for this ticker, date, and horizon
- `actual_return` — the realized forward return from `mart_ml_features`; `NULL` on inference rows until the window closes
- `residual` — `actual_return - predicted_return`; your primary error signal
- `abs_error`, `squared_error` — row-level inputs to MAE and RMSE aggregates in `mart_model_evaluation`
- `directional_correct` — `TRUE` if `sign(predicted_return) = sign(actual_return)`; whether the model got the direction right even if the magnitude was off

> **Why track actuals here instead of joining on the fly?** As time passes, `mart_ml_features` gains new forward return values for dates that were previously in the inference window. Rather than re-joining every time you want to evaluate model performance, `mart_ml_predictions` captures the predicted/actual pair in one place. This makes `mart_model_evaluation` a fast aggregate over a pre-joined table rather than a slow cross-temporal join.

---

#### `mart_model_evaluation`

Grain: one row per (`horizon`, `model_version`, `sector`), plus an `'ALL'` sector aggregate row per horizon + model version. The definitive performance report for every model that has run.

> **Why IC (Information Coefficient) instead of just RMSE?** RMSE tells you how big your errors are in absolute terms. IC tells you whether your model's *rankings* are correct — whether the stocks it predicts will do best actually do best relative to their peers. In equity investing, getting the ranking right is what matters: you overweight the high-predicted stocks and underweight the low-predicted ones. A model with IC of 0.05+ is adding value. IC above 0.10 is considered strong in the industry.

Key metrics:
- `mae` — Mean Absolute Error; how far off predictions are on average
- `rmse` — Root Mean Squared Error; penalizes large errors more heavily than MAE
- `directional_accuracy` — fraction of predictions where sign(predicted) = sign(actual)
- `ic_overall` — Pearson correlation between predicted and actual returns across all dates
- `mean_daily_ic` — average cross-sectional IC per date; the standard hedge fund signal quality metric
- `stddev_daily_ic` — volatility of the daily IC; a consistent IC is more valuable than a volatile one
- `mean_daily_rank_ic` — Spearman rank IC; less sensitive to outliers than Pearson IC
- `ic_ir` — IC Information Ratio (`mean_daily_ic / stddev_daily_ic`); measures signal consistency; the higher the better

The `by_sector` rows let you see whether the model works better in some industries than others — useful for tilting predictions or weighting them differently by sector.

---

#### `mart_data_gaps`

An operational table (not an analytics table) that identifies missing trading days per ticker. Run it before the backfill script:

```bash
dbt run --select mart_data_gaps --profiles-dir .
python3 Python_Scripts/backfill_missing_dates.py
```

---

#### `mart_delisted_tickers`

Grain: one row per delisted ticker (i.e., every ticker where `int_ticker_coverage.coverage_status = 'DELISTED'`). Provides context on why each stock stopped trading.

> **Why bother with a delisted tickers mart?** Three reasons. First, survivorship bias: if you train your model only on currently-listed stocks, you're implicitly excluding stocks that went bankrupt or were acquired. Understanding which stocks exited the universe and why helps you assess whether that bias is material. Second, corporate actions: acquisitions often generate large positive returns right before delisting — if the model saw those returns in training data without knowing they came from an M&A event, it might learn the wrong signal. Third, it's good operational hygiene: when a ticker disappears from `mart_ml_features`, you want to know immediately whether it's a data load failure or an actual delisting.

Key columns:
- `inferred_reason` — `'ACQUISITION_OR_MERGER'`, `'BANKRUPTCY_OR_COLLAPSE'`, `'DISTRESSED_EXIT'`, `'DELISTED_NO_DATA'`, or `'UNKNOWN'`; inferred from price behavior at the last trade date relative to historical peak
- `price_pct_of_peak_at_exit` — last adjusted close divided by all-time peak; high = likely acquisition, low = likely bankruptcy
- `price_30d_return_at_exit` — 30-trading-day return into the last trade date; steep decline = distressed
- `calendar_days_since_last_trade` — how long since the ticker went dark

The exit reason logic:
- ≥ 85% of peak price at exit → `ACQUISITION_OR_MERGER` (stable / elevated prices suggest a takeout)
- < 40% of peak → `BANKRUPTCY_OR_COLLAPSE` (severe deterioration)
- 40–85% of peak AND 30d return < -15% → `DISTRESSED_EXIT`
- No `RAW_COMPANY_OVERVIEW` record → `DELISTED_NO_DATA` (AlphaVantage returns empty for confirmed delists)

---

## ML Training Pipeline

The pipeline doesn't just produce features — it trains models, writes predictions back to Snowflake, and tracks performance automatically.

### How the loop works

```
mart_ml_features (training split)
         │
         ▼
   train_ml_model.py              ← trains 4 XGBoost models (3m / 6m / 9m / 12m)
         │
         ▼
  RAW_ML_PREDICTIONS              ← raw predictions written to Snowflake
         │
         ▼
   stg_ml_predictions             ← deduplicates by (ticker, date, horizon, model_version)
         │
         ▼
  mart_ml_predictions             ← joins actuals as they materialize; row-level metrics
         │
         ▼
  mart_model_evaluation           ← IC, ICIR, MAE, RMSE, directional accuracy
```

### `train_ml_model.py`

Trains one XGBoost regressor per forward-return horizon (3m, 6m, 9m, 12m) and writes predictions for all rows in `mart_ml_features` — both `training` and `inference` splits — to `PUBLIC.RAW_ML_PREDICTIONS`.

```bash
cd DBTSTOCKPROJECT_SNOWFLAKE
set -a && source DBTSTOCKPROJECT/.env && set +a
python3 Python_Scripts/train_ml_model.py
```

**Key design decisions:**
- **Four separate models, not one:** Each horizon has different predictive dynamics. What predicts 3-month returns well (momentum, RSI) may not predict 12-month returns well (valuation, macro). Training separately lets each model specialize.
- **Excluded columns:** Raw price levels (`close`, `adjusted_close`, `volume`), raw share counts, and all forward-return targets and their availability flags are excluded from features. Normalised ratios (e.g., `price_to_ma_20d`) are kept. This prevents the model from memorizing price scales instead of learning structural relationships.
- **SimpleImputer before XGBoost:** Monthly economic indicators have `NULL` on non-reporting trading days. Rather than dropping those rows, the pipeline median-imputes them. This matches what you'd do in production inference.
- **model_version** is a timestamp string (e.g., `"2026-05-30T01:00:00Z"`). Each nightly run writes a new model version, preserving the history of every model that has ever run.

After running the script, rebuild the prediction marts:

```bash
cd DBTSTOCKPROJECT
dbt run --select mart_ml_predictions mart_model_evaluation --profiles-dir .
```

### `backfill_predictions.py`

A walk-forward backtesting script that simulates how the model would have performed historically without any look-ahead bias.

```bash
cd DBTSTOCKPROJECT_SNOWFLAKE
set -a && source DBTSTOCKPROJECT/.env && set +a
python3 Python_Scripts/backfill_predictions.py
```

**What walk-forward means:** For each historical cutoff date (spaced quarterly), the script trains a model using only data available *at* that cutoff — no rows from the future are included in training. It then predicts returns for the ~65 trading days immediately preceding the cutoff. Since those dates are all in the past, their actual forward returns are already in `mart_ml_features`. This gives you a clean out-of-sample backtest without having to wait months for real predictions to mature.

Each backtest prediction uses `model_version = "backtest_YYYY-MM-DD"` to distinguish it from live model versions.

After running the backfill, populate the evaluation marts to see real out-of-sample IC and RMSE:

```bash
cd DBTSTOCKPROJECT
dbt run --select mart_ml_predictions mart_model_evaluation --profiles-dir .
```

---

## The ML Feature Mart

### Training vs. Inference Split

The `dataset_split` column is computed entirely from whether the forward return target exists:

```sql
CASE
    WHEN fr.has_3m_target THEN 'training'
    ELSE 'inference'
END AS dataset_split
```

- **Training rows** have a complete 3-month forward window, meaning the future price is already in the database. Use these to fit your model.
- **Inference rows** are the most recent ~63 trading days. The future hasn't happened yet, so these targets are `NULL`. Use these to make predictions.

This split is rebuilt from scratch on every `dbt run`. If you add new price data today, today's row becomes an inference row; yesterday's row eventually graduates to training as time passes.

### Avoiding Lookahead Bias

Every feature in `mart_ml_features` is computed from data that existed on or before `date`. The window functions use `LAG()` (backward-looking) for features and `LEAD()` (forward-looking) only for the target columns. This is enforced by design at the intermediate layer — `int_stock_price_features` and `int_forward_returns` are separate models with a clear directional contract.

---

## Data Quality Tests

The project uses 291+ data tests across all layers, enforced on every `dbt test` run. Beyond the standard dbt tests (`not_null`, `unique`, `accepted_values`), eight custom macros address stock-specific data quality concerns that generic tests cannot catch.

### Custom Test Macros

| Macro | What it checks | Example use |
|---|---|---|
| `test_min_row_count` | Model has at least N rows (default: 400) | Catches silently empty mart builds |
| `test_recency` | At least 1 row within the last N days/weeks | Detects stale data before it reaches production |
| `test_not_empty_string` | A string column is not just whitespace | Catches tickers loaded as `''` or `'   '` |
| `test_no_future_date` | All dates are ≤ today | Catches API responses with erroneous future-dated rows |
| `test_no_weekend_date` | No Saturday or Sunday dates | Stock prices on weekends = data corruption |
| `test_column_gte_column` | Column A ≥ Column B (nulls allowed) | Catches high < low in OHLC data |
| `test_ohlc_integrity` | high ≥ low, open, close; low ≤ open, close | Enforces all four OHLC relationships together |
| `test_positive_values` | Column > 0 and not null | Catches negative prices or volumes |

**Why custom tests instead of just `not_null`?** The `not_null` test confirms a value exists. It does not tell you whether a stock's daily high was lower than its low — which is physically impossible but can happen when raw data is corrupted. These domain-specific tests catch issues that generic tests miss.

### Running Tests

```bash
# All tests
dbt test --profiles-dir .

# Only tests for a specific model
dbt test --select mart_ml_features --profiles-dir .

# Only tests for the staging layer
dbt test --select staging --profiles-dir .
```

---

## Automation & Nightly Pipeline

### GitHub Actions: `.github/workflows/nightly_dbt_refresh.yml`

The pipeline runs automatically each weekday at **8 PM ET** (1 AM UTC) via GitHub Actions. It can also be triggered manually from the GitHub UI (`workflow_dispatch`) or by a Snowflake Task via `repository_dispatch`.

The nightly sequence:
1. **Fetch company fundamentals** (`fetch_av_company_overview.py`) — refreshes P/E, analyst ratings, sector/industry metadata
2. **Fetch fixed income** (`fetch_alphavantage_fixed_income.py`) — Treasury yields, Fed Funds Rate, CPI
3. **Fetch FRED macro** (`fetch_fred_macro_data.py`) — VIX, oil, HY spread, breakeven inflation
4. **Fetch GDELT events** (`fetch_gdelt_events.py`) — yesterday's geopolitical sentiment
5. **Fetch news sentiment** (`fetch_av_news_sentiment.py`) — AlphaVantage topic sentiment
6. **Backfill missing stock prices + dbt run** (`backfill_missing_dates.py`) — detects gaps in `mart_data_gaps`, fetches missing dates from AlphaVantage, uploads to S3, triggers `COPY INTO`, then runs `dbt run`
7. **dbt test** — validates all data quality tests; failure here blocks the pipeline and sends an alert

> **Why is the dbt run triggered from inside `backfill_missing_dates.py` rather than as a separate CI step?** The backfill script needs to know whether the COPY INTO finished successfully before dbt runs — otherwise dbt might build the marts before the new price data has landed. Running dbt as a subprocess at the end of the backfill script gives it that guarantee. The CI step then runs `dbt test` as a final gate.

**Authentication:** The nightly CI job uses key-pair authentication for dbt (a private key stored as a GitHub Actions secret). This avoids storing the Snowflake password in CI while keeping password-based auth available locally for development.

### Snowflake Task (Fallback)

A Snowflake Task defined in `analyses/snowflake_task_setup.sql` fires a `repository_dispatch` event to GitHub Actions, which then runs the same pipeline. This is the primary trigger; the GitHub Actions cron schedule is the fallback if the Snowflake Task is unavailable.

---

## dbt Patterns & Best Practices

### Why `ref()` instead of table names

Every model reference uses `{{ ref('model_name') }}` rather than a hard-coded Snowflake path like `JDS_INTERMEDIATE.JDS_INT_INT_STOCK_PRICE_FEATURES`. This does three things:

1. **Builds the DAG** — dbt knows which models depend on which, so it can run them in the right order and parallelize where safe
2. **Handles schema routing** — the `generate_schema_name.sql` macro applies the `JDS_STG_` / `JDS_INT_` / `JDS_MART_` prefix automatically; you never type the schema manually
3. **Enables environment targeting** — if you add a `prod` target to `profiles.yml`, `ref()` automatically points to the production schema without changing any model code

### Why `env_var()` in `profiles.yml`

```yaml
password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
```

Credentials are never written into a file that could be committed to git. The `.env` file is listed in `.gitignore`. To run dbt, you load the env vars first:

```bash
set -a && source .env && set +a
```

### Schema Aliasing

The `macros/generate_schema_name.sql` macro intercepts dbt's default schema naming and applies project-specific prefixes:

| Layer | Schema pattern | Example |
|---|---|---|
| Staging | `JDS_STG_{model}` | `JDS_STG_STG_STOCKPRICE` |
| Intermediate | `JDS_INT_{model}` | `JDS_INT_INT_STOCK_PRICE_FEATURES` |
| Marts | `JDS_MART_{model}` | `JDS_MART_MART_ML_FEATURES` |

This keeps all objects clearly organized in Snowflake and avoids collisions with other projects in the same database.

### `on_schema_change = 'fail'` on Marts

```sql
{{ config(on_schema_change='fail') }}
```

If a dbt run tries to add or remove a column from a mart, it will fail loudly before touching the table. This is the correct default for ML pipelines — a Python training script that references `forward_return_3m` by column name will break silently at runtime if a schema change removes or renames that column. Failing at the dbt layer is far easier to debug.

### Iceberg Table Format

```sql
{{ config(table_format='iceberg', storage_serialization_policy='COMPATIBLE') }}
```

Iceberg is the open table format standard for cloud data lakes. Using Snowflake-managed Iceberg means:
- The table is stored in an open format (not Snowflake-proprietary) — it can be read by Spark, DuckDB, or any Iceberg-compatible engine in the future
- Snowflake's catalog manages file organization and metadata
- `COMPATIBLE` serialization ensures cross-engine compatibility

---

## Daily Operations

### Standard daily run (local development)

```bash
cd DBTSTOCKPROJECT
set -a && source .env && set +a
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### ML model training (run after the dbt pipeline)

```bash
# Train XGBoost models and write predictions to Snowflake
python3 Python_Scripts/train_ml_model.py

# Rebuild prediction tracking and evaluation marts
cd DBTSTOCKPROJECT
dbt run --select mart_ml_predictions mart_model_evaluation --profiles-dir .
```

### Selective runs (faster for development)

```bash
# Rebuild only the ML mart and its upstream dependencies
dbt run --select +mart_ml_features --profiles-dir .

# Rebuild a single intermediate model
dbt run --select int_stock_price_features --profiles-dir .

# Rebuild everything downstream of a model that changed
dbt run --select int_stock_price_features+ --profiles-dir .
```

> **Selector syntax:** `+model` = model + all its upstream dependencies. `model+` = model + all downstream dependents. `+model+` = both directions.

### Ingestion script schedule

Run these scripts before `dbt run` so the raw tables are current:

| Script | Cadence | Approximate runtime |
|---|---|---|
| `fetch_fred_macro_data.py` | Daily (after 5 PM ET) | < 30 seconds |
| `fetch_alphavantage_fixed_income.py` | Daily (after 5 PM ET) | ~30 seconds |
| `fetch_gdelt_events.py` | Daily (yesterday's data available at 6 AM) | ~2 seconds per day |
| `fetch_av_news_sentiment.py` | Daily (after 4 PM market close) | ~1–2 minutes |
| `fetch_av_company_overview.py` | Monthly (or when fundamentals need refreshing) | ~15–30 minutes for all tickers |
| `backfill_missing_dates.py` | Daily (after 4 PM market close); runs dbt internally | Varies by gap count |
| `train_ml_model.py` | Weekly or after significant data changes | ~10–20 minutes |

### Debugging a failed model

```bash
# See the compiled SQL that actually ran in Snowflake
cat DBTSTOCKPROJECT/target/run/DBTSTOCKPROJECT/models/marts/mart_ml_features.sql

# Rerun a single model with verbose output
dbt run --select mart_ml_features --profiles-dir . --log-level debug
```

---

## Project Reference

| Setting | Value |
|---|---|
| dbt project name | `DBTSTOCKPROJECT` |
| Profile name | `dbt_stockproject` |
| Snowflake account | `TPRFGUJ-JNC76647` |
| Database | `DBT_STOCKPROJECT` |
| Warehouse | `DBT_STOCKPROJECT` |
| Role | `DBT_ROLE` |
| dbt Core version | 1.11.0 |
| dbt-snowflake adapter | 1.10.3 |
| Python | 3.11 |
| Packages | `dbt_utils >= 1.0.0`, `dbt_semantic_view 1.0.3` |
