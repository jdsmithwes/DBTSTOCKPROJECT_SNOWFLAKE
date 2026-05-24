# CLAUDE.md — DBT Stock Market Project

## Purpose & Persona

**Role:** Portfolio manager at a top NYC quantitative hedge fund.
**Audience:** Family members picking S&P 500 stocks for retirement portfolios.
**Goal:** Determine how well an ML model can predict S&P 500 stock performance at four horizons:

| Horizon | Target column |
|---|---|
| 1 month  | `forward_return_1m`  |
| 3 months | `forward_return_3m`  |
| 6 months | `forward_return_6m`  |
| 9 months | `forward_return_9m`  |
| 12 months | `forward_return_12m` |

**Design principle:** Institutional-grade feature engineering, family-friendly interpretability.
Prioritize explainable, moderately accurate models over black-box complexity — end users are
retirement savers, not quant researchers.

---

Quantitative data pipeline for stock performance prediction. Feeds a nightly ML feature mart
from AlphaVantage market data and Snowflake Marketplace economic indicators.

---

## Stack

| Tool | Version |
|---|---|
| dbt Core | 1.11.0-rc2 |
| dbt-snowflake adapter | 1.10.3 |
| Python | 3.11.9 |
| Platform | macOS (arm64) |
| Packages | dbt_utils >=1.0.0, dbt_semantic_view 1.0.3 |

---

## Repo Layout

```
DBTSTOCKPROJECT_SNOWFLAKE/        ← git root (branch: dev → main)
├── CLAUDE.md                     ← this file
├── DBTSTOCKPROJECT/              ← dbt project root (run all dbt commands from here)
│   ├── dbt_project.yml
│   ├── profiles.yml              ← reads from .env via env_var()
│   ├── .env                      ← gitignored; real Snowflake creds
│   ├── .env.example              ← committed credential template
│   ├── models/
│   │   ├── staging/              ← ephemeral; sources: AlphaVantage + Snowflake Marketplace
│   │   ├── intermediate/         ← view (default) or table (model-level override)
│   │   └── marts/                ← Snowflake-managed Iceberg tables
│   ├── macros/                   ← schema aliasing + 8 custom test macros
│   ├── seeds/                    ← seed_industry_mapping.csv, us_market_holidays.csv
│   ├── analyses/                 ← cost_monitoring.sql (run in Snowflake worksheet)
│   └── tests/                    ← empty; all tests live in schema YAML or macros/
├── Python_Scripts/
│   └── backfill_missing_dates.py ← reads mart_data_gaps → fetches API → uploads to S3
└── .claude/
    └── settings.local.json       ← git add/commit permissions
```

---

## Snowflake Connection

| Param | Value |
|---|---|
| Account | `TPRFGUJ-JNC76647` |
| User | `jdsmithwes` |
| Role | `DBT_ROLE` |
| Warehouse | `DBT_STOCKPROJECT` |
| Database | `DBT_STOCKPROJECT` |
| Profile name | `dbt_stockproject` |

**⚠ Known blocker:** Snowflake network policy blocks IP `73.207.5.79`.
Fix at home via Snowflake console (ACCOUNTADMIN):
```sql
ALTER NETWORK POLICY <policy_name> ADD ALLOWED_IP_LIST = ('73.207.5.79');
-- or exempt the user:
ALTER USER jdsmithwes SET NETWORK_POLICY = NULL;
```

---

## Running dbt

All dbt commands must be run from `DBTSTOCKPROJECT/` with env vars loaded:

```bash
cd DBTSTOCKPROJECT
set -a && source .env && set +a

dbt debug --profiles-dir .           # test connection
dbt run --profiles-dir .             # run all models
dbt test --profiles-dir .            # run all tests
dbt run --select mart_ml_features --profiles-dir .
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
```

---

## Model Layers

### Staging — `ephemeral`
No storage cost. Cleaned + type-cast source data only. No business logic.

| Model | Source | Grain |
|---|---|---|
| `stg_stockprice` | `ALPHAVANTAGE_API.RAW_STOCK_DATA` | ticker, date |
| `stg_companyoverview` | `ALPHAVANTAGE_API.RAW_COMPANY_OVERVIEW` | ticker |
| `stg_indicator_metadata` | `ECONOMIC_INDICATORS.INDUSTRY_LEADING_INDICATORS_METADATA` | indicatorid |
| `stg_indicator_timeseries` | `ECONOMIC_INDICATORS.INDUSTRY_LEADING_INDICATORS_TIMESERIES` | indicatorid, date |

### Intermediate — `view` (or `table` via model-level config override)
Feature engineering and aggregations. No raw source reads.

| Model | Purpose |
|---|---|
| `int_stock_price_features` | Technical indicators: MA-20/50/200, RSI-14, Bollinger %B, volatility, returns |
| `int_forward_returns` | Lead-based 1m/3m forward return targets |
| `int_analyst_signals` | Consensus ratings, net bullish score, upside |
| `int_economic_signals` | Industry-level economic indicator aggregates |
| `int_trading_calendar` | NYSE trading days 2020–present (excludes weekends + holidays) |
| `int_industry_mapping` | company_industry → indicator_industry bridge |
| `company_overview_analystrating` | Analyst rating counts per ticker |
| `company_overview_nonfinancial` | Sector, industry, country per ticker |
| `company_overview_valuation` | PE, PB, EV/EBITDA, beta, etc. per ticker |
| `stock_performance_with_indicators` | Wide join: price + company + economic (fan-out on indicator) |
| `industry_indicators_descriptions` | Metadata + timeseries joined on indicatorid |

> **Naming note:** New intermediate models should use `int_` prefix. The `company_overview_*`,
> `stock_performance_with_indicators`, and `industry_indicators_descriptions` models predate
> this convention — rename only when able to run `dbt compile` to verify refs.

### Marts — Snowflake-managed Iceberg table
| Model | Purpose | Cluster key |
|---|---|---|
| `mart_ml_features` | ML training/inference matrix (59 cols, grain: ticker+date) | `[ticker, date]` |
| `mart_data_gaps` | Missing trading days per ticker (input to backfill script) | — |

Both marts: `on_schema_change='fail'`, `storage_serialization_policy='COMPATIBLE'`.

**ML feature mart usage:**
```sql
-- Training data
SELECT * FROM JDS_MARTS.JDS_MART_MART_ML_FEATURES WHERE dataset_split = 'training';
-- Inference (most recent ~63 trading days, no target yet)
SELECT * FROM JDS_MARTS.JDS_MART_MART_ML_FEATURES WHERE dataset_split = 'inference';
```

---

## Schema Aliases

Controlled by `macros/generate_schema_name.sql`:

| Layer | Alias pattern | Example |
|---|---|---|
| staging | `JDS_STG_{model}` | `JDS_STG_STG_STOCKPRICE` |
| intermediate | `JDS_INT_{model}` | `JDS_INT_INT_STOCK_PRICE_FEATURES` |
| marts | `JDS_MART_{model}` | `JDS_MART_MART_ML_FEATURES` |

---

## Custom Test Macros (`macros/`)

| Macro | What it checks |
|---|---|
| `test_min_row_count` | Row count >= min_rows (default 400) |
| `test_recency` | At least 1 row within last N dateparts |
| `test_not_empty_string` | trimmed string != '' |
| `test_no_future_date` | date <= current_date() |
| `test_no_weekend_date` | DAYOFWEEK not Saturday/Sunday |
| `test_column_gte_column` | column_a >= column_b (nulls ignored) |
| `test_ohlc_integrity` | high >= low/open/close, low <= open/close |
| `test_positive_values` | column > 0 and not null |

---

## Data Sources

| Source | Type | Refresh | Notes |
|---|---|---|---|
| AlphaVantage API | Daily OHLCV + fundamentals | Daily / monthly | 5 req/min free tier |
| AlphaVantage API (fixed income) | Treasury yields (6 maturities), Fed Funds Rate, CPI | Daily / monthly | Same key; `fetch_alphavantage_fixed_income.py` |
| FRED API (St. Louis Fed) | VIX, WTI/Brent oil, 10yr inflation breakeven, HY spread | Daily | Free; key in `.env` as `FRED_API_KEY` |
| Snowflake Marketplace — Industry Economic Indicators | Leading/lagging/coincident indicators | Monthly | Pre-loaded, no API call |

---

## Claude Code Behavior Rules

### 1. Think Before Coding
- Never make assumptions about undocumented APIs or configurations.
- Ask clarifying questions if a task's requirements are ambiguous.

### 2. Surgical Changes
- Modify only the minimum necessary lines of code to achieve the goal.
- Avoid refactoring adjacent or unrelated files unless explicitly asked.
- Match existing style, even if you would write it differently.

### 3. Simplicity First
- Do not write speculative helper functions or complex abstractions.
- Prioritize simple, readable code over clever or DRY patterns.

### 4. Goal-Driven Execution
- Establish clear test or verification criteria before writing any code.
- Run local tests or build steps to verify changes actually work before completion.

---

## Project Rules

- **Never hardcode credentials.** All connection params live in `.env` (gitignored).
- **Never commit `.env`.** It is blocked by `DBTSTOCKPROJECT/.gitignore`.
- **Staging = ephemeral.** No storage cost; no business logic in staging layer.
- **Marts = Iceberg.** Required for the Snowflake-managed catalog; `table_format='iceberg'` must stay.
- **on_schema_change = fail on all marts.** Prevents accidental schema drift breaking ML pipelines.
- **Economic columns are monthly cadence** — most trading-day rows will be null; forward-fill in Python before training.
- **Mac scripts:** Use `sed -i ''` (not `sed -i`). Use `gdate` for GNU date (install via `brew install coreutils`).

---

## Best Practices Reference

`DBTSTOCKPROJECT/md files/DBT_BEST_PRACTICES_HEDGE_FUND.md` — hedge fund specific dbt guide.
Covers: materialization strategy, clustering, custom tests, cost monitoring, ML data leakage prevention.
