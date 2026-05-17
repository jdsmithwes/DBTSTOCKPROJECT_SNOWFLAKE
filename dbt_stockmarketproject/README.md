# dbt Stock Market Project

A dbt + Snowflake analytics pipeline that ingests daily S&P 500 stock prices and company fundamentals from the AlphaVantage API, transforms the raw data through a layered model architecture, and produces analytics-ready marts for investment analysis, sector benchmarking, dividend screening, and multi-factor stock ranking.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Ingestion Process](#ingestion-process)
3. [Data Models](#data-models)
   - [Staging Layer](#staging-layer)
   - [Intermediate Layer](#intermediate-layer)
   - [Marts Layer](#marts-layer)
4. [DAG — Data Lineage](#dag--data-lineage)
5. [Data Quality Tests](#data-quality-tests)
6. [Project Configuration](#project-configuration)
7. [Setup & Running the Project](#setup--running-the-project)

---

## Architecture Overview

```
AlphaVantage API
      │
      ├── Stock Prices (OHLCV)
      │       ├── Python → S3 → Snowpipe → RAW.stock_price_data_raw
      │       └── Snowpark Stored Procedure → RAW_STOCK_DATA
      │
      └── Company Fundamentals (JSON)
              ├── Python → S3 → Snowpipe → RAW.company_overview_json_raw
              └── Snowpark Stored Procedure → RAW_COMPANY_OVERVIEW
                            │
                            ▼
                    dbt Transformation
              ┌─────────────────────────┐
              │  Staging  →  Intermediate  →  Marts  │
              └─────────────────────────┘
                            │
                            ▼
                  BI / Downstream Analytics
```

- **~500+ S&P 500 tickers** updated daily
- **2 raw data sources** (prices + fundamentals)
- **2 staging models**, **9 intermediate models**, **11 mart models**
- **Snowflake** as the data warehouse; dbt Core for transformation
- **AlphaVantage** as the market data provider

---

## Ingestion Process

Data enters the pipeline through Python scripts and Snowpark stored procedures. There are two ingestion paths — one for stock prices and one for company fundamentals — each available in both a local Python variant (S3 → Snowpipe) and a Snowflake-native variant (Snowpark stored procedure).

### Stock Price Data

| Script | Method | Destination |
|---|---|---|
| `Python_Scripts/StockDataApiCallScript.py` | S3 upload → Snowpipe | `RAW.stock_price_data_raw` |
| `Python_Scripts/Snowpark_API_Script.py` | Snowpark stored proc | `PUBLIC.RAW_STOCK_DATA` |

**What it does:**
- Calls the AlphaVantage `TIME_SERIES_DAILY_ADJUSTED` endpoint for each S&P 500 ticker
- Fetches OHLCV data (Open, High, Low, Close, Adjusted Close, Volume) along with dividend amount and split coefficient
- Determines the date window automatically: starts from the day after the last loaded date through today
- Skips non-trading days using NYSE calendar logic
- Rate-limited to 25 requests/min (configurable to 75 for premium API keys)
- Retries failed requests up to 3 times

**Output columns:** `date`, `open`, `high`, `low`, `close`, `adjusted_close`, `volume`, `dividend_amount`, `split_coefficient`, `ticker`

### Company Fundamentals Data

| Script | Method | Destination |
|---|---|---|
| `Python_Scripts/CompanyInfoScript.py` | S3 upload → Snowpipe | `RAW.company_overview_json_raw` |
| `Python_Scripts/Snowpark_CompanyInfo_Script.py` | Snowpark stored proc | `PUBLIC.RAW_COMPANY_OVERVIEW` |

**What it does:**
- Fetches the S&P 500 ticker list from datahub.io
- Calls the AlphaVantage `OVERVIEW` endpoint for each ticker to retrieve 50+ fundamental metrics
- Captures valuation ratios (P/E, P/B, EV/EBITDA), profitability metrics (margins, ROE, ROA), growth rates, dividend information, and technical indicators (52-week high/low, moving averages, Beta)
- The Snowpark variant uses MERGE/UPSERT keyed on ticker symbol to keep fundamentals current
- Raw JSON responses are stored as-is in Snowflake for maximum flexibility

### Scheduling

The Snowpark stored procedures can be registered as **Snowflake Tasks** running on a CRON schedule (default: 8 PM ET daily). See `Python_Scripts/register_sproc.py` for setup instructions.

---

## Data Models

The project follows a three-layer dbt architecture: **Staging → Intermediate → Marts**.

### Staging Layer

Staging models clean and type-cast raw source data. They are materialized as **views** (lightweight, no storage cost) with the exception of `stg_stockpricedata`, which uses an **incremental** strategy to process only new records.

| Model | Source | Purpose |
|---|---|---|
| `stg_stockpricedata` | `RAW.stock_price_data_raw` | Casts raw price columns to proper types (DATE, DOUBLE, NUMBER). Incremental — processes only new rows by LOAD_TIME. |
| `stg_stockoverview` | `RAW.company_overview_json_raw` | Parses 50+ fields from nested JSON into typed columns for downstream use. |

**Key staging output columns:**

`stg_stockpricedata`: `TICKER`, `TRADING_DATE`, `OPEN_PRICE`, `HIGH_PRICE`, `LOW_PRICE`, `CLOSE_PRICE`, `ADJUSTED_CLOSE_PRICE`, `VOLUME`, `DIVIDEND_AMOUNT`, `SPLIT_COEFFICIENT`, `LOAD_TIME`

`stg_stockoverview`: `TICKER`, `NAME`, `SECTOR`, `INDUSTRY`, `MARKETCAPITALIZATION`, `PERATIO`, `EPS`, `REVENUETTM`, `DIVIDENDYIELD`, `BETA`, `WEEK52HIGH`, `WEEK52LOW`, and 40+ additional fundamental fields

---

### Intermediate Layer

Intermediate models apply business logic — window functions, joins, aggregations, and factor calculations — on top of staging data. They are materialized as **tables** (for heavy computations) or **views** (for lightweight pass-throughs).

| Model | Materialization | Sources | Purpose |
|---|---|---|---|
| `int_stock_daily_price` | Table | `stg_stockpricedata` | Cleansed daily OHLCV data ready for downstream use |
| `int_stock_overview` | View | `stg_stockoverview` | Cleansed company fundamentals (selects ~40 key columns) |
| `int_fundamentals` | Incremental (MERGE) | `stg_stockoverview` | Lightweight core fundamentals (market cap, EBITDA, P/E, EPS, revenue) for factor analysis |
| `int_company_fundamentals` | Table | `int_stock_overview` + `stg_stockpricedata` | Fundamentals enriched with the most recent close price per ticker |
| `int_stock_stats` | Table | `stg_stockpricedata` | Daily return calculations using `LAG` window functions: `DAILY_RETURN = (close / prev_close) - 1` |
| `int_price_momentum` | Table | `stg_stockpricedata` | Multi-period return calculations: 1-month (~21 days), 3-month (~63 days), 6-month (~126 days) using `LAG` |
| `int_growth_factors` | View | `stg_stockoverview` | Revenue & earnings growth multipliers, EPS growth factor, and a weighted blended growth score (40% revenue + 40% earnings + 20% EPS growth) |
| `int_quality_factors` | Table | `stg_stockoverview` + `int_stock_stats` | Quality/profitability metrics: ROE, ROA, margins, gross margin, 30-day rolling volatility, Beta, and a composite quality score |
| `int_sectors` | Table | `stg_stockoverview` | Industry-level aggregates: company count, avg market cap, avg EBITDA, avg P/E, avg revenue |

---

### Marts Layer

Marts are the analytics-ready output of the pipeline. They are organized into five domains and materialized as **views** (for flexibility and instant updates) or **incremental tables** (for fact tables that grow daily).

#### Finance

| Model | Materialization | Purpose |
|---|---|---|
| `mart_company_master` | View | Central company dashboard: fundamentals joined with recent price momentum. Computes a `blended_growth_score = (0.5 × 1M return) + (0.5 × 3M return)`. Primary analytical mart. |
| `mart_factor_model` | View | Factor model dataset derived from `mart_company_master` — exposes value, momentum, and growth factors for quantitative screening |
| `mart_valuation_summary` | View | Quick valuation snapshot per company: ticker, name, sector, latest close price, market cap, P/E ratio |

#### Dimensions

| Model | Materialization | Purpose |
|---|---|---|
| `dim_company` | Table | Slowly-changing dimension for company reference data: ticker, name, sector, industry, country, exchange |

#### Facts

| Model | Materialization | Purpose |
|---|---|---|
| `fct_price_metrics` | Incremental (MERGE) | Daily return metrics (1-month and 3-month returns) per ticker. Appends only new trading dates on each run. |

#### Analytics

| Model | Materialization | Purpose |
|---|---|---|
| `mart_top_stocks` | View | Filtered view of top-performing stocks from `mart_company_master` (ranking applied in BI layer) |
| `mart_sector_summary` | View | Sector-level aggregation: company count, total/avg market cap, avg EBITDA, avg P/E, avg 1M/3M returns, avg growth score |
| `mart_price_history_enriched` | View | Full daily price history enriched with daily returns and 1M/3M/6M momentum metrics — ideal for time-series analysis |

#### Dividends

| Model | Materialization | Purpose |
|---|---|---|
| `mart_dividend_dashboard` | View | Filters to dividend-paying trading days and computes `DIVIDEND_YIELD_DAILY = dividend_amount / close_price` |

---

## DAG — Data Lineage

The diagram below shows how data flows from raw sources through each layer to the final analytics marts.

```
RAW SOURCES
├── stock_price_data_raw (Snowflake RAW schema, loaded via Snowpipe)
└── company_overview_json_raw (Snowflake RAW schema, loaded via Snowpipe)
         │                              │
         ▼                              ▼
  stg_stockpricedata            stg_stockoverview
  (Incremental View)              (View)
         │                              │
    ┌────┼────────────┐        ┌────────┼──────────────────┐
    │    │            │        │        │                  │
    ▼    ▼            ▼        ▼        ▼                  ▼
int_stock  int_stock  int_price  int_stock  int_growth  int_sectors
_daily     _stats     _momentum  _overview  _factors
_price       │            │         │
             │            │    ┌────┼────────────────┐
             ▼            ▼    ▼    ▼                 ▼
       int_quality    fct_price  int_company    int_fundamentals
       _factors       _metrics   _fundamentals  (Incremental)
                          │            │
                          └─────┬──────┘
                                ▼
                        mart_company_master
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                 ▼
       mart_factor_model  mart_top_stocks  mart_sector_summary
       mart_valuation_summary
       mart_price_history_enriched ◄── (stg_stockpricedata
                                         + int_stock_stats
                                         + int_price_momentum)
       mart_dividend_dashboard ◄── stg_stockpricedata
       dim_company ◄── stg_stockoverview
```

### Lineage Summary by Source

| Source Table | Downstream Models |
|---|---|
| `stock_price_data_raw` | stg_stockpricedata → int_stock_daily_price, int_stock_stats, int_price_momentum, int_company_fundamentals → fct_price_metrics → mart_company_master, mart_price_history_enriched, mart_dividend_dashboard |
| `company_overview_json_raw` | stg_stockoverview → int_stock_overview, int_fundamentals, int_growth_factors, int_quality_factors, int_sectors, dim_company → int_company_fundamentals → mart_valuation_summary |

---

## Data Quality Tests

The project uses `dbt_utils` to enforce data quality at every layer.

### Source Tests

| Table | Column | Test |
|---|---|---|
| `stock_price_data_raw` | `DATE` | not_null |
| `stock_price_data_raw` | `TICKER` | not_null |
| `stock_price_data_raw` | `CLOSE` | accepted_range (min: 0) |
| `stock_price_data_raw` | `VOLUME` | accepted_range (min: 0) |
| `company_overview_json_raw` | `LOAD_TIME` | not_null |

### Staging Tests

| Model | Column | Test |
|---|---|---|
| `stg_stockpricedata` | `trading_date` | not_null |
| `stg_stockpricedata` | `ticker` | not_null |
| `stg_stockpricedata` | `close_price` | accepted_range (0 – 1,000,000,000) |
| `stg_stockpricedata` | `volume` | accepted_range (min: 0) |
| `stg_stockpricedata` | *(model-level)* | recency: data within 90 days |

### Intermediate Tests

| Model | Column | Test |
|---|---|---|
| `int_price_momentum` | `ticker` | not_null |

### Mart Tests

| Model | Column | Test |
|---|---|---|
| `mart_company_master` | `ticker` | not_null, unique |
| `mart_company_master` | `sector` | not_null |
| `mart_company_master` | `blended_growth_score` | accepted_range (-100 to 100) |
| `mart_factor_model` | `ticker` | not_null |
| `mart_factor_model` | `one_month_return` | accepted_range (-1 to 1) |

Run all tests with:

```bash
dbt test
```

---

## Project Configuration

| Setting | Value |
|---|---|
| **dbt project name** | `dbt_stockmarketproject` |
| **Profile** | `dbt_stockmarketproject` |
| **Warehouse** | Snowflake |
| **Staging schema** | `STAGING` |
| **Intermediate schema** | `INTERMEDIATE` |
| **Marts schema** | `ANALYTICS` |
| **Packages** | `dbt_utils >= 1.3.0` |

### Materialization Strategy

| Layer | Default Materialization |
|---|---|
| Staging | View (incremental for `stg_stockpricedata`) |
| Intermediate | Table (view for lightweight models) |
| Marts — Dimensions | Table |
| Marts — Facts | Incremental (MERGE) |
| Marts — Analytics/Finance/Dividends | View |

---

## Setup & Running the Project

### Prerequisites

- dbt Core installed
- Snowflake account with `STOCK_DATA.RAW` schema accessible
- AlphaVantage API key
- AWS credentials (if using the S3/Snowpipe ingestion path)

### Install dependencies

```bash
dbt deps
```

### Run the full pipeline

```bash
dbt run
```

### Run a specific layer

```bash
dbt run --select staging
dbt run --select intermediate
dbt run --select marts
```

### Run tests

```bash
dbt test
```

### Trigger ingestion manually

```bash
# Load stock prices (local Python variant)
python Python_Scripts/StockDataApiCallScript.py

# Load company fundamentals (local Python variant)
python Python_Scripts/CompanyInfoScript.py
```

Or call the Snowflake stored procedures directly:

```sql
-- Fetch today's stock prices
CALL STOCK_SPROC();

-- Fetch a specific date range
CALL STOCK_SPROC('2026-01-01', '2026-03-31');
```
