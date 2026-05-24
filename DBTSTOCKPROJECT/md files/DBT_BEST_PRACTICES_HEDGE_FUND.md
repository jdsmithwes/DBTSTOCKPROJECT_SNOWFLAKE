# DBT Best Practices for Quantitative Hedge Funds

---

## Executive Summary

This guide applies DBT best practices to quantitative hedge fund environments, focusing on:
- **Data Integrity:** Ensure model accuracy for ML training
- **Cost Optimization:** Minimize Snowflake costs without sacrificing performance
- **Compliance:** Maintain audit trails and data lineage
- **Efficiency:** Model dependencies, incremental updates, testing

---

## 1. Directory Structure & Naming Conventions

### 1.1 Model Organization

**Principle:** Organize by data flow, not business domains. Each stage serves a clear purpose.

```
dbt/models/
├── staging/
│   ├── stg_raw_*.sql          # Direct source → cleaned & standardized
│   ├── _staging__sources.yml   # Source documentation
│   └── _staging__meta.yml      # Metadata for staging layer
│
├── intermediate/
│   ├── int_*_metrics.sql       # Feature engineering & aggregations
│   ├── int_*_transformations.sql
│   └── _intermediate__docs.yml # Intermediate layer docs
│
├── marts/
│   ├── ml_features.sql         # Input for ML models (PRIMARY)
│   ├── prediction_targets.sql  # Target variables for supervised learning
│   ├── backtest_results.sql    # Backtest output storage
│   ├── risk_metrics.sql        # Risk calculations
│   └── fct_daily_portfolio.sql # Portfolio snapshots
│
└── tools/
    ├── schema_tests.sql        # Custom data validation
    └── data_quality.sql        # Quality metrics & monitoring
```

### 1.2 Naming Conventions

**Staging Tables** → `stg_<source>_<entity>`
```sql
stg_raw_pricing_data           -- From Alpha Vantage
stg_raw_fundamentals_annual    -- From SEC EDGAR
stg_raw_earnings_surprises     -- From market data provider
```

**Intermediate Tables** → `int_<purpose>_<grain>`
```sql
int_technical_indicators_daily  -- Daily tech indicators
int_returns_lagged_features     -- Lagged return features
int_earnings_season_flags       -- Binary flags for earnings
```

**Mart Tables** → `fct_<measure>` or `dim_<dimension>`
```sql
fct_ml_feature_set_daily       -- Daily ML feature matrix
fct_prediction_targets_monthly  -- Monthly prediction targets
dim_security_universe           -- Reference: active securities
```

**Tests** → `test_<layer>_<rule>`
```sql
test_staging_pricing_not_null
test_marts_ml_features_valid_ranges
test_intermediate_no_duplicate_dates
```

---

## 2. Staging Layer: Raw Data → Cleaned

### 2.1 Staging Model Template

```sql
/*
    stg_raw_<source>_<entity>
    
    Purpose: Transform raw ingested data into standardized format
    
    Key transformations:
    1. Type casting (string → date, numeric)
    2. Column renaming (source naming → standard naming)
    3. Handling nulls (explicit null or derived)
    4. Removing obvious duplicates
    5. Basic validation filters
    
    NO business logic here. That belongs in intermediate layer.
    
    Source: {{ source('raw', '<table_name>') }}
    Grain: <one row per...>
    Updated: <frequency>
*/

-- CTE 1: Extract with type standardization
with source_data as (
    select
        -- Identify columns
        cast(symbol_code as varchar) as symbol,
        cast(date_field as date) as trading_date,
        
        -- Numeric conversions
        try_cast(price_close as decimal(10, 4)) as close_price,
        try_cast(volume_traded as bigint) as trading_volume,
        
        -- Audit columns
        cast(ingestion_timestamp as timestamp) as _ingested_at,
        cast(current_row_timestamp as timestamp) as _source_updated_at
        
    from {{ source('raw', '<table_name>') }}
)

-- CTE 2: Data quality filters
, validate_data as (
    select
        *
    from source_data
    where 
        -- Remove null key components
        symbol is not null
        and trading_date is not null
        
        -- Remove obviously invalid values
        and close_price > 0
        and trading_volume >= 0
        
        -- Remove data outside expected range
        and trading_date >= '2015-01-01'
        and trading_date <= current_date
)

-- CTE 3: Deduplicate (keep most recent)
, deduplicated as (
    select
        *
    from validate_data
    -- Keep only one row per (symbol, trading_date) combination
    qualify row_number() over (
        partition by symbol, trading_date
        order by _ingested_at desc
    ) = 1
)

-- Final select: Add metadata
select
    *,
    current_timestamp() as dbt_loaded_at
from deduplicated
EOF
```

### 2.2 Staging Model YAML Documentation

Every staging model needs source documentation:

```yaml
# dbt/models/staging/_staging__sources.yml

version: 2

sources:
  - name: raw
    description: Raw data tables ingested from external APIs
    database: raw_data
    schema: public
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
    loaded_at_field: ingestion_timestamp
    
    tables:
      - name: pricing_data
        description: |
          Daily OHLCV pricing from Alpha Vantage API
          - Updated daily at 4:00 PM ET (after market close)
          - Covers US equities in our universe
          - Retention: 5 years
        columns:
          - name: symbol_code
            description: Stock ticker (e.g., AAPL)
            tests:
              - not_null
          - name: date_field
            description: Trading date (YYYY-MM-DD)
            tests:
              - not_null
          - name: price_close
            description: Closing price in USD
            tests:
              - not_null
              - positive_values

models:
  - name: stg_raw_pricing_data
    description: |
      Cleaned pricing data from Alpha Vantage
      - Removes duplicates (by symbol + date)
      - Validates prices > 0
      - Converts to standard schema
    columns:
      - name: symbol
        description: Ticker symbol
        tests:
          - not_null
          - accepted_values:
              values: ['AAPL', 'GOOGL', 'MSFT']  # Or load from seeds
      - name: close_price
        description: Adjusted closing price
        tests:
          - not_null
          - custom_test_positive
```

### 2.3 Materialization: Staging Models

**Always use `ephemeral` for staging models:**

```yaml
# dbt_project.yml
models:
  staging:
    materialized: ephemeral  # Cost: $0 (not stored)
```

**Why:** Staging models are intermediate steps. They're merged into downstream tables at query time (CTEs). No storage cost, no redundancy.

---

## 3. Intermediate Layer: Feature Engineering

### 3.1 Intermediate Model Example

```sql
/*
    int_technical_indicators_daily
    
    Purpose: Calculate technical indicators for ML features
    
    Technical indicators for quantitative models:
    - Momentum: RSI, MACD, Rate of Change
    - Volatility: ATR, Bollinger Bands
    - Trend: Moving averages, ADX
    
    Grain: One row per (symbol, trading_date)
    Dependencies: stg_raw_pricing_data
    Materialization: view (recalculated on query—no storage cost)
*/

with daily_prices as (
    select
        symbol,
        trading_date,
        close_price,
        trading_volume,
        high_price,
        low_price
    from {{ ref('stg_raw_pricing_data') }}
)

-- Calculate 20-day moving average
, moving_average_20 as (
    select
        symbol,
        trading_date,
        avg(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) as ma_20
    from daily_prices
)

-- Calculate daily returns
, daily_returns as (
    select
        symbol,
        trading_date,
        close_price,
        lag(close_price) over (
            partition by symbol 
            order by trading_date
        ) as prior_close,
        
        -- Return: (close - prior_close) / prior_close
        (close_price - lag(close_price) over (
            partition by symbol 
            order by trading_date
        )) / 
        nullif(lag(close_price) over (
            partition by symbol 
            order by trading_date
        ), 0) as daily_return_pct
        
    from daily_prices
)

-- Calculate volatility (20-day rolling std dev)
, volatility as (
    select
        symbol,
        trading_date,
        stddev_pop(daily_return_pct) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) as volatility_20d
    from daily_returns
)

-- Final feature set
select
    daily_returns.symbol,
    daily_returns.trading_date,
    daily_returns.close_price,
    daily_returns.daily_return_pct,
    moving_average_20.ma_20,
    volatility.volatility_20d,
    
    -- Additional feature: Price vs MA ratio
    daily_returns.close_price / moving_average_20.ma_20 as price_ma_ratio,
    
    current_timestamp() as dbt_loaded_at
    
from daily_returns
left join moving_average_20 
    on daily_returns.symbol = moving_average_20.symbol
    and daily_returns.trading_date = moving_average_20.trading_date
left join volatility 
    on daily_returns.symbol = volatility.symbol
    and daily_returns.trading_date = volatility.trading_date

where daily_returns.trading_date >= '2020-01-01'
```

### 3.2 Materialization: Intermediate Models

**Use `view` (default) for intermediate models:**

```yaml
# dbt_project.yml
models:
  intermediate:
    materialized: view  # Cost: $0 (no storage, recalculated per query)
```

**Trade-off:** 
- ✅ Zero storage cost
- ✅ Always up-to-date
- ❌ Recalculated on every query (slower for complex calculations)

**Alternative for expensive calculations:** Use `incremental`

```sql
{{
    config(
        materialized='incremental',
        unique_key='symbol_date',
        on_schema_change='fail'
    )
}}

-- Your model SQL...

{% if execute and execute_macros and 'dbt_internal_metadata' in graph.nodes %}
  where trading_date > (select max(trading_date) from {{ this }})
{% endif %}
```

---

## 4. Marts Layer: ML-Ready Features

### 4.1 ML Feature Mart Template

```sql
/*
    fct_ml_feature_set_daily
    
    Purpose: Daily feature matrix for ML model training
    
    Business logic:
    - Aggregates all features for each security
    - Handles missing values systematically
    - Ensures no data leakage (forward-looking features)
    - Aligned with prediction target dates
    
    Grain: One row per (symbol, trading_date)
    Updated: Daily, after market close
    
    CRITICAL: This table is the direct input to your ML pipeline.
    Any errors here directly impact model accuracy.
*/

with feature_base as (
    select
        symbol,
        trading_date,
        
        -- Price-based features (from intermediate layer)
        close_price,
        daily_return_pct,
        ma_20,
        volatility_20d,
        price_ma_ratio
        
    from {{ ref('int_technical_indicators_daily') }}
)

-- Join fundamental features (if available)
, with_fundamentals as (
    select
        feature_base.*,
        
        -- Fundamental data (quarterly, forward-filled)
        fundamentals.pe_ratio,
        fundamentals.pb_ratio,
        fundamentals.dividend_yield,
        fundamentals.debt_to_equity
        
    from feature_base
    left join {{ ref('int_fundamental_features_daily') }} as fundamentals
        on feature_base.symbol = fundamentals.symbol
        and feature_base.trading_date = fundamentals.trading_date
)

-- Handle missing values
, filled_features as (
    select
        *,
        
        -- Forward-fill missing fundamentals (last known value)
        last_value(pe_ratio ignore nulls) over (
            partition by symbol 
            order by trading_date 
            rows between unbounded preceding and current row
        ) as pe_ratio_filled,
        
        last_value(pb_ratio ignore nulls) over (
            partition by symbol 
            order by trading_date 
            rows between unbounded preceding and current row
        ) as pb_ratio_filled
        
    from with_fundamentals
)

-- Final validation
, validated as (
    select
        symbol,
        trading_date,
        
        -- Required features must not be null
        close_price,
        daily_return_pct,
        ma_20,
        volatility_20d,
        price_ma_ratio,
        pe_ratio_filled,
        pb_ratio_filled,
        
        -- Data quality flags
        case 
            when close_price is null then 'MISSING_PRICE'
            when volatility_20d is null then 'INSUFFICIENT_HISTORY'
            else 'VALID'
        end as feature_quality_flag,
        
        current_timestamp() as dbt_loaded_at
        
    from filled_features
    
    -- Exclude rows with data quality issues
    where feature_quality_flag = 'VALID'
)

select *
from validated
```

### 4.2 Materialization: Marts

**Always use `table` for marts:**

```yaml
# dbt_project.yml
models:
  marts:
    materialized: table  # Store in Snowflake
    clustering_key: ['symbol', 'trading_date']  # Cost optimization
    on_schema_change: fail  # Protect from accidental schema changes
```

**Why:**
- ✅ Fast access for ML pipelines (no recalculation)
- ✅ Can apply clustering for performance
- ❌ Storage cost (but necessary for production use)
- ❌ Must be manually refreshed

### 4.3 Clustering for Cost Optimization

```sql
-- dbt/models/marts/fct_ml_feature_set_daily.sql

{{
    config(
        materialized='table',
        clustering_key=['symbol', 'trading_date'],
        unique_key=['symbol', 'trading_date'],
        on_schema_change='fail',
        tags=['marts', 'ml_input']
    )
}}

-- Your SELECT statement...
```

**Impact:** 
- Queries filtering by `symbol` and `trading_date` scan 50-70% less data
- Cost reduction: ~40-60% for typical ML queries

---

## 5. Data Quality Testing

### 5.1 Generic Tests (Built-in)

```yaml
# dbt/models/staging/_staging__meta.yml

models:
  - name: stg_raw_pricing_data
    columns:
      - name: symbol
        tests:
          - not_null          # Column must have value
          - unique            # No duplicates
          
      - name: trading_date
        tests:
          - not_null
          
      - name: close_price
        tests:
          - not_null
          - positive_values   # Custom test (see below)
```

### 5.2 Custom Tests (Hedge Fund Specific)

```sql
-- dbt/tests/generic/test_positive_values.sql

/*
    Custom test: Verify numeric column contains only positive values
    Used for: prices, volumes, volatility
*/

{% test positive_values(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} < 0
        or {{ column_name }} is null

{% endtest %}
```

```sql
-- dbt/tests/generic/test_unique_composite_key.sql

/*
    Custom test: Ensure combination of columns is unique
    Used for: (symbol, date) pairs in pricing data
*/

{% test unique_composite_key(model, column_names) %}

    select 
        {% for col in column_names %}
            {{ col }}{{ "," if not loop.last }}
        {% endfor %}
    from {{ model }}
    group by {% for col in column_names %}
        {{ col }}{{ "," if not loop.last }}
    {% endfor %}
    having count(*) > 1

{% endtest %}
```

### 5.3 Data Quality Test YAML

```yaml
# dbt/tests/data_tests/test_pricing_data_quality.yml

version: 2

models:
  - name: stg_raw_pricing_data
    tests:
      # Row count test: Ensure we have data
      - dbt_utils.recency:
          datepart: day
          field: trading_date
          interval: 1
          
      # No duplicate dates per symbol
      - unique_composite_key:
          column_names: ['symbol', 'trading_date']
          
      # Price changes within reasonable bounds
      - dbt_utils.expression_is_true:
          expression: "abs(daily_return_pct) < 0.20"  # < 20% daily move
          description: "Daily returns should be < 20%"
          
    columns:
      - name: symbol
        tests:
          - not_null
          - relationships:
              to: ref('dim_security_universe')
              field: symbol
              
      - name: close_price
        tests:
          - not_null
          - positive_values
          - accepted_range:
              min_value: 0.01
              max_value: 500000
```

### 5.4 Run Tests

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select stg_raw_pricing_data

# Run only schema tests (not data tests)
dbt test --select tag:schema_test

# Run with detailed output
dbt test --debug
```

---

## 6. Incremental Models for Large Datasets

### 6.1 Incremental Strategy

For daily pricing data (large volume, frequent updates):

```sql
-- dbt/models/intermediate/int_price_history_incremental.sql

{{
    config(
        materialized='incremental',
        unique_key=['symbol', 'trading_date'],
        on_schema_change='fail',
        tags=['incremental']
    )
}}

with source as (
    select
        symbol,
        trading_date,
        close_price,
        trading_volume,
        daily_return_pct
    from {{ ref('stg_raw_pricing_data') }}
    
    {% if execute %}
        -- Only process new data in incremental runs
        {% if is_incremental() %}
            where trading_date > (select max(trading_date) from {{ this }})
        {% endif %}
    {% endif %}
)

select *
from source
```

**Benefits:**
- Only loads new data (since yesterday)
- 50-90% faster than full refresh
- Significant cost savings

### 6.2 Full Refresh Strategy

```bash
# Periodic full refresh (weekly/monthly) to catch missed data
dbt run --select int_price_history_incremental --full-refresh
```

---

## 7. Cost Optimization Strategies

### 7.1 Warehouse Sizing

```yaml
# config/profiles.yml

dev:
  warehouse: COMPUTE_XS      # $2/hour - Development
  
prod:
  warehouse: COMPUTE_L       # $8/hour - Production
```

**Rules:**
- ✅ Dev: Use `COMPUTE_XS` (smallest)
- ✅ Prod: Use `COMPUTE_L` only when necessary
- ✅ Always use `COMPUTE_M` as middle ground

### 7.2 Query Optimization

```sql
-- ❌ Expensive: Full table scan
select * from fct_ml_feature_set_daily
where symbol = 'AAPL';

-- ✅ Optimized: Uses clustering key
select * from fct_ml_feature_set_daily
where symbol = 'AAPL'
  and trading_date between '2024-01-01' and '2024-12-31';
```

### 7.3 Cost Monitoring

```sql
-- dbt/tools/cost_monitoring.sql

-- See query costs in Snowflake
select
    query_id,
    user_name,
    warehouse_name,
    total_elapsed_time,
    compilation_time,
    (credits_used_compute) as estimated_cost_usd
from snowflake.account_usage.query_history
where start_time >= current_date - interval '7 days'
order by credits_used_compute desc
limit 20;
```

---

## 8. Documentation & Lineage

### 8.1 Model Documentation Template

Every model should have:

```yaml
# dbt/models/staging/_staging__meta.yml

models:
  - name: stg_raw_pricing_data
    description: |
      Cleaned daily pricing data from external source.
      
      **Business Context:**
      - Used for all technical indicator calculations
      - Feeds into ML feature engineering pipeline
      - Primary use: Daily model retraining
      
      **Data Quality:**
      - Deduplicated by (symbol, trading_date)
      - Prices must be > 0
      - Data coverage: 5 years rolling
      
      **Update Frequency:** Daily (4:00 PM ET)
      
      **Owner:** Data Engineering Team
      **Contacts:** [email]
      
    meta:
      owner: "data_engineering"
      tier: "critical"
      sla: "4:00 PM ET daily"
      refresh_frequency: "daily"
      
    columns:
      - name: symbol
        description: Stock ticker (e.g., AAPL)
        meta:
          format: "uppercase"
          
      - name: trading_date
        description: Date of trading day (YYYY-MM-DD)
        
      - name: close_price
        description: Closing price in USD, adjusted for splits
```

### 8.2 Generate & Serve Docs

```bash
# Generate documentation
dbt docs generate

# Serve locally (opens browser)
dbt docs serve  # localhost:8000
```

---

## 9. Common Pitfalls & Solutions

### 9.1 Pitfall: Forward-Looking Bias

❌ **Wrong:** Using future data to predict the past
```sql
-- This has DATA LEAKAGE!
select
    trading_date,
    next_week_return,  -- This is FUTURE data!
    current_features
from feature_set
```

✅ **Correct:** Only use past data to predict future
```sql
select
    trading_date,
    lead(return_pct, 5) over (
        partition by symbol order by trading_date
    ) as return_5d_forward,  -- Target (future)
    lag(volatility, 20) over (
        partition by symbol order by trading_date
    ) as volatility_20d_lagged  -- Feature (past)
from feature_set
```

### 9.2 Pitfall: Not Handling Missing Values

❌ **Wrong:**
```sql
select * from features
-- Implicit nulls cause ML model failures
```

✅ **Correct:**
```sql
select
    *,
    case 
        when price is null then 'INVALID'
        when volume = 0 then 'NO_VOLUME'
        else 'VALID'
    end as data_quality_flag
from features
where data_quality_flag = 'VALID'
```

### 9.3 Pitfall: Ignoring Survivorship Bias

Hedge funds often filter to "currently trading" securities. This introduces bias.

✅ **Solution:**
```sql
-- Include delisted securities in historical analysis
select *
from fct_ml_feature_set_daily
where trading_date < delisting_date  -- Or null if still trading
  or delisting_date is null
```

---

## 10. Production Deployment Checklist

- [ ] All models have tests (zero test failures)
- [ ] Documentation complete for all critical tables
- [ ] Incremental models tested with full refresh
- [ ] Clustering keys defined on large tables
- [ ] Cost analysis completed (monthly budget approved)
- [ ] Data lineage documented in YAML
- [ ] Backup/retention strategy defined
- [ ] Alerting set up for test failures
- [ ] Code reviewed by second engineer
- [ ] Production credentials in `.env` (never hardcoded)
- [ ] Git history cleaned (no credential leaks)

---

## References

- DBT Best Practices: https://docs.getdbt.com/guides/best-practices
- Snowflake Performance: https://docs.snowflake.com/en/user-guide/cost-optimization-tips
- Quantitative Analysis: CFA Institute (Quantitative Investment Analysis)

---

**Last Updated:** May 2026  
**Status:** Approved for Use  
**Maintainer:** Senior Data Engineer
