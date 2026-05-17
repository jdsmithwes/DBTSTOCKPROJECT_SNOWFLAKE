-- ============================================================
-- DATA PIPELINE ARCHITECTURE
-- ============================================================
--
-- PATH 1 — REGULAR DAILY LOAD (Snowflake Task DAG, 8 PM ET)
-- ──────────────────────────────────────────────────────────
--   DATA_PIPELINE_ROOT_TASK (cron schedule)
--     ├── STOCK_SPROC_DAILY_TASK  → CALL STOCK_SPROC(NULL, NULL)
--     │     Writes directly to PUBLIC.RAW_STOCK_DATA via stored procedure.
--     │     No S3 or Snowpipe involved.
--     └── COMPANY_INFO_DAILY_TASK → CALL COMPANY_INFO_SPROC()
--           Writes directly to PUBLIC.RAW_COMPANY_OVERVIEW.
--     Both ↓
--     DBT_SEED_TASK → dbt seed (loads seed CSVs, e.g. us_market_holidays)
--     ↓
--     DBT_REFRESH_TASK → dbt run (rebuilds all models)
--
-- PATH 2 — BACKFILL (Python script, run on demand)
-- ──────────────────────────────────────────────────────────
--   backfill_missing_dates.py
--     Phase 1: detect gaps in RAW_STOCK_DATA vs NYSE calendar
--     Phase 2: fetch missing data from AlphaVantage API
--     Phase 3: upload CSV to s3://dbtstockprojectdata/stock_prices/
--     Phase 4: COPY INTO RAW_STOCK_DATA FROM @S3_DBTSTOCKPROJECT_STAGE_STOCKPRICES
--              (synchronous — bypasses Snowpipe/SNS entirely)
--     Phase 5: dbt seed + dbt run (rebuilds all downstream models)
--
-- ============================================================

USE ROLE DBT_ROLE;
USE WAREHOUSE DBT_STOCKPROJECT;
USE DATABASE DBT_STOCKPROJECT;
USE SCHEMA DBT_DEV_JDS_STAGING;

-- ──────────────────────────────────────────────────────────
-- STEP 1: Suspend existing tasks before restructuring
-- ──────────────────────────────────────────────────────────
ALTER TASK DBT_REFRESH_TASK          SUSPEND;
ALTER TASK STOCK_SPROC_DAILY_TASK    SUSPEND;
ALTER TASK COMPANY_INFO_DAILY_TASK   SUSPEND;

-- ──────────────────────────────────────────────────────────
-- STEP 2: Root task — owns the schedule, kicks off the DAG
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE TASK DATA_PIPELINE_ROOT_TASK
  WAREHOUSE = DBT_STOCKPROJECT
  SCHEDULE  = 'USING CRON 0 20 * * * America/New_York'
  COMMENT   = 'Root task — triggers PATH 1 daily data pipeline at 8 PM ET'
AS
  SELECT 1;

-- ──────────────────────────────────────────────────────────
-- STEP 3: PATH 1 data-loading tasks (children of root)
--         Both tasks run in parallel after the root fires.
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE TASK STOCK_SPROC_DAILY_TASK
  WAREHOUSE = DBT_STOCKPROJECT
  COMMENT   = 'PATH 1: loads today''s stock price data directly into RAW_STOCK_DATA via stored procedure'
  AFTER DATA_PIPELINE_ROOT_TASK
AS
  CALL STOCK_SPROC(NULL, NULL);

CREATE OR REPLACE TASK COMPANY_INFO_DAILY_TASK
  WAREHOUSE = DBT_STOCKPROJECT
  COMMENT   = 'PATH 1: loads company overview data directly into RAW_COMPANY_OVERVIEW via stored procedure'
  AFTER DATA_PIPELINE_ROOT_TASK
AS
  CALL COMPANY_INFO_SPROC();

-- ──────────────────────────────────────────────────────────
-- STEP 4: dbt seed task — runs after both data tasks finish,
--         before dbt run so seed tables (e.g. us_market_holidays)
--         exist when models reference them.
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE TASK DBT_SEED_TASK
  WAREHOUSE = DBT_STOCKPROJECT
  COMMENT   = 'PATH 1: loads dbt seed CSVs (e.g. us_market_holidays) before model run'
  AFTER STOCK_SPROC_DAILY_TASK, COMPANY_INFO_DAILY_TASK
AS
  EXECUTE DBT PROJECT FROM WORKSPACE USER$.PUBLIC."DBTSTOCKPROJECT_SNOWFLAKE_GITREPO"
    ARGS = 'seed --target dev'
    PROJECT_ROOT = 'DBTSTOCKPROJECT';

-- ──────────────────────────────────────────────────────────
-- STEP 5: dbt run task — runs after seeds are loaded
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE TASK DBT_REFRESH_TASK
  WAREHOUSE = DBT_STOCKPROJECT
  COMMENT   = 'PATH 1: rebuilds all dbt models after seeds and data loading complete'
  AFTER DBT_SEED_TASK
AS
  EXECUTE DBT PROJECT FROM WORKSPACE USER$.PUBLIC."DBTSTOCKPROJECT_SNOWFLAKE_GITREPO"
    ARGS = 'run --target dev'
    PROJECT_ROOT = 'DBTSTOCKPROJECT';

-- ──────────────────────────────────────────────────────────
-- STEP 6: Resume tasks (children before root per Snowflake requirement)
-- ──────────────────────────────────────────────────────────
ALTER TASK DBT_REFRESH_TASK          RESUME;
ALTER TASK DBT_SEED_TASK             RESUME;
ALTER TASK STOCK_SPROC_DAILY_TASK    RESUME;
ALTER TASK COMPANY_INFO_DAILY_TASK   RESUME;
ALTER TASK DATA_PIPELINE_ROOT_TASK   RESUME;

-- ──────────────────────────────────────────────────────────
-- VERIFY: DAG structure
-- ──────────────────────────────────────────────────────────
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
  TASK_NAME => 'DATA_PIPELINE_ROOT_TASK',
  RECURSIVE => TRUE
));
