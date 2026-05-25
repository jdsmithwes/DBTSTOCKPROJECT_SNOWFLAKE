-- ============================================================
-- snowflake_task_setup.sql
-- ============================================================
-- Run this once in a Snowflake worksheet as ACCOUNTADMIN.
-- Creates a Snowflake Task that fires at 8 PM ET on weekdays
-- and triggers the GitHub Actions nightly pipeline via the
-- GitHub repository_dispatch API.
--
-- Architecture:
--   Snowflake Task (cron, 8 PM ET)
--     → Python stored procedure
--       → GitHub REST API (repository_dispatch)
--         → .github/workflows/nightly_dbt_refresh.yml
--           → fetch scripts → dbt build --target prod → dbt test
--
-- Prerequisites:
--   1. A GitHub Personal Access Token with repo + workflow scope.
--      Create at: github.com → Settings → Developer settings → PATs.
--   2. Replace '<YOUR_GITHUB_PAT>' below before running.
--   3. Run the ALTER TASK RESUME at the bottom when ready to activate.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DBT_STOCKPROJECT;
USE SCHEMA PUBLIC;


-- ── Step 1: Allow Snowflake to call the GitHub API ───────────────────────────

CREATE OR REPLACE NETWORK RULE github_api_egress
    TYPE        = HOST_PORT
    MODE        = EGRESS
    VALUE_LIST  = ('api.github.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION github_api_access
    ALLOWED_NETWORK_RULES = (github_api_egress)
    ENABLED               = TRUE;


-- ── Step 2: Store the GitHub PAT as a Snowflake Secret ───────────────────────
-- Replace the placeholder value with your actual PAT before running.

CREATE OR REPLACE SECRET github_pat
    TYPE          = GENERIC_STRING
    SECRET_STRING = '<YOUR_GITHUB_PAT>';   -- needs repo + workflow scope


-- ── Step 3: Stored procedure to trigger the GitHub Actions workflow ───────────

CREATE OR REPLACE PROCEDURE DBT_STOCKPROJECT.PUBLIC.TRIGGER_NIGHTLY_DBT_REFRESH()
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python', 'requests')
    SECRETS = ('github_secret' = github_pat)
    EXTERNAL_ACCESS_INTEGRATIONS = (github_api_access)
    HANDLER = 'trigger'
    EXECUTE AS OWNER
AS $$
import _snowflake
import requests
import json

REPO = 'jdsmithwes/DBTSTOCKPROJECT_SNOWFLAKE'

def trigger(session):
    token = _snowflake.get_generic_secret_string('github_secret')

    resp = requests.post(
        f'https://api.github.com/repos/{REPO}/dispatches',
        headers={
            'Authorization': f'Bearer {token}',
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28',
        },
        json={'event_type': 'trigger-dbt-refresh'},
        timeout=15,
    )

    success = resp.status_code == 204
    return {
        'triggered': success,
        'http_status': resp.status_code,
        'repo': REPO,
    }
$$;


-- ── Step 4: Create the Task (starts SUSPENDED — resume when ready) ────────────

CREATE OR REPLACE TASK DBT_STOCKPROJECT.PUBLIC.NIGHTLY_DBT_REFRESH
    WAREHOUSE = DBT_STOCKPROJECT
    SCHEDULE  = 'USING CRON 0 20 * * 1-5 America/New_York'   -- 8 PM ET, Mon–Fri
    COMMENT   = 'Triggers GitHub Actions nightly dbt pipeline via repository_dispatch'
AS
    CALL DBT_STOCKPROJECT.PUBLIC.TRIGGER_NIGHTLY_DBT_REFRESH();


-- ── Monitoring queries ────────────────────────────────────────────────────────

-- Show task state (SUSPENDED until you run the RESUME below)
SHOW TASKS IN DATABASE DBT_STOCKPROJECT;

-- View run history and last result
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'NIGHTLY_DBT_REFRESH',
    SCHEDULED_TIME_RANGE_START => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
))
ORDER BY scheduled_time DESC;


-- ── Activate when ready ───────────────────────────────────────────────────────
-- Uncomment and run after configuring GitHub secrets in the repo settings.

-- ALTER TASK DBT_STOCKPROJECT.PUBLIC.NIGHTLY_DBT_REFRESH RESUME;


-- ── Manual trigger (test without waiting for cron) ────────────────────────────

-- EXECUTE TASK DBT_STOCKPROJECT.PUBLIC.NIGHTLY_DBT_REFRESH;
