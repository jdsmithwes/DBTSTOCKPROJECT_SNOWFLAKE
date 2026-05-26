-- ============================================================
-- snowflake_native_dbt_task.sql
-- ============================================================
-- Fully Snowflake-native nightly dbt pipeline.
-- Runs entirely inside Snowflake compute — no GitHub Actions,
-- no external runners, no network policy issues.
--
-- Architecture:
--   Snowflake Task (cron 8 PM ET Mon–Fri)
--     → Python stored procedure
--       → downloads repo ZIP from GitHub (public repo, no token)
--       → pip install dbt-snowflake
--       → writes profiles.yml using RSA key from Snowflake Secret
--       → dbt run --target snowflake   (rebuilds all prod models)
--       → dbt test --target snowflake  (data quality gate)
--       → returns VARIANT result log
--
-- Prerequisites (run once):
--   1. Create the RSA private key secret (Step 2 below).
--      Value = contents of ~/.ssh/snowflake_rsa_key.p8
--   2. Run Steps 1–5 as ACCOUNTADMIN in a Snowflake worksheet.
--   3. Resume the Task (Step 6) when ready to activate.
--
-- Run this entire file top-to-bottom in Snowsight.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DBT_STOCKPROJECT;
USE SCHEMA PUBLIC;
USE WAREHOUSE DBT_STOCKPROJECT;


-- ── Step 1: External network access (GitHub ZIP + PyPI) ──────────────────────

CREATE OR REPLACE NETWORK RULE dbt_pipeline_egress
    TYPE       = HOST_PORT
    MODE       = EGRESS
    VALUE_LIST = (
        'codeload.github.com:443',    -- GitHub ZIP archive downloads
        'pypi.org:443',               -- pip package index
        'files.pythonhosted.org:443'  -- pip wheel downloads
    )
    COMMENT = 'Outbound access for nightly dbt pipeline SP';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_pipeline_access
    ALLOWED_NETWORK_RULES = (dbt_pipeline_egress)
    ENABLED               = TRUE;


-- ── Step 2: RSA private key secret ──────────────────────────────────────────
-- Replace <PASTE_KEY_CONTENT_HERE> with the full contents of
-- ~/.ssh/snowflake_rsa_key.p8 (including the -----BEGIN/END----- lines).

CREATE OR REPLACE SECRET dbt_rsa_private_key
    TYPE          = GENERIC_STRING
    SECRET_STRING = '<PASTE_KEY_CONTENT_HERE>'
    COMMENT       = 'RSA private key for jdsmithwes dbt Snowflake connections';


-- ── Step 3: Stored procedure ─────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE DBT_STOCKPROJECT.PUBLIC.RUN_NIGHTLY_DBT()
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    SECRETS = ('rsa_key' = dbt_rsa_private_key)
    EXTERNAL_ACCESS_INTEGRATIONS = (dbt_pipeline_access)
    HANDLER = 'run_pipeline'
    EXECUTE AS OWNER
AS $$
import _snowflake
import subprocess
import sys
import os
import json
import zipfile
import shutil
import urllib.request

REPO_ZIP_URL = (
    'https://codeload.github.com/jdsmithwes/'
    'DBTSTOCKPROJECT_SNOWFLAKE/zip/refs/heads/main'
)
EXTRACT_ROOT  = '/tmp/dbt_run'
REPO_DIR      = f'{EXTRACT_ROOT}/DBTSTOCKPROJECT_SNOWFLAKE-main'
PROJECT_DIR   = f'{REPO_DIR}/DBTSTOCKPROJECT'
KEY_PATH      = f'{EXTRACT_ROOT}/snowflake_rsa_key.p8'
PROFILES_PATH = f'{PROJECT_DIR}/profiles.yml'

PROFILES_TEMPLATE = """\
dbt_stockproject:
  target: snowflake
  outputs:
    snowflake:
      type: snowflake
      account: TPRFGUJ-JNC76647
      user: jdsmithwes
      private_key_path: {key_path}
      role: DBT_ROLE
      database: DBT_STOCKPROJECT
      warehouse: DBT_STOCKPROJECT
      schema: JDS_STAGING
      threads: 4
"""


def _run(cmd, **kwargs):
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    return {
        'returncode': result.returncode,
        'stdout': result.stdout[-3000:],
        'stderr': result.stderr[-1000:],
    }


def run_pipeline(session):
    log = {}

    # ── Cleanup any prior run ────────────────────────────────
    shutil.rmtree(EXTRACT_ROOT, ignore_errors=True)
    os.makedirs(EXTRACT_ROOT, exist_ok=True)

    # ── 1. Install dbt-snowflake ─────────────────────────────
    log['pip'] = _run([
        sys.executable, '-m', 'pip', 'install',
        'dbt-snowflake==1.10.3', '--quiet', '--no-warn-script-location',
    ])
    if log['pip']['returncode'] != 0:
        return {'status': 'FAILED', 'stage': 'pip_install', 'log': log}

    # ── 2. Download repo ZIP ─────────────────────────────────
    zip_path = f'{EXTRACT_ROOT}/repo.zip'
    urllib.request.urlretrieve(REPO_ZIP_URL, zip_path)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(EXTRACT_ROOT)
    os.unlink(zip_path)

    # ── 3. Write RSA key ─────────────────────────────────────
    private_key = _snowflake.get_generic_secret_string('rsa_key')
    with open(KEY_PATH, 'w') as f:
        f.write(private_key)
    os.chmod(KEY_PATH, 0o600)

    # ── 4. Write profiles.yml with key-pair auth ─────────────
    with open(PROFILES_PATH, 'w') as f:
        f.write(PROFILES_TEMPLATE.format(key_path=KEY_PATH))

    # ── 5. Locate dbt binary ─────────────────────────────────
    dbt = os.path.join(os.path.dirname(sys.executable), 'dbt')
    base_flags = [
        '--profiles-dir', PROJECT_DIR,
        '--project-dir',  PROJECT_DIR,
        '--target',       'snowflake',
        '--no-use-colors',
    ]

    # ── 6. dbt run ───────────────────────────────────────────
    log['dbt_run'] = _run([dbt, 'run'] + base_flags, cwd=PROJECT_DIR)
    if log['dbt_run']['returncode'] != 0:
        _cleanup()
        return {'status': 'FAILED', 'stage': 'dbt_run', 'log': log}

    # ── 7. dbt test ──────────────────────────────────────────
    log['dbt_test'] = _run([dbt, 'test'] + base_flags, cwd=PROJECT_DIR)
    test_ok = log['dbt_test']['returncode'] == 0

    # ── 8. Cleanup ───────────────────────────────────────────
    _cleanup()

    return {
        'status': 'PASSED' if test_ok else 'TESTS_FAILED',
        'log': log,
    }


def _cleanup():
    shutil.rmtree(EXTRACT_ROOT, ignore_errors=True)
$$;


-- ── Step 4: Verify the procedure compiles ────────────────────────────────────
-- (Do not execute yet — just describe to confirm it exists)
DESCRIBE PROCEDURE DBT_STOCKPROJECT.PUBLIC.RUN_NIGHTLY_DBT();


-- ── Step 5: Create the Task (starts SUSPENDED) ───────────────────────────────

CREATE OR REPLACE TASK DBT_STOCKPROJECT.PUBLIC.NIGHTLY_DBT_PIPELINE
    WAREHOUSE   = DBT_STOCKPROJECT
    SCHEDULE    = 'USING CRON 0 20 * * 1-5 America/New_York'
    COMMENT     = 'Nightly dbt run + test — fully Snowflake-native'
AS
    CALL DBT_STOCKPROJECT.PUBLIC.RUN_NIGHTLY_DBT();


-- ── Step 6: Activate ─────────────────────────────────────────────────────────
-- Run this after pasting the RSA key in Step 2 and confirming
-- the manual test in Step 7 succeeds.

-- ALTER TASK DBT_STOCKPROJECT.PUBLIC.NIGHTLY_DBT_PIPELINE RESUME;


-- ── Step 7: Manual test ───────────────────────────────────────────────────────
-- Run the procedure once manually before activating the Task.
-- Expect ~5–8 minutes for pip install + dbt run + dbt test.

-- CALL DBT_STOCKPROJECT.PUBLIC.RUN_NIGHTLY_DBT();


-- ── Step 8: Monitor ──────────────────────────────────────────────────────────

-- Task run history (last 7 days)
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     TASK_NAME => 'NIGHTLY_DBT_PIPELINE',
--     SCHEDULED_TIME_RANGE_START => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
-- ))
-- ORDER BY scheduled_time DESC;

-- Last procedure result
-- SELECT RESULT FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
