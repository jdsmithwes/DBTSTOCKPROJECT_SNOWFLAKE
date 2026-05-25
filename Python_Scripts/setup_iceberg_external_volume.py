"""
setup_iceberg_external_volume.py
─────────────────────────────────
Step 2 of the Iceberg external volume setup. Run this AFTER the CloudFormation
stack in infrastructure/iceberg_iam.yml has been deployed successfully.

What this script does:
  Phase 1  — Create the Snowflake external volume (references the IAM role ARN)
  Phase 2  — Read Snowflake's AWS IAM user ARN and external ID from the volume
  Phase 3  — Print the AWS CLI command to update the CloudFormation stack with
             the real Snowflake trust values (re-deploy locks down the role)
  Phase 4  — Validate the external volume is ACTIVE after the stack update

Setup order:
  1. Deploy infrastructure/iceberg_iam.yml from the AWS console (admin account)
  2. Run this script:   python3 Python_Scripts/setup_iceberg_external_volume.py
  3. Copy the printed `aws cloudformation update-stack` command and run it as admin
  4. Re-run with --validate-only to confirm the volume is ACTIVE

Required environment variables (loaded from DBTSTOCKPROJECT/.env):
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
  SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE
"""

import argparse
import logging
import os
import time
from pathlib import Path

import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
AWS_ACCOUNT_ID   = "573509103721"
AWS_REGION       = os.environ.get("AWS_REGION", "us-east-1")
CFN_STACK_NAME   = "snowflake-iceberg-dbt-stockproject"

ICEBERG_BUCKET   = "dbt-stockproject-iceberg"
IAM_ROLE_ARN     = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/snowflake-iceberg-dbt-stockproject-role"
EXTERNAL_VOLUME  = "DBT_STOCKPROJECT_ICEBERG_VOL"
STORAGE_LOCATION = "dbt-stockproject-iceberg-us-east-1"

SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE",      "DBT_ROLE")
SNOWFLAKE_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE",  "DBT_STOCKPROJECT")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "DBT_STOCKPROJECT")


def _snowflake_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )


# ── Phase 1 ───────────────────────────────────────────────────────────────────
def phase1_create_volume(conn):
    log.info("Phase 1: creating Snowflake external volume …")
    sql = f"""
        CREATE EXTERNAL VOLUME IF NOT EXISTS {EXTERNAL_VOLUME}
          STORAGE_LOCATIONS = (
            (
              NAME                 = '{STORAGE_LOCATION}'
              STORAGE_PROVIDER     = 'S3'
              STORAGE_AWS_ROLE_ARN = '{IAM_ROLE_ARN}'
              STORAGE_BASE_URL     = 's3://{ICEBERG_BUCKET}/'
            )
          );
    """
    conn.cursor().execute(sql)
    log.info(f"  ✅ External volume {EXTERNAL_VOLUME} created (or already exists)")


# ── Phase 2 ───────────────────────────────────────────────────────────────────
def phase2_read_trust_values(conn) -> tuple[str, str]:
    log.info("Phase 2: reading Snowflake trust values from external volume …")
    import json
    cur = conn.cursor()
    cur.execute(f"DESCRIBE EXTERNAL VOLUME {EXTERNAL_VOLUME}")
    rows = cur.fetchall()

    # DESCRIBE EXTERNAL VOLUME returns rows of (group, property, type, value, default).
    # Trust values are embedded as JSON inside the STORAGE_LOCATION_1 property value.
    for row in rows:
        prop_name = row[1]
        prop_val  = row[3]
        if prop_name == "STORAGE_LOCATION_1" and prop_val:
            try:
                loc = json.loads(prop_val)
                sf_iam_user_arn = loc.get("STORAGE_AWS_IAM_USER_ARN")
                sf_external_id  = loc.get("STORAGE_AWS_EXTERNAL_ID")
                if sf_iam_user_arn and sf_external_id:
                    log.info(f"  Snowflake IAM user ARN : {sf_iam_user_arn}")
                    log.info(f"  Snowflake external ID  : {sf_external_id}")
                    return sf_iam_user_arn, sf_external_id
            except json.JSONDecodeError:
                pass

    raise RuntimeError(
        "Could not parse trust values from DESCRIBE EXTERNAL VOLUME.\n"
        "Raw output:\n" + "\n".join(str(r) for r in rows)
    )


# ── Phase 3 ───────────────────────────────────────────────────────────────────
def phase3_print_cfn_update(sf_iam_user_arn: str, sf_external_id: str):
    log.info("Phase 3: CloudFormation update command …")

    cfn_template = (
        Path(__file__).resolve().parent.parent / "infrastructure" / "iceberg_iam.yml"
    )

    cmd = (
        f"aws cloudformation update-stack \\\n"
        f"  --stack-name {CFN_STACK_NAME} \\\n"
        f"  --template-body file://{cfn_template} \\\n"
        f"  --capabilities CAPABILITY_NAMED_IAM \\\n"
        f"  --parameters \\\n"
        f"    ParameterKey=SnowflakeIAMUserARN,ParameterValue='{sf_iam_user_arn}' \\\n"
        f"    ParameterKey=SnowflakeExternalID,ParameterValue='{sf_external_id}'"
    )

    print()
    print("=" * 72)
    print("ACTION REQUIRED — run this command as an AWS admin to lock down the")
    print("IAM trust policy so only Snowflake can assume the role:")
    print()
    print(cmd)
    print("=" * 72)
    print()
    log.info("After the stack update completes, re-run with --validate-only")


# ── Phase 4 ───────────────────────────────────────────────────────────────────
def phase4_validate(conn):
    log.info("Phase 4: validating external volume is ACTIVE …")
    for attempt in range(6):
        cur = conn.cursor()
        cur.execute(f"DESCRIBE EXTERNAL VOLUME {EXTERNAL_VOLUME}")
        rows = cur.fetchall()
        cols = [d[0].lower() for d in cur.description]
        for row in rows:
            row_dict = dict(zip(cols, row))
            # DESCRIBE EXTERNAL VOLUME returns an "ACTIVE" property whose value is
            # the name of the active storage location (non-empty = active).
            if row_dict.get("property") == "ACTIVE":
                active_loc = row_dict.get("property_value", "")
                if active_loc:
                    log.info(f"  ✅ {EXTERNAL_VOLUME} is ACTIVE (storage location: {active_loc})")
                    return
                else:
                    log.warning(
                        f"  ⚠️  ACTIVE storage location is empty "
                        f"(attempt {attempt + 1}/6 — IAM trust may not have propagated yet)"
                    )
        time.sleep(10)

    log.warning(
        "  External volume not yet ACTIVE. IAM propagation can take up to 60 s. "
        "Re-run with --validate-only once the CloudFormation update completes."
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Skip creation; only check that the external volume is ACTIVE",
    )
    args = parser.parse_args()

    log.info("🚀 Snowflake Iceberg external volume setup")
    log.info(f"   S3 bucket    : s3://{ICEBERG_BUCKET}/")
    log.info(f"   IAM role     : {IAM_ROLE_ARN}")
    log.info(f"   Ext volume   : {EXTERNAL_VOLUME}")

    conn = _snowflake_conn()
    try:
        if args.validate_only:
            phase4_validate(conn)
        else:
            phase1_create_volume(conn)
            sf_iam_user_arn, sf_external_id = phase2_read_trust_values(conn)
            phase3_print_cfn_update(sf_iam_user_arn, sf_external_id)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
