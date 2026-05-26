-- ============================================================
-- home_network_policy.sql
-- ============================================================
-- Run once in Snowsight as ACCOUNTADMIN.
-- Creates a user-level network policy allowing the home ISP
-- /16 subnet so connections from home always succeed, even if
-- the ISP rotates the last two octets of your IP.
--
-- Scope: user-level only (not account-wide) so the Snowflake-
-- native dbt Task (stored procedure) is unaffected — SPs run
-- inside Snowflake compute and are not subject to user network
-- policies.
--
-- If you work from another location:
--   ALTER USER jdsmithwes SET NETWORK_POLICY = NULL;
-- To re-enable:
--   ALTER USER jdsmithwes SET NETWORK_POLICY = home_access_policy;
-- ============================================================

USE ROLE ACCOUNTADMIN;


-- ── Step 1: Create the policy ────────────────────────────────────────────────
-- 73.207.0.0/16 covers the full /16 block of your home ISP (73.207.5.79).
-- Widen to /8 (73.0.0.0/8) only if your ISP reassigns outside /16.

CREATE OR REPLACE NETWORK POLICY home_access_policy
    ALLOWED_IP_LIST = ('73.207.0.0/16')
    COMMENT         = 'Allows home ISP /16 subnet for persistent dev access';


-- ── Step 2: Apply to your user only ─────────────────────────────────────────
ALTER USER jdsmithwes SET NETWORK_POLICY = home_access_policy;


-- ── Step 3: Verify ──────────────────────────────────────────────────────────
SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN USER jdsmithwes;
SHOW NETWORK POLICIES;
