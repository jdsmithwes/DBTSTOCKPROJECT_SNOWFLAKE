#!/bin/bash
# Sources .env and launches the Snowflake MCP server with credentials as CLI args.
# Credentials stay in .env (gitignored) — this script contains no secrets.
set -a
source "$(dirname "$0")/DBTSTOCKPROJECT/.env"
set +a

exec /Library/Frameworks/Python.framework/Versions/3.11/bin/mcp_server_snowflake \
    --account  "$SNOWFLAKE_ACCOUNT" \
    --user     "$SNOWFLAKE_USER" \
    --password "$SNOWFLAKE_PASSWORD" \
    --database "$SNOWFLAKE_DATABASE" \
    --warehouse "$SNOWFLAKE_WAREHOUSE" \
    --role     "$SNOWFLAKE_ROLE" \
    --schema   "PUBLIC"
