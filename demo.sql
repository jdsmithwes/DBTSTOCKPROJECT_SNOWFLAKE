USE DATABASE DASH_DB_SI;
USE SCHEMA RETAIL;
CREATE OR REPLACE DYNAMIC TABLE enriched_marketing_intelligence
TARGET_LAG = '1 hours'
WAREHOUSE = dash_wh_si
AS
SELECT m.campaign_name, m.clicks, s.product AS product_name,
       SNOWFLAKE.CORTEX.SENTIMENT(s.transcript) AS avg_sentiment
FROM marketing_campaign_metrics m
JOIN support_cases s ON m.category = s.product;