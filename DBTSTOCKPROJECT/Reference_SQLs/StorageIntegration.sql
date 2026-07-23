--role and warehouse assignment
USE ROLE accountadmin;
USE WAREHOUSE snowflake_learning_wh;

-- database where data where land
CREATE DATABASE dbt_stockproject;
USE DATABASE dbt_stockproject;

SET user_name = current_user();
SET schema_name = CONCAT($user_name, '_LOAD_DATA_FROM_AMAZON_AWS');

--create table where data will land

CREATE OR REPLACE TABLE DBT_STOCKPROJECT.PUBLIC.RAW_STOCK_DATA (
    DATE DATE,
    OPEN FLOAT,
    HIGH FLOAT,
    LOW FLOAT,
    CLOSE FLOAT,
    ADJUSTED_CLOSE FLOAT,
    VOLUME NUMBER(38,0),
    DIVIDEND_AMOUNT FLOAT,
    SPLIT_COEFFICIENT FLOAT,
    TICKER VARCHAR(12)
)
COMMENT = 'Table to be loaded from S3 stockdata file';
--test table was created successfully
select * from raw_stock_data;

--table for company overview
CREATE OR REPLACE TABLE DBT_STOCKPROJECT.PUBLIC.RAW_COMPANY_OVERVIEW (
    RAW_PAYLOAD      VARIANT,
    METADATA_FILENAME STRING,
    METADATA_FILE_ROW_NUMBER NUMBER,
    LOAD_TIMESTAMP   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Table to be loaded from S3 companyoverview data';

--test that table was created correctly
select * from raw_company_overview;

SELECT
  RAW_PAYLOAD:"Symbol",
  RAW_PAYLOAD:"MarketCapitalization",
  RAW_PAYLOAD:"50DayMovingAverage"
FROM DBT_STOCKPROJECT.PUBLIC.RAW_COMPANY_OVERVIEW
LIMIT 5;

--create storage integration
CREATE OR REPLACE STORAGE INTEGRATION s3_stock_data_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::573509103721:role/snowflake_stockdata_integration'
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('s3://dbtstockprojectdata/stock_prices/','s3://dbtstockprojectdata/backfill/','s3://dbtstockprojectdata/company_overview_json/');

--test storage integration creation
DESCRIBE INTEGRATION s3_stock_data_integration;
SHOW INTEGRATIONS;

--create stage
CREATE OR REPLACE STAGE s3_dbtstockproject_stage_stockprices
STORAGE_INTEGRATION = s3_stock_data_integration
URL = 's3://dbtstockprojectdata/stock_prices/'
FILE_FORMAT = (TYPE = 'CSV',SKIP_HEADER=1);

CREATE OR REPLACE STAGE s3_dbtstockproject_stage_backfill
STORAGE_INTEGRATION = s3_stock_data_integration
URL = 's3://dbtstockprojectdata/stock_prices/'
FILE_FORMAT = (TYPE = 'CSV',SKIP_HEADER=1);

CREATE OR REPLACE STAGE s3_dbtstockproject_stage_companyoverview
STORAGE_INTEGRATION = s3_stock_data_integration
URL = 's3://dbtstockprojectdata/company_overview_json/'
FILE_FORMAT = (TYPE = 'JSON');

--test stage creation
SHOW STAGES;
USE ROLE ACCOUNTADMIN;
--load data from stage
COPY INTO DBT_STOCKPROJECT.PUBLIC.RAW_STOCK_DATA
FROM @DBT_STOCKPROJECT.PUBLIC.S3_DBTSTOCKPROJECT_STAGE_STOCKPRICES
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

COPY INTO DBT_STOCKPROJECT.PUBLIC.RAW_STOCK_DATA
FROM @DBT_STOCKPROJECT.PUBLIC.S3_DBTSTOCKPROJECT_STAGE_backfill
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

COPY INTO DBT_STOCKPROJECT.PUBLIC.RAW_COMPANY_OVERVIEW (
    RAW_PAYLOAD,
    METADATA_FILENAME,
    METADATA_FILE_ROW_NUMBER
)
FROM (
    SELECT
        $1                          AS RAW_PAYLOAD,
        METADATA$FILENAME           AS METADATA_FILENAME,
        METADATA$FILE_ROW_NUMBER    AS METADATA_FILE_ROW_NUMBER
    FROM @DBT_STOCKPROJECT.PUBLIC.S3_DBTSTOCKPROJECT_STAGE_companyoverview
)
FILE_FORMAT = (TYPE = JSON)
ON_ERROR = 'CONTINUE';

--create pipe for autoload of data as it hits S3
CREATE OR REPLACE PIPE dbt_stockproject.public.stock_price_pipe
AUTO_INGEST = TRUE
AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:573509103721:PIPE_StockData'
AS
COPY INTO dbt_stockproject.public.raw_stock_data
FROM @DBT_STOCKPROJECT.PUBLIC.S3_DBTSTOCKPROJECT_STAGE_STOCKPRICES
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

CREATE OR REPLACE PIPE dbt_stockproject.public.stock_backfill_pipe
AUTO_INGEST = TRUE
AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:573509103721:PIPE_StockData'
AS
COPY INTO dbt_stockproject.public.raw_stock_data
FROM @DBT_STOCKPROJECT.PUBLIC.S3_DBTSTOCKPROJECT_STAGE_backfill
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

CREATE OR REPLACE PIPE DBT_STOCKPROJECT.PUBLIC.PIPE_COMPANY_OVERVIEW
AUTO_INGEST = TRUE
AS
COPY INTO DBT_STOCKPROJECT.PUBLIC.RAW_COMPANY_OVERVIEW (
    RAW_PAYLOAD,
    METADATA_FILENAME,
    METADATA_FILE_ROW_NUMBER
)
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @DBT_STOCKPROJECT.PUBLIC.S3_DBTSTOCKPROJECT_STAGE_companyoverview
)
FILE_FORMAT = (TYPE = JSON)
ON_ERROR = 'CONTINUE';

--check creation of PIPES was correct
SHOW PIPES;

--security settings for PIPE
-- Create a role to contain the Snowpipe privileges
USE ROLE SECURITYADMIN;

CREATE OR REPLACE ROLE snowpipe_role;

-- Grant the required privileges on the database objects
GRANT USAGE ON DATABASE DBT_STOCKPROJECT TO ROLE snowpipe_role;

GRANT USAGE ON SCHEMA DBT_STOCKPROJECT.PUBLIC TO ROLE snowpipe_role;

GRANT INSERT, SELECT ON DBT_STOCKPROJECT.PUBLIC.RAW_STOCK_DATA TO ROLE snowpipe_role;

GRANT USAGE ON STAGE DBT_STOCKPROJECT.PUBLIC.S3_DBTSTOCKPROJECT_STAGE_STOCKPRICES TO ROLE snowpipe_role;
GRANT USAGE ON STAGE DBT_STOCKPROJECT.PUBLIC.S3_DBTSTOCKPROJECT_STAGE_BACKFILL TO ROLE snowpipe_role;

-- Pause the pipe for OWNERSHIP transfer
ALTER PIPE DBT_STOCKPROJECT.PUBLIC.STOCK_PRICE_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE DBT_STOCKPROJECT.PUBLIC.STOCK_BACKFILL_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;

-- Grant the OWNERSHIP privilege on the pipe object
GRANT OWNERSHIP ON PIPE DBT_STOCKPROJECT.PUBLIC.STOCK_PRICE_PIPE TO ROLE snowpipe_role;
GRANT OWNERSHIP ON PIPE DBT_STOCKPROJECT.PUBLIC.STOCK_BACKFILL_PIPE TO ROLE snowpipe_role;

-- Grant the role to a user
GRANT ROLE snowpipe_role TO USER JDSMITHWES;

-- Set the role as the default role for the user
ALTER USER JDSMITHWES SET DEFAULT_ROLE = snowpipe_role;

-- Resume the pipe
ALTER PIPE DBT_STOCKPROJECT.PUBLIC.STOCK_PRICE_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;
ALTER PIPE DBT_STOCKPROJECT.PUBLIC.STOCK_BACKFILL_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;

--Configure Event Notifications
SHOW PIPES;
select system$get_aws_sns_iam_policy('arn:aws:sns:us-east-1:573509103721:PIPE_StockData');