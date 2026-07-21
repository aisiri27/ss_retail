-- ============================================================
-- FILE NAME: stage_creation.sql
-- FILE 02: GCS STAGE
-- Project : S&S Retail — End-to-End Snowflake Pipeline
-- Run this SECOND, after definitions.sql
-- CHANGE: URL updated to root of bucket (was subfolder)
-- ============================================================

USE ROLE SNOWFLAKE_LEARNING_ROLE;
USE DATABASE DB_TEAM1;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;
USE SCHEMA DB_TEAM1.BRONZE;

CREATE OR REPLACE STAGE DB_TEAM1.BRONZE.GCS_RAW_STAGE
    URL = 'gcs://data-integration-scrape/'
    STORAGE_INTEGRATION = GCS_INT_TEAM1
    FILE_FORMAT = (
        TYPE                         = 'CSV'
        FIELD_DELIMITER              = '|'
        SKIP_HEADER                  = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF                      = ('', 'NULL', 'null')
        EMPTY_FIELD_AS_NULL          = TRUE
    );

-- verify
DESC STAGE DB_TEAM1.BRONZE.GCS_RAW_STAGE;
