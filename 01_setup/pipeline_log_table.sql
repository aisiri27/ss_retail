-- ============================================================
-- FILE NAME: pipeline_log_table.sql
-- Project : S&S Retail — End-to-End Snowflake Pipeline
-- Run this FIRST, before any of the task files below
-- Creates the audit table that tracks every pipeline run
-- ============================================================

USE ROLE SNOWFLAKE_LEARNING_ROLE;
USE DATABASE DB_TEAM1;
USE SCHEMA DB_TEAM1.BRONZE;

CREATE TABLE IF NOT EXISTS DB_TEAM1.BRONZE.PIPELINE_LOG (
    RUN_ID            NUMBER AUTOINCREMENT,
    RUN_TIMESTAMP     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RUN_TYPE          STRING,        -- 'PRIMARY' or 'FALLBACK'
    STATUS            STRING,        -- 'SUCCESS' or 'FAILED'
    TRANSACTIONS_ROWS NUMBER,
    INVENTORY_ROWS    NUMBER,
    PRODUCT_ROWS      NUMBER,
    STORE_ROWS        NUMBER,
    ERROR_MESSAGE     STRING
);

-- verify
DESCRIBE TABLE DB_TEAM1.BRONZE.PIPELINE_LOG;
SELECT COUNT(*) AS LOG_ROW_COUNT FROM DB_TEAM1.BRONZE.PIPELINE_LOG;
