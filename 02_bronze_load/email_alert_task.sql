-- ============================================================
-- FILE NAME: email_alert_task.sql
-- Project : S&S Retail — End-to-End Snowflake Pipeline
-- Run this SIXTH and FINAL, after gold_task_rebuilt.sql
-- Sends an email to subscribers if both the primary (12:30pm IST)
-- and fallback (4:00pm IST) runs failed to load data today.
-- Runs as a predecessor of the fallback task so it always
-- checks the latest state after both attempts have had a chance.
--
-- PREREQUISITE: EMAIL_NOTIFICATIONS_TEAM1 integration must exist.
-- Run this as ACCOUNTADMIN first (or ask Siva to run it):
--
--   CREATE NOTIFICATION INTEGRATION IF NOT EXISTS EMAIL_NOTIFICATIONS_TEAM1
--       TYPE = EMAIL
--       ENABLED = TRUE;
--   GRANT USAGE ON INTEGRATION EMAIL_NOTIFICATIONS_TEAM1
--       TO ROLE SNOWFLAKE_LEARNING_ROLE
-- ============================================================

USE ROLE SNOWFLAKE_LEARNING_ROLE;
USE DATABASE DB_TEAM1;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

-- verify the integration exists and you have access before proceeding
SHOW INTEGRATIONS LIKE 'EMAIL_NOTIFICATIONS_TEAM1';

-- ============================================================
-- EDIT THIS LIST: add every subscriber email, comma-separated,
-- all inside the single pair of quotes, no spaces around commas
-- example: 'person1@impactanalytics.co,person2@impactanalytics.co'
-- ============================================================

CREATE OR REPLACE TASK DB_TEAM1.BRONZE.TASK_SEND_FAILURE_EMAIL
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
    COMMENT   = 'Sends email if both primary and fallback runs failed today'
    AFTER DB_TEAM1.BRONZE.TASK_FALLBACK_BRONZE_LOAD
AS
DECLARE
    v_any_success INTEGER DEFAULT 0;
    v_error_summary STRING DEFAULT '';
BEGIN
    -- check if either run succeeded today
    SELECT COUNT(*) INTO :v_any_success
    FROM DB_TEAM1.BRONZE.PIPELINE_LOG
    WHERE STATUS = 'SUCCESS'
      AND DATE(RUN_TIMESTAMP) = CURRENT_DATE();

    IF (:v_any_success > 0) THEN
        -- at least one run succeeded today, no email needed
        RETURN 'A run succeeded today, no alert needed';
    END IF;

    -- build the error summary from today's log rows
    SELECT LISTAGG(
        'Run type: ' || RUN_TYPE ||
        ' | Status: ' || STATUS ||
        ' | Error: ' || COALESCE(ERROR_MESSAGE, 'No new files found in GCS') ||
        ' | Time: ' || RUN_TIMESTAMP::STRING,
        CHAR(10)
    ) INTO :v_error_summary
    FROM DB_TEAM1.BRONZE.PIPELINE_LOG
    WHERE DATE(RUN_TIMESTAMP) = CURRENT_DATE();

    CALL SYSTEM$SEND_EMAIL(
        'EMAIL_NOTIFICATIONS_TEAM1',
        'name@comapnyid.co,name@comapnyid.co',
        'S&S Retail Pipeline FAILED - ' || CURRENT_DATE()::STRING,
        'Both the 12:30pm IST primary run and the 4:00pm IST fallback run failed to load new data today.' ||
        CHAR(10) || CHAR(10) ||
        'Details from DB_TEAM1.BRONZE.PIPELINE_LOG:' ||
        CHAR(10) || CHAR(10) ||
        :v_error_summary ||
        CHAR(10) || CHAR(10) ||
        'Please check the GCS bucket data-integration-scrape for missing files, ' ||
        'or query the PIPELINE_LOG table directly for full error details.'
    );
END;

ALTER TASK DB_TEAM1.BRONZE.TASK_SEND_FAILURE_EMAIL RESUME;

ALTER TASK DB_TEAM1.BRONZE.TASK_FALLBACK_BRONZE_LOAD RESUME;

-- verify the full graph
SHOW TASKS IN SCHEMA DB_TEAM1.BRONZE;