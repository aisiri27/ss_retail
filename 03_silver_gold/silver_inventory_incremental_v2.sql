-- ============================================================
-- FILE NAME: silver_inventory_incremental_v2.sql
-- Project : S&S Retail — End-to-End Snowflake Pipeline
-- Purpose : Converts TASK_SILVER_INVENTORY from full rebuild
--           (~20 min/day, growing) to incremental MERGE v2.
--
-- WHY V2: the first MERGE attempt (2026-07-05) timed out at
--   3,600s because it joined the stream batch against ALL 1.4B
--   target rows. V2 adds DATE PRUNING: the join is restricted
--   to target partitions whose INVENTORY_DATE >= the minimum
--   date in the batch. Since INVENTORY_DATE is part of the
--   merge key, this cannot change results — it only lets
--   Snowflake skip ~470 days of partitions that cannot match.
--
-- SAFETY:
--   - Section 1 tests on a ZERO-COPY CLONE, simulating the
--     batch from Bronze (NOT the stream — reading the stream
--     in DML would consume it and starve the live task).
--   - Deploy (Section 2) only after Section 1 passes.
--   - Rollback: silver_tasks_fullrefresh_backup.sql
--
-- RUN ORDER TODAY:
--   Section 1 AFTER the 12:30pm IST run completes (~1:15pm),
--   Section 2 in the afternoon window (avoid 4:00pm moment),
--   Section 3 tomorrow after the first live v2 run.
-- ==========================================================


-- ============================================================
-- SECTION 2 — DEPLOY (only after Section 1 passes; avoid 4pm)
-- ============================================================
USE ROLE SNOWFLAKE_LEARNING_ROLE;
USE DATABASE DB_TEAM1;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

-- 1. suspend (root first)
ALTER TASK DB_TEAM1.BRONZE.TASK_DAILY_BRONZE_LOAD SUSPEND;
ALTER TASK DB_TEAM1.BRONZE.TASK_GOLD_REFRESH      SUSPEND;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_INVENTORY  SUSPEND;

-- 2. reset the stream — its 1.4B-row backlog is already in Silver
--    (full rebuild ran today 00:41), so discarding loses nothing
CREATE OR REPLACE STREAM DB_TEAM1.BRONZE.STREAM_INVENTORY
    ON TABLE DB_TEAM1.BRONZE.INVENTORY
    APPEND_ONLY = TRUE;

-- 3. deploy v2
CREATE OR REPLACE TASK DB_TEAM1.BRONZE.TASK_SILVER_INVENTORY
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
    COMMENT   = 'Incremental MERGE v2 of new Bronze inventory into Silver (stream + date pruning)'
    USER_TASK_TIMEOUT_MS = 7200000
    AFTER DB_TEAM1.BRONZE.TASK_DAILY_BRONZE_LOAD
    WHEN SYSTEM$STREAM_HAS_DATA('DB_TEAM1.BRONZE.STREAM_INVENTORY')
AS
DECLARE
    v_min_date DATE;
BEGIN
    SELECT MIN(TRY_TO_DATE(INVENTORY_DATE::STRING)) INTO :v_min_date
    FROM DB_TEAM1.BRONZE.STREAM_INVENTORY;

    MERGE INTO DB_TEAM1.SILVER.INVENTORY AS TGT
    USING (
        SELECT
            CHANNEL_ID,
            CHANNEL_DESC,
            SKU_ID,
            STORE_ID::NUMBER                    AS STORE_ID,
            TRY_TO_DATE(INVENTORY_DATE::STRING) AS INVENTORY_DATE,
            ONHAND_QTY::NUMBER                  AS ONHAND_QTY,
            INTRANSIT_QTY::NUMBER               AS INTRANSIT_QTY,
            ONORDER_QTY::NUMBER                 AS ONORDER_QTY,
            RECEIPTS_QTY::NUMBER                AS RECEIPTS_QTY,
            DROP_SHIP_QTY::NUMBER               AS DROP_SHIP_QTY,
            _SOURCE_FILE,
            _LOADED_AT,
            CURRENT_TIMESTAMP()                 AS _STAGED_AT,
            CASE WHEN SKU_ID IS NULL
                THEN 'FAIL' ELSE 'PASS' END     AS DQ_SKU_ID,
            CASE WHEN STORE_ID IS NULL
                THEN 'FAIL' ELSE 'PASS' END     AS DQ_STORE_ID,
            CASE WHEN INVENTORY_DATE IS NULL
                THEN 'FAIL' ELSE 'PASS' END     AS DQ_INVENTORY_DATE,
            CASE WHEN ONHAND_QTY::NUMBER < 0
                THEN 'FAIL' ELSE 'PASS' END     AS DQ_ONHAND_QTY
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY CHANNEL_ID, SKU_ID, STORE_ID, INVENTORY_DATE
                    ORDER BY _LOADED_AT DESC
                ) AS RN
            FROM DB_TEAM1.BRONZE.STREAM_INVENTORY
        )
        WHERE RN = 1
    ) AS SRC
    ON  TGT.INVENTORY_DATE >= :v_min_date
    AND TGT.CHANNEL_ID      = SRC.CHANNEL_ID
    AND TGT.SKU_ID          = SRC.SKU_ID
    AND TGT.STORE_ID        = SRC.STORE_ID
    AND TGT.INVENTORY_DATE  = SRC.INVENTORY_DATE
    WHEN MATCHED THEN UPDATE SET
        TGT.CHANNEL_DESC       = SRC.CHANNEL_DESC,
        TGT.ONHAND_QTY         = SRC.ONHAND_QTY,
        TGT.INTRANSIT_QTY      = SRC.INTRANSIT_QTY,
        TGT.ONORDER_QTY        = SRC.ONORDER_QTY,
        TGT.RECEIPTS_QTY       = SRC.RECEIPTS_QTY,
        TGT.DROP_SHIP_QTY      = SRC.DROP_SHIP_QTY,
        TGT._SOURCE_FILE       = SRC._SOURCE_FILE,
        TGT._LOADED_AT         = SRC._LOADED_AT,
        TGT._STAGED_AT         = SRC._STAGED_AT,
        TGT.DQ_SKU_ID          = SRC.DQ_SKU_ID,
        TGT.DQ_STORE_ID        = SRC.DQ_STORE_ID,
        TGT.DQ_INVENTORY_DATE  = SRC.DQ_INVENTORY_DATE,
        TGT.DQ_ONHAND_QTY      = SRC.DQ_ONHAND_QTY
    WHEN NOT MATCHED THEN INSERT (
        CHANNEL_ID, CHANNEL_DESC, SKU_ID, STORE_ID, INVENTORY_DATE,
        ONHAND_QTY, INTRANSIT_QTY, ONORDER_QTY, RECEIPTS_QTY, DROP_SHIP_QTY,
        _SOURCE_FILE, _LOADED_AT, _STAGED_AT,
        DQ_SKU_ID, DQ_STORE_ID, DQ_INVENTORY_DATE, DQ_ONHAND_QTY
    ) VALUES (
        SRC.CHANNEL_ID, SRC.CHANNEL_DESC, SRC.SKU_ID, SRC.STORE_ID, SRC.INVENTORY_DATE,
        SRC.ONHAND_QTY, SRC.INTRANSIT_QTY, SRC.ONORDER_QTY, SRC.RECEIPTS_QTY, SRC.DROP_SHIP_QTY,
        SRC._SOURCE_FILE, SRC._LOADED_AT, SRC._STAGED_AT,
        SRC.DQ_SKU_ID, SRC.DQ_STORE_ID, SRC.DQ_INVENTORY_DATE, SRC.DQ_ONHAND_QTY
    );
END;

-- 4. resume bottom-up
ALTER TASK DB_TEAM1.BRONZE.TASK_GOLD_REFRESH      RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_INVENTORY  RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_DAILY_BRONZE_LOAD RESUME;

-- 5. verify
SHOW TASKS IN SCHEMA DB_TEAM1.BRONZE;
SELECT SYSTEM$STREAM_HAS_DATA('DB_TEAM1.BRONZE.STREAM_INVENTORY') AS INV_STREAM;  -- expect FALSE

-- ============================================================
-- SECTION 3 — POST-DEPLOY VALIDATION (tomorrow, after 12:30 run)
-- ============================================================

-- task succeeded, and note DURATION (expect minutes, not 60)
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME,
       DATEDIFF('second', SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SEC,
       ERROR_MESSAGE
FROM TABLE(DB_TEAM1.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -4, CURRENT_TIMESTAMP())
))
WHERE NAME = 'TASK_SILVER_INVENTORY'
ORDER BY SCHEDULED_TIME DESC;

-- data advanced and stayed clean
SELECT MAX(INVENTORY_DATE) AS MAX_INV, COUNT(*) AS TOTAL_ROWS
FROM DB_TEAM1.SILVER.INVENTORY;

SELECT CHANNEL_ID, SKU_ID, STORE_ID, INVENTORY_DATE, COUNT(*)
FROM DB_TEAM1.SILVER.INVENTORY
GROUP BY 1,2,3,4
HAVING COUNT(*) > 1
LIMIT 5;

-- stream consumed? (FALSE = the MERGE committed and advanced it)
SELECT SYSTEM$STREAM_HAS_DATA('DB_TEAM1.BRONZE.STREAM_INVENTORY') AS INV_STREAM;

-- once all pass, clean up the test clone:
-- DROP TABLE IF EXISTS DB_TEAM1.SILVER.INVENTORY_TEST;