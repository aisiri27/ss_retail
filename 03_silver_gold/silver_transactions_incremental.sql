-- ============================================================
-- FILE NAME: silver_transactions_incremental.sql
-- Project : S&S Retail — End-to-End Snowflake Pipeline
-- Purpose : Converts TASK_SILVER_TRANSACTIONS from full rebuild
--           (CREATE OR REPLACE TABLE) to incremental MERGE.
--           Reads ONLY new rows from STREAM_TRANSACTIONS and
--           upserts them into the existing SILVER.TRANSACTIONS.
--
-- WHY: full rebuild re-processes all 17.5M rows daily.
--      Incremental processes only the new day's rows (~32K).
--
-- ROLLBACK: silver_tasks_fullrefresh_backup.sql restores the
--           original full-refresh version of all 4 Silver tasks.
--
-- KEY DESIGN POINTS:
--   - Source is the STREAM, not the Bronze table
--   - Stream offset advances ONLY if the MERGE commits
--     (task body = single MERGE = atomic; a failure leaves the
--      stream intact, so the next run retries the same rows)
--   - Dedup WITHIN the new batch via ROW_NUMBER (same keys as before)
--   - Collisions with EXISTING Silver rows handled by MERGE keys
--   - MERGE keys: TRANSACTION_ID + LINE_ID
-- ============================================================

USE ROLE SNOWFLAKE_LEARNING_ROLE;
USE DATABASE DB_TEAM1;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

-- ============================================================
-- STEP 1: SUSPEND THE GRAPH (root first, then affected tasks)
-- Required before any task in the graph can be replaced.
-- ============================================================
ALTER TASK DB_TEAM1.BRONZE.TASK_DAILY_BRONZE_LOAD    SUSPEND;
ALTER TASK DB_TEAM1.BRONZE.TASK_GOLD_REFRESH         SUSPEND;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_TRANSACTIONS  SUSPEND;

-- ============================================================
-- STEP 2: RECREATE THE TASK WITH INCREMENTAL MERGE LOGIC
-- Same trigger (AFTER primary + WHEN stream has data).
-- Body is now a single atomic MERGE instead of a full rebuild.
-- ============================================================
CREATE OR REPLACE TASK DB_TEAM1.BRONZE.TASK_SILVER_TRANSACTIONS
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
    COMMENT   = 'Incremental MERGE of new Bronze transactions into Silver (reads from stream)'
    AFTER DB_TEAM1.BRONZE.TASK_DAILY_BRONZE_LOAD
    WHEN SYSTEM$STREAM_HAS_DATA('DB_TEAM1.BRONZE.STREAM_TRANSACTIONS')
AS
MERGE INTO DB_TEAM1.SILVER.TRANSACTIONS AS TGT
USING (
    SELECT
        CHANNEL_ID,
        CHANNEL_DESC,
        TRANSACTION_ID,
        TRY_TO_DATE(TRANSACTION_DATE::STRING)           AS TRANSACTION_DATE,
        TRANSACTION_TYPE,
        LINE_ID::NUMBER                                 AS LINE_ID,
        SKU_ID,
        STORE_ID_ORIGIN,
        STORE_ID_FULLFILLED,
        ORDER_FULFILLMENT_METHOD,
        QUANTITY_SOLD::NUMBER                           AS QUANTITY_SOLD,
        CURRENT_MSRP::FLOAT                             AS CURRENT_MSRP,
        UNIT_RETAIL_PRICE::FLOAT                        AS UNIT_RETAIL_PRICE,
        DISCOUNT_AMOUNT::FLOAT                          AS DISCOUNT_AMOUNT,
        UNIT_NET_SELLING_PRICE::FLOAT                   AS UNIT_NET_SELLING_PRICE,
        TOTAL_EXTENDED_LINE_AMOUNT::FLOAT               AS TOTAL_EXTENDED_LINE_AMOUNT,
        UNIT_COST::FLOAT                                AS UNIT_COST,
        TOTAL_COST::FLOAT                               AS TOTAL_COST,
        PRICE_STATUS,
        CLEARANCE_INDICATOR,
        TRY_TO_DATE(LAUNCH_DATE::STRING)                AS LAUNCH_DATE,
        LAUNCH_PRICE::FLOAT                             AS LAUNCH_PRICE,
        -- IBM DB2 timestamp fix
        TRY_TO_TIMESTAMP(
            REPLACE(CREATED_TIMESTAMP, '-', ' '),
            'YYYY MM DD HH24.MI.SS.FF6'
        )                                               AS CREATED_TIMESTAMP,
        TRY_TO_TIMESTAMP(
            REPLACE(UPDATED_TIMESTAMP, '-', ' '),
            'YYYY MM DD HH24.MI.SS.FF6'
        )                                               AS UPDATED_TIMESTAMP,
        _SOURCE_FILE,
        _LOADED_AT,
        CURRENT_TIMESTAMP()                             AS _STAGED_AT,

        -- DQ FLAGS (same logic as full-refresh version)
        CASE WHEN TRANSACTION_ID IS NULL
            THEN 'FAIL' ELSE 'PASS' END                 AS DQ_TRANSACTION_ID,
        CASE WHEN SKU_ID IS NULL
            THEN 'FAIL' ELSE 'PASS' END                 AS DQ_SKU_ID,
        CASE WHEN STORE_ID_ORIGIN IS NULL
            THEN 'FAIL' ELSE 'PASS' END                 AS DQ_STORE_ID,
        CASE WHEN QUANTITY_SOLD IS NULL OR QUANTITY_SOLD::NUMBER < 0
            THEN 'FAIL' ELSE 'PASS' END                 AS DQ_QUANTITY,
        CASE WHEN TOTAL_EXTENDED_LINE_AMOUNT IS NULL
             OR TOTAL_EXTENDED_LINE_AMOUNT::FLOAT < 0
            THEN 'FAIL' ELSE 'PASS' END                 AS DQ_AMOUNT
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY TRANSACTION_ID, LINE_ID
                ORDER BY _LOADED_AT DESC
            ) AS RN
        FROM DB_TEAM1.BRONZE.STREAM_TRANSACTIONS   -- << THE STREAM, not the table
    )
    WHERE RN = 1
) AS SRC
ON  TGT.TRANSACTION_ID = SRC.TRANSACTION_ID
AND TGT.LINE_ID        = SRC.LINE_ID
WHEN MATCHED THEN UPDATE SET
    TGT.CHANNEL_ID                  = SRC.CHANNEL_ID,
    TGT.CHANNEL_DESC                = SRC.CHANNEL_DESC,
    TGT.TRANSACTION_DATE            = SRC.TRANSACTION_DATE,
    TGT.TRANSACTION_TYPE            = SRC.TRANSACTION_TYPE,
    TGT.SKU_ID                      = SRC.SKU_ID,
    TGT.STORE_ID_ORIGIN             = SRC.STORE_ID_ORIGIN,
    TGT.STORE_ID_FULLFILLED         = SRC.STORE_ID_FULLFILLED,
    TGT.ORDER_FULFILLMENT_METHOD    = SRC.ORDER_FULFILLMENT_METHOD,
    TGT.QUANTITY_SOLD               = SRC.QUANTITY_SOLD,
    TGT.CURRENT_MSRP                = SRC.CURRENT_MSRP,
    TGT.UNIT_RETAIL_PRICE           = SRC.UNIT_RETAIL_PRICE,
    TGT.DISCOUNT_AMOUNT             = SRC.DISCOUNT_AMOUNT,
    TGT.UNIT_NET_SELLING_PRICE      = SRC.UNIT_NET_SELLING_PRICE,
    TGT.TOTAL_EXTENDED_LINE_AMOUNT  = SRC.TOTAL_EXTENDED_LINE_AMOUNT,
    TGT.UNIT_COST                   = SRC.UNIT_COST,
    TGT.TOTAL_COST                  = SRC.TOTAL_COST,
    TGT.PRICE_STATUS                = SRC.PRICE_STATUS,
    TGT.CLEARANCE_INDICATOR         = SRC.CLEARANCE_INDICATOR,
    TGT.LAUNCH_DATE                 = SRC.LAUNCH_DATE,
    TGT.LAUNCH_PRICE                = SRC.LAUNCH_PRICE,
    TGT.CREATED_TIMESTAMP           = SRC.CREATED_TIMESTAMP,
    TGT.UPDATED_TIMESTAMP           = SRC.UPDATED_TIMESTAMP,
    TGT._SOURCE_FILE                = SRC._SOURCE_FILE,
    TGT._LOADED_AT                  = SRC._LOADED_AT,
    TGT._STAGED_AT                  = SRC._STAGED_AT,
    TGT.DQ_TRANSACTION_ID           = SRC.DQ_TRANSACTION_ID,
    TGT.DQ_SKU_ID                   = SRC.DQ_SKU_ID,
    TGT.DQ_STORE_ID                 = SRC.DQ_STORE_ID,
    TGT.DQ_QUANTITY                 = SRC.DQ_QUANTITY,
    TGT.DQ_AMOUNT                   = SRC.DQ_AMOUNT
WHEN NOT MATCHED THEN INSERT (
    CHANNEL_ID, CHANNEL_DESC, TRANSACTION_ID, TRANSACTION_DATE,
    TRANSACTION_TYPE, LINE_ID, SKU_ID, STORE_ID_ORIGIN,
    STORE_ID_FULLFILLED, ORDER_FULFILLMENT_METHOD, QUANTITY_SOLD,
    CURRENT_MSRP, UNIT_RETAIL_PRICE, DISCOUNT_AMOUNT,
    UNIT_NET_SELLING_PRICE, TOTAL_EXTENDED_LINE_AMOUNT,
    UNIT_COST, TOTAL_COST, PRICE_STATUS, CLEARANCE_INDICATOR,
    LAUNCH_DATE, LAUNCH_PRICE, CREATED_TIMESTAMP, UPDATED_TIMESTAMP,
    _SOURCE_FILE, _LOADED_AT, _STAGED_AT,
    DQ_TRANSACTION_ID, DQ_SKU_ID, DQ_STORE_ID, DQ_QUANTITY, DQ_AMOUNT
) VALUES (
    SRC.CHANNEL_ID, SRC.CHANNEL_DESC, SRC.TRANSACTION_ID, SRC.TRANSACTION_DATE,
    SRC.TRANSACTION_TYPE, SRC.LINE_ID, SRC.SKU_ID, SRC.STORE_ID_ORIGIN,
    SRC.STORE_ID_FULLFILLED, SRC.ORDER_FULFILLMENT_METHOD, SRC.QUANTITY_SOLD,
    SRC.CURRENT_MSRP, SRC.UNIT_RETAIL_PRICE, SRC.DISCOUNT_AMOUNT,
    SRC.UNIT_NET_SELLING_PRICE, SRC.TOTAL_EXTENDED_LINE_AMOUNT,
    SRC.UNIT_COST, SRC.TOTAL_COST, SRC.PRICE_STATUS, SRC.CLEARANCE_INDICATOR,
    SRC.LAUNCH_DATE, SRC.LAUNCH_PRICE, SRC.CREATED_TIMESTAMP, SRC.UPDATED_TIMESTAMP,
    SRC._SOURCE_FILE, SRC._LOADED_AT, SRC._STAGED_AT,
    SRC.DQ_TRANSACTION_ID, SRC.DQ_SKU_ID, SRC.DQ_STORE_ID, SRC.DQ_QUANTITY, SRC.DQ_AMOUNT
);

-- ============================================================
-- STEP 3: RESUME THE GRAPH (bottom-up: Gold, then Silver, then root)
-- ============================================================
ALTER TASK DB_TEAM1.BRONZE.TASK_GOLD_REFRESH         RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_TRANSACTIONS  RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_DAILY_BRONZE_LOAD    RESUME;

-- ============================================================
-- STEP 4: VERIFY
-- a) Task graph intact — all tasks started, Gold still shows
--    all 4 Silver tasks as predecessors
-- ============================================================
SHOW TASKS IN SCHEMA DB_TEAM1.BRONZE;

-- b) Baseline row count BEFORE any incremental run (record this)
SELECT COUNT(*) AS SILVER_TXN_ROWS,
       MAX(TRANSACTION_DATE) AS MAX_DATE
FROM DB_TEAM1.SILVER.TRANSACTIONS;