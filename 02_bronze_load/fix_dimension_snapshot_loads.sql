-- ============================================================
-- FILE NAME: fix_dimension_snapshot_loads.sql
-- Project : S&S Retail — End-to-End Snowflake Pipeline
-- Date    : 2026-07-07
--
-- BUG BEING FIXED (found by teammate review):
--   Bronze STORE and PRODUCT were reloading ALL ~483 historical
--   files every day, not just the daily snapshot. Root cause:
--   TRUNCATE TABLE also erases the table's COPY load history,
--   so the broad PATTERN ('20[0-9][0-9]-.*/Store/...') matched
--   and re-ingested the entire bucket history daily.
--   Consequences:
--     - Bronze STORE = 73K rows (483 days x ~154), PRODUCT = 229M
--     - ~6 early files (Mar 2025) have an OLD 27-column schema
--       (no Perimeter). Their shifted rows put raw timestamps in
--       PERIMETER and NULL in UPDATED_TIMESTAMP.
--     - All rows share one _LOADED_AT, so Silver's dedup
--       tie-breaks arbitrarily -> Silver kept stale/shifted rows
--       (194 stores incl. 40 historical; wobbling Aperto count).
--
-- THE FIX:
--   Load ONLY the current day's file for STORE and PRODUCT,
--   with a guard: if today's file is missing, skip the truncate
--   and keep yesterday's snapshot (prevents an empty dimension).
--   Transactions/Inventory are untouched (no truncate -> load
--   history intact -> they were never affected).
--
-- RUN ORDER:
--   SECTION 1  one-time cleanup of Bronze+Silver STORE/PRODUCT (today)
--   SECTION 2  replace TASK_DAILY_BRONZE_LOAD (primary)
--   SECTION 3  replace TASK_FALLBACK_BRONZE_LOAD (fallback)
--   SECTION 4  validation
--   Avoid 12:25-13:00 and ~16:00 IST windows.
-- ============================================================

USE ROLE SNOWFLAKE_LEARNING_ROLE;
USE DATABASE DB_TEAM1;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;


-- ============================================================
-- SECTION 1 — ONE-TIME CLEANUP (fixes today's data right now)
-- ============================================================

-- 1a. confirm today's files exist before touching anything
LIST @DB_TEAM1.BRONZE.GCS_RAW_STAGE PATTERN = '2026-07-07/Store/Store_20260707\.csv';
LIST @DB_TEAM1.BRONZE.GCS_RAW_STAGE PATTERN = '2026-07-07/Product/Product_20260707\.csv';
-- Both must return exactly 1 row. If not, STOP and adjust the date.

-- 1b. reload Bronze STORE from today's file only
TRUNCATE TABLE DB_TEAM1.BRONZE.STORE;
COPY INTO DB_TEAM1.BRONZE.STORE (
    CHANNEL_DESC, CHANNEL_ID, STORE_ID, STORE_NAME, STORE_STATUS,
    STORE_OPEN_DATE, STORE_CLOSE_DATE, STORE_TYPE, LONGITUDE, LATITUDE,
    COUNTRY, DISTRICT, REGION, CITY, STATE, ZIPCODE,
    LOCATION_TYPE, CLIMATE, TOTAL_STORE_AREA, STORE_SELLING_AREA,
    TRAFFIC, LIKE_STORE_ID, STORE_TIER_GRADE, COMP_DATE,
    COMP_STATUS, PERIMETER, CREATED_TIMESTAMP, UPDATED_TIMESTAMP,
    _SOURCE_FILE
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
           $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
           $21,$22,$23,$24,$25,$26,$27,$28, METADATA$FILENAME
    FROM @DB_TEAM1.BRONZE.GCS_RAW_STAGE
)
PATTERN = '2026-07-07/Store/Store_20260707\.csv'
FILE_FORMAT = (
    TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE
)
ON_ERROR = 'CONTINUE';

SELECT COUNT(*) AS BRONZE_STORE_NOW FROM DB_TEAM1.BRONZE.STORE;  -- expect ~154

-- 1c. reload Bronze PRODUCT from today's file only
TRUNCATE TABLE DB_TEAM1.BRONZE.PRODUCT;
COPY INTO DB_TEAM1.BRONZE.PRODUCT (
    DIVISION_CODE, DIVISION_DESC, DEPARTMENT_CODE, DEPARTMENT_DESC,
    CLASS_CODE, CLASS_DESC, SUBCLASS_CODE, SUBCLASS_DESC,
    VENDOR_ID, VENDOR_NAME, STYLE_ID, STYLE_DESC, COLOR_ID,
    COLOR_DESC, COLOR_FAMILY_DESC, SIZE, SKU_ID, PRODUCT_STATUS,
    UNIT_RETAIL_PRICE, UNIT_LAUNCH_PRICE, CURRENT_MSRP, UNIT_COST,
    LAUNCH_DATE, SEASON_CODE, SEASON_CODE_DESC, FASHION_GRADE,
    EAN, CARRYOVER, COLLECTION, PRICE_STATUS, EXIT_DATE,
    BRAND, MATERIAL, PRODUCT_LIFE_CYCLE,
    CREATED_TIMESTAMP, UPDATED_TIMESTAMP, _SOURCE_FILE
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
           $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
           $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
           $31,$32,$33,$34,$35,$36, METADATA$FILENAME
    FROM @DB_TEAM1.BRONZE.GCS_RAW_STAGE
)
PATTERN = '2026-07-07/Product/Product_20260707\.csv'
FILE_FORMAT = (
    TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE
)
ON_ERROR = 'CONTINUE';

SELECT COUNT(*) AS BRONZE_PRODUCT_NOW FROM DB_TEAM1.BRONZE.PRODUCT;  -- one day's catalog

-- 1d. rebuild Silver STORE and PRODUCT from the clean snapshots
--     (same definitions as the live tasks; safe to run manually)
CREATE OR REPLACE TABLE DB_TEAM1.SILVER.STORE AS
SELECT
    CHANNEL_DESC, CHANNEL_ID,
    STORE_ID::NUMBER                        AS STORE_ID,
    STORE_NAME, STORE_STATUS,
    TRY_TO_DATE(STORE_OPEN_DATE::STRING)    AS STORE_OPEN_DATE,
    TRY_TO_DATE(STORE_CLOSE_DATE::STRING)   AS STORE_CLOSE_DATE,
    STORE_TYPE,
    LONGITUDE::FLOAT                        AS LONGITUDE,
    LATITUDE::FLOAT                         AS LATITUDE,
    COUNTRY, DISTRICT, REGION, CITY, STATE, ZIPCODE,
    LOCATION_TYPE, CLIMATE,
    TOTAL_STORE_AREA::NUMBER                AS TOTAL_STORE_AREA,
    STORE_SELLING_AREA::NUMBER              AS STORE_SELLING_AREA,
    TRAFFIC::NUMBER                         AS TRAFFIC,
    LIKE_STORE_ID, STORE_TIER_GRADE,
    TRY_TO_DATE(COMP_DATE::STRING)          AS COMP_DATE,
    COMP_STATUS, PERIMETER,
    TRY_TO_TIMESTAMP(REPLACE(CREATED_TIMESTAMP, '-', ' '), 'YYYY MM DD HH24.MI.SS.FF6') AS CREATED_TIMESTAMP,
    TRY_TO_TIMESTAMP(REPLACE(UPDATED_TIMESTAMP, '-', ' '), 'YYYY MM DD HH24.MI.SS.FF6') AS UPDATED_TIMESTAMP,
    _SOURCE_FILE, _LOADED_AT,
    CURRENT_TIMESTAMP()                     AS _STAGED_AT,
    CASE WHEN STORE_ID IS NULL THEN 'FAIL' ELSE 'PASS' END        AS DQ_STORE_ID,
    CASE WHEN STORE_NAME IS NULL THEN 'FAIL' ELSE 'PASS' END      AS DQ_STORE_NAME,
    CASE WHEN STORE_STATUS IS NULL THEN 'FAIL' ELSE 'PASS' END    AS DQ_STORE_STATUS,
    CASE WHEN STORE_OPEN_DATE IS NULL THEN 'FAIL' ELSE 'PASS' END AS DQ_OPEN_DATE
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY STORE_ID ORDER BY _LOADED_AT DESC) AS RN
    FROM DB_TEAM1.BRONZE.STORE
)
WHERE RN = 1;

CREATE OR REPLACE TABLE DB_TEAM1.SILVER.PRODUCT AS
SELECT
    DIVISION_CODE, DIVISION_DESC, DEPARTMENT_CODE, DEPARTMENT_DESC,
    CLASS_CODE, CLASS_DESC, SUBCLASS_CODE, SUBCLASS_DESC,
    VENDOR_ID, VENDOR_NAME, STYLE_ID, STYLE_DESC, COLOR_ID,
    COLOR_DESC, COLOR_FAMILY_DESC, SIZE, SKU_ID, PRODUCT_STATUS,
    UNIT_RETAIL_PRICE::FLOAT            AS UNIT_RETAIL_PRICE,
    UNIT_LAUNCH_PRICE::FLOAT            AS UNIT_LAUNCH_PRICE,
    CURRENT_MSRP::FLOAT                 AS CURRENT_MSRP,
    UNIT_COST::FLOAT                    AS UNIT_COST,
    TRY_TO_DATE(LAUNCH_DATE::STRING)    AS LAUNCH_DATE,
    SEASON_CODE, SEASON_CODE_DESC, FASHION_GRADE, EAN,
    CARRYOVER, COLLECTION, PRICE_STATUS,
    TRY_TO_DATE(EXIT_DATE::STRING)      AS EXIT_DATE,
    BRAND, MATERIAL, PRODUCT_LIFE_CYCLE,
    TRY_TO_TIMESTAMP(REPLACE(CREATED_TIMESTAMP, '-', ' '), 'YYYY MM DD HH24.MI.SS.FF6') AS CREATED_TIMESTAMP,
    TRY_TO_TIMESTAMP(REPLACE(UPDATED_TIMESTAMP, '-', ' '), 'YYYY MM DD HH24.MI.SS.FF6') AS UPDATED_TIMESTAMP,
    _SOURCE_FILE, _LOADED_AT,
    CURRENT_TIMESTAMP()                 AS _STAGED_AT,
    CASE WHEN SKU_ID IS NULL THEN 'FAIL' ELSE 'PASS' END            AS DQ_SKU_ID,
    CASE WHEN PRODUCT_STATUS IS NULL THEN 'FAIL' ELSE 'PASS' END    AS DQ_PRODUCT_STATUS,
    CASE WHEN UNIT_RETAIL_PRICE IS NULL OR UNIT_RETAIL_PRICE::FLOAT < 0 THEN 'FAIL' ELSE 'PASS' END AS DQ_PRICE
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY SKU_ID ORDER BY _LOADED_AT DESC) AS RN
    FROM DB_TEAM1.BRONZE.PRODUCT
)
WHERE RN = 1;

-- 1e. quick checks (full validation in SECTION 4)
SELECT COUNT(*) AS SILVER_STORE FROM DB_TEAM1.SILVER.STORE;              -- expect 154
SELECT STORE_STATUS, COUNT(*) FROM DB_TEAM1.SILVER.STORE GROUP BY 1;     -- stable split
SELECT PERIMETER, COUNT(*) FROM DB_TEAM1.SILVER.STORE GROUP BY 1;        -- LFL / New Opening etc, NO timestamps

-- NOTE: Gold still holds yesterday's store/product attributes until its
-- next refresh. Either let tomorrow's 12:30 run refresh it, or manually
-- run the three Gold CREATE OR REPLACE statements from gold_task_rebuilt.sql
-- (~20 min) if you want the dashboard exact today.


-- ============================================================
-- FILE NAME: fix_dimension_snapshot_loads_v2.sql
-- Project : S&S Retail — End-to-End Snowflake Pipeline
--
-- WHY v2 OF THE FIX: the first version checked "does today's file
-- exist?" using  LIST + RESULT_SCAN(LAST_QUERY_ID()) . That is
-- unreliable inside a task body (RESULT_SCAN of a LIST does not
-- register predictably; LAST_QUERY_ID can point at the wrong
-- statement). If it misfired, the guard could skip the load and
-- leave STORE stale, or error into the handler.
--
-- ROBUST APPROACH (staging swap):
--   1. COPY today's file into a STAGING table (a plain table).
--   2. Check COUNT(*) on that staging table -- ordinary SQL,
--      always works in a task.
--   3. Only if staging has rows: swap it into the real Bronze
--      table (TRUNCATE + INSERT). If staging is empty (file
--      missing/late), leave Bronze untouched -> keep yesterday's
--      snapshot. Bronze is NEVER left empty.
--
-- Net effect identical to intended v1: dims load ONLY the current
-- day's file, and a missing file can never blank the dimension.
--
-- Transactions/Inventory are unchanged (incremental; never
-- truncated; the accumulation bug never affected them).
-- ============================================================

USE ROLE SNOWFLAKE_LEARNING_ROLE;
USE DATABASE DB_TEAM1;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;


-- ============================================================
-- SECTION 2 — REPLACE THE PRIMARY LOAD TASK
-- ============================================================

ALTER TASK DB_TEAM1.BRONZE.TASK_DAILY_BRONZE_LOAD    SUSPEND;
ALTER TASK DB_TEAM1.BRONZE.TASK_GOLD_REFRESH         SUSPEND;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_TRANSACTIONS  SUSPEND;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_INVENTORY     SUSPEND;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_PRODUCT       SUSPEND;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_STORE         SUSPEND;

CREATE OR REPLACE TASK DB_TEAM1.BRONZE.TASK_DAILY_BRONZE_LOAD
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
    SCHEDULE  = 'USING CRON 30 12 * * * Asia/Kolkata'
    COMMENT   = 'Primary daily load 12:30pm IST. Dims (Product/Store) load ONLY the current-day file via staging swap; missing file keeps prior snapshot.'
AS
DECLARE
    v_error         STRING  DEFAULT NULL;
    v_ist_date      DATE;
    v_folder        STRING;
    v_compact       STRING;
    v_store_pattern STRING;
    v_prod_pattern  STRING;
    v_store_rows    INTEGER DEFAULT 0;
    v_prod_rows     INTEGER DEFAULT 0;
BEGIN
    v_ist_date      := CONVERT_TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP())::DATE;
    v_folder        := TO_CHAR(:v_ist_date, 'YYYY-MM-DD');
    v_compact       := TO_CHAR(:v_ist_date, 'YYYYMMDD');
    v_store_pattern := :v_folder || '/Store/Store_'     || :v_compact || '\\.csv';
    v_prod_pattern  := :v_folder || '/Product/Product_' || :v_compact || '\\.csv';

    BEGIN
        -- ---------- transactions (incremental; unchanged) ----------
        COPY INTO DB_TEAM1.BRONZE.TRANSACTIONS (
            CHANNEL_ID, CHANNEL_DESC, TRANSACTION_ID, TRANSACTION_DATE,
            TRANSACTION_TYPE, LINE_ID, SKU_ID, STORE_ID_ORIGIN,
            STORE_ID_FULLFILLED, ORDER_FULFILLMENT_METHOD, QUANTITY_SOLD,
            CURRENT_MSRP, UNIT_RETAIL_PRICE, DISCOUNT_AMOUNT,
            UNIT_NET_SELLING_PRICE, TOTAL_EXTENDED_LINE_AMOUNT,
            UNIT_COST, TOTAL_COST, PRICE_STATUS, CLEARANCE_INDICATOR,
            LAUNCH_DATE, LAUNCH_PRICE, CREATED_TIMESTAMP, UPDATED_TIMESTAMP,
            _SOURCE_FILE
        )
        FROM (
            SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                   $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                   $21,$22,$23,$24, METADATA$FILENAME
            FROM @DB_TEAM1.BRONZE.GCS_RAW_STAGE
        )
        PATTERN = '20[0-9][0-9]-.*/Transactions/Transactions_.*\.csv'
        FILE_FORMAT = (
            TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE
        )
        ON_ERROR = 'CONTINUE';

        -- ---------- inventory (incremental; unchanged) ----------
        COPY INTO DB_TEAM1.BRONZE.INVENTORY (
            CHANNEL_ID, CHANNEL_DESC, SKU_ID, STORE_ID, INVENTORY_DATE,
            ONHAND_QTY, INTRANSIT_QTY, ONORDER_QTY, RECEIPTS_QTY,
            DROP_SHIP_QTY, _SOURCE_FILE
        )
        FROM (
            SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10, METADATA$FILENAME
            FROM @DB_TEAM1.BRONZE.GCS_RAW_STAGE
        )
        PATTERN = '20[0-9][0-9]-.*/Inventory/Inventory_.*\.csv'
        FILE_FORMAT = (
            TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE
        )
        ON_ERROR = 'CONTINUE';

        -- ---------- product (staging swap: today's file only) ----------
        -- staging table mirrors Bronze PRODUCT structure, starts empty
        CREATE OR REPLACE TABLE DB_TEAM1.BRONZE.PRODUCT_STAGING
            LIKE DB_TEAM1.BRONZE.PRODUCT;

        EXECUTE IMMEDIATE $$
            COPY INTO DB_TEAM1.BRONZE.PRODUCT_STAGING (
                DIVISION_CODE, DIVISION_DESC, DEPARTMENT_CODE, DEPARTMENT_DESC,
                CLASS_CODE, CLASS_DESC, SUBCLASS_CODE, SUBCLASS_DESC,
                VENDOR_ID, VENDOR_NAME, STYLE_ID, STYLE_DESC, COLOR_ID,
                COLOR_DESC, COLOR_FAMILY_DESC, SIZE, SKU_ID, PRODUCT_STATUS,
                UNIT_RETAIL_PRICE, UNIT_LAUNCH_PRICE, CURRENT_MSRP, UNIT_COST,
                LAUNCH_DATE, SEASON_CODE, SEASON_CODE_DESC, FASHION_GRADE,
                EAN, CARRYOVER, COLLECTION, PRICE_STATUS, EXIT_DATE,
                BRAND, MATERIAL, PRODUCT_LIFE_CYCLE,
                CREATED_TIMESTAMP, UPDATED_TIMESTAMP, _SOURCE_FILE
            )
            FROM (
                SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                       $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                       $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
                       $31,$32,$33,$34,$35,$36, METADATA$FILENAME
                FROM @DB_TEAM1.BRONZE.GCS_RAW_STAGE
            )
            PATTERN = '$$ || :v_prod_pattern || $$'
            FILE_FORMAT = (
                TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE
            )
            ON_ERROR = 'CONTINUE'
        $$;

        -- plain COUNT on a real table: always reliable
        SELECT COUNT(*) INTO :v_prod_rows FROM DB_TEAM1.BRONZE.PRODUCT_STAGING;

        -- swap in ONLY if today's file actually produced rows
        IF (:v_prod_rows > 0) THEN
            TRUNCATE TABLE DB_TEAM1.BRONZE.PRODUCT;
            INSERT INTO DB_TEAM1.BRONZE.PRODUCT
                SELECT * FROM DB_TEAM1.BRONZE.PRODUCT_STAGING;
        END IF;  -- else keep yesterday's snapshot untouched

        -- ---------- store (staging swap: today's file only) ----------
        CREATE OR REPLACE TABLE DB_TEAM1.BRONZE.STORE_STAGING
            LIKE DB_TEAM1.BRONZE.STORE;

        EXECUTE IMMEDIATE $$
            COPY INTO DB_TEAM1.BRONZE.STORE_STAGING (
                CHANNEL_DESC, CHANNEL_ID, STORE_ID, STORE_NAME, STORE_STATUS,
                STORE_OPEN_DATE, STORE_CLOSE_DATE, STORE_TYPE, LONGITUDE, LATITUDE,
                COUNTRY, DISTRICT, REGION, CITY, STATE, ZIPCODE,
                LOCATION_TYPE, CLIMATE, TOTAL_STORE_AREA, STORE_SELLING_AREA,
                TRAFFIC, LIKE_STORE_ID, STORE_TIER_GRADE, COMP_DATE,
                COMP_STATUS, PERIMETER, CREATED_TIMESTAMP, UPDATED_TIMESTAMP,
                _SOURCE_FILE
            )
            FROM (
                SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                       $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                       $21,$22,$23,$24,$25,$26,$27,$28, METADATA$FILENAME
                FROM @DB_TEAM1.BRONZE.GCS_RAW_STAGE
            )
            PATTERN = '$$ || :v_store_pattern || $$'
            FILE_FORMAT = (
                TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE
            )
            ON_ERROR = 'CONTINUE'
        $$;

        SELECT COUNT(*) INTO :v_store_rows FROM DB_TEAM1.BRONZE.STORE_STAGING;

        IF (:v_store_rows > 0) THEN
            TRUNCATE TABLE DB_TEAM1.BRONZE.STORE;
            INSERT INTO DB_TEAM1.BRONZE.STORE
                SELECT * FROM DB_TEAM1.BRONZE.STORE_STAGING;
        END IF;

    EXCEPTION
        WHEN OTHER THEN
            v_error := SQLERRM;
    END;

    INSERT INTO DB_TEAM1.BRONZE.PIPELINE_LOG (
        RUN_TYPE, STATUS, TRANSACTIONS_ROWS, INVENTORY_ROWS,
        PRODUCT_ROWS, STORE_ROWS, ERROR_MESSAGE
    )
    SELECT
        'PRIMARY',
        CASE WHEN :v_error IS NOT NULL THEN 'FAILED' ELSE 'SUCCESS' END,
        (SELECT COUNT(*) FROM DB_TEAM1.BRONZE.TRANSACTIONS),
        (SELECT COUNT(*) FROM DB_TEAM1.BRONZE.INVENTORY),
        (SELECT COUNT(*) FROM DB_TEAM1.BRONZE.PRODUCT),
        (SELECT COUNT(*) FROM DB_TEAM1.BRONZE.STORE),
        :v_error;
END;


-- ============================================================
-- SECTION 3 — REPLACE THE FALLBACK LOAD TASK (same staging logic)
-- ============================================================

ALTER TASK DB_TEAM1.BRONZE.TASK_FALLBACK_BRONZE_LOAD SUSPEND;
ALTER TASK DB_TEAM1.BRONZE.TASK_SEND_FAILURE_EMAIL   SUSPEND;

CREATE OR REPLACE TASK DB_TEAM1.BRONZE.TASK_FALLBACK_BRONZE_LOAD
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
    SCHEDULE  = 'USING CRON 0 16 * * * Asia/Kolkata'
    COMMENT   = 'Fallback 4pm IST if primary failed. Dims load ONLY current-day file via staging swap; missing file keeps prior snapshot.'
AS
DECLARE
    v_error              STRING  DEFAULT NULL;
    v_already_succeeded  INTEGER DEFAULT 0;
    v_ist_date           DATE;
    v_folder             STRING;
    v_compact            STRING;
    v_store_pattern      STRING;
    v_prod_pattern       STRING;
    v_store_rows         INTEGER DEFAULT 0;
    v_prod_rows          INTEGER DEFAULT 0;
BEGIN
    SELECT COUNT(*) INTO :v_already_succeeded
    FROM DB_TEAM1.BRONZE.PIPELINE_LOG
    WHERE RUN_TYPE = 'PRIMARY' AND STATUS = 'SUCCESS'
      AND DATE(RUN_TIMESTAMP) = CURRENT_DATE();

    IF (:v_already_succeeded > 0) THEN
        RETURN 'Primary succeeded today, fallback skipped';
    END IF;

    v_ist_date      := CONVERT_TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP())::DATE;
    v_folder        := TO_CHAR(:v_ist_date, 'YYYY-MM-DD');
    v_compact       := TO_CHAR(:v_ist_date, 'YYYYMMDD');
    v_store_pattern := :v_folder || '/Store/Store_'     || :v_compact || '\\.csv';
    v_prod_pattern  := :v_folder || '/Product/Product_' || :v_compact || '\\.csv';

    BEGIN
        COPY INTO DB_TEAM1.BRONZE.TRANSACTIONS (
            CHANNEL_ID, CHANNEL_DESC, TRANSACTION_ID, TRANSACTION_DATE,
            TRANSACTION_TYPE, LINE_ID, SKU_ID, STORE_ID_ORIGIN,
            STORE_ID_FULLFILLED, ORDER_FULFILLMENT_METHOD, QUANTITY_SOLD,
            CURRENT_MSRP, UNIT_RETAIL_PRICE, DISCOUNT_AMOUNT,
            UNIT_NET_SELLING_PRICE, TOTAL_EXTENDED_LINE_AMOUNT,
            UNIT_COST, TOTAL_COST, PRICE_STATUS, CLEARANCE_INDICATOR,
            LAUNCH_DATE, LAUNCH_PRICE, CREATED_TIMESTAMP, UPDATED_TIMESTAMP,
            _SOURCE_FILE
        )
        FROM (
            SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                   $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                   $21,$22,$23,$24, METADATA$FILENAME
            FROM @DB_TEAM1.BRONZE.GCS_RAW_STAGE
        )
        PATTERN = '20[0-9][0-9]-.*/Transactions/Transactions_.*\.csv'
        FILE_FORMAT = (
            TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE
        )
        ON_ERROR = 'CONTINUE';

        COPY INTO DB_TEAM1.BRONZE.INVENTORY (
            CHANNEL_ID, CHANNEL_DESC, SKU_ID, STORE_ID, INVENTORY_DATE,
            ONHAND_QTY, INTRANSIT_QTY, ONORDER_QTY, RECEIPTS_QTY,
            DROP_SHIP_QTY, _SOURCE_FILE
        )
        FROM (
            SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10, METADATA$FILENAME
            FROM @DB_TEAM1.BRONZE.GCS_RAW_STAGE
        )
        PATTERN = '20[0-9][0-9]-.*/Inventory/Inventory_.*\.csv'
        FILE_FORMAT = (
            TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE
        )
        ON_ERROR = 'CONTINUE';

        -- product staging swap
        CREATE OR REPLACE TABLE DB_TEAM1.BRONZE.PRODUCT_STAGING
            LIKE DB_TEAM1.BRONZE.PRODUCT;
        EXECUTE IMMEDIATE $$
            COPY INTO DB_TEAM1.BRONZE.PRODUCT_STAGING (
                DIVISION_CODE, DIVISION_DESC, DEPARTMENT_CODE, DEPARTMENT_DESC,
                CLASS_CODE, CLASS_DESC, SUBCLASS_CODE, SUBCLASS_DESC,
                VENDOR_ID, VENDOR_NAME, STYLE_ID, STYLE_DESC, COLOR_ID,
                COLOR_DESC, COLOR_FAMILY_DESC, SIZE, SKU_ID, PRODUCT_STATUS,
                UNIT_RETAIL_PRICE, UNIT_LAUNCH_PRICE, CURRENT_MSRP, UNIT_COST,
                LAUNCH_DATE, SEASON_CODE, SEASON_CODE_DESC, FASHION_GRADE,
                EAN, CARRYOVER, COLLECTION, PRICE_STATUS, EXIT_DATE,
                BRAND, MATERIAL, PRODUCT_LIFE_CYCLE,
                CREATED_TIMESTAMP, UPDATED_TIMESTAMP, _SOURCE_FILE
            )
            FROM (
                SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                       $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                       $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
                       $31,$32,$33,$34,$35,$36, METADATA$FILENAME
                FROM @DB_TEAM1.BRONZE.GCS_RAW_STAGE
            )
            PATTERN = '$$ || :v_prod_pattern || $$'
            FILE_FORMAT = (
                TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE
            )
            ON_ERROR = 'CONTINUE'
        $$;
        SELECT COUNT(*) INTO :v_prod_rows FROM DB_TEAM1.BRONZE.PRODUCT_STAGING;
        IF (:v_prod_rows > 0) THEN
            TRUNCATE TABLE DB_TEAM1.BRONZE.PRODUCT;
            INSERT INTO DB_TEAM1.BRONZE.PRODUCT
                SELECT * FROM DB_TEAM1.BRONZE.PRODUCT_STAGING;
        END IF;

        -- store staging swap
        CREATE OR REPLACE TABLE DB_TEAM1.BRONZE.STORE_STAGING
            LIKE DB_TEAM1.BRONZE.STORE;
        EXECUTE IMMEDIATE $$
            COPY INTO DB_TEAM1.BRONZE.STORE_STAGING (
                CHANNEL_DESC, CHANNEL_ID, STORE_ID, STORE_NAME, STORE_STATUS,
                STORE_OPEN_DATE, STORE_CLOSE_DATE, STORE_TYPE, LONGITUDE, LATITUDE,
                COUNTRY, DISTRICT, REGION, CITY, STATE, ZIPCODE,
                LOCATION_TYPE, CLIMATE, TOTAL_STORE_AREA, STORE_SELLING_AREA,
                TRAFFIC, LIKE_STORE_ID, STORE_TIER_GRADE, COMP_DATE,
                COMP_STATUS, PERIMETER, CREATED_TIMESTAMP, UPDATED_TIMESTAMP,
                _SOURCE_FILE
            )
            FROM (
                SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                       $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                       $21,$22,$23,$24,$25,$26,$27,$28, METADATA$FILENAME
                FROM @DB_TEAM1.BRONZE.GCS_RAW_STAGE
            )
            PATTERN = '$$ || :v_store_pattern || $$'
            FILE_FORMAT = (
                TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE
            )
            ON_ERROR = 'CONTINUE'
        $$;
        SELECT COUNT(*) INTO :v_store_rows FROM DB_TEAM1.BRONZE.STORE_STAGING;
        IF (:v_store_rows > 0) THEN
            TRUNCATE TABLE DB_TEAM1.BRONZE.STORE;
            INSERT INTO DB_TEAM1.BRONZE.STORE
                SELECT * FROM DB_TEAM1.BRONZE.STORE_STAGING;
        END IF;

    EXCEPTION
        WHEN OTHER THEN
            v_error := SQLERRM;
    END;

    INSERT INTO DB_TEAM1.BRONZE.PIPELINE_LOG (
        RUN_TYPE, STATUS, TRANSACTIONS_ROWS, INVENTORY_ROWS,
        PRODUCT_ROWS, STORE_ROWS, ERROR_MESSAGE
    )
    SELECT
        'FALLBACK',
        CASE WHEN :v_error IS NOT NULL THEN 'FAILED' ELSE 'SUCCESS' END,
        (SELECT COUNT(*) FROM DB_TEAM1.BRONZE.TRANSACTIONS),
        (SELECT COUNT(*) FROM DB_TEAM1.BRONZE.INVENTORY),
        (SELECT COUNT(*) FROM DB_TEAM1.BRONZE.PRODUCT),
        (SELECT COUNT(*) FROM DB_TEAM1.BRONZE.STORE),
        :v_error;
END;

-- resume everything, children first, roots last
ALTER TASK DB_TEAM1.BRONZE.TASK_GOLD_REFRESH         RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_TRANSACTIONS  RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_INVENTORY     RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_PRODUCT       RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_STORE         RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_SEND_FAILURE_EMAIL   RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_FALLBACK_BRONZE_LOAD RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_DAILY_BRONZE_LOAD    RESUME;



-- ============================================================
-- SECTION 4 — VALIDATION
-- ============================================================

-- all 8 tasks started, both roots show the new comments
SHOW TASKS IN SCHEMA DB_TEAM1.BRONZE;

-- snapshot sizes correct
SELECT COUNT(*) AS BRONZE_STORE   FROM DB_TEAM1.BRONZE.STORE;    -- ~154
SELECT COUNT(*) AS BRONZE_PRODUCT FROM DB_TEAM1.BRONZE.PRODUCT;  -- one day's catalog
SELECT COUNT(*) AS SILVER_STORE   FROM DB_TEAM1.SILVER.STORE;    -- 154

-- PERIMETER now holds real values (no timestamps)
SELECT PERIMETER, COUNT(*) FROM DB_TEAM1.SILVER.STORE GROUP BY 1 ORDER BY 2 DESC;

-- store status split is now DETERMINISTIC (re-run twice, same answer)
SELECT STORE_STATUS, COUNT(*) FROM DB_TEAM1.SILVER.STORE GROUP BY 1;

-- TOMORROW after 12:30pm IST: confirm PIPELINE_LOG STORE_ROWS stays ~154
-- (flat, not +154/day) and PRODUCT_ROWS stays at one-day size.
SELECT RUN_TIMESTAMP, STORE_ROWS, PRODUCT_ROWS
FROM DB_TEAM1.BRONZE.PIPELINE_LOG
ORDER BY RUN_TIMESTAMP DESC LIMIT 5;

EXECUTE TASK DB_TEAM1.BRONZE.TASK_DAILY_BRONZE_LOAD;