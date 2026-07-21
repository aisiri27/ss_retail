-- ============================================================
-- FILE NAME: primary_load_task.sql
-- Project : S&S Retail — End-to-End Snowflake Pipeline
-- Run order: after pipeline_log_table.sql (creates the primary
--            root task of the graph).
--
-- Primary daily load, 12:30pm IST (USING CRON ... Asia/Kolkata).
--   - Transactions & Inventory: incremental COPY (new files only;
--     never truncated, so COPY load-history protects them).
--   - Product & Store: DAILY SNAPSHOT loaded via STAGING SWAP —
--     only the current-day file is loaded, and the real Bronze
--     table is replaced only if today's file produced rows. A
--     missing/late file leaves yesterday's snapshot intact
--     (Bronze is never left empty).
--
-- This is the CORRECTED version (staging swap). It supersedes the
-- earlier truncate-all-history version, which re-ingested every
-- historical file each day and corrupted the Store dimension.
-- The body here matches what is deployed live.
-- ============================================================

USE ROLE SNOWFLAKE_LEARNING_ROLE;
USE DATABASE DB_TEAM1;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

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

-- activate
ALTER TASK DB_TEAM1.BRONZE.TASK_DAILY_BRONZE_LOAD RESUME;

-- verify
SHOW TASKS IN SCHEMA DB_TEAM1.BRONZE;
