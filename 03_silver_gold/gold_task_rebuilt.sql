-- ============================================================
-- FILE NAME: gold_task_rebuilt.sql
-- Project : S&S Retail — End-to-End Snowflake Pipeline
-- Run this FIFTH, after silver_tasks_rebuilt.sql
-- Rebuilds the Gold refresh task to run AFTER all 4 Silver
-- tasks succeed, instead of a fixed 60-minute timer.
-- All original join logic, DQ filters, and STOCK_STATUS
-- calculation preserved exactly.
-- ============================================================

USE ROLE SNOWFLAKE_LEARNING_ROLE;
USE DATABASE DB_TEAM1;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

-- suspend the silver tasks first so the graph can be modified
ALTER TASK IF EXISTS DB_TEAM1.BRONZE.TASK_SILVER_TRANSACTIONS SUSPEND;
ALTER TASK IF EXISTS DB_TEAM1.BRONZE.TASK_SILVER_INVENTORY    SUSPEND;
ALTER TASK IF EXISTS DB_TEAM1.BRONZE.TASK_SILVER_PRODUCT      SUSPEND;
ALTER TASK IF EXISTS DB_TEAM1.BRONZE.TASK_SILVER_STORE        SUSPEND;
ALTER TASK IF EXISTS DB_TEAM1.SILVER.TASK_GOLD_REFRESH        SUSPEND;

-- drop the old gold task in SILVER schema since tasks in a
-- dependency graph must all live in the same schema as their
-- predecessors — moving it to BRONZE alongside the Silver tasks
DROP TASK IF EXISTS DB_TEAM1.SILVER.TASK_GOLD_REFRESH;

CREATE OR REPLACE TASK DB_TEAM1.BRONZE.TASK_GOLD_REFRESH
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
    COMMENT   = 'Refreshes all Gold mart tables from Silver, runs after all Silver tasks'
    AFTER
        DB_TEAM1.BRONZE.TASK_SILVER_TRANSACTIONS,
        DB_TEAM1.BRONZE.TASK_SILVER_INVENTORY,
        DB_TEAM1.BRONZE.TASK_SILVER_PRODUCT,
        DB_TEAM1.BRONZE.TASK_SILVER_STORE
AS
BEGIN
    -- gold 1: daily sales summary
    CREATE OR REPLACE TABLE DB_TEAM1.GOLD.DAILY_SALES_SUMMARY AS
    SELECT
        T.TRANSACTION_DATE,
        T.STORE_ID_ORIGIN                               AS STORE_ID,
        S.STORE_NAME, S.CITY, S.REGION, S.COUNTRY, S.STORE_TYPE,
        COUNT(DISTINCT T.TRANSACTION_ID)                AS TOTAL_TRANSACTIONS,
        SUM(T.QUANTITY_SOLD)                            AS TOTAL_UNITS_SOLD,
        SUM(T.TOTAL_EXTENDED_LINE_AMOUNT)               AS TOTAL_REVENUE,
        SUM(T.TOTAL_COST)                               AS TOTAL_COST,
        SUM(T.TOTAL_EXTENDED_LINE_AMOUNT)
            - SUM(T.TOTAL_COST)                         AS GROSS_PROFIT,
        SUM(T.DISCOUNT_AMOUNT)                          AS TOTAL_DISCOUNT,
        AVG(T.UNIT_NET_SELLING_PRICE)                   AS AVG_SELLING_PRICE
    FROM DB_TEAM1.SILVER.TRANSACTIONS T
    LEFT JOIN DB_TEAM1.SILVER.STORE S
        ON LPAD(T.STORE_ID_ORIGIN, 3, '0') = LPAD(S.STORE_ID::STRING, 3, '0')
    WHERE T.DQ_TRANSACTION_ID = 'PASS'
      AND T.DQ_QUANTITY       = 'PASS'
      AND T.DQ_AMOUNT         = 'PASS'
    GROUP BY 1, 2, 3, 4, 5, 6, 7;

    -- gold 2: product sales performance
    CREATE OR REPLACE TABLE DB_TEAM1.GOLD.PRODUCT_SALES_PERFORMANCE AS
    SELECT
        T.TRANSACTION_DATE, T.SKU_ID,
        P.STYLE_DESC, P.DIVISION_DESC, P.DEPARTMENT_DESC,
        P.CLASS_DESC, P.BRAND, P.PRODUCT_STATUS, P.COLOR_DESC, P.SIZE,
        COUNT(DISTINCT T.TRANSACTION_ID)                AS TOTAL_TRANSACTIONS,
        SUM(T.QUANTITY_SOLD)                            AS TOTAL_UNITS_SOLD,
        SUM(T.TOTAL_EXTENDED_LINE_AMOUNT)               AS TOTAL_REVENUE,
        SUM(T.TOTAL_COST)                               AS TOTAL_COST,
        SUM(T.TOTAL_EXTENDED_LINE_AMOUNT)
            - SUM(T.TOTAL_COST)                         AS GROSS_PROFIT,
        SUM(T.DISCOUNT_AMOUNT)                          AS TOTAL_DISCOUNT,
        AVG(T.DISCOUNT_AMOUNT)                          AS AVG_DISCOUNT
    FROM DB_TEAM1.SILVER.TRANSACTIONS T
    LEFT JOIN DB_TEAM1.SILVER.PRODUCT P ON T.SKU_ID = P.SKU_ID
    WHERE T.DQ_TRANSACTION_ID = 'PASS'
      AND T.DQ_QUANTITY       = 'PASS'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

    -- gold 3: inventory health
    CREATE OR REPLACE TABLE DB_TEAM1.GOLD.INVENTORY_HEALTH AS
    SELECT
        I.INVENTORY_DATE, I.STORE_ID,
        S.STORE_NAME, S.CITY, S.REGION, S.COUNTRY,
        I.SKU_ID, P.STYLE_DESC, P.DIVISION_DESC, P.DEPARTMENT_DESC, P.BRAND,
        I.CHANNEL_ID, I.CHANNEL_DESC,
        I.ONHAND_QTY, I.INTRANSIT_QTY, I.ONORDER_QTY,
        I.RECEIPTS_QTY, I.DROP_SHIP_QTY,
        I.ONHAND_QTY + I.INTRANSIT_QTY + I.ONORDER_QTY AS TOTAL_AVAILABLE_QTY,
        CASE
            WHEN I.ONHAND_QTY = 0  THEN 'OUT OF STOCK'
            WHEN I.ONHAND_QTY < 5  THEN 'LOW STOCK'
            ELSE                        'IN STOCK'
        END                                             AS STOCK_STATUS
    FROM DB_TEAM1.SILVER.INVENTORY I
    LEFT JOIN DB_TEAM1.SILVER.STORE   S ON I.STORE_ID = S.STORE_ID
    LEFT JOIN DB_TEAM1.SILVER.PRODUCT P ON I.SKU_ID   = P.SKU_ID
    WHERE I.DQ_SKU_ID   = 'PASS'
      AND I.DQ_STORE_ID = 'PASS';
END;

-- resume everything in the chain
ALTER TASK DB_TEAM1.BRONZE.TASK_GOLD_REFRESH         RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_TRANSACTIONS  RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_INVENTORY     RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_PRODUCT       RESUME;
ALTER TASK DB_TEAM1.BRONZE.TASK_SILVER_STORE         RESUME;

-- verify full graph
SHOW TASKS IN DATABASE DB_TEAM1;
