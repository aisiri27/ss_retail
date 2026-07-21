-- ============================================================
--FILE NAME :definitions.sql
-- FILE 01: SCHEMAS + BRONZE (RAW) LAYER
-- Project : S&S Retail — End-to-End Snowflake Pipeline
-- Run this FIRST before any other file
-- ============================================================

USE DATABASE DB_TEAM1;
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

-- ============================================================
-- STEP 1: CREATE SCHEMAS
-- ============================================================

CREATE SCHEMA IF NOT EXISTS DB_TEAM1.BRONZE;
CREATE SCHEMA IF NOT EXISTS DB_TEAM1.SILVER;
CREATE SCHEMA IF NOT EXISTS DB_TEAM1.GOLD;

USE SCHEMA DB_TEAM1.BRONZE;

-- ============================================================
-- BRONZE: TRANSACTIONS
-- ============================================================
CREATE OR REPLACE TABLE DB_TEAM1.BRONZE.TRANSACTIONS (
  CHANNEL_ID                  STRING,
  CHANNEL_DESC                STRING,
  TRANSACTION_ID              STRING,
  TRANSACTION_DATE            DATE,
  TRANSACTION_TYPE            STRING,
  LINE_ID                     NUMBER,
  SKU_ID                      STRING,
  STORE_ID_ORIGIN             STRING,    -- "001" zero-padded — must stay STRING
  STORE_ID_FULLFILLED         STRING,
  ORDER_FULFILLMENT_METHOD    STRING,
  QUANTITY_SOLD               NUMBER,
  CURRENT_MSRP                FLOAT,
  UNIT_RETAIL_PRICE           FLOAT,
  DISCOUNT_AMOUNT             FLOAT,
  UNIT_NET_SELLING_PRICE      FLOAT,
  TOTAL_EXTENDED_LINE_AMOUNT  FLOAT,
  UNIT_COST                   FLOAT,
  TOTAL_COST                  FLOAT,
  PRICE_STATUS                STRING,
  CLEARANCE_INDICATOR         STRING,
  LAUNCH_DATE                 DATE,
  LAUNCH_PRICE                FLOAT,
  CREATED_TIMESTAMP           STRING,    -- IBM DB2: 2026-04-01-04.17.41.503611
  UPDATED_TIMESTAMP           STRING,    -- IBM DB2: fixed in SILVER
  _SOURCE_FILE                STRING,
  _LOADED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- BRONZE: INVENTORY
-- ============================================================
CREATE OR REPLACE TABLE DB_TEAM1.BRONZE.INVENTORY (
  CHANNEL_ID      STRING,
  CHANNEL_DESC    STRING,
  SKU_ID          STRING,
  STORE_ID        NUMBER,
  INVENTORY_DATE  DATE,
  ONHAND_QTY      NUMBER,
  INTRANSIT_QTY   NUMBER,
  ONORDER_QTY     NUMBER,
  RECEIPTS_QTY    NUMBER,
  DROP_SHIP_QTY   NUMBER,
  _SOURCE_FILE    STRING,
  _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- BRONZE: PRODUCT
-- ============================================================
CREATE OR REPLACE TABLE DB_TEAM1.BRONZE.PRODUCT (
  DIVISION_CODE       STRING,
  DIVISION_DESC       STRING,
  DEPARTMENT_CODE     STRING,
  DEPARTMENT_DESC     STRING,
  CLASS_CODE          STRING,
  CLASS_DESC          STRING,
  SUBCLASS_CODE       STRING,
  SUBCLASS_DESC       STRING,
  VENDOR_ID           STRING,
  VENDOR_NAME         STRING,
  STYLE_ID            STRING,
  STYLE_DESC          STRING,
  COLOR_ID            STRING,
  COLOR_DESC          STRING,
  COLOR_FAMILY_DESC   STRING,
  SIZE                STRING,
  SKU_ID              STRING,
  PRODUCT_STATUS      STRING,
  UNIT_RETAIL_PRICE   FLOAT,
  UNIT_LAUNCH_PRICE   FLOAT,
  CURRENT_MSRP        FLOAT,
  UNIT_COST           FLOAT,
  LAUNCH_DATE         DATE,
  SEASON_CODE         STRING,
  SEASON_CODE_DESC    STRING,
  FASHION_GRADE       STRING,
  EAN                 STRING,
  CARRYOVER           STRING,
  COLLECTION          STRING,
  PRICE_STATUS        STRING,
  EXIT_DATE           DATE,
  BRAND               STRING,
  MATERIAL            STRING,
  PRODUCT_LIFE_CYCLE  STRING,
  CREATED_TIMESTAMP   STRING,    -- IBM DB2: fixed in SILVER
  UPDATED_TIMESTAMP   STRING,    -- IBM DB2: fixed in SILVER
  _SOURCE_FILE        STRING,
  _LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- BRONZE: STORE
-- ============================================================
CREATE OR REPLACE TABLE DB_TEAM1.BRONZE.STORE (
  CHANNEL_DESC        STRING,
  CHANNEL_ID          STRING,
  STORE_ID            NUMBER,
  STORE_NAME          STRING,
  STORE_STATUS        STRING,
  STORE_OPEN_DATE     DATE,
  STORE_CLOSE_DATE    DATE,
  STORE_TYPE          STRING,
  LONGITUDE           FLOAT,
  LATITUDE            FLOAT,
  COUNTRY             STRING,
  DISTRICT            STRING,
  REGION              STRING,
  CITY                STRING,
  STATE               STRING,
  ZIPCODE             STRING,
  LOCATION_TYPE       STRING,
  CLIMATE             STRING,
  TOTAL_STORE_AREA    NUMBER,
  STORE_SELLING_AREA  NUMBER,
  TRAFFIC             NUMBER,
  LIKE_STORE_ID       STRING,
  STORE_TIER_GRADE    STRING,
  COMP_DATE           DATE,
  COMP_STATUS         STRING,
  PERIMETER           STRING,
  CREATED_TIMESTAMP   STRING,    -- IBM DB2: fixed in SILVER
  UPDATED_TIMESTAMP   STRING,    -- IBM DB2: fixed in SILVER
  _SOURCE_FILE        STRING,
  _LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- VERIFY
-- ============================================================
SHOW SCHEMAS IN DATABASE DB_TEAM1;
SHOW TABLES IN SCHEMA DB_TEAM1.BRONZE;