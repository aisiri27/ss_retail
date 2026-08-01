S&S Retail — End-to-End Snowflake Data Pipeline
Client: PittaRosso (S&S Retail) — Italian fashion retail chain-
Platform: Snowflake (DB_TEAM1)
Architecture: Medallion (Bronze → Silver → Gold)
Schedule: Fully automated, daily at 12:30pm IST
________________________________________
Project Overview
This project builds a production-grade, fully automated data pipeline on Snowflake for PittaRosso, an Italian fashion retailer operating ~154 stores across Italy. Raw operational data (sales transactions, inventory, product catalog, and store master data) is delivered daily as pipe-delimited CSV files into a Google Cloud Storage bucket. The pipeline picks up those files automatically, cleans and validates them, and produces business-ready tables that power a live Streamlit dashboard showing revenue, profitability, inventory health, and store performance.
The pipeline requires zero manual intervention on a healthy day. It handles late files gracefully, alerts the team on failure, and maintains a full audit trail of every run.
________________________________________
Objectives and Goals
Primary objective: deliver a reliable, automated daily data pipeline that turns raw retail files into trusted business metrics with no manual intervention.
Specific goals:
•	Ingest four daily datasets from GCS into Snowflake within 30 minutes of file arrival
•	Clean, type-cast, and deduplicate all data before it reaches the business layer
•	Flag data quality issues explicitly (DQ columns) so bad data is visible rather than silently corrupting numbers
•	Scale to 1.45 billion inventory rows without performance degradation — achieved via incremental MERGE with date pruning
•	Maintain a complete audit trail (source file, load timestamp, pipeline log) for every row
•	Provide automated failure detection with email alerting
•	Serve a live business dashboard showing KPIs, store performance, and inventory health
________________________________________
Source Data
GCS bucket: data-integration-scrape
Delivery pattern: files arrive daily, dated T-1 (a file dated July 10 contains July 9 business data)
Format: pipe-delimited CSV (|), UTF-8, with a header row
Path convention: YYYY-MM-DD/<Dataset>/<Dataset>_YYYYMMDD.csv
Dataset	Description	Columns	Load strategy
Transactions	Every line item sold across all channels	24	Incremental MERGE
Inventory	Daily stock levels per SKU per store per channel	10	Incremental MERGE + date pruning
Product	Full product catalog snapshot	36	Daily snapshot (staging swap)
Store	Full store master snapshot	28	Daily snapshot (staging swap)

________________________________________
Fact vs dimension distinction (drives all design decisions):
•	Transactions and Inventory are facts — they only grow. Yesterday's sales never change; today just adds more rows. These use incremental loading (MERGE from a stream) so only today's new rows are processed.
•	Product and Store are dimensions — they are daily snapshots. Today's file is the complete, current picture; it replaces yesterday's entirely. These use a staging swap to reload safely from a single day's file.
________________________________________





Architecture
 
 
Why three layers?
•	Bronze keeps a perfect raw copy of the source data. If anything goes wrong downstream, Bronze can be used to rebuild Silver and Gold without re-downloading files.
•	Silver is where all cleaning happens — type casting, deduplication, timestamp parsing, quality flags. One place to fix data problems.
•	Gold is shaped for business consumption — pre-joined, aggregated, fast to query. The dashboard never touches raw data.
________________________________________
Task Graph (Orchestration)
All automation is handled by Snowflake tasks. The graph has two independent root tasks (primary and fallback) and seven dependent tasks.
 
Key design decisions:

•	All schedules use Asia/Kolkata (not IST, which Snowflake does not recognise).
•	Silver Transactions and Inventory use a WHEN SYSTEM$STREAM_HAS_DATA(...) gate — they fire only if new Bronze data arrived, skipping the MERGE on days when no new files were loaded.
•	Silver Product and Store fire unconditionally after Bronze — they always reload the current snapshot regardless of stream state.
•	Gold fires only after all four Silver tasks complete, ensuring it always joins fresh data.
•	All tasks run on SNOWFLAKE_LEARNING_WH (X-Small warehouse, auto-suspend enabled).
________________________________________
Layer-by-Layer Detail
Bronze — Raw Landing
Purpose: land source data exactly as-is, with minimal transformation. Bronze is the system of record for raw data.
What happens:
For Transactions and Inventory (facts):
•	COPY INTO loads only new files — Snowflake's file-load history prevents re-ingestion of already-loaded files.
•	Audit columns _SOURCE_FILE (which file each row came from) and _LOADED_AT (when it was loaded) are added.
•	These tables are never truncated, so load-history is always preserved.
For Product and Store (dimensions — staging swap):
1.	Today's IST date is computed via CONVERT_TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP()).
2.	A date-specific pattern is built (e.g. 2026-07-21/Store/Store_20260721.csv).
3.	A _STAGING table (empty clone of the real table) is created.
4.	Today's single file is loaded into staging via EXECUTE IMMEDIATE with the date-scoped pattern.
5.	If staging has rows → TRUNCATE the real table → INSERT FROM staging.
6.	If staging is empty (file missing or late) → real table is untouched, yesterday's snapshot is kept.
This staging-swap pattern means the Bronze dimension tables can never end up empty, regardless of whether today's file arrived on time.
PIPELINE_LOG: after every Bronze run, the task writes a log entry recording run type (PRIMARY or FALLBACK), status (SUCCESS or FAILED), row counts for all four tables, and any error message. This is the pipeline's audit diary.
Silver — Cleaned and Validated
Purpose: clean, type-cast, deduplicate, and quality-flag the raw Bronze data. Silver is what downstream joins and aggregations run on.
Common transformations applied to all tables:
•	All columns cast to correct types (::NUMBER, ::FLOAT, TRY_TO_DATE(...))
•	IBM DB2 timestamp format (2026-07-21-04.17.28.988325) converted to standard Snowflake timestamp via TRY_TO_TIMESTAMP(REPLACE(col, '-', ' '), 'YYYY MM DD HH24.MI.SS.FF6')
•	Deduplication via ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY _LOADED_AT DESC) — keeps only the newest-loaded copy of each unique key
•	_STAGED_AT column added (when Silver processed this row)
•	DQ (data quality) flag columns added per table
DQ flag logic:
Table	Flags
Transactions	DQ_TRANSACTION_ID, DQ_SKU_ID, DQ_STORE_ID, DQ_QUANTITY, DQ_AMOUNT
Inventory	DQ_SKU_ID, DQ_STORE_ID, DQ_INVENTORY_DATE, DQ_ONHAND_QTY
Product	DQ_SKU_ID, DQ_PRODUCT_STATUS, DQ_PRICE
Store	DQ_STORE_ID, DQ_STORE_NAME, DQ_STORE_STATUS, DQ_OPEN_DATE
Each flag is 'PASS' or 'FAIL'. Gold filters on DQ = 'PASS' so bad rows are quarantined, not deleted.
Unique keys (used for dedup and MERGE):
Table	Key
Transactions	TRANSACTION_ID + LINE_ID
Inventory	CHANNEL_ID + SKU_ID + STORE_ID + INVENTORY_DATE
Product	SKU_ID
Store	STORE_ID
Load methods:
Transactions — incremental MERGE: Reads only new rows from STREAM_TRANSACTIONS. Deduplicates within the batch, then MERGEs into Silver: rows matching the key are updated; new rows are inserted. Only today's delta is processed. Result: ~7 seconds.
Inventory — incremental MERGE with date pruning: Same MERGE pattern, but with an additional pruning condition on the target:
sql
ON TGT.INVENTORY_DATE >= :v_min_date   -- skip partitions older than the batch
AND TGT.CHANNEL_ID = SRC.CHANNEL_ID
...
The v_min_date is the earliest date in today's stream batch. This tells Snowflake to skip all micro-partitions older than that date, reducing the effective scan from 1.45 billion rows to only the recent window. Result: ~11 seconds (was a 1-hour timeout with full-table scan).
Product and Store — full refresh CREATE OR REPLACE: Since Bronze now holds only today's single snapshot, Silver simply rebuilds from that clean source:
sql
CREATE OR REPLACE TABLE SILVER.STORE AS
SELECT ... FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY STORE_ID ORDER BY _LOADED_AT DESC) AS RN
    FROM BRONZE.STORE
) WHERE RN = 1;
Result: seconds (154 stores / ~556K products).
Gold — Business Marts
Purpose: join the cleaned fact and dimension tables, aggregate to business granularity, and serve as the query layer for the dashboard.
Three mart tables:
DAILY_SALES_SUMMARY
Revenue, profit, transactions, and units sold per store per day. Joins Transactions (fact) to Store (dimension) via a zero-padded store ID join: LPAD(STORE_ID_ORIGIN,3,'0') = LPAD(STORE_ID::STRING,3,'0'). Filters to DQ_PASS rows only.
Key metrics: TOTAL_REVENUE, TOTAL_COST, GROSS_PROFIT, TOTAL_TRANSACTIONS, TOTAL_UNITS_SOLD, TOTAL_DISCOUNT, AVG_SELLING_PRICE.
PRODUCT_SALES_PERFORMANCE
Revenue and units sold per product (SKU) per day. Joins Transactions to Product. Enriches with BRAND, STYLE_DESC, DEPARTMENT_DESC, COLOR_DESC, SIZE. Filters to DQ_PASS rows.
INVENTORY_HEALTH
Stock status per product per store per day. Joins Inventory to both Store and Product. Derives STOCK_STATUS:
•	ONHAND_QTY = 0 → OUT OF STOCK
•	ONHAND_QTY < 5 → LOW STOCK
•	Otherwise → IN STOCK
Filters to rows where SKU and store passed DQ checks.

Dashboard
The pipeline powers a Snowflake-native Streamlit dashboard (streamlit_app.py) with:
•	Freshness strip: last load date, run type, status
•	KPI row: total revenue (EUR), gross profit, margin %, units sold, operating stores
•	Revenue over time: line chart across the full 16-month history
•	Top performers: top stores by revenue, top brands by units
•	Inventory health: out-of-stock and low-stock percentages
•	Pipeline operations: collapsible section showing source/schedule, task graph state, data volumes by layer, and run history
All figures query the Gold layer live (10-minute cache). The dashboard reflects data as of the previous business day (T-1).
________________________________________
Data Coverage
Metric	Value
History	2025-03-11 to present (T-1)
Transactions	~17.2 million rows
Inventory	~1.45 billion rows
Products (SKUs)	~556,000
Stores	~154 (134 Aperto / 20 Chiuso)
Total revenue (Gold)	~€387.9 million
Gross margin	~59.5%
Missing dates	2025-12-25 (Christmas), 2026-01-01 (New Year's) — stores closed
________________________________________
Known Source Data Characteristics
These are upstream data observations surfaced by the DQ framework, not pipeline defects:
•	127 SKUs appear in Transactions but are absent from the Product catalog. These are products sold through the POS system but not registered in the master catalog export. They appear in Gold with NULL product attributes (LEFT JOIN preserves the sales).
•	352,970 inventory rows (~0.024%) have negative on-hand quantity — a normal retail phenomenon caused by overselling, return processing timing, or inventory-system sync lag. Flagged by DQ_ONHAND_QTY = 'FAIL'; not excluded from Gold inventory health.
•	12 transaction rows have negative extended line amounts on Normal Sales type — likely sub-cent rounding artifacts from promotional pricing. Flagged by DQ_AMOUNT = 'FAIL' and excluded from Gold revenue totals.
