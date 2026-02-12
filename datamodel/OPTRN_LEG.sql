/*
================================================================================
DATA PRESERVATION ARCHITECTURE FOR INFORMATICA IDMC CDC TABLES
================================================================================
Purpose: Protect historical data from IDMC job redeployment truncation events
Source: D_BRONZE.SADB.OPTRN_LEG_BASE (IDMC CDC base table)
Target: D_BRONZE.SADB.OPTRN_LEG (Data Preservation Table)

Architecture Flow:
[IDMC CDC] -> [_BASE Table] -> [Stream] -> [Task/SP] -> [Data Preserved Table]

Key Features:
1. Initial Load: Captures complete dataset on first run using SHOW_INITIAL_ROWS=TRUE
2. CDC Processing: Handles INSERT, UPDATE, DELETE via standard stream
3. Truncate Recovery: Detects stream staleness and auto-recovers
4. Soft Delete Preservation: Maintains IS_DELETED flag instead of physical delete
================================================================================
*/

-- =============================================================================
-- STEP 1: SETUP - Create Target Data Preservation Table (if not exists)
-- =============================================================================
USE ROLE D-SNW-DEVBI1-ETL;
USE WAREHOUSE INFA_INGEST_WH;
USE DATABASE D_BRONZE;
USE SCHEMA SADB;

CREATE TABLE IF NOT EXISTS D_BRONZE.SADB.OPTRN_LEG (
    -- Primary Key columns (adjust based on your actual _BASE table structure)
    OPTRN_LEG_ID NUMBER(38,0) NOT NULL,
    
    -- Business columns from _BASE table (add your actual columns)
    -- Example columns - REPLACE WITH YOUR ACTUAL COLUMNS:
    LEG_NUM NUMBER(38,0),
    OPTRN_ID NUMBER(38,0),
    PRODUCT_ID NUMBER(38,0),
    QUANTITY NUMBER(38,6),
    PRICE NUMBER(38,6),
    TRADE_DATE DATE,
    SETTLEMENT_DATE DATE,
    STATUS VARCHAR(50),
    
    -- CDC Metadata columns for data preservation
    CDC_OPERATION VARCHAR(10),           -- INSERT, UPDATE, DELETE
    CDC_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IS_DELETED BOOLEAN DEFAULT FALSE,    -- Soft delete flag
    RECORD_CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RECORD_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_LOAD_BATCH_ID VARCHAR(100),   -- Track which batch loaded this record
    
    PRIMARY KEY (OPTRN_LEG_ID)
);

COMMENT ON TABLE D_BRONZE.SADB.OPTRN_LEG IS 
'Data Preservation table for OPTRN_LEG_BASE - Protects against IDMC redeployment truncation. Contains full history with soft deletes.';

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source _BASE Table
-- =============================================================================
ALTER TABLE D_BRONZE.SADB.OPTRN_LEG_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 14,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 14;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM
ON TABLE D_BRONZE.SADB.OPTRN_LEG_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for OPTRN_LEG_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing with Truncate Recovery
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_batch_id VARCHAR;
    v_stream_stale BOOLEAN DEFAULT FALSE;
    v_staging_count NUMBER DEFAULT 0;
    v_rows_merged NUMBER DEFAULT 0;
    v_result VARCHAR;
BEGIN
    v_batch_id := 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    
    -- =========================================================================
    -- CHECK 1: Detect if stream is stale (happens after IDMC truncate/reload)
    -- =========================================================================
    BEGIN
        SHOW STREAMS LIKE 'OPTRN_LEG_BASE_STREAM' IN SCHEMA D_BRONZE.SADB;
        
        SELECT "stale"::BOOLEAN INTO v_stream_stale
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "name" = 'OPTRN_LEG_BASE_STREAM';
        
    EXCEPTION
        WHEN OTHER THEN
            v_stream_stale := TRUE;
    END;
    
    -- =========================================================================
    -- RECOVERY: If stream is stale, recreate it and do differential load
    -- =========================================================================
    IF (v_stream_stale = TRUE) THEN
        v_result := 'STREAM_STALE_DETECTED - Initiating recovery at ' || CURRENT_TIMESTAMP()::VARCHAR;
        
        CREATE OR REPLACE STREAM D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM
        ON TABLE D_BRONZE.SADB.OPTRN_LEG_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection at ' || CURRENT_TIMESTAMP()::VARCHAR;
        
        MERGE INTO D_BRONZE.SADB.OPTRN_LEG AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_BRONZE.SADB.OPTRN_LEG_BASE src
            LEFT JOIN D_BRONZE.SADB.OPTRN_LEG tgt 
                ON src.OPTRN_LEG_ID = tgt.OPTRN_LEG_ID
            WHERE tgt.OPTRN_LEG_ID IS NULL
               OR tgt.IS_DELETED = TRUE
        ) AS src
        ON tgt.OPTRN_LEG_ID = src.OPTRN_LEG_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.LEG_NUM = src.LEG_NUM,
            tgt.OPTRN_ID = src.OPTRN_ID,
            tgt.PRODUCT_ID = src.PRODUCT_ID,
            tgt.QUANTITY = src.QUANTITY,
            tgt.PRICE = src.PRICE,
            tgt.TRADE_DATE = src.TRADE_DATE,
            tgt.SETTLEMENT_DATE = src.SETTLEMENT_DATE,
            tgt.STATUS = src.STATUS,
            tgt.CDC_OPERATION = 'RELOADED',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        WHEN NOT MATCHED THEN INSERT (
            OPTRN_LEG_ID, LEG_NUM, OPTRN_ID, PRODUCT_ID, QUANTITY, PRICE,
            TRADE_DATE, SETTLEMENT_DATE, STATUS,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, 
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.OPTRN_LEG_ID, src.LEG_NUM, src.OPTRN_ID, src.PRODUCT_ID, 
            src.QUANTITY, src.PRICE, src.TRADE_DATE, src.SETTLEMENT_DATE, src.STATUS,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
        
        v_rows_merged := SQLROWCOUNT;
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_merged || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- CHECK 2: Stage stream data into temp table (BEST PRACTICE - single read)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING AS
    SELECT 
        OPTRN_LEG_ID,
        LEG_NUM,
        OPTRN_ID,
        PRODUCT_ID,
        QUANTITY,
        PRICE,
        TRADE_DATE,
        SETTLEMENT_DATE,
        STATUS,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM;
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING;
        RETURN 'NO_DATA: Stream has no changes to process at ' || CURRENT_TIMESTAMP()::VARCHAR;
    END IF;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.SADB.OPTRN_LEG AS tgt
    USING (
        SELECT 
            OPTRN_LEG_ID,
            LEG_NUM,
            OPTRN_ID,
            PRODUCT_ID,
            QUANTITY,
            PRICE,
            TRADE_DATE,
            SETTLEMENT_DATE,
            STATUS,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING
    ) AS src
    ON tgt.OPTRN_LEG_ID = src.OPTRN_LEG_ID
    
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
        UPDATE SET
            tgt.LEG_NUM = src.LEG_NUM,
            tgt.OPTRN_ID = src.OPTRN_ID,
            tgt.PRODUCT_ID = src.PRODUCT_ID,
            tgt.QUANTITY = src.QUANTITY,
            tgt.PRICE = src.PRICE,
            tgt.TRADE_DATE = src.TRADE_DATE,
            tgt.SETTLEMENT_DATE = src.SETTLEMENT_DATE,
            tgt.STATUS = src.STATUS,
            tgt.CDC_OPERATION = 'UPDATE',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    WHEN MATCHED AND src.CDC_ACTION = 'DELETE' AND src.CDC_IS_UPDATE = FALSE THEN 
        UPDATE SET
            tgt.CDC_OPERATION = 'DELETE',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = TRUE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = FALSE THEN
        UPDATE SET
            tgt.LEG_NUM = src.LEG_NUM,
            tgt.OPTRN_ID = src.OPTRN_ID,
            tgt.PRODUCT_ID = src.PRODUCT_ID,
            tgt.QUANTITY = src.QUANTITY,
            tgt.PRICE = src.PRICE,
            tgt.TRADE_DATE = src.TRADE_DATE,
            tgt.SETTLEMENT_DATE = src.SETTLEMENT_DATE,
            tgt.STATUS = src.STATUS,
            tgt.CDC_OPERATION = 'INSERT',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    WHEN NOT MATCHED AND src.CDC_ACTION = 'INSERT' THEN 
        INSERT (
            OPTRN_LEG_ID, LEG_NUM, OPTRN_ID, PRODUCT_ID, QUANTITY, PRICE,
            TRADE_DATE, SETTLEMENT_DATE, STATUS,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.OPTRN_LEG_ID, src.LEG_NUM, src.OPTRN_ID, src.PRODUCT_ID,
            src.QUANTITY, src.PRICE, src.TRADE_DATE, src.SETTLEMENT_DATE, src.STATUS,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    DROP TABLE IF EXISTS _CDC_STAGING;
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes. Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_BRONZE.SADB.TASK_PROCESS_OPTRN_LEG_CDC
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process OPTRN_LEG_BASE CDC changes into data preservation table'
WHEN
    SYSTEM$STREAM_HAS_DATA('D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM')
AS
    CALL D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC();

ALTER TASK D_BRONZE.SADB.TASK_PROCESS_OPTRN_LEG_CDC RESUME;

-- =============================================================================
-- STEP 6: Create Monitoring Views
-- =============================================================================
CREATE OR REPLACE VIEW D_BRONZE.SADB.VW_OPTRN_LEG_STREAM_STATUS AS
SELECT 
    'OPTRN_LEG_BASE_STREAM' AS STREAM_NAME,
    SYSTEM$STREAM_HAS_DATA('D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM') AS HAS_DATA,
    CURRENT_TIMESTAMP() AS CHECK_TIME;

CREATE OR REPLACE VIEW D_BRONZE.SADB.VW_OPTRN_LEG_CDC_STATS AS
SELECT 
    SOURCE_LOAD_BATCH_ID,
    CDC_OPERATION,
    COUNT(*) AS RECORD_COUNT,
    MIN(CDC_TIMESTAMP) AS FIRST_CHANGE,
    MAX(CDC_TIMESTAMP) AS LAST_CHANGE
FROM D_BRONZE.SADB.OPTRN_LEG
GROUP BY SOURCE_LOAD_BATCH_ID, CDC_OPERATION
ORDER BY MAX(CDC_TIMESTAMP) DESC;

/*
================================================================================
IDMC TRUNCATE/RELOAD SCENARIO - HOW IT WORKS
================================================================================

SCENARIO: IDMC job is redeployed and truncates D_BRONZE.SADB.OPTRN_LEG_BASE

WHAT HAPPENS:
1. TRUNCATE on _BASE table invalidates the stream (makes it STALE)
2. IDMC reloads data from source (last 45 days) into _BASE
3. Next task run detects STALE stream via SHOW STREAMS

AUTOMATIC RECOVERY:
1. SP detects stream staleness
2. Recreates stream with SHOW_INITIAL_ROWS=TRUE
3. Performs differential MERGE:
   - Records in _BASE but NOT in preserved table -> INSERT (new data)
   - Records previously soft-deleted -> UPDATE (IS_DELETED = FALSE)
   - Records already in preserved table -> UNCHANGED (history preserved!)

RESULT:
- All historical data in D_BRONZE.SADB.OPTRN_LEG is PRESERVED
- New records from source are added
- Soft-deleted records can be "resurrected" if they reappear
- Full audit trail maintained

================================================================================
TESTING STEPS
================================================================================

-- TEST 1: Verify Initial Setup
SHOW TABLES LIKE 'OPTRN_LEG%' IN SCHEMA D_BRONZE.SADB;
SHOW STREAMS LIKE 'OPTRN_LEG%' IN SCHEMA D_BRONZE.SADB;
SHOW TASKS LIKE 'TASK_PROCESS%' IN SCHEMA D_BRONZE.SADB;

-- TEST 2: Run Initial Load
SELECT COUNT(*) AS base_count FROM D_BRONZE.SADB.OPTRN_LEG_BASE;
CALL D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC();
SELECT COUNT(*) AS preserved_count FROM D_BRONZE.SADB.OPTRN_LEG;

-- TEST 3: Verify Data Matches
SELECT COUNT(*) AS base_count FROM D_BRONZE.SADB.OPTRN_LEG_BASE;
SELECT COUNT(*) AS preserved_active FROM D_BRONZE.SADB.OPTRN_LEG WHERE IS_DELETED = FALSE;

-- TEST 4: Simulate INSERT in _BASE (via IDMC or manual)
INSERT INTO D_BRONZE.SADB.OPTRN_LEG_BASE 
VALUES (999999, 1, 1, 1, 100, 50.00, CURRENT_DATE(), CURRENT_DATE()+3, 'TEST_INSERT');
CALL D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC();
SELECT * FROM D_BRONZE.SADB.OPTRN_LEG WHERE OPTRN_LEG_ID = 999999;

-- TEST 5: Simulate UPDATE in _BASE
UPDATE D_BRONZE.SADB.OPTRN_LEG_BASE SET STATUS = 'UPDATED_STATUS' WHERE OPTRN_LEG_ID = 999999;
CALL D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC();
SELECT CDC_OPERATION, STATUS FROM D_BRONZE.SADB.OPTRN_LEG WHERE OPTRN_LEG_ID = 999999;

-- TEST 6: Simulate DELETE (soft delete in preserved table)
DELETE FROM D_BRONZE.SADB.OPTRN_LEG_BASE WHERE OPTRN_LEG_ID = 999999;
CALL D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC();
SELECT IS_DELETED, CDC_OPERATION FROM D_BRONZE.SADB.OPTRN_LEG WHERE OPTRN_LEG_ID = 999999;
-- Result: IS_DELETED = TRUE, record preserved!

-- TEST 7: Simulate IDMC Truncate/Reload (TEST ENVIRONMENT ONLY!)
-- Step A: Record current preserved count
SELECT COUNT(*) FROM D_BRONZE.SADB.OPTRN_LEG;

-- Step B: Simulate truncate (THIS WILL MAKE STREAM STALE)
TRUNCATE TABLE D_BRONZE.SADB.OPTRN_LEG_BASE;

-- Step C: Reload data (simulate IDMC reload)
-- INSERT INTO D_BRONZE.SADB.OPTRN_LEG_BASE SELECT * FROM <staging_source>;

-- Step D: Run procedure - it will auto-recover
CALL D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC();
-- Expected: RECOVERY_COMPLETE message

-- Step E: Verify historical data preserved
SELECT COUNT(*) FROM D_BRONZE.SADB.OPTRN_LEG;  -- Should be >= original count

-- TEST 8: Check Task Execution History
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_PROCESS_OPTRN_LEG_CDC',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 20;

-- TEST 9: View CDC Statistics
SELECT * FROM D_BRONZE.SADB.VW_OPTRN_LEG_CDC_STATS;

-- Cleanup test data
DELETE FROM D_BRONZE.SADB.OPTRN_LEG_BASE WHERE OPTRN_LEG_ID = 999999;
CALL D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC();

================================================================================
*/