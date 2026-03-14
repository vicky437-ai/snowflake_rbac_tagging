/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_RAW.SADB.EQPMNT_NON_RGSTRD_BASE
================================================================================
Source Table : D_RAW.SADB.EQPMNT_NON_RGSTRD_BASE
Target Table : D_BRONZE.SADB.EQPMNT_NON_RGSTRD
Stream       : D_RAW.SADB.EQPMNT_NON_RGSTRD_BASE_HIST_STREAM
Procedure    : D_RAW.SADB.SP_PROCESS_EQPMNT_NON_RGSTRD()
Task         : D_RAW.SADB.TASK_PROCESS_EQPMNT_NON_RGSTRD
Primary Key  : EQPMNT_ID (Single)
Total Columns: 8 source + 3 SNW metadata + 6 CDC metadata = 17
Filter       : SNW_OPERATION_OWNER NOT IN ('TSDPRG','EMEPRG') (exclude purged records)
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE OR ALTER TABLE D_BRONZE.SADB.EQPMNT_NON_RGSTRD (
    EQPMNT_ID NUMBER(18,0) NOT NULL,
    RECORD_CREATE_TMS TIMESTAMP_NTZ(0),
    CREATE_USER_ID VARCHAR(32),
    RECORD_UPDATE_TMS TIMESTAMP_NTZ(0),
    UPDATE_USER_ID VARCHAR(32),
    MARK_CD VARCHAR(16),
    EQPUN_NBR VARCHAR(40),
    CHECK_DIGIT_NBR VARCHAR(8),
    SNW_OPERATION_TYPE VARCHAR(1),
    SNW_LAST_REPLICATED TIMESTAMP_NTZ(9),

    CDC_OPERATION VARCHAR(10),
    CDC_TIMESTAMP TIMESTAMP_NTZ,
    IS_DELETED BOOLEAN,
    RECORD_CREATED_AT TIMESTAMP_NTZ,
    RECORD_UPDATED_AT TIMESTAMP_NTZ,
    SOURCE_LOAD_BATCH_ID VARCHAR(100),
    SNW_OPERATION_OWNER VARCHAR(256),

    PRIMARY KEY (EQPMNT_ID)
);

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_RAW.SADB.EQPMNT_NON_RGSTRD_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 45,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 15;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_RAW.SADB.EQPMNT_NON_RGSTRD_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.EQPMNT_NON_RGSTRD_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for EQPMNT_NON_RGSTRD_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.SADB.SP_PROCESS_EQPMNT_NON_RGSTRD()
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
    v_error_msg VARCHAR;
BEGIN
    v_batch_id := 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    
    -- =========================================================================
    -- CHECK 1: Detect if stream is stale (happens after IDMC truncate/reload)
    -- =========================================================================
    BEGIN
        SELECT COUNT(*) INTO v_staging_count 
        FROM D_RAW.SADB.EQPMNT_NON_RGSTRD_BASE_HIST_STREAM
        WHERE 1=0;
        
        v_stream_stale := FALSE;
        
    EXCEPTION
        WHEN OTHER THEN
            v_stream_stale := TRUE;
            v_error_msg := SQLERRM;
    END;
    
    -- =========================================================================
    -- RECOVERY: If stream is stale, recreate it and do differential load
    -- =========================================================================
    IF (v_stream_stale = TRUE) THEN
        v_result := 'STREAM_STALE_DETECTED: ' || NVL(v_error_msg, 'Unknown') || ' - Initiating recovery at ' || CURRENT_TIMESTAMP()::VARCHAR;
        
        CREATE OR REPLACE STREAM D_RAW.SADB.EQPMNT_NON_RGSTRD_BASE_HIST_STREAM
        ON TABLE D_RAW.SADB.EQPMNT_NON_RGSTRD_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection';
        
        MERGE INTO D_BRONZE.SADB.EQPMNT_NON_RGSTRD AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_RAW.SADB.EQPMNT_NON_RGSTRD_BASE_HIST_STREAM src
            WHERE NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')
        ) AS src
        ON tgt.EQPMNT_ID = src.EQPMNT_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.MARK_CD = src.MARK_CD,
            tgt.EQPUN_NBR = src.EQPUN_NBR,
            tgt.CHECK_DIGIT_NBR = src.CHECK_DIGIT_NBR,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_OPERATION_OWNER = src.SNW_OPERATION_OWNER,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'RELOADED',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        WHEN NOT MATCHED THEN INSERT (
            EQPMNT_ID, RECORD_CREATE_TMS, CREATE_USER_ID, RECORD_UPDATE_TMS, UPDATE_USER_ID,
            MARK_CD, EQPUN_NBR, CHECK_DIGIT_NBR,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.EQPMNT_ID, src.RECORD_CREATE_TMS, src.CREATE_USER_ID, src.RECORD_UPDATE_TMS, src.UPDATE_USER_ID,
            src.MARK_CD, src.EQPUN_NBR, src.CHECK_DIGIT_NBR,
            src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
        
        v_rows_merged := SQLROWCOUNT;
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_merged || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- CHECK 2: Stage stream data into temp table (BEST PRACTICE - single read)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_EQPMNT_NON_RGSTRD AS
    SELECT 
        EQPMNT_ID, RECORD_CREATE_TMS, CREATE_USER_ID, RECORD_UPDATE_TMS, UPDATE_USER_ID,
        MARK_CD, EQPUN_NBR, CHECK_DIGIT_NBR,
        SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_RAW.SADB.EQPMNT_NON_RGSTRD_BASE_HIST_STREAM
    WHERE NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG');
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_EQPMNT_NON_RGSTRD;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_EQPMNT_NON_RGSTRD;
        RETURN 'NO_DATA: Stream has no changes to process at ' || CURRENT_TIMESTAMP()::VARCHAR;
    END IF;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.SADB.EQPMNT_NON_RGSTRD AS tgt
    USING (
        SELECT 
            EQPMNT_ID, RECORD_CREATE_TMS, CREATE_USER_ID, RECORD_UPDATE_TMS, UPDATE_USER_ID,
            MARK_CD, EQPUN_NBR, CHECK_DIGIT_NBR,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_EQPMNT_NON_RGSTRD
    ) AS src
    ON tgt.EQPMNT_ID = src.EQPMNT_ID
    
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
        UPDATE SET
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.MARK_CD = src.MARK_CD,
            tgt.EQPUN_NBR = src.EQPUN_NBR,
            tgt.CHECK_DIGIT_NBR = src.CHECK_DIGIT_NBR,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_OPERATION_OWNER = src.SNW_OPERATION_OWNER,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
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
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.MARK_CD = src.MARK_CD,
            tgt.EQPUN_NBR = src.EQPUN_NBR,
            tgt.CHECK_DIGIT_NBR = src.CHECK_DIGIT_NBR,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_OPERATION_OWNER = src.SNW_OPERATION_OWNER,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'INSERT',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    WHEN NOT MATCHED AND src.CDC_ACTION = 'INSERT' THEN 
        INSERT (
            EQPMNT_ID, RECORD_CREATE_TMS, CREATE_USER_ID, RECORD_UPDATE_TMS, UPDATE_USER_ID,
            MARK_CD, EQPUN_NBR, CHECK_DIGIT_NBR,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.EQPMNT_ID, src.RECORD_CREATE_TMS, src.CREATE_USER_ID, src.RECORD_UPDATE_TMS, src.UPDATE_USER_ID,
            src.MARK_CD, src.EQPUN_NBR, src.CHECK_DIGIT_NBR,
            src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    DROP TABLE IF EXISTS _CDC_STAGING_EQPMNT_NON_RGSTRD;
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes. Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS _CDC_STAGING_EQPMNT_NON_RGSTRD;
        RETURN 'ERROR: ' || SQLERRM || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.SADB.TASK_PROCESS_EQPMNT_NON_RGSTRD
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process EQPMNT_NON_RGSTRD_BASE CDC changes into data preservation table'
AS
    CALL D_RAW.SADB.SP_PROCESS_EQPMNT_NON_RGSTRD();

ALTER TASK D_RAW.SADB.TASK_PROCESS_EQPMNT_NON_RGSTRD RESUME;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'EQPMNT_NON_RGSTRD%' IN SCHEMA D_BRONZE.SADB;
-- SHOW STREAMS LIKE 'EQPMNT_NON_RGSTRD_BASE_HIST_STREAM%' IN SCHEMA D_RAW.SADB;
-- SHOW TASKS LIKE 'TASK_PROCESS_EQPMNT_NON_RGSTRD%' IN SCHEMA D_RAW.SADB;
-- CALL D_RAW.SADB.SP_PROCESS_EQPMNT_NON_RGSTRD();
