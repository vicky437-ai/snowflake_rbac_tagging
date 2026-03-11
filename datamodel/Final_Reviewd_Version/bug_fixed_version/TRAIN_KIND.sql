/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_RAW.SADB.TRAIN_KIND_BASE
================================================================================
Source Table : D_RAW.SADB.TRAIN_KIND_BASE
Target Table : D_BRONZE.SADB.TRAIN_KIND
Stream       : D_RAW.SADB.TRAIN_KIND_BASE_HIST_STREAM
Procedure    : D_RAW.SADB.SP_PROCESS_TRAIN_KIND()
Task         : D_RAW.SADB.TASK_PROCESS_TRAIN_KIND
Primary Key  : TRAIN_KIND_CD (Single)
Total Columns: 11 source + 6 CDC metadata = 17
Filter       : SNW_OPERATION_OWNER NOT IN ('TSDPRG','EMEPRG') (exclude purged records)
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE OR ALTER TABLE D_BRONZE.SADB.TRAIN_KIND (
    TRAIN_KIND_CD VARCHAR(16) NOT NULL,
    DSCRPT_TXT VARCHAR(400),
    EXPIRY_DT TIMESTAMP_NTZ(0),
    RECORD_CREATE_TMS TIMESTAMP_NTZ(0),
    RECORD_UPDATE_TMS TIMESTAMP_NTZ(0),
    CREATE_USER_ID VARCHAR(32),
    UPDATE_USER_ID VARCHAR(32),
    TRAIN_KIND_ROLLUP_ID NUMBER(18,0),
    SNW_OPERATION_TYPE VARCHAR(1),
    SNW_LAST_REPLICATED TIMESTAMP_NTZ(9),

    CDC_OPERATION VARCHAR(10),
    CDC_TIMESTAMP TIMESTAMP_NTZ,
    IS_DELETED BOOLEAN,
    RECORD_CREATED_AT TIMESTAMP_NTZ,
    RECORD_UPDATED_AT TIMESTAMP_NTZ,
    SOURCE_LOAD_BATCH_ID VARCHAR(100),
    SNW_OPERATION_OWNER VARCHAR(256),

    PRIMARY KEY (TRAIN_KIND_CD)
);

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_RAW.SADB.TRAIN_KIND_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 45,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 15;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_RAW.SADB.TRAIN_KIND_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.TRAIN_KIND_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for TRAIN_KIND_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.SADB.SP_PROCESS_TRAIN_KIND()
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
        FROM D_RAW.SADB.TRAIN_KIND_BASE_HIST_STREAM
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
        
        CREATE OR REPLACE STREAM D_RAW.SADB.TRAIN_KIND_BASE_HIST_STREAM
        ON TABLE D_RAW.SADB.TRAIN_KIND_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection';
        
        MERGE INTO D_BRONZE.SADB.TRAIN_KIND AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_RAW.SADB.TRAIN_KIND_BASE_HIST_STREAM src
            WHERE NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')
        ) AS src
        ON tgt.TRAIN_KIND_CD = src.TRAIN_KIND_CD
        WHEN MATCHED THEN UPDATE SET
            tgt.DSCRPT_TXT = src.DSCRPT_TXT,
            tgt.EXPIRY_DT = src.EXPIRY_DT,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.TRAIN_KIND_ROLLUP_ID = src.TRAIN_KIND_ROLLUP_ID,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_OPERATION_OWNER = src.SNW_OPERATION_OWNER,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'RELOADED',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        WHEN NOT MATCHED THEN INSERT (
            TRAIN_KIND_CD, DSCRPT_TXT, EXPIRY_DT, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            CREATE_USER_ID, UPDATE_USER_ID, TRAIN_KIND_ROLLUP_ID,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.TRAIN_KIND_CD, src.DSCRPT_TXT, src.EXPIRY_DT, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS,
            src.CREATE_USER_ID, src.UPDATE_USER_ID, src.TRAIN_KIND_ROLLUP_ID,
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
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_TRAIN_KIND AS
    SELECT 
        TRAIN_KIND_CD, DSCRPT_TXT, EXPIRY_DT, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
        CREATE_USER_ID, UPDATE_USER_ID, TRAIN_KIND_ROLLUP_ID,
        SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_RAW.SADB.TRAIN_KIND_BASE_HIST_STREAM
    WHERE NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG');
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_TRAIN_KIND;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_TRAIN_KIND;
        RETURN 'NO_DATA: Stream has no changes to process at ' || CURRENT_TIMESTAMP()::VARCHAR;
    END IF;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.SADB.TRAIN_KIND AS tgt
    USING (
        SELECT 
            TRAIN_KIND_CD, DSCRPT_TXT, EXPIRY_DT, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            CREATE_USER_ID, UPDATE_USER_ID, TRAIN_KIND_ROLLUP_ID,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_TRAIN_KIND
    ) AS src
    ON tgt.TRAIN_KIND_CD = src.TRAIN_KIND_CD
    
    -- UPDATE scenario (METADATA$ACTION='INSERT' AND METADATA$ISUPDATE=TRUE)
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
        UPDATE SET
            tgt.DSCRPT_TXT = src.DSCRPT_TXT,
            tgt.EXPIRY_DT = src.EXPIRY_DT,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.TRAIN_KIND_ROLLUP_ID = src.TRAIN_KIND_ROLLUP_ID,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_OPERATION_OWNER = src.SNW_OPERATION_OWNER,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'UPDATE',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    -- DELETE scenario (METADATA$ACTION='DELETE' AND METADATA$ISUPDATE=FALSE)
    WHEN MATCHED AND src.CDC_ACTION = 'DELETE' AND src.CDC_IS_UPDATE = FALSE THEN 
        UPDATE SET
            tgt.CDC_OPERATION = 'DELETE',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = TRUE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    -- RE-INSERT scenario (record exists but being re-inserted)
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = FALSE THEN
        UPDATE SET
            tgt.DSCRPT_TXT = src.DSCRPT_TXT,
            tgt.EXPIRY_DT = src.EXPIRY_DT,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.TRAIN_KIND_ROLLUP_ID = src.TRAIN_KIND_ROLLUP_ID,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_OPERATION_OWNER = src.SNW_OPERATION_OWNER,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'INSERT',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    -- NEW INSERT scenario
    WHEN NOT MATCHED AND src.CDC_ACTION = 'INSERT' THEN 
        INSERT (
            TRAIN_KIND_CD, DSCRPT_TXT, EXPIRY_DT, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            CREATE_USER_ID, UPDATE_USER_ID, TRAIN_KIND_ROLLUP_ID,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.TRAIN_KIND_CD, src.DSCRPT_TXT, src.EXPIRY_DT, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS,
            src.CREATE_USER_ID, src.UPDATE_USER_ID, src.TRAIN_KIND_ROLLUP_ID,
            src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    DROP TABLE IF EXISTS _CDC_STAGING_TRAIN_KIND;
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes. Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS _CDC_STAGING_TRAIN_KIND;
        RETURN 'ERROR: ' || SQLERRM || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.SADB.TASK_PROCESS_TRAIN_KIND
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process TRAIN_KIND_BASE CDC changes into data preservation table'
AS
    CALL D_RAW.SADB.SP_PROCESS_TRAIN_KIND();

ALTER TASK D_RAW.SADB.TASK_PROCESS_TRAIN_KIND RESUME;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'TRAIN_KIND%' IN SCHEMA D_BRONZE.SADB;
-- SHOW STREAMS LIKE 'TRAIN_KIND%' IN SCHEMA D_RAW.SADB;
-- SHOW TASKS LIKE 'TASK_PROCESS_TRAIN_KIND%' IN SCHEMA D_RAW.SADB;
-- CALL D_RAW.SADB.SP_PROCESS_TRAIN_KIND();
