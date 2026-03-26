/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_RAW.SADB.TRAIN_PLAN_LEG_BASE
================================================================================
Source Table : D_RAW.SADB.TRAIN_PLAN_LEG_BASE
Target Table : D_BRONZE.SADB.TRAIN_PLAN_LEG
Stream       : D_RAW.SADB.TRAIN_PLAN_LEG_BASE_HIST_STREAM
Procedure    : D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_LEG()
Task         : D_RAW.SADB.TASK_SP_PROCESS_TRAIN_PLAN_LEG
Primary Key  : TRAIN_PLAN_LEG_ID (Single)
Total Columns: 17 source + 6 CDC metadata = 23
Filter       : SNW_OPERATION_OWNER NOT IN ('TSDPRG','EMEPRG') (exclude purged records)
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE OR ALTER TABLE D_BRONZE.SADB.TRAIN_PLAN_LEG (
    TRAIN_PLAN_LEG_ID NUMBER(18,0) NOT NULL,
    TRAIN_PLAN_ID NUMBER(18,0),
    TRAIN_DRCTN_CD VARCHAR(20),
    TRAIN_PLAN_LEG_NM VARCHAR(32),
    MTP_TITAN_NBR NUMBER(6,0),
    RECORD_CREATE_TMS TIMESTAMP_NTZ(0),
    RECORD_UPDATE_TMS TIMESTAMP_NTZ(0),
    CREATE_USER_ID VARCHAR(32),
    UPDATE_USER_ID VARCHAR(32),
    TURN_LEG_SQNC_NBR NUMBER(1,0),
    TYES_TRAIN_ID NUMBER(18,0),
    MTP_TOTAL_RTPNT_SENT_QTY NUMBER(4,0),
    MTP_ROUTE_CMPLT_CD VARCHAR(4),
    MTP_TRAIN_STATE_CD VARCHAR(4),
    SNW_OPERATION_TYPE VARCHAR(1),
    SNW_LAST_REPLICATED TIMESTAMP_NTZ(9),

    CDC_OPERATION VARCHAR(10),
    CDC_TIMESTAMP TIMESTAMP_NTZ,
    IS_DELETED BOOLEAN,
    RECORD_CREATED_AT TIMESTAMP_NTZ,
    RECORD_UPDATED_AT TIMESTAMP_NTZ,
    SOURCE_LOAD_BATCH_ID VARCHAR(100),
    SNW_OPERATION_OWNER VARCHAR(256),

    PRIMARY KEY (TRAIN_PLAN_LEG_ID)
)
COMMENT = 'Bronze data preservation layer for Train Plan Legs.
Source: Oracle SADB (TRAIN_PLAN_LEG) replicated via IDMC CDC into D_RAW.SADB.TRAIN_PLAN_LEG_BASE.
Data Source Tables: D_RAW.SADB.TRAIN_PLAN_LEG_BASE (17 source columns + 3 SNW metadata).
Transformations: Purge filter excludes SNW_OPERATION_OWNER IN (TSDPRG, EMEPRG). Six CDC metadata columns added (CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID).
Pipeline Objects: Stream D_RAW.SADB.TRAIN_PLAN_LEG_BASE_HIST_STREAM | Procedure D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_LEG() | Task D_RAW.SADB.TASK_SP_PROCESS_TRAIN_PLAN_LEG.
Refresh Frequency: Every 5 minutes via Snowflake Task (incremental CDC MERGE).
Primary Key: TRAIN_PLAN_LEG_ID.';

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_RAW.SADB.TRAIN_PLAN_LEG_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 45,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 15;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_RAW.SADB.TRAIN_PLAN_LEG_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.TRAIN_PLAN_LEG_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for TRAIN_PLAN_LEG_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_LEG()
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
    v_rows_inserted NUMBER DEFAULT 0;
    v_rows_updated NUMBER DEFAULT 0;
    v_rows_deleted NUMBER DEFAULT 0;
    v_result VARCHAR;
    v_error_msg VARCHAR;
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
    v_execution_status VARCHAR DEFAULT 'SUCCESS';
BEGIN
    v_batch_id := 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    v_start_time := CURRENT_TIMESTAMP();
    
    -- =========================================================================
    -- CHECK 1: Detect if stream is stale (happens after IDMC truncate/reload)
    -- =========================================================================
    BEGIN
            SELECT COUNT(*) INTO v_staging_count 
            FROM D_RAW.SADB.TRAIN_PLAN_LEG_BASE_HIST_STREAM
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
        v_execution_status := 'RECOVERY';
        
        CREATE OR REPLACE STREAM D_RAW.SADB.TRAIN_PLAN_LEG_BASE_HIST_STREAM
        ON TABLE D_RAW.SADB.TRAIN_PLAN_LEG_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection';
        
        MERGE INTO D_BRONZE.SADB.TRAIN_PLAN_LEG AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_RAW.SADB.TRAIN_PLAN_LEG_BASE_HIST_STREAM src
            WHERE NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')
        ) AS src
        ON tgt.TRAIN_PLAN_LEG_ID = src.TRAIN_PLAN_LEG_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.TRAIN_PLAN_ID = src.TRAIN_PLAN_ID,
            tgt.TRAIN_DRCTN_CD = src.TRAIN_DRCTN_CD,
            tgt.TRAIN_PLAN_LEG_NM = src.TRAIN_PLAN_LEG_NM,
            tgt.MTP_TITAN_NBR = src.MTP_TITAN_NBR,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.TURN_LEG_SQNC_NBR = src.TURN_LEG_SQNC_NBR,
            tgt.TYES_TRAIN_ID = src.TYES_TRAIN_ID,
            tgt.MTP_TOTAL_RTPNT_SENT_QTY = src.MTP_TOTAL_RTPNT_SENT_QTY,
            tgt.MTP_ROUTE_CMPLT_CD = src.MTP_ROUTE_CMPLT_CD,
            tgt.MTP_TRAIN_STATE_CD = src.MTP_TRAIN_STATE_CD,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_OPERATION_OWNER = src.SNW_OPERATION_OWNER,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'RELOADED',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        WHEN NOT MATCHED THEN INSERT (
            TRAIN_PLAN_LEG_ID, TRAIN_PLAN_ID, TRAIN_DRCTN_CD, TRAIN_PLAN_LEG_NM, MTP_TITAN_NBR,
            RECORD_CREATE_TMS, RECORD_UPDATE_TMS, CREATE_USER_ID, UPDATE_USER_ID,
            TURN_LEG_SQNC_NBR, TYES_TRAIN_ID, MTP_TOTAL_RTPNT_SENT_QTY,
            MTP_ROUTE_CMPLT_CD, MTP_TRAIN_STATE_CD, SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, 
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.TRAIN_PLAN_LEG_ID, src.TRAIN_PLAN_ID, src.TRAIN_DRCTN_CD, src.TRAIN_PLAN_LEG_NM, src.MTP_TITAN_NBR,
            src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS, src.CREATE_USER_ID, src.UPDATE_USER_ID,
            src.TURN_LEG_SQNC_NBR, src.TYES_TRAIN_ID, src.MTP_TOTAL_RTPNT_SENT_QTY,
            src.MTP_ROUTE_CMPLT_CD, src.MTP_TRAIN_STATE_CD, src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
        
        v_rows_merged := SQLROWCOUNT;
        v_end_time := CURRENT_TIMESTAMP();
        
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
            TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME,
            ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED,
            ERROR_MESSAGE, CREATED_AT
        ) VALUES (
            'TRAIN_PLAN_LEG', :v_batch_id, 'RECOVERY', :v_start_time, :v_end_time,
            :v_rows_merged, :v_rows_merged, 0, 0,
            :v_error_msg, CURRENT_TIMESTAMP()
        );
        
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_merged || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- CHECK 2: Stage stream data into temp table (BEST PRACTICE - single read)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_TRAIN_PLAN_LEG AS
    SELECT 
        TRAIN_PLAN_LEG_ID, TRAIN_PLAN_ID, TRAIN_DRCTN_CD, TRAIN_PLAN_LEG_NM, MTP_TITAN_NBR,
        RECORD_CREATE_TMS, RECORD_UPDATE_TMS, CREATE_USER_ID, UPDATE_USER_ID,
        TURN_LEG_SQNC_NBR, TYES_TRAIN_ID, MTP_TOTAL_RTPNT_SENT_QTY,
        MTP_ROUTE_CMPLT_CD, MTP_TRAIN_STATE_CD, SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_RAW.SADB.TRAIN_PLAN_LEG_BASE_HIST_STREAM
    WHERE NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG');
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_TRAIN_PLAN_LEG;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_TRAIN_PLAN_LEG;
        v_end_time := CURRENT_TIMESTAMP();
        
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
            TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME,
            ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED,
            ERROR_MESSAGE, CREATED_AT
        ) VALUES (
            'TRAIN_PLAN_LEG', :v_batch_id, 'NO_DATA', :v_start_time, :v_end_time,
            0, 0, 0, 0,
            NULL, CURRENT_TIMESTAMP()
        );
        
        RETURN 'NO_DATA: Stream has no changes to process at ' || CURRENT_TIMESTAMP()::VARCHAR;
    END IF;
    
    -- =========================================================================
    -- PRE-MERGE METRICS: Count by operation type for logging
    -- =========================================================================
    SELECT 
        COUNT(CASE WHEN CDC_ACTION = 'INSERT' AND CDC_IS_UPDATE = FALSE THEN 1 END),
        COUNT(CASE WHEN CDC_ACTION = 'INSERT' AND CDC_IS_UPDATE = TRUE THEN 1 END),
        COUNT(CASE WHEN CDC_ACTION = 'DELETE' AND CDC_IS_UPDATE = FALSE THEN 1 END)
    INTO v_rows_inserted, v_rows_updated, v_rows_deleted
    FROM _CDC_STAGING_TRAIN_PLAN_LEG;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.SADB.TRAIN_PLAN_LEG AS tgt
    USING (
        SELECT 
            TRAIN_PLAN_LEG_ID, TRAIN_PLAN_ID, TRAIN_DRCTN_CD, TRAIN_PLAN_LEG_NM, MTP_TITAN_NBR,
            RECORD_CREATE_TMS, RECORD_UPDATE_TMS, CREATE_USER_ID, UPDATE_USER_ID,
            TURN_LEG_SQNC_NBR, TYES_TRAIN_ID, MTP_TOTAL_RTPNT_SENT_QTY,
            MTP_ROUTE_CMPLT_CD, MTP_TRAIN_STATE_CD, SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_TRAIN_PLAN_LEG
    ) AS src
    ON tgt.TRAIN_PLAN_LEG_ID = src.TRAIN_PLAN_LEG_ID
    
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
        UPDATE SET
            tgt.TRAIN_PLAN_ID = src.TRAIN_PLAN_ID,
            tgt.TRAIN_DRCTN_CD = src.TRAIN_DRCTN_CD,
            tgt.TRAIN_PLAN_LEG_NM = src.TRAIN_PLAN_LEG_NM,
            tgt.MTP_TITAN_NBR = src.MTP_TITAN_NBR,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.TURN_LEG_SQNC_NBR = src.TURN_LEG_SQNC_NBR,
            tgt.TYES_TRAIN_ID = src.TYES_TRAIN_ID,
            tgt.MTP_TOTAL_RTPNT_SENT_QTY = src.MTP_TOTAL_RTPNT_SENT_QTY,
            tgt.MTP_ROUTE_CMPLT_CD = src.MTP_ROUTE_CMPLT_CD,
            tgt.MTP_TRAIN_STATE_CD = src.MTP_TRAIN_STATE_CD,
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
            tgt.TRAIN_PLAN_ID = src.TRAIN_PLAN_ID,
            tgt.TRAIN_DRCTN_CD = src.TRAIN_DRCTN_CD,
            tgt.TRAIN_PLAN_LEG_NM = src.TRAIN_PLAN_LEG_NM,
            tgt.MTP_TITAN_NBR = src.MTP_TITAN_NBR,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.TURN_LEG_SQNC_NBR = src.TURN_LEG_SQNC_NBR,
            tgt.TYES_TRAIN_ID = src.TYES_TRAIN_ID,
            tgt.MTP_TOTAL_RTPNT_SENT_QTY = src.MTP_TOTAL_RTPNT_SENT_QTY,
            tgt.MTP_ROUTE_CMPLT_CD = src.MTP_ROUTE_CMPLT_CD,
            tgt.MTP_TRAIN_STATE_CD = src.MTP_TRAIN_STATE_CD,
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
            TRAIN_PLAN_LEG_ID, TRAIN_PLAN_ID, TRAIN_DRCTN_CD, TRAIN_PLAN_LEG_NM, MTP_TITAN_NBR,
            RECORD_CREATE_TMS, RECORD_UPDATE_TMS, CREATE_USER_ID, UPDATE_USER_ID,
            TURN_LEG_SQNC_NBR, TYES_TRAIN_ID, MTP_TOTAL_RTPNT_SENT_QTY,
            MTP_ROUTE_CMPLT_CD, MTP_TRAIN_STATE_CD, SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.TRAIN_PLAN_LEG_ID, src.TRAIN_PLAN_ID, src.TRAIN_DRCTN_CD, src.TRAIN_PLAN_LEG_NM, src.MTP_TITAN_NBR,
            src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS, src.CREATE_USER_ID, src.UPDATE_USER_ID,
            src.TURN_LEG_SQNC_NBR, src.TYES_TRAIN_ID, src.MTP_TOTAL_RTPNT_SENT_QTY,
            src.MTP_ROUTE_CMPLT_CD, src.MTP_TRAIN_STATE_CD, src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    v_end_time := CURRENT_TIMESTAMP();
    
    DROP TABLE IF EXISTS _CDC_STAGING_TRAIN_PLAN_LEG;
    
    -- =========================================================================
    -- EXECUTION LOGGING: Write metrics to CDC_EXECUTION_LOG
    -- =========================================================================
    INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
        TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME,
        ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED,
        ERROR_MESSAGE, CREATED_AT
    ) VALUES (
        'TRAIN_PLAN_LEG', :v_batch_id, 'SUCCESS', :v_start_time, :v_end_time,
        :v_rows_merged, :v_rows_inserted, :v_rows_updated, :v_rows_deleted,
        NULL, CURRENT_TIMESTAMP()
    );
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes (I:' || v_rows_inserted || 
           ' U:' || v_rows_updated || ' D:' || v_rows_deleted || '). Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        v_end_time := CURRENT_TIMESTAMP();
        v_error_msg := SQLERRM;
        
        DROP TABLE IF EXISTS _CDC_STAGING_TRAIN_PLAN_LEG;
        
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
            TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME,
            ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED,
            ERROR_MESSAGE, CREATED_AT
        ) VALUES (
            'TRAIN_PLAN_LEG', :v_batch_id, 'ERROR', :v_start_time, :v_end_time,
            0, 0, 0, 0,
            :v_error_msg, CURRENT_TIMESTAMP()
        );
        
        RETURN 'ERROR: ' || v_error_msg || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.SADB.TASK_SP_PROCESS_TRAIN_PLAN_LEG
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process TRAIN_PLAN_LEG_BASE CDC changes into data preservation table '
AS
    CALL D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_LEG();

ALTER TASK D_RAW.SADB.TASK_SP_PROCESS_TRAIN_PLAN_LEG RESUME;


-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'TRAIN_PLAN_LEG%' IN SCHEMA D_BRONZE.SADB;
-- SHOW STREAMS LIKE 'TRAIN_PLAN_LEG%' IN SCHEMA D_RAW.SADB;
-- SHOW TASKS LIKE 'TASK_SP_PROCESS_TRAIN_PLAN_LEG%' IN SCHEMA D_RAW.SADB;
-- CALL D_RAW.SADB.SP_PROCESS_TRAIN_PLAN_LEG();

