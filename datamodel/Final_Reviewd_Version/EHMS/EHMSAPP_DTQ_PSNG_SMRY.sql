/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_RAW.EHMS.EHMSAPP_DTQ_PSNG_SMRY_BASE
================================================================================
Source Table : D_RAW.EHMS.EHMSAPP_DTQ_PSNG_SMRY_BASE
Target Table : D_BRONZE.EHMS.EHMSAPP_DTQ_PSNG_SMRY
Stream       : D_RAW.EHMS.EHMSAPP_DTQ_PSNG_SMRY_BASE_HIST_STREAM
Procedure    : D_RAW.EHMS.SP_PROCESS_EHMSAPP_DTQ_PSNG_SMRY()
Task         : D_RAW.EHMS.TASK_PROCESS_EHMSAPP_DTQ_PSNG_SMRY
Primary Key  : PSNG_SMRY_ID (Single)
Total Columns: 27 source + 6 CDC metadata = 33
Filter       : NONE (no purge filter required for EHMS schema)
================================================================================
VERSION      : v1.0
DATE         : 2026-03-17
CHANGES      :
  - Added execution logging to D_BRONZE.MONITORING.CDC_EXECUTION_LOG
  - Staleness detection via SELECT COUNT(*) WHERE 1=0 pattern
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE OR ALTER TABLE D_BRONZE.EHMS.EHMSAPP_DTQ_PSNG_SMRY (
    PSNG_SMRY_ID NUMBER(18,0) NOT NULL,
    CREATE_USER_ID VARCHAR(32),
    RECORD_CREATE_TMS TIMESTAMP_NTZ(0),
    RECORD_UPDATE_TMS TIMESTAMP_NTZ(0),
    UPDATE_USER_ID VARCHAR(32),
    PSNG_SMRY_FILE_NM VARCHAR(400),
    SOURCE_CREATE_TMS TIMESTAMP_NTZ(0),
    SOURCE_UPDATE_TMS TIMESTAMP_NTZ(0),
    WYSD_DTCTN_DEVICE_ID NUMBER(18,0),
    DEVICE_CNFGRT_ID NUMBER(18,0),
    RPRTD_DEVICE_FILE_NM VARCHAR(160),
    RPRTD_DEVICE_INTGRT_ALARM_CD NUMBER(1,0),
    REPORT_TYPE_CD VARCHAR(24),
    REPORT_EXPIRY_DT TIMESTAMP_NTZ(0),
    REPORT_EXPIRY_REASON_TXT VARCHAR(1000),
    REPORT_EXPIRY_USER_ID VARCHAR(32),
    REPORT_PERIOD_START_TMS TIMESTAMP_NTZ(0),
    REPORT_PERIOD_END_TMS TIMESTAMP_NTZ(0),
    RPRTD_BNGLW_STATUS_CD NUMBER(1,0),
    SITE_REPORT_TMS TIMESTAMP_NTZ(0),
    RPRTD_CLBRTN_TMS TIMESTAMP_NTZ(0),
    RPRTD_PRTCL_MAJOR_VRSN_NBR NUMBER(3,0),
    RPRTD_PRTCL_MINOR_VRSN_NBR NUMBER(3,0),
    DEVICE_CMPNT_CNFGRT_ID NUMBER(18,0),
    SNW_OPERATION_TYPE VARCHAR(1),
    SNW_LAST_REPLICATED TIMESTAMP_NTZ(9),

    CDC_OPERATION VARCHAR(10),
    CDC_TIMESTAMP TIMESTAMP_NTZ,
    IS_DELETED BOOLEAN,
    RECORD_CREATED_AT TIMESTAMP_NTZ,
    RECORD_UPDATED_AT TIMESTAMP_NTZ,
    SOURCE_LOAD_BATCH_ID VARCHAR(100),
    SNW_OPERATION_OWNER VARCHAR(256),

    PRIMARY KEY (PSNG_SMRY_ID)
);

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_RAW.EHMS.EHMSAPP_DTQ_PSNG_SMRY_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 45,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 15;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_RAW.EHMS.EHMSAPP_DTQ_PSNG_SMRY_BASE_HIST_STREAM
ON TABLE D_RAW.EHMS.EHMSAPP_DTQ_PSNG_SMRY_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for EHMSAPP_DTQ_PSNG_SMRY_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing (ENHANCED v1)
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.EHMS.SP_PROCESS_EHMSAPP_DTQ_PSNG_SMRY()
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
        FROM D_RAW.EHMS.EHMSAPP_DTQ_PSNG_SMRY_BASE_HIST_STREAM
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
        
        CREATE OR REPLACE STREAM D_RAW.EHMS.EHMSAPP_DTQ_PSNG_SMRY_BASE_HIST_STREAM
        ON TABLE D_RAW.EHMS.EHMSAPP_DTQ_PSNG_SMRY_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection';
        
        MERGE INTO D_BRONZE.EHMS.EHMSAPP_DTQ_PSNG_SMRY AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_RAW.EHMS.EHMSAPP_DTQ_PSNG_SMRY_BASE_HIST_STREAM src
        ) AS src
        ON tgt.PSNG_SMRY_ID = src.PSNG_SMRY_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.PSNG_SMRY_FILE_NM = src.PSNG_SMRY_FILE_NM,
            tgt.SOURCE_CREATE_TMS = src.SOURCE_CREATE_TMS,
            tgt.SOURCE_UPDATE_TMS = src.SOURCE_UPDATE_TMS,
            tgt.WYSD_DTCTN_DEVICE_ID = src.WYSD_DTCTN_DEVICE_ID,
            tgt.DEVICE_CNFGRT_ID = src.DEVICE_CNFGRT_ID,
            tgt.RPRTD_DEVICE_FILE_NM = src.RPRTD_DEVICE_FILE_NM,
            tgt.RPRTD_DEVICE_INTGRT_ALARM_CD = src.RPRTD_DEVICE_INTGRT_ALARM_CD,
            tgt.REPORT_TYPE_CD = src.REPORT_TYPE_CD,
            tgt.REPORT_EXPIRY_DT = src.REPORT_EXPIRY_DT,
            tgt.REPORT_EXPIRY_REASON_TXT = src.REPORT_EXPIRY_REASON_TXT,
            tgt.REPORT_EXPIRY_USER_ID = src.REPORT_EXPIRY_USER_ID,
            tgt.REPORT_PERIOD_START_TMS = src.REPORT_PERIOD_START_TMS,
            tgt.REPORT_PERIOD_END_TMS = src.REPORT_PERIOD_END_TMS,
            tgt.RPRTD_BNGLW_STATUS_CD = src.RPRTD_BNGLW_STATUS_CD,
            tgt.SITE_REPORT_TMS = src.SITE_REPORT_TMS,
            tgt.RPRTD_CLBRTN_TMS = src.RPRTD_CLBRTN_TMS,
            tgt.RPRTD_PRTCL_MAJOR_VRSN_NBR = src.RPRTD_PRTCL_MAJOR_VRSN_NBR,
            tgt.RPRTD_PRTCL_MINOR_VRSN_NBR = src.RPRTD_PRTCL_MINOR_VRSN_NBR,
            tgt.DEVICE_CMPNT_CNFGRT_ID = src.DEVICE_CMPNT_CNFGRT_ID,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_OPERATION_OWNER = src.SNW_OPERATION_OWNER,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'RELOADED',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        WHEN NOT MATCHED THEN INSERT (
            PSNG_SMRY_ID, CREATE_USER_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS, UPDATE_USER_ID,
            PSNG_SMRY_FILE_NM, SOURCE_CREATE_TMS, SOURCE_UPDATE_TMS, WYSD_DTCTN_DEVICE_ID, DEVICE_CNFGRT_ID,
            RPRTD_DEVICE_FILE_NM, RPRTD_DEVICE_INTGRT_ALARM_CD, REPORT_TYPE_CD, REPORT_EXPIRY_DT,
            REPORT_EXPIRY_REASON_TXT, REPORT_EXPIRY_USER_ID, REPORT_PERIOD_START_TMS, REPORT_PERIOD_END_TMS,
            RPRTD_BNGLW_STATUS_CD, SITE_REPORT_TMS, RPRTD_CLBRTN_TMS,
            RPRTD_PRTCL_MAJOR_VRSN_NBR, RPRTD_PRTCL_MINOR_VRSN_NBR, DEVICE_CMPNT_CNFGRT_ID,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.PSNG_SMRY_ID, src.CREATE_USER_ID, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS, src.UPDATE_USER_ID,
            src.PSNG_SMRY_FILE_NM, src.SOURCE_CREATE_TMS, src.SOURCE_UPDATE_TMS, src.WYSD_DTCTN_DEVICE_ID, src.DEVICE_CNFGRT_ID,
            src.RPRTD_DEVICE_FILE_NM, src.RPRTD_DEVICE_INTGRT_ALARM_CD, src.REPORT_TYPE_CD, src.REPORT_EXPIRY_DT,
            src.REPORT_EXPIRY_REASON_TXT, src.REPORT_EXPIRY_USER_ID, src.REPORT_PERIOD_START_TMS, src.REPORT_PERIOD_END_TMS,
            src.RPRTD_BNGLW_STATUS_CD, src.SITE_REPORT_TMS, src.RPRTD_CLBRTN_TMS,
            src.RPRTD_PRTCL_MAJOR_VRSN_NBR, src.RPRTD_PRTCL_MINOR_VRSN_NBR, src.DEVICE_CMPNT_CNFGRT_ID,
            src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
        
        v_rows_merged := SQLROWCOUNT;
        v_end_time := CURRENT_TIMESTAMP();
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME, ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ERROR_MESSAGE, CREATED_AT) VALUES ('EHMSAPP_DTQ_PSNG_SMRY', :v_batch_id, 'RECOVERY', :v_start_time, :v_end_time, :v_rows_merged, :v_rows_merged, 0, 0, :v_error_msg, CURRENT_TIMESTAMP());
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_merged || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- CHECK 2: Stage stream data into temp table (BEST PRACTICE - single read)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_EHMSAPP_DTQ_PSNG_SMRY AS
    SELECT 
        PSNG_SMRY_ID, CREATE_USER_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS, UPDATE_USER_ID,
        PSNG_SMRY_FILE_NM, SOURCE_CREATE_TMS, SOURCE_UPDATE_TMS, WYSD_DTCTN_DEVICE_ID, DEVICE_CNFGRT_ID,
        RPRTD_DEVICE_FILE_NM, RPRTD_DEVICE_INTGRT_ALARM_CD, REPORT_TYPE_CD, REPORT_EXPIRY_DT,
        REPORT_EXPIRY_REASON_TXT, REPORT_EXPIRY_USER_ID, REPORT_PERIOD_START_TMS, REPORT_PERIOD_END_TMS,
        RPRTD_BNGLW_STATUS_CD, SITE_REPORT_TMS, RPRTD_CLBRTN_TMS,
        RPRTD_PRTCL_MAJOR_VRSN_NBR, RPRTD_PRTCL_MINOR_VRSN_NBR, DEVICE_CMPNT_CNFGRT_ID,
        SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_RAW.EHMS.EHMSAPP_DTQ_PSNG_SMRY_BASE_HIST_STREAM;
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_EHMSAPP_DTQ_PSNG_SMRY;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_EHMSAPP_DTQ_PSNG_SMRY;
        v_end_time := CURRENT_TIMESTAMP();
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME, ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ERROR_MESSAGE, CREATED_AT) VALUES ('EHMSAPP_DTQ_PSNG_SMRY', :v_batch_id, 'NO_DATA', :v_start_time, :v_end_time, 0, 0, 0, 0, NULL, CURRENT_TIMESTAMP());
        RETURN 'NO_DATA: Stream has no changes to process at ' || CURRENT_TIMESTAMP()::VARCHAR;
    END IF;
    
    -- =========================================================================
    -- PRE-MERGE METRICS
    -- =========================================================================
    SELECT 
        COUNT(CASE WHEN CDC_ACTION = 'INSERT' AND CDC_IS_UPDATE = FALSE THEN 1 END),
        COUNT(CASE WHEN CDC_ACTION = 'INSERT' AND CDC_IS_UPDATE = TRUE THEN 1 END),
        COUNT(CASE WHEN CDC_ACTION = 'DELETE' AND CDC_IS_UPDATE = FALSE THEN 1 END)
    INTO v_rows_inserted, v_rows_updated, v_rows_deleted
    FROM _CDC_STAGING_EHMSAPP_DTQ_PSNG_SMRY;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.EHMS.EHMSAPP_DTQ_PSNG_SMRY AS tgt
    USING (
        SELECT 
            PSNG_SMRY_ID, CREATE_USER_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS, UPDATE_USER_ID,
            PSNG_SMRY_FILE_NM, SOURCE_CREATE_TMS, SOURCE_UPDATE_TMS, WYSD_DTCTN_DEVICE_ID, DEVICE_CNFGRT_ID,
            RPRTD_DEVICE_FILE_NM, RPRTD_DEVICE_INTGRT_ALARM_CD, REPORT_TYPE_CD, REPORT_EXPIRY_DT,
            REPORT_EXPIRY_REASON_TXT, REPORT_EXPIRY_USER_ID, REPORT_PERIOD_START_TMS, REPORT_PERIOD_END_TMS,
            RPRTD_BNGLW_STATUS_CD, SITE_REPORT_TMS, RPRTD_CLBRTN_TMS,
            RPRTD_PRTCL_MAJOR_VRSN_NBR, RPRTD_PRTCL_MINOR_VRSN_NBR, DEVICE_CMPNT_CNFGRT_ID,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_ACTION, CDC_IS_UPDATE, ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_EHMSAPP_DTQ_PSNG_SMRY
    ) AS src
    ON tgt.PSNG_SMRY_ID = src.PSNG_SMRY_ID
    
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
        UPDATE SET
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.PSNG_SMRY_FILE_NM = src.PSNG_SMRY_FILE_NM,
            tgt.SOURCE_CREATE_TMS = src.SOURCE_CREATE_TMS,
            tgt.SOURCE_UPDATE_TMS = src.SOURCE_UPDATE_TMS,
            tgt.WYSD_DTCTN_DEVICE_ID = src.WYSD_DTCTN_DEVICE_ID,
            tgt.DEVICE_CNFGRT_ID = src.DEVICE_CNFGRT_ID,
            tgt.RPRTD_DEVICE_FILE_NM = src.RPRTD_DEVICE_FILE_NM,
            tgt.RPRTD_DEVICE_INTGRT_ALARM_CD = src.RPRTD_DEVICE_INTGRT_ALARM_CD,
            tgt.REPORT_TYPE_CD = src.REPORT_TYPE_CD,
            tgt.REPORT_EXPIRY_DT = src.REPORT_EXPIRY_DT,
            tgt.REPORT_EXPIRY_REASON_TXT = src.REPORT_EXPIRY_REASON_TXT,
            tgt.REPORT_EXPIRY_USER_ID = src.REPORT_EXPIRY_USER_ID,
            tgt.REPORT_PERIOD_START_TMS = src.REPORT_PERIOD_START_TMS,
            tgt.REPORT_PERIOD_END_TMS = src.REPORT_PERIOD_END_TMS,
            tgt.RPRTD_BNGLW_STATUS_CD = src.RPRTD_BNGLW_STATUS_CD,
            tgt.SITE_REPORT_TMS = src.SITE_REPORT_TMS,
            tgt.RPRTD_CLBRTN_TMS = src.RPRTD_CLBRTN_TMS,
            tgt.RPRTD_PRTCL_MAJOR_VRSN_NBR = src.RPRTD_PRTCL_MAJOR_VRSN_NBR,
            tgt.RPRTD_PRTCL_MINOR_VRSN_NBR = src.RPRTD_PRTCL_MINOR_VRSN_NBR,
            tgt.DEVICE_CMPNT_CNFGRT_ID = src.DEVICE_CMPNT_CNFGRT_ID,
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
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.PSNG_SMRY_FILE_NM = src.PSNG_SMRY_FILE_NM,
            tgt.SOURCE_CREATE_TMS = src.SOURCE_CREATE_TMS,
            tgt.SOURCE_UPDATE_TMS = src.SOURCE_UPDATE_TMS,
            tgt.WYSD_DTCTN_DEVICE_ID = src.WYSD_DTCTN_DEVICE_ID,
            tgt.DEVICE_CNFGRT_ID = src.DEVICE_CNFGRT_ID,
            tgt.RPRTD_DEVICE_FILE_NM = src.RPRTD_DEVICE_FILE_NM,
            tgt.RPRTD_DEVICE_INTGRT_ALARM_CD = src.RPRTD_DEVICE_INTGRT_ALARM_CD,
            tgt.REPORT_TYPE_CD = src.REPORT_TYPE_CD,
            tgt.REPORT_EXPIRY_DT = src.REPORT_EXPIRY_DT,
            tgt.REPORT_EXPIRY_REASON_TXT = src.REPORT_EXPIRY_REASON_TXT,
            tgt.REPORT_EXPIRY_USER_ID = src.REPORT_EXPIRY_USER_ID,
            tgt.REPORT_PERIOD_START_TMS = src.REPORT_PERIOD_START_TMS,
            tgt.REPORT_PERIOD_END_TMS = src.REPORT_PERIOD_END_TMS,
            tgt.RPRTD_BNGLW_STATUS_CD = src.RPRTD_BNGLW_STATUS_CD,
            tgt.SITE_REPORT_TMS = src.SITE_REPORT_TMS,
            tgt.RPRTD_CLBRTN_TMS = src.RPRTD_CLBRTN_TMS,
            tgt.RPRTD_PRTCL_MAJOR_VRSN_NBR = src.RPRTD_PRTCL_MAJOR_VRSN_NBR,
            tgt.RPRTD_PRTCL_MINOR_VRSN_NBR = src.RPRTD_PRTCL_MINOR_VRSN_NBR,
            tgt.DEVICE_CMPNT_CNFGRT_ID = src.DEVICE_CMPNT_CNFGRT_ID,
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
            PSNG_SMRY_ID, CREATE_USER_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS, UPDATE_USER_ID,
            PSNG_SMRY_FILE_NM, SOURCE_CREATE_TMS, SOURCE_UPDATE_TMS, WYSD_DTCTN_DEVICE_ID, DEVICE_CNFGRT_ID,
            RPRTD_DEVICE_FILE_NM, RPRTD_DEVICE_INTGRT_ALARM_CD, REPORT_TYPE_CD, REPORT_EXPIRY_DT,
            REPORT_EXPIRY_REASON_TXT, REPORT_EXPIRY_USER_ID, REPORT_PERIOD_START_TMS, REPORT_PERIOD_END_TMS,
            RPRTD_BNGLW_STATUS_CD, SITE_REPORT_TMS, RPRTD_CLBRTN_TMS,
            RPRTD_PRTCL_MAJOR_VRSN_NBR, RPRTD_PRTCL_MINOR_VRSN_NBR, DEVICE_CMPNT_CNFGRT_ID,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.PSNG_SMRY_ID, src.CREATE_USER_ID, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS, src.UPDATE_USER_ID,
            src.PSNG_SMRY_FILE_NM, src.SOURCE_CREATE_TMS, src.SOURCE_UPDATE_TMS, src.WYSD_DTCTN_DEVICE_ID, src.DEVICE_CNFGRT_ID,
            src.RPRTD_DEVICE_FILE_NM, src.RPRTD_DEVICE_INTGRT_ALARM_CD, src.REPORT_TYPE_CD, src.REPORT_EXPIRY_DT,
            src.REPORT_EXPIRY_REASON_TXT, src.REPORT_EXPIRY_USER_ID, src.REPORT_PERIOD_START_TMS, src.REPORT_PERIOD_END_TMS,
            src.RPRTD_BNGLW_STATUS_CD, src.SITE_REPORT_TMS, src.RPRTD_CLBRTN_TMS,
            src.RPRTD_PRTCL_MAJOR_VRSN_NBR, src.RPRTD_PRTCL_MINOR_VRSN_NBR, src.DEVICE_CMPNT_CNFGRT_ID,
            src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    v_end_time := CURRENT_TIMESTAMP();
    DROP TABLE IF EXISTS _CDC_STAGING_EHMSAPP_DTQ_PSNG_SMRY;
    
    INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME, ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ERROR_MESSAGE, CREATED_AT) VALUES ('EHMSAPP_DTQ_PSNG_SMRY', :v_batch_id, 'SUCCESS', :v_start_time, :v_end_time, :v_rows_merged, :v_rows_inserted, :v_rows_updated, :v_rows_deleted, NULL, CURRENT_TIMESTAMP());
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes (I:' || v_rows_inserted || ' U:' || v_rows_updated || ' D:' || v_rows_deleted || '). Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        v_end_time := CURRENT_TIMESTAMP();
        v_error_msg := SQLERRM;
        DROP TABLE IF EXISTS _CDC_STAGING_EHMSAPP_DTQ_PSNG_SMRY;
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME, ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ERROR_MESSAGE, CREATED_AT) VALUES ('EHMSAPP_DTQ_PSNG_SMRY', :v_batch_id, 'ERROR', :v_start_time, :v_end_time, 0, 0, 0, 0, :v_error_msg, CURRENT_TIMESTAMP());
        RETURN 'ERROR: ' || v_error_msg || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.EHMS.TASK_PROCESS_EHMSAPP_DTQ_PSNG_SMRY
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process EHMSAPP_DTQ_PSNG_SMRY_BASE CDC changes into data preservation table'
AS
    CALL D_RAW.EHMS.SP_PROCESS_EHMSAPP_DTQ_PSNG_SMRY();

ALTER TASK D_RAW.EHMS.TASK_PROCESS_EHMSAPP_DTQ_PSNG_SMRY RESUME;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'EHMSAPP_DTQ_PSNG_SMRY%' IN SCHEMA D_BRONZE.EHMS;
-- SHOW STREAMS LIKE 'EHMSAPP_DTQ_PSNG_SMRY%' IN SCHEMA D_RAW.EHMS;
-- SHOW TASKS LIKE 'TASK_PROCESS_EHMSAPP_DTQ_PSNG_SMRY%' IN SCHEMA D_RAW.EHMS;
-- CALL D_RAW.EHMS.SP_PROCESS_EHMSAPP_DTQ_PSNG_SMRY();
