/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE
================================================================================
Source Table : D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE
Target Table : D_BRONZE.SADB.EQPMV_EQPMT_EVENT_TYPE
Stream       : D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE_HIST_STREAM
Procedure    : D_RAW.SADB.SP_PROCESS_EQPMV_EQPMT_EVENT_TYPE()
Task         : D_RAW.SADB.TASK_PROCESS_EQPMV_EQPMT_EVENT_TYPE
Primary Key  : EQPMT_EVENT_TYPE_ID (Single)
Total Columns: 24 source + 6 CDC metadata = 30
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE TABLE IF NOT EXISTS D_BRONZE.SADB.EQPMV_EQPMT_EVENT_TYPE (
    EQPMT_EVENT_TYPE_ID NUMBER(18,0) NOT NULL,
    TRNII_EVENT_CD VARCHAR(16),
    YARDS_EVENT_CD VARCHAR(12),
    IMDL_EVENT_CD VARCHAR(12),
    TRAIN_EVENT_CD VARCHAR(16),
    MTP_CAR_EVENT_CD VARCHAR(4),
    FSTWY_EVENT_CD VARCHAR(12),
    BAD_ORDER_EVENT_CD VARCHAR(12),
    SMS_EVENT_CD VARCHAR(12),
    AEI_EVENT_CD VARCHAR(32),
    DSCRPT_TEXT VARCHAR(320),
    CREATE_USER_ID VARCHAR(32),
    UPDATE_USER_ID VARCHAR(32),
    RECORD_CREATE_TMS TIMESTAMP_NTZ(0),
    RECORD_UPDATE_TMS TIMESTAMP_NTZ(0),
    LMS_EVENT_CD VARCHAR(16),
    EVENT_ACTIVE_IND VARCHAR(4),
    EVENT_TYPE_CD VARCHAR(40),
    AAR_STNDRD_EVENT_CD VARCHAR(16),
    EDI_EVENT_CD VARCHAR(8),
    EDI_EVENT_CD_QLFR VARCHAR(48),
    YARD_TRACK_MVMNT_EVENT_CD VARCHAR(32),
    SNW_OPERATION_TYPE VARCHAR(1),
    SNW_LAST_REPLICATED TIMESTAMP_NTZ(9),

    -- CDC Metadata columns for data preservation
    CDC_OPERATION VARCHAR(10),  
    CDC_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IS_DELETED BOOLEAN DEFAULT FALSE,    -- Soft delete flag
    RECORD_CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RECORD_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_LOAD_BATCH_ID VARCHAR(100), 

    PRIMARY KEY (EQPMT_EVENT_TYPE_ID)
);

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 45,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 15;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for EQPMV_EQPMT_EVENT_TYPE_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.SADB.SP_PROCESS_EQPMV_EQPMT_EVENT_TYPE()
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
        FROM D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE_HIST_STREAM
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
        
        CREATE OR REPLACE STREAM D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE_HIST_STREAM
        ON TABLE D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection';
        
        MERGE INTO D_BRONZE.SADB.EQPMV_EQPMT_EVENT_TYPE AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE src
            LEFT JOIN D_BRONZE.SADB.EQPMV_EQPMT_EVENT_TYPE tgt 
                ON src.EQPMT_EVENT_TYPE_ID = tgt.EQPMT_EVENT_TYPE_ID
            WHERE tgt.EQPMT_EVENT_TYPE_ID IS NULL
               OR tgt.IS_DELETED = TRUE
        ) AS src
        ON tgt.EQPMT_EVENT_TYPE_ID = src.EQPMT_EVENT_TYPE_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.TRNII_EVENT_CD = src.TRNII_EVENT_CD,
            tgt.YARDS_EVENT_CD = src.YARDS_EVENT_CD,
            tgt.IMDL_EVENT_CD = src.IMDL_EVENT_CD,
            tgt.TRAIN_EVENT_CD = src.TRAIN_EVENT_CD,
            tgt.MTP_CAR_EVENT_CD = src.MTP_CAR_EVENT_CD,
            tgt.FSTWY_EVENT_CD = src.FSTWY_EVENT_CD,
            tgt.BAD_ORDER_EVENT_CD = src.BAD_ORDER_EVENT_CD,
            tgt.SMS_EVENT_CD = src.SMS_EVENT_CD,
            tgt.AEI_EVENT_CD = src.AEI_EVENT_CD,
            tgt.DSCRPT_TEXT = src.DSCRPT_TEXT,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.LMS_EVENT_CD = src.LMS_EVENT_CD,
            tgt.EVENT_ACTIVE_IND = src.EVENT_ACTIVE_IND,
            tgt.EVENT_TYPE_CD = src.EVENT_TYPE_CD,
            tgt.AAR_STNDRD_EVENT_CD = src.AAR_STNDRD_EVENT_CD,
            tgt.EDI_EVENT_CD = src.EDI_EVENT_CD,
            tgt.EDI_EVENT_CD_QLFR = src.EDI_EVENT_CD_QLFR,
            tgt.YARD_TRACK_MVMNT_EVENT_CD = src.YARD_TRACK_MVMNT_EVENT_CD,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'RELOADED',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        WHEN NOT MATCHED THEN INSERT (
            EQPMT_EVENT_TYPE_ID, TRNII_EVENT_CD, YARDS_EVENT_CD, IMDL_EVENT_CD, TRAIN_EVENT_CD,
            MTP_CAR_EVENT_CD, FSTWY_EVENT_CD, BAD_ORDER_EVENT_CD, SMS_EVENT_CD, AEI_EVENT_CD,
            DSCRPT_TEXT, CREATE_USER_ID, UPDATE_USER_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            LMS_EVENT_CD, EVENT_ACTIVE_IND, EVENT_TYPE_CD, AAR_STNDRD_EVENT_CD, EDI_EVENT_CD,
            EDI_EVENT_CD_QLFR, YARD_TRACK_MVMNT_EVENT_CD, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.EQPMT_EVENT_TYPE_ID, src.TRNII_EVENT_CD, src.YARDS_EVENT_CD, src.IMDL_EVENT_CD, src.TRAIN_EVENT_CD,
            src.MTP_CAR_EVENT_CD, src.FSTWY_EVENT_CD, src.BAD_ORDER_EVENT_CD, src.SMS_EVENT_CD, src.AEI_EVENT_CD,
            src.DSCRPT_TEXT, src.CREATE_USER_ID, src.UPDATE_USER_ID, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS,
            src.LMS_EVENT_CD, src.EVENT_ACTIVE_IND, src.EVENT_TYPE_CD, src.AAR_STNDRD_EVENT_CD, src.EDI_EVENT_CD,
            src.EDI_EVENT_CD_QLFR, src.YARD_TRACK_MVMNT_EVENT_CD, src.SNW_OPERATION_TYPE, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
        
        v_rows_merged := SQLROWCOUNT;
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_merged || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- CHECK 2: Stage stream data into temp table (BEST PRACTICE - single read)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_EQPMV_EQPMT_EVENT_TYPE AS
    SELECT 
        EQPMT_EVENT_TYPE_ID, TRNII_EVENT_CD, YARDS_EVENT_CD, IMDL_EVENT_CD, TRAIN_EVENT_CD,
        MTP_CAR_EVENT_CD, FSTWY_EVENT_CD, BAD_ORDER_EVENT_CD, SMS_EVENT_CD, AEI_EVENT_CD,
        DSCRPT_TEXT, CREATE_USER_ID, UPDATE_USER_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
        LMS_EVENT_CD, EVENT_ACTIVE_IND, EVENT_TYPE_CD, AAR_STNDRD_EVENT_CD, EDI_EVENT_CD,
        EDI_EVENT_CD_QLFR, YARD_TRACK_MVMNT_EVENT_CD, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE_HIST_STREAM;
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_EQPMV_EQPMT_EVENT_TYPE;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_EQPMV_EQPMT_EVENT_TYPE;
        RETURN 'NO_DATA: Stream has no changes to process at ' || CURRENT_TIMESTAMP()::VARCHAR;
    END IF;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.SADB.EQPMV_EQPMT_EVENT_TYPE AS tgt
    USING (
        SELECT 
            EQPMT_EVENT_TYPE_ID, TRNII_EVENT_CD, YARDS_EVENT_CD, IMDL_EVENT_CD, TRAIN_EVENT_CD,
            MTP_CAR_EVENT_CD, FSTWY_EVENT_CD, BAD_ORDER_EVENT_CD, SMS_EVENT_CD, AEI_EVENT_CD,
            DSCRPT_TEXT, CREATE_USER_ID, UPDATE_USER_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            LMS_EVENT_CD, EVENT_ACTIVE_IND, EVENT_TYPE_CD, AAR_STNDRD_EVENT_CD, EDI_EVENT_CD,
            EDI_EVENT_CD_QLFR, YARD_TRACK_MVMNT_EVENT_CD, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_EQPMV_EQPMT_EVENT_TYPE
    ) AS src
    ON tgt.EQPMT_EVENT_TYPE_ID = src.EQPMT_EVENT_TYPE_ID
    
    -- UPDATE scenario (METADATA$ACTION='INSERT' AND METADATA$ISUPDATE=TRUE)
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
        UPDATE SET
            tgt.TRNII_EVENT_CD = src.TRNII_EVENT_CD,
            tgt.YARDS_EVENT_CD = src.YARDS_EVENT_CD,
            tgt.IMDL_EVENT_CD = src.IMDL_EVENT_CD,
            tgt.TRAIN_EVENT_CD = src.TRAIN_EVENT_CD,
            tgt.MTP_CAR_EVENT_CD = src.MTP_CAR_EVENT_CD,
            tgt.FSTWY_EVENT_CD = src.FSTWY_EVENT_CD,
            tgt.BAD_ORDER_EVENT_CD = src.BAD_ORDER_EVENT_CD,
            tgt.SMS_EVENT_CD = src.SMS_EVENT_CD,
            tgt.AEI_EVENT_CD = src.AEI_EVENT_CD,
            tgt.DSCRPT_TEXT = src.DSCRPT_TEXT,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.LMS_EVENT_CD = src.LMS_EVENT_CD,
            tgt.EVENT_ACTIVE_IND = src.EVENT_ACTIVE_IND,
            tgt.EVENT_TYPE_CD = src.EVENT_TYPE_CD,
            tgt.AAR_STNDRD_EVENT_CD = src.AAR_STNDRD_EVENT_CD,
            tgt.EDI_EVENT_CD = src.EDI_EVENT_CD,
            tgt.EDI_EVENT_CD_QLFR = src.EDI_EVENT_CD_QLFR,
            tgt.YARD_TRACK_MVMNT_EVENT_CD = src.YARD_TRACK_MVMNT_EVENT_CD,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
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
            tgt.TRNII_EVENT_CD = src.TRNII_EVENT_CD,
            tgt.YARDS_EVENT_CD = src.YARDS_EVENT_CD,
            tgt.IMDL_EVENT_CD = src.IMDL_EVENT_CD,
            tgt.TRAIN_EVENT_CD = src.TRAIN_EVENT_CD,
            tgt.MTP_CAR_EVENT_CD = src.MTP_CAR_EVENT_CD,
            tgt.FSTWY_EVENT_CD = src.FSTWY_EVENT_CD,
            tgt.BAD_ORDER_EVENT_CD = src.BAD_ORDER_EVENT_CD,
            tgt.SMS_EVENT_CD = src.SMS_EVENT_CD,
            tgt.AEI_EVENT_CD = src.AEI_EVENT_CD,
            tgt.DSCRPT_TEXT = src.DSCRPT_TEXT,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.LMS_EVENT_CD = src.LMS_EVENT_CD,
            tgt.EVENT_ACTIVE_IND = src.EVENT_ACTIVE_IND,
            tgt.EVENT_TYPE_CD = src.EVENT_TYPE_CD,
            tgt.AAR_STNDRD_EVENT_CD = src.AAR_STNDRD_EVENT_CD,
            tgt.EDI_EVENT_CD = src.EDI_EVENT_CD,
            tgt.EDI_EVENT_CD_QLFR = src.EDI_EVENT_CD_QLFR,
            tgt.YARD_TRACK_MVMNT_EVENT_CD = src.YARD_TRACK_MVMNT_EVENT_CD,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'INSERT',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    -- NEW INSERT scenario
    WHEN NOT MATCHED AND src.CDC_ACTION = 'INSERT' THEN 
        INSERT (
            EQPMT_EVENT_TYPE_ID, TRNII_EVENT_CD, YARDS_EVENT_CD, IMDL_EVENT_CD, TRAIN_EVENT_CD,
            MTP_CAR_EVENT_CD, FSTWY_EVENT_CD, BAD_ORDER_EVENT_CD, SMS_EVENT_CD, AEI_EVENT_CD,
            DSCRPT_TEXT, CREATE_USER_ID, UPDATE_USER_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            LMS_EVENT_CD, EVENT_ACTIVE_IND, EVENT_TYPE_CD, AAR_STNDRD_EVENT_CD, EDI_EVENT_CD,
            EDI_EVENT_CD_QLFR, YARD_TRACK_MVMNT_EVENT_CD, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.EQPMT_EVENT_TYPE_ID, src.TRNII_EVENT_CD, src.YARDS_EVENT_CD, src.IMDL_EVENT_CD, src.TRAIN_EVENT_CD,
            src.MTP_CAR_EVENT_CD, src.FSTWY_EVENT_CD, src.BAD_ORDER_EVENT_CD, src.SMS_EVENT_CD, src.AEI_EVENT_CD,
            src.DSCRPT_TEXT, src.CREATE_USER_ID, src.UPDATE_USER_ID, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS,
            src.LMS_EVENT_CD, src.EVENT_ACTIVE_IND, src.EVENT_TYPE_CD, src.AAR_STNDRD_EVENT_CD, src.EDI_EVENT_CD,
            src.EDI_EVENT_CD_QLFR, src.YARD_TRACK_MVMNT_EVENT_CD, src.SNW_OPERATION_TYPE, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    DROP TABLE IF EXISTS _CDC_STAGING_EQPMV_EQPMT_EVENT_TYPE;
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes. Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS _CDC_STAGING_EQPMV_EQPMT_EVENT_TYPE;
        RETURN 'ERROR: ' || SQLERRM || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.SADB.TASK_PROCESS_EQPMV_EQPMT_EVENT_TYPE
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process EQPMV_EQPMT_EVENT_TYPE_BASE CDC changes into data preservation table'
WHEN
    SYSTEM$STREAM_HAS_DATA('D_RAW.SADB.EQPMV_EQPMT_EVENT_TYPE_BASE_HIST_STREAM')
AS
    CALL D_RAW.SADB.SP_PROCESS_EQPMV_EQPMT_EVENT_TYPE();

ALTER TASK D_RAW.SADB.TASK_PROCESS_EQPMV_EQPMT_EVENT_TYPE RESUME;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'EQPMV_EQPMT_EVENT_TYPE%' IN SCHEMA D_BRONZE.SADB;
-- SHOW STREAMS LIKE 'EQPMV_EQPMT_EVENT_TYPE%' IN SCHEMA D_RAW.SADB;
-- SHOW TASKS LIKE 'TASK_PROCESS_EQPMV_EQPMT_EVENT_TYPE%' IN SCHEMA D_RAW.SADB;
-- CALL D_RAW.SADB.SP_PROCESS_EQPMV_EQPMT_EVENT_TYPE();
