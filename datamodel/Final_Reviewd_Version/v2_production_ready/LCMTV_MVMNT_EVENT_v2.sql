/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE
================================================================================
Source Table : D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE
Target Table : D_BRONZE.SADB.LCMTV_MVMNT_EVENT
Stream       : D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE_HIST_STREAM
Procedure    : D_RAW.SADB.SP_PROCESS_LCMTV_MVMNT_EVENT()
Task         : D_RAW.SADB.TASK_PROCESS_LCMTV_MVMNT_EVENT
Primary Key  : EVENT_ID (Single)
Total Columns: 44 source + 6 CDC metadata = 50
Filter       : SNW_OPERATION_OWNER NOT IN ('TSDPRG','EMEPRG') (exclude purged records)
================================================================================
VERSION      : v1.0
DATE         : 2026-03-13
CHANGES      : 
  - Added execution logging to D_BRONZE.MONITORING.CDC_EXECUTION_LOG
  - Staleness detection via SELECT COUNT(*) WHERE 1=0 pattern
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE OR ALTER TABLE D_BRONZE.SADB.LCMTV_MVMNT_EVENT (
    EVENT_ID NUMBER(18,0) NOT NULL,
    EQPMT_EVENT_TYPE_ID NUMBER(18,0),
    SCAC_CD VARCHAR(16),
    FSAC_CD VARCHAR(20),
    TRSTN_VRSN_NBR NUMBER(5,0),
    REPORT_TMS TIMESTAMP_NTZ(0),
    SQNC_NBR NUMBER(5,0),
    DRCTN_CD VARCHAR(4),
    SOURCE_SYSTEM_NM VARCHAR(40),
    MARK_CD VARCHAR(16),
    EQPUN_NBR VARCHAR(40),
    OPTRN_EVENT_ID NUMBER(18,0),
    ORNTTN_CD VARCHAR(4),
    DEAD_HEAD_IND VARCHAR(4),
    RGN_NM_TRK_NBR NUMBER(18,0),
    LMS_PRFL_SQNC_NBR NUMBER(3,0),
    MILE_NBR NUMBER(8,3),
    PLAN_EVENT_ID NUMBER(18,0),
    RECORD_CREATE_TMS TIMESTAMP_NTZ(0),
    RECORD_UPDATE_TMS TIMESTAMP_NTZ(0),
    CREATE_USER_ID VARCHAR(32),
    UPDATE_USER_ID VARCHAR(32),
    AEIRD_NBR VARCHAR(16),
    CNST_NBR VARCHAR(16),
    CNST_ORIGIN_SCAC_CD VARCHAR(16),
    CNST_ORIGIN_FSAC_CD VARCHAR(20),
    COMMON_YARDS_SITE_CD VARCHAR(4),
    COMMON_YARDS_TRACK_NM VARCHAR(20),
    DSTNC_RUN_MILES_QTY NUMBER(6,1),
    INTRCH_SCAC_CD VARCHAR(16),
    MTP_ROUTE_POINT_SQNC_NBR NUMBER(3,0),
    REPORT_TIME_ZONE_CD VARCHAR(8),
    RUN_NBR_CD VARCHAR(12),
    SHORT_DSTRCT_NM VARCHAR(40),
    SPLC_CD VARCHAR(24),
    TITAN_NBR NUMBER(6,0),
    CNST_DSTNTN_SCAC_CD VARCHAR(16),
    CNST_DSTNTN_FSAC_CD VARCHAR(20),
    TRAVEL_DRCTN_CD VARCHAR(20),
    SWITCH_LIST_NBR NUMBER(5,0),
    TYES_TRAIN_ID NUMBER(18,0),
    SNW_OPERATION_TYPE VARCHAR(1),
    SNW_LAST_REPLICATED TIMESTAMP_NTZ(9),

    CDC_OPERATION VARCHAR(10),
    CDC_TIMESTAMP TIMESTAMP_NTZ,
    IS_DELETED BOOLEAN,
    RECORD_CREATED_AT TIMESTAMP_NTZ,
    RECORD_UPDATED_AT TIMESTAMP_NTZ,
    SOURCE_LOAD_BATCH_ID VARCHAR(100),
    SNW_OPERATION_OWNER VARCHAR(256),

    PRIMARY KEY (EVENT_ID)
);

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 45,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 15;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for LCMTV_MVMNT_EVENT_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing (ENHANCED v1)
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.SADB.SP_PROCESS_LCMTV_MVMNT_EVENT()
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
            FROM D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE_HIST_STREAM
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
        
        CREATE OR REPLACE STREAM D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE_HIST_STREAM
        ON TABLE D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection';
        
        MERGE INTO D_BRONZE.SADB.LCMTV_MVMNT_EVENT AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE_HIST_STREAM src
            WHERE NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')
        ) AS src
        ON tgt.EVENT_ID = src.EVENT_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.EQPMT_EVENT_TYPE_ID = src.EQPMT_EVENT_TYPE_ID,
            tgt.SCAC_CD = src.SCAC_CD,
            tgt.FSAC_CD = src.FSAC_CD,
            tgt.TRSTN_VRSN_NBR = src.TRSTN_VRSN_NBR,
            tgt.REPORT_TMS = src.REPORT_TMS,
            tgt.SQNC_NBR = src.SQNC_NBR,
            tgt.DRCTN_CD = src.DRCTN_CD,
            tgt.SOURCE_SYSTEM_NM = src.SOURCE_SYSTEM_NM,
            tgt.MARK_CD = src.MARK_CD,
            tgt.EQPUN_NBR = src.EQPUN_NBR,
            tgt.OPTRN_EVENT_ID = src.OPTRN_EVENT_ID,
            tgt.ORNTTN_CD = src.ORNTTN_CD,
            tgt.DEAD_HEAD_IND = src.DEAD_HEAD_IND,
            tgt.RGN_NM_TRK_NBR = src.RGN_NM_TRK_NBR,
            tgt.LMS_PRFL_SQNC_NBR = src.LMS_PRFL_SQNC_NBR,
            tgt.MILE_NBR = src.MILE_NBR,
            tgt.PLAN_EVENT_ID = src.PLAN_EVENT_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.AEIRD_NBR = src.AEIRD_NBR,
            tgt.CNST_NBR = src.CNST_NBR,
            tgt.CNST_ORIGIN_SCAC_CD = src.CNST_ORIGIN_SCAC_CD,
            tgt.CNST_ORIGIN_FSAC_CD = src.CNST_ORIGIN_FSAC_CD,
            tgt.COMMON_YARDS_SITE_CD = src.COMMON_YARDS_SITE_CD,
            tgt.COMMON_YARDS_TRACK_NM = src.COMMON_YARDS_TRACK_NM,
            tgt.DSTNC_RUN_MILES_QTY = src.DSTNC_RUN_MILES_QTY,
            tgt.INTRCH_SCAC_CD = src.INTRCH_SCAC_CD,
            tgt.MTP_ROUTE_POINT_SQNC_NBR = src.MTP_ROUTE_POINT_SQNC_NBR,
            tgt.REPORT_TIME_ZONE_CD = src.REPORT_TIME_ZONE_CD,
            tgt.RUN_NBR_CD = src.RUN_NBR_CD,
            tgt.SHORT_DSTRCT_NM = src.SHORT_DSTRCT_NM,
            tgt.SPLC_CD = src.SPLC_CD,
            tgt.TITAN_NBR = src.TITAN_NBR,
            tgt.CNST_DSTNTN_SCAC_CD = src.CNST_DSTNTN_SCAC_CD,
            tgt.CNST_DSTNTN_FSAC_CD = src.CNST_DSTNTN_FSAC_CD,
            tgt.TRAVEL_DRCTN_CD = src.TRAVEL_DRCTN_CD,
            tgt.SWITCH_LIST_NBR = src.SWITCH_LIST_NBR,
            tgt.TYES_TRAIN_ID = src.TYES_TRAIN_ID,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_OPERATION_OWNER = src.SNW_OPERATION_OWNER,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'RELOADED',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        WHEN NOT MATCHED THEN INSERT (
            EVENT_ID, EQPMT_EVENT_TYPE_ID, SCAC_CD, FSAC_CD, TRSTN_VRSN_NBR,
            REPORT_TMS, SQNC_NBR, DRCTN_CD, SOURCE_SYSTEM_NM, MARK_CD,
            EQPUN_NBR, OPTRN_EVENT_ID, ORNTTN_CD, DEAD_HEAD_IND, RGN_NM_TRK_NBR,
            LMS_PRFL_SQNC_NBR, MILE_NBR, PLAN_EVENT_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            CREATE_USER_ID, UPDATE_USER_ID, AEIRD_NBR, CNST_NBR, CNST_ORIGIN_SCAC_CD,
            CNST_ORIGIN_FSAC_CD, COMMON_YARDS_SITE_CD, COMMON_YARDS_TRACK_NM, DSTNC_RUN_MILES_QTY, INTRCH_SCAC_CD,
            MTP_ROUTE_POINT_SQNC_NBR, REPORT_TIME_ZONE_CD, RUN_NBR_CD, SHORT_DSTRCT_NM, SPLC_CD,
            TITAN_NBR, CNST_DSTNTN_SCAC_CD, CNST_DSTNTN_FSAC_CD, TRAVEL_DRCTN_CD, SWITCH_LIST_NBR,
            TYES_TRAIN_ID, SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, 
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.EVENT_ID, src.EQPMT_EVENT_TYPE_ID, src.SCAC_CD, src.FSAC_CD, src.TRSTN_VRSN_NBR,
            src.REPORT_TMS, src.SQNC_NBR, src.DRCTN_CD, src.SOURCE_SYSTEM_NM, src.MARK_CD,
            src.EQPUN_NBR, src.OPTRN_EVENT_ID, src.ORNTTN_CD, src.DEAD_HEAD_IND, src.RGN_NM_TRK_NBR,
            src.LMS_PRFL_SQNC_NBR, src.MILE_NBR, src.PLAN_EVENT_ID, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS,
            src.CREATE_USER_ID, src.UPDATE_USER_ID, src.AEIRD_NBR, src.CNST_NBR, src.CNST_ORIGIN_SCAC_CD,
            src.CNST_ORIGIN_FSAC_CD, src.COMMON_YARDS_SITE_CD, src.COMMON_YARDS_TRACK_NM, src.DSTNC_RUN_MILES_QTY, src.INTRCH_SCAC_CD,
            src.MTP_ROUTE_POINT_SQNC_NBR, src.REPORT_TIME_ZONE_CD, src.RUN_NBR_CD, src.SHORT_DSTRCT_NM, src.SPLC_CD,
            src.TITAN_NBR, src.CNST_DSTNTN_SCAC_CD, src.CNST_DSTNTN_FSAC_CD, src.TRAVEL_DRCTN_CD, src.SWITCH_LIST_NBR,
            src.TYES_TRAIN_ID, src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
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
            'LCMTV_MVMNT_EVENT', :v_batch_id, 'RECOVERY', :v_start_time, :v_end_time,
            :v_rows_merged, :v_rows_merged, 0, 0,
            :v_error_msg, CURRENT_TIMESTAMP()
        );
        
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_merged || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- CHECK 2: Stage stream data into temp table (BEST PRACTICE - single read)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_LCMTV_MVMNT_EVENT AS
    SELECT 
        EVENT_ID, EQPMT_EVENT_TYPE_ID, SCAC_CD, FSAC_CD, TRSTN_VRSN_NBR,
        REPORT_TMS, SQNC_NBR, DRCTN_CD, SOURCE_SYSTEM_NM, MARK_CD,
        EQPUN_NBR, OPTRN_EVENT_ID, ORNTTN_CD, DEAD_HEAD_IND, RGN_NM_TRK_NBR,
        LMS_PRFL_SQNC_NBR, MILE_NBR, PLAN_EVENT_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
        CREATE_USER_ID, UPDATE_USER_ID, AEIRD_NBR, CNST_NBR, CNST_ORIGIN_SCAC_CD,
        CNST_ORIGIN_FSAC_CD, COMMON_YARDS_SITE_CD, COMMON_YARDS_TRACK_NM, DSTNC_RUN_MILES_QTY, INTRCH_SCAC_CD,
        MTP_ROUTE_POINT_SQNC_NBR, REPORT_TIME_ZONE_CD, RUN_NBR_CD, SHORT_DSTRCT_NM, SPLC_CD,
        TITAN_NBR, CNST_DSTNTN_SCAC_CD, CNST_DSTNTN_FSAC_CD, TRAVEL_DRCTN_CD, SWITCH_LIST_NBR,
        TYES_TRAIN_ID, SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE_HIST_STREAM
    WHERE NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG');
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_LCMTV_MVMNT_EVENT;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_LCMTV_MVMNT_EVENT;
        v_end_time := CURRENT_TIMESTAMP();
        
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
            TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME,
            ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED,
            ERROR_MESSAGE, CREATED_AT
        ) VALUES (
            'LCMTV_MVMNT_EVENT', :v_batch_id, 'NO_DATA', :v_start_time, :v_end_time,
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
    FROM _CDC_STAGING_LCMTV_MVMNT_EVENT;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.SADB.LCMTV_MVMNT_EVENT AS tgt
    USING (
        SELECT 
            EVENT_ID, EQPMT_EVENT_TYPE_ID, SCAC_CD, FSAC_CD, TRSTN_VRSN_NBR,
            REPORT_TMS, SQNC_NBR, DRCTN_CD, SOURCE_SYSTEM_NM, MARK_CD,
            EQPUN_NBR, OPTRN_EVENT_ID, ORNTTN_CD, DEAD_HEAD_IND, RGN_NM_TRK_NBR,
            LMS_PRFL_SQNC_NBR, MILE_NBR, PLAN_EVENT_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            CREATE_USER_ID, UPDATE_USER_ID, AEIRD_NBR, CNST_NBR, CNST_ORIGIN_SCAC_CD,
            CNST_ORIGIN_FSAC_CD, COMMON_YARDS_SITE_CD, COMMON_YARDS_TRACK_NM, DSTNC_RUN_MILES_QTY, INTRCH_SCAC_CD,
            MTP_ROUTE_POINT_SQNC_NBR, REPORT_TIME_ZONE_CD, RUN_NBR_CD, SHORT_DSTRCT_NM, SPLC_CD,
            TITAN_NBR, CNST_DSTNTN_SCAC_CD, CNST_DSTNTN_FSAC_CD, TRAVEL_DRCTN_CD, SWITCH_LIST_NBR,
            TYES_TRAIN_ID, SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_LCMTV_MVMNT_EVENT
    ) AS src
    ON tgt.EVENT_ID = src.EVENT_ID
    
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
        UPDATE SET
            tgt.EQPMT_EVENT_TYPE_ID = src.EQPMT_EVENT_TYPE_ID,
            tgt.SCAC_CD = src.SCAC_CD,
            tgt.FSAC_CD = src.FSAC_CD,
            tgt.TRSTN_VRSN_NBR = src.TRSTN_VRSN_NBR,
            tgt.REPORT_TMS = src.REPORT_TMS,
            tgt.SQNC_NBR = src.SQNC_NBR,
            tgt.DRCTN_CD = src.DRCTN_CD,
            tgt.SOURCE_SYSTEM_NM = src.SOURCE_SYSTEM_NM,
            tgt.MARK_CD = src.MARK_CD,
            tgt.EQPUN_NBR = src.EQPUN_NBR,
            tgt.OPTRN_EVENT_ID = src.OPTRN_EVENT_ID,
            tgt.ORNTTN_CD = src.ORNTTN_CD,
            tgt.DEAD_HEAD_IND = src.DEAD_HEAD_IND,
            tgt.RGN_NM_TRK_NBR = src.RGN_NM_TRK_NBR,
            tgt.LMS_PRFL_SQNC_NBR = src.LMS_PRFL_SQNC_NBR,
            tgt.MILE_NBR = src.MILE_NBR,
            tgt.PLAN_EVENT_ID = src.PLAN_EVENT_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.AEIRD_NBR = src.AEIRD_NBR,
            tgt.CNST_NBR = src.CNST_NBR,
            tgt.CNST_ORIGIN_SCAC_CD = src.CNST_ORIGIN_SCAC_CD,
            tgt.CNST_ORIGIN_FSAC_CD = src.CNST_ORIGIN_FSAC_CD,
            tgt.COMMON_YARDS_SITE_CD = src.COMMON_YARDS_SITE_CD,
            tgt.COMMON_YARDS_TRACK_NM = src.COMMON_YARDS_TRACK_NM,
            tgt.DSTNC_RUN_MILES_QTY = src.DSTNC_RUN_MILES_QTY,
            tgt.INTRCH_SCAC_CD = src.INTRCH_SCAC_CD,
            tgt.MTP_ROUTE_POINT_SQNC_NBR = src.MTP_ROUTE_POINT_SQNC_NBR,
            tgt.REPORT_TIME_ZONE_CD = src.REPORT_TIME_ZONE_CD,
            tgt.RUN_NBR_CD = src.RUN_NBR_CD,
            tgt.SHORT_DSTRCT_NM = src.SHORT_DSTRCT_NM,
            tgt.SPLC_CD = src.SPLC_CD,
            tgt.TITAN_NBR = src.TITAN_NBR,
            tgt.CNST_DSTNTN_SCAC_CD = src.CNST_DSTNTN_SCAC_CD,
            tgt.CNST_DSTNTN_FSAC_CD = src.CNST_DSTNTN_FSAC_CD,
            tgt.TRAVEL_DRCTN_CD = src.TRAVEL_DRCTN_CD,
            tgt.SWITCH_LIST_NBR = src.SWITCH_LIST_NBR,
            tgt.TYES_TRAIN_ID = src.TYES_TRAIN_ID,
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
            tgt.EQPMT_EVENT_TYPE_ID = src.EQPMT_EVENT_TYPE_ID,
            tgt.SCAC_CD = src.SCAC_CD,
            tgt.FSAC_CD = src.FSAC_CD,
            tgt.TRSTN_VRSN_NBR = src.TRSTN_VRSN_NBR,
            tgt.REPORT_TMS = src.REPORT_TMS,
            tgt.SQNC_NBR = src.SQNC_NBR,
            tgt.DRCTN_CD = src.DRCTN_CD,
            tgt.SOURCE_SYSTEM_NM = src.SOURCE_SYSTEM_NM,
            tgt.MARK_CD = src.MARK_CD,
            tgt.EQPUN_NBR = src.EQPUN_NBR,
            tgt.OPTRN_EVENT_ID = src.OPTRN_EVENT_ID,
            tgt.ORNTTN_CD = src.ORNTTN_CD,
            tgt.DEAD_HEAD_IND = src.DEAD_HEAD_IND,
            tgt.RGN_NM_TRK_NBR = src.RGN_NM_TRK_NBR,
            tgt.LMS_PRFL_SQNC_NBR = src.LMS_PRFL_SQNC_NBR,
            tgt.MILE_NBR = src.MILE_NBR,
            tgt.PLAN_EVENT_ID = src.PLAN_EVENT_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.AEIRD_NBR = src.AEIRD_NBR,
            tgt.CNST_NBR = src.CNST_NBR,
            tgt.CNST_ORIGIN_SCAC_CD = src.CNST_ORIGIN_SCAC_CD,
            tgt.CNST_ORIGIN_FSAC_CD = src.CNST_ORIGIN_FSAC_CD,
            tgt.COMMON_YARDS_SITE_CD = src.COMMON_YARDS_SITE_CD,
            tgt.COMMON_YARDS_TRACK_NM = src.COMMON_YARDS_TRACK_NM,
            tgt.DSTNC_RUN_MILES_QTY = src.DSTNC_RUN_MILES_QTY,
            tgt.INTRCH_SCAC_CD = src.INTRCH_SCAC_CD,
            tgt.MTP_ROUTE_POINT_SQNC_NBR = src.MTP_ROUTE_POINT_SQNC_NBR,
            tgt.REPORT_TIME_ZONE_CD = src.REPORT_TIME_ZONE_CD,
            tgt.RUN_NBR_CD = src.RUN_NBR_CD,
            tgt.SHORT_DSTRCT_NM = src.SHORT_DSTRCT_NM,
            tgt.SPLC_CD = src.SPLC_CD,
            tgt.TITAN_NBR = src.TITAN_NBR,
            tgt.CNST_DSTNTN_SCAC_CD = src.CNST_DSTNTN_SCAC_CD,
            tgt.CNST_DSTNTN_FSAC_CD = src.CNST_DSTNTN_FSAC_CD,
            tgt.TRAVEL_DRCTN_CD = src.TRAVEL_DRCTN_CD,
            tgt.SWITCH_LIST_NBR = src.SWITCH_LIST_NBR,
            tgt.TYES_TRAIN_ID = src.TYES_TRAIN_ID,
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
            EVENT_ID, EQPMT_EVENT_TYPE_ID, SCAC_CD, FSAC_CD, TRSTN_VRSN_NBR,
            REPORT_TMS, SQNC_NBR, DRCTN_CD, SOURCE_SYSTEM_NM, MARK_CD,
            EQPUN_NBR, OPTRN_EVENT_ID, ORNTTN_CD, DEAD_HEAD_IND, RGN_NM_TRK_NBR,
            LMS_PRFL_SQNC_NBR, MILE_NBR, PLAN_EVENT_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            CREATE_USER_ID, UPDATE_USER_ID, AEIRD_NBR, CNST_NBR, CNST_ORIGIN_SCAC_CD,
            CNST_ORIGIN_FSAC_CD, COMMON_YARDS_SITE_CD, COMMON_YARDS_TRACK_NM, DSTNC_RUN_MILES_QTY, INTRCH_SCAC_CD,
            MTP_ROUTE_POINT_SQNC_NBR, REPORT_TIME_ZONE_CD, RUN_NBR_CD, SHORT_DSTRCT_NM, SPLC_CD,
            TITAN_NBR, CNST_DSTNTN_SCAC_CD, CNST_DSTNTN_FSAC_CD, TRAVEL_DRCTN_CD, SWITCH_LIST_NBR,
            TYES_TRAIN_ID, SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.EVENT_ID, src.EQPMT_EVENT_TYPE_ID, src.SCAC_CD, src.FSAC_CD, src.TRSTN_VRSN_NBR,
            src.REPORT_TMS, src.SQNC_NBR, src.DRCTN_CD, src.SOURCE_SYSTEM_NM, src.MARK_CD,
            src.EQPUN_NBR, src.OPTRN_EVENT_ID, src.ORNTTN_CD, src.DEAD_HEAD_IND, src.RGN_NM_TRK_NBR,
            src.LMS_PRFL_SQNC_NBR, src.MILE_NBR, src.PLAN_EVENT_ID, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS,
            src.CREATE_USER_ID, src.UPDATE_USER_ID, src.AEIRD_NBR, src.CNST_NBR, src.CNST_ORIGIN_SCAC_CD,
            src.CNST_ORIGIN_FSAC_CD, src.COMMON_YARDS_SITE_CD, src.COMMON_YARDS_TRACK_NM, src.DSTNC_RUN_MILES_QTY, src.INTRCH_SCAC_CD,
            src.MTP_ROUTE_POINT_SQNC_NBR, src.REPORT_TIME_ZONE_CD, src.RUN_NBR_CD, src.SHORT_DSTRCT_NM, src.SPLC_CD,
            src.TITAN_NBR, src.CNST_DSTNTN_SCAC_CD, src.CNST_DSTNTN_FSAC_CD, src.TRAVEL_DRCTN_CD, src.SWITCH_LIST_NBR,
            src.TYES_TRAIN_ID, src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    v_end_time := CURRENT_TIMESTAMP();
    
    DROP TABLE IF EXISTS _CDC_STAGING_LCMTV_MVMNT_EVENT;
    
    -- =========================================================================
    -- EXECUTION LOGGING: Write metrics to CDC_EXECUTION_LOG
    -- =========================================================================
    INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
        TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME,
        ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED,
        ERROR_MESSAGE, CREATED_AT
    ) VALUES (
        'LCMTV_MVMNT_EVENT', :v_batch_id, 'SUCCESS', :v_start_time, :v_end_time,
        :v_rows_merged, :v_rows_inserted, :v_rows_updated, :v_rows_deleted,
        NULL, CURRENT_TIMESTAMP()
    );
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes (I:' || v_rows_inserted || 
           ' U:' || v_rows_updated || ' D:' || v_rows_deleted || '). Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        v_end_time := CURRENT_TIMESTAMP();
        v_error_msg := SQLERRM;
        
        DROP TABLE IF EXISTS _CDC_STAGING_LCMTV_MVMNT_EVENT;
        
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
            TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME,
            ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED,
            ERROR_MESSAGE, CREATED_AT
        ) VALUES (
            'LCMTV_MVMNT_EVENT', :v_batch_id, 'ERROR', :v_start_time, :v_end_time,
            0, 0, 0, 0,
            :v_error_msg, CURRENT_TIMESTAMP()
        );
        
        RETURN 'ERROR: ' || v_error_msg || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.SADB.TASK_PROCESS_LCMTV_MVMNT_EVENT
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process LCMTV_MVMNT_EVENT_BASE CDC changes into data preservation table (v1 - enhanced logging)'
AS
    CALL D_RAW.SADB.SP_PROCESS_LCMTV_MVMNT_EVENT();

ALTER TASK D_RAW.SADB.TASK_PROCESS_LCMTV_MVMNT_EVENT RESUME;

-- =============================================================================
-- PREREQUISITE: Ensure monitoring table exists (run once before first execution)
-- =============================================================================
/*
CREATE TABLE IF NOT EXISTS D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
    LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    TABLE_NAME VARCHAR(256) NOT NULL,
    BATCH_ID VARCHAR(100) NOT NULL,
    EXECUTION_STATUS VARCHAR(20) NOT NULL,
    START_TIME TIMESTAMP_NTZ,
    END_TIME TIMESTAMP_NTZ,
    ROWS_PROCESSED NUMBER DEFAULT 0,
    ROWS_INSERTED NUMBER DEFAULT 0,
    ROWS_UPDATED NUMBER DEFAULT 0,
    ROWS_DELETED NUMBER DEFAULT 0,
    ERROR_MESSAGE VARCHAR(4000),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
*/

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'LCMTV_MVMNT_EVENT%' IN SCHEMA D_BRONZE.SADB;
-- SHOW STREAMS LIKE 'LCMTV_MVMNT_EVENT%' IN SCHEMA D_RAW.SADB;
-- SHOW TASKS LIKE 'TASK_PROCESS_LCMTV_MVMNT_EVENT%' IN SCHEMA D_RAW.SADB;
-- CALL D_RAW.SADB.SP_PROCESS_LCMTV_MVMNT_EVENT();
-- SELECT * FROM D_BRONZE.MONITORING.CDC_EXECUTION_LOG WHERE TABLE_NAME = 'LCMTV_MVMNT_EVENT' ORDER BY CREATED_AT DESC LIMIT 10;
