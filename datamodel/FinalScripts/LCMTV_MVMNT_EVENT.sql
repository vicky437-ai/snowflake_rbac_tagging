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
Total Columns: 43 source + 6 CDC metadata = 49
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE TABLE IF NOT EXISTS D_BRONZE.SADB.LCMTV_MVMNT_EVENT (
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

    -- CDC Metadata columns for data preservation
    CDC_OPERATION VARCHAR(10),  
    CDC_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IS_DELETED BOOLEAN DEFAULT FALSE,    -- Soft delete flag
    RECORD_CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RECORD_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_LOAD_BATCH_ID VARCHAR(100), 

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
-- STEP 4: Create Stored Procedure for CDC Processing
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
    v_result VARCHAR;
    v_error_msg VARCHAR;
BEGIN
    v_batch_id := 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    
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
            FROM D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE src
            LEFT JOIN D_BRONZE.SADB.LCMTV_MVMNT_EVENT tgt 
                ON src.EVENT_ID = tgt.EVENT_ID
            WHERE tgt.EVENT_ID IS NULL
               OR tgt.IS_DELETED = TRUE
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
            TYES_TRAIN_ID, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
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
            src.TYES_TRAIN_ID, src.SNW_OPERATION_TYPE, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
        
        v_rows_merged := SQLROWCOUNT;
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
        TYES_TRAIN_ID, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE_HIST_STREAM;
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_LCMTV_MVMNT_EVENT;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_LCMTV_MVMNT_EVENT;
        RETURN 'NO_DATA: Stream has no changes to process at ' || CURRENT_TIMESTAMP()::VARCHAR;
    END IF;
    
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
            TYES_TRAIN_ID, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_LCMTV_MVMNT_EVENT
    ) AS src
    ON tgt.EVENT_ID = src.EVENT_ID
    
    -- UPDATE scenario (METADATA$ACTION='INSERT' AND METADATA$ISUPDATE=TRUE)
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
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'INSERT',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    -- NEW INSERT scenario
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
            TYES_TRAIN_ID, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
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
            src.TYES_TRAIN_ID, src.SNW_OPERATION_TYPE, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    DROP TABLE IF EXISTS _CDC_STAGING_LCMTV_MVMNT_EVENT;
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes. Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS _CDC_STAGING_LCMTV_MVMNT_EVENT;
        RETURN 'ERROR: ' || SQLERRM || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.SADB.TASK_PROCESS_LCMTV_MVMNT_EVENT
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process LCMTV_MVMNT_EVENT_BASE CDC changes into data preservation table'
WHEN
    SYSTEM$STREAM_HAS_DATA('D_RAW.SADB.LCMTV_MVMNT_EVENT_BASE_HIST_STREAM')
AS
    CALL D_RAW.SADB.SP_PROCESS_LCMTV_MVMNT_EVENT();

ALTER TASK D_RAW.SADB.TASK_PROCESS_LCMTV_MVMNT_EVENT RESUME;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'LCMTV_MVMNT_EVENT%' IN SCHEMA D_BRONZE.SADB;
-- SHOW STREAMS LIKE 'LCMTV_MVMNT_EVENT%' IN SCHEMA D_RAW.SADB;
-- SHOW TASKS LIKE 'TASK_PROCESS_LCMTV_MVMNT_EVENT%' IN SCHEMA D_RAW.SADB;
-- CALL D_RAW.SADB.SP_PROCESS_LCMTV_MVMNT_EVENT();
