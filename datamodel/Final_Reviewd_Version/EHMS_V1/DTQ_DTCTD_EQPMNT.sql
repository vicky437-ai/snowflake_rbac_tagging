/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE
================================================================================
Source Table : D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE
Target Table : D_BRONZE.EHMS.DTQ_DTCTD_EQPMNT
Stream       : D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE_HIST_STREAM
Procedure    : D_RAW.EHMS.SP_PROCESS_DTQ_DTCTD_EQPMNT()
Task         : D_RAW.EHMS.TASK_SP_PROCESS_DTQ_DTCTD_EQPMNT
Primary Key  : DTCTD_EQPMNT_ID (Single)
Total Columns: 49 source + 6 CDC metadata = 55
Filter       : NONE (no purge filter required for EHMS schema)
================================================================================
VERSION      : v2.1
DATE         : 2026-03-18
CHANGES      :
  - Added execution logging to D_BRONZE.MONITORING.CDC_EXECUTION_LOG
  - Staleness detection via SELECT COUNT(*) WHERE 1=0 pattern
  - Renamed objects: EHMSAPP_ prefix removed (EHMSAPP_DTQ_DTCTD_EQPMNT -> DTQ_DTCTD_EQPMNT) including source table
  - Added inline table COMMENT with lineage metadata
  - Task naming updated to TASK_SP_PROCESS_ pattern
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE OR ALTER TABLE D_BRONZE.EHMS.DTQ_DTCTD_EQPMNT (
    DTCTD_EQPMNT_ID NUMBER(18,0) NOT NULL,
    CREATE_USER_ID VARCHAR(32),
    RECORD_CREATE_TMS TIMESTAMP_NTZ(0),
    RECORD_UPDATE_TMS TIMESTAMP_NTZ(0),
    UPDATE_USER_ID VARCHAR(32),
    DTCTD_TRAIN_ID NUMBER(18,0),
    EQPMNT_SQNC_NBR NUMBER(4,0),
    RPRTD_MARK_CD VARCHAR(16),
    RPRTD_EQPUN_NBR VARCHAR(40),
    RPRTD_AEI_RAW_DATA_TXT VARCHAR(1020),
    RPRTD_AXLE_QTY NUMBER(4,0),
    RPRTD_EQPMNT_CTGRY_TXT VARCHAR(160),
    RPRTD_EQPMNT_TYPE_CD VARCHAR(8),
    RPRTD_EQPMNT_ORNTN_CD VARCHAR(4),
    RPRTD_SPEED_QTY NUMBER(5,2),
    RPRTD_WEIGHT_QTY NUMBER(8,2),
    CNFDNC_NBR NUMBER(6,3),
    GROSS_WEIGHT_QTY NUMBER(7,0),
    TRUCK_QTY NUMBER(3,0),
    TARE_WEIGHT_QTY NUMBER(7,0),
    VRFD_EQPMNT_ID NUMBER(18,0),
    VRFD_EQPMNT_ORNTN_CD VARCHAR(4),
    VRFD_ORIGIN_SCAC_CD VARCHAR(16),
    VRFD_ORIGIN_FSAC_CD VARCHAR(20),
    VRFD_ORIGIN_TRSTN_NM VARCHAR(120),
    VRFD_DSTNTN_SCAC_CD VARCHAR(16),
    VRFD_DSTNTN_FSAC_CD VARCHAR(20),
    VRFD_DSTNTN_TRSTN_NM VARCHAR(120),
    VRFD_LOAD_EMPTY_CD VARCHAR(4),
    VRFD_NET_WEIGHT_QTY NUMBER(10,0),
    WEIGHT_UOM_BASIS_CD VARCHAR(8),
    DTCTD_EQPMNT_CTGRY_CD VARCHAR(16),
    RPRTD_TRUCK_QTY NUMBER(2,0),
    CPR_EQPMNT_POOL_ID VARCHAR(28),
    OWNER_MARK_CD VARCHAR(16),
    MNTNC_RSPNSB_PARTY_CD VARCHAR(16),
    STCC_CD VARCHAR(28),
    CAR_OVERLOAD NUMBER(10,3),
    RATIO_ETE NUMBER(6,3),
    RATIO_STS NUMBER(6,3),
    ALERT_STATUS VARCHAR(4),
    SUMNOMINAL_A NUMBER(8,3),
    SUMNOMINAL_B NUMBER(8,3),
    SUMNOMINAL_L NUMBER(8,3),
    SUMNOMINAL_R NUMBER(8,3),
    SUMNOMINAL NUMBER(8,3),
    SNW_OPERATION_TYPE VARCHAR(1),
    SNW_LAST_REPLICATED TIMESTAMP_NTZ(9),

    CDC_OPERATION VARCHAR(10),
    CDC_TIMESTAMP TIMESTAMP_NTZ,
    IS_DELETED BOOLEAN,
    RECORD_CREATED_AT TIMESTAMP_NTZ,
    RECORD_UPDATED_AT TIMESTAMP_NTZ,
    SOURCE_LOAD_BATCH_ID VARCHAR(100),
    SNW_OPERATION_OWNER VARCHAR(256),

    PRIMARY KEY (DTCTD_EQPMNT_ID)
)
COMMENT = 'Bronze data preservation layer for EHMS Detected Equipment.
Source: Oracle EHMS (DTQ_DTCTD_EQPMNT) replicated via IDMC CDC into D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE.
Data Source Tables: D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE (49 source columns + 3 SNW metadata).
Transformations: No purge filter (EHMS schema). Six CDC metadata columns added (CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID).
Pipeline Objects: Stream D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE_HIST_STREAM | Procedure D_RAW.EHMS.SP_PROCESS_DTQ_DTCTD_EQPMNT() | Task D_RAW.EHMS.TASK_SP_PROCESS_DTQ_DTCTD_EQPMNT.
Refresh Frequency: Every 5 minutes via Snowflake Task (incremental CDC MERGE).
Primary Key: DTCTD_EQPMNT_ID.';

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 45,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 15;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE_HIST_STREAM
ON TABLE D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for DTQ_DTCTD_EQPMNT_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing (ENHANCED v1)
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.EHMS.SP_PROCESS_DTQ_DTCTD_EQPMNT()
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
        FROM D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE_HIST_STREAM
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
        
        CREATE OR REPLACE STREAM D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE_HIST_STREAM
        ON TABLE D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection';
        
        MERGE INTO D_BRONZE.EHMS.DTQ_DTCTD_EQPMNT AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE_HIST_STREAM src
        ) AS src
        ON tgt.DTCTD_EQPMNT_ID = src.DTCTD_EQPMNT_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.DTCTD_TRAIN_ID = src.DTCTD_TRAIN_ID,
            tgt.EQPMNT_SQNC_NBR = src.EQPMNT_SQNC_NBR,
            tgt.RPRTD_MARK_CD = src.RPRTD_MARK_CD,
            tgt.RPRTD_EQPUN_NBR = src.RPRTD_EQPUN_NBR,
            tgt.RPRTD_AEI_RAW_DATA_TXT = src.RPRTD_AEI_RAW_DATA_TXT,
            tgt.RPRTD_AXLE_QTY = src.RPRTD_AXLE_QTY,
            tgt.RPRTD_EQPMNT_CTGRY_TXT = src.RPRTD_EQPMNT_CTGRY_TXT,
            tgt.RPRTD_EQPMNT_TYPE_CD = src.RPRTD_EQPMNT_TYPE_CD,
            tgt.RPRTD_EQPMNT_ORNTN_CD = src.RPRTD_EQPMNT_ORNTN_CD,
            tgt.RPRTD_SPEED_QTY = src.RPRTD_SPEED_QTY,
            tgt.RPRTD_WEIGHT_QTY = src.RPRTD_WEIGHT_QTY,
            tgt.CNFDNC_NBR = src.CNFDNC_NBR,
            tgt.GROSS_WEIGHT_QTY = src.GROSS_WEIGHT_QTY,
            tgt.TRUCK_QTY = src.TRUCK_QTY,
            tgt.TARE_WEIGHT_QTY = src.TARE_WEIGHT_QTY,
            tgt.VRFD_EQPMNT_ID = src.VRFD_EQPMNT_ID,
            tgt.VRFD_EQPMNT_ORNTN_CD = src.VRFD_EQPMNT_ORNTN_CD,
            tgt.VRFD_ORIGIN_SCAC_CD = src.VRFD_ORIGIN_SCAC_CD,
            tgt.VRFD_ORIGIN_FSAC_CD = src.VRFD_ORIGIN_FSAC_CD,
            tgt.VRFD_ORIGIN_TRSTN_NM = src.VRFD_ORIGIN_TRSTN_NM,
            tgt.VRFD_DSTNTN_SCAC_CD = src.VRFD_DSTNTN_SCAC_CD,
            tgt.VRFD_DSTNTN_FSAC_CD = src.VRFD_DSTNTN_FSAC_CD,
            tgt.VRFD_DSTNTN_TRSTN_NM = src.VRFD_DSTNTN_TRSTN_NM,
            tgt.VRFD_LOAD_EMPTY_CD = src.VRFD_LOAD_EMPTY_CD,
            tgt.VRFD_NET_WEIGHT_QTY = src.VRFD_NET_WEIGHT_QTY,
            tgt.WEIGHT_UOM_BASIS_CD = src.WEIGHT_UOM_BASIS_CD,
            tgt.DTCTD_EQPMNT_CTGRY_CD = src.DTCTD_EQPMNT_CTGRY_CD,
            tgt.RPRTD_TRUCK_QTY = src.RPRTD_TRUCK_QTY,
            tgt.CPR_EQPMNT_POOL_ID = src.CPR_EQPMNT_POOL_ID,
            tgt.OWNER_MARK_CD = src.OWNER_MARK_CD,
            tgt.MNTNC_RSPNSB_PARTY_CD = src.MNTNC_RSPNSB_PARTY_CD,
            tgt.STCC_CD = src.STCC_CD,
            tgt.CAR_OVERLOAD = src.CAR_OVERLOAD,
            tgt.RATIO_ETE = src.RATIO_ETE,
            tgt.RATIO_STS = src.RATIO_STS,
            tgt.ALERT_STATUS = src.ALERT_STATUS,
            tgt.SUMNOMINAL_A = src.SUMNOMINAL_A,
            tgt.SUMNOMINAL_B = src.SUMNOMINAL_B,
            tgt.SUMNOMINAL_L = src.SUMNOMINAL_L,
            tgt.SUMNOMINAL_R = src.SUMNOMINAL_R,
            tgt.SUMNOMINAL = src.SUMNOMINAL,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_OPERATION_OWNER = src.SNW_OPERATION_OWNER,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'RELOADED',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        WHEN NOT MATCHED THEN INSERT (
            DTCTD_EQPMNT_ID, CREATE_USER_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS, UPDATE_USER_ID,
            DTCTD_TRAIN_ID, EQPMNT_SQNC_NBR, RPRTD_MARK_CD, RPRTD_EQPUN_NBR, RPRTD_AEI_RAW_DATA_TXT,
            RPRTD_AXLE_QTY, RPRTD_EQPMNT_CTGRY_TXT, RPRTD_EQPMNT_TYPE_CD, RPRTD_EQPMNT_ORNTN_CD, RPRTD_SPEED_QTY,
            RPRTD_WEIGHT_QTY, CNFDNC_NBR, GROSS_WEIGHT_QTY, TRUCK_QTY, TARE_WEIGHT_QTY,
            VRFD_EQPMNT_ID, VRFD_EQPMNT_ORNTN_CD, VRFD_ORIGIN_SCAC_CD, VRFD_ORIGIN_FSAC_CD, VRFD_ORIGIN_TRSTN_NM,
            VRFD_DSTNTN_SCAC_CD, VRFD_DSTNTN_FSAC_CD, VRFD_DSTNTN_TRSTN_NM, VRFD_LOAD_EMPTY_CD, VRFD_NET_WEIGHT_QTY,
            WEIGHT_UOM_BASIS_CD, DTCTD_EQPMNT_CTGRY_CD, RPRTD_TRUCK_QTY, CPR_EQPMNT_POOL_ID, OWNER_MARK_CD,
            MNTNC_RSPNSB_PARTY_CD, STCC_CD, CAR_OVERLOAD, RATIO_ETE, RATIO_STS,
            ALERT_STATUS, SUMNOMINAL_A, SUMNOMINAL_B, SUMNOMINAL_L, SUMNOMINAL_R, SUMNOMINAL,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.DTCTD_EQPMNT_ID, src.CREATE_USER_ID, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS, src.UPDATE_USER_ID,
            src.DTCTD_TRAIN_ID, src.EQPMNT_SQNC_NBR, src.RPRTD_MARK_CD, src.RPRTD_EQPUN_NBR, src.RPRTD_AEI_RAW_DATA_TXT,
            src.RPRTD_AXLE_QTY, src.RPRTD_EQPMNT_CTGRY_TXT, src.RPRTD_EQPMNT_TYPE_CD, src.RPRTD_EQPMNT_ORNTN_CD, src.RPRTD_SPEED_QTY,
            src.RPRTD_WEIGHT_QTY, src.CNFDNC_NBR, src.GROSS_WEIGHT_QTY, src.TRUCK_QTY, src.TARE_WEIGHT_QTY,
            src.VRFD_EQPMNT_ID, src.VRFD_EQPMNT_ORNTN_CD, src.VRFD_ORIGIN_SCAC_CD, src.VRFD_ORIGIN_FSAC_CD, src.VRFD_ORIGIN_TRSTN_NM,
            src.VRFD_DSTNTN_SCAC_CD, src.VRFD_DSTNTN_FSAC_CD, src.VRFD_DSTNTN_TRSTN_NM, src.VRFD_LOAD_EMPTY_CD, src.VRFD_NET_WEIGHT_QTY,
            src.WEIGHT_UOM_BASIS_CD, src.DTCTD_EQPMNT_CTGRY_CD, src.RPRTD_TRUCK_QTY, src.CPR_EQPMNT_POOL_ID, src.OWNER_MARK_CD,
            src.MNTNC_RSPNSB_PARTY_CD, src.STCC_CD, src.CAR_OVERLOAD, src.RATIO_ETE, src.RATIO_STS,
            src.ALERT_STATUS, src.SUMNOMINAL_A, src.SUMNOMINAL_B, src.SUMNOMINAL_L, src.SUMNOMINAL_R, src.SUMNOMINAL,
            src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
        
        v_rows_merged := SQLROWCOUNT;
        v_end_time := CURRENT_TIMESTAMP();
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME, ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ERROR_MESSAGE, CREATED_AT) VALUES ('DTQ_DTCTD_EQPMNT', :v_batch_id, 'RECOVERY', :v_start_time, :v_end_time, :v_rows_merged, :v_rows_merged, 0, 0, :v_error_msg, CURRENT_TIMESTAMP());
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_merged || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- CHECK 2: Stage stream data into temp table (BEST PRACTICE - single read)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_DTQ_DTCTD_EQPMNT AS
    SELECT 
        DTCTD_EQPMNT_ID, CREATE_USER_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS, UPDATE_USER_ID,
        DTCTD_TRAIN_ID, EQPMNT_SQNC_NBR, RPRTD_MARK_CD, RPRTD_EQPUN_NBR, RPRTD_AEI_RAW_DATA_TXT,
        RPRTD_AXLE_QTY, RPRTD_EQPMNT_CTGRY_TXT, RPRTD_EQPMNT_TYPE_CD, RPRTD_EQPMNT_ORNTN_CD, RPRTD_SPEED_QTY,
        RPRTD_WEIGHT_QTY, CNFDNC_NBR, GROSS_WEIGHT_QTY, TRUCK_QTY, TARE_WEIGHT_QTY,
        VRFD_EQPMNT_ID, VRFD_EQPMNT_ORNTN_CD, VRFD_ORIGIN_SCAC_CD, VRFD_ORIGIN_FSAC_CD, VRFD_ORIGIN_TRSTN_NM,
        VRFD_DSTNTN_SCAC_CD, VRFD_DSTNTN_FSAC_CD, VRFD_DSTNTN_TRSTN_NM, VRFD_LOAD_EMPTY_CD, VRFD_NET_WEIGHT_QTY,
        WEIGHT_UOM_BASIS_CD, DTCTD_EQPMNT_CTGRY_CD, RPRTD_TRUCK_QTY, CPR_EQPMNT_POOL_ID, OWNER_MARK_CD,
        MNTNC_RSPNSB_PARTY_CD, STCC_CD, CAR_OVERLOAD, RATIO_ETE, RATIO_STS,
        ALERT_STATUS, SUMNOMINAL_A, SUMNOMINAL_B, SUMNOMINAL_L, SUMNOMINAL_R, SUMNOMINAL,
        SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE_HIST_STREAM;
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_DTQ_DTCTD_EQPMNT;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_DTQ_DTCTD_EQPMNT;
        v_end_time := CURRENT_TIMESTAMP();
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME, ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ERROR_MESSAGE, CREATED_AT) VALUES ('DTQ_DTCTD_EQPMNT', :v_batch_id, 'NO_DATA', :v_start_time, :v_end_time, 0, 0, 0, 0, NULL, CURRENT_TIMESTAMP());
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
    FROM _CDC_STAGING_DTQ_DTCTD_EQPMNT;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.EHMS.DTQ_DTCTD_EQPMNT AS tgt
    USING (
        SELECT 
            DTCTD_EQPMNT_ID, CREATE_USER_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS, UPDATE_USER_ID,
            DTCTD_TRAIN_ID, EQPMNT_SQNC_NBR, RPRTD_MARK_CD, RPRTD_EQPUN_NBR, RPRTD_AEI_RAW_DATA_TXT,
            RPRTD_AXLE_QTY, RPRTD_EQPMNT_CTGRY_TXT, RPRTD_EQPMNT_TYPE_CD, RPRTD_EQPMNT_ORNTN_CD, RPRTD_SPEED_QTY,
            RPRTD_WEIGHT_QTY, CNFDNC_NBR, GROSS_WEIGHT_QTY, TRUCK_QTY, TARE_WEIGHT_QTY,
            VRFD_EQPMNT_ID, VRFD_EQPMNT_ORNTN_CD, VRFD_ORIGIN_SCAC_CD, VRFD_ORIGIN_FSAC_CD, VRFD_ORIGIN_TRSTN_NM,
            VRFD_DSTNTN_SCAC_CD, VRFD_DSTNTN_FSAC_CD, VRFD_DSTNTN_TRSTN_NM, VRFD_LOAD_EMPTY_CD, VRFD_NET_WEIGHT_QTY,
            WEIGHT_UOM_BASIS_CD, DTCTD_EQPMNT_CTGRY_CD, RPRTD_TRUCK_QTY, CPR_EQPMNT_POOL_ID, OWNER_MARK_CD,
            MNTNC_RSPNSB_PARTY_CD, STCC_CD, CAR_OVERLOAD, RATIO_ETE, RATIO_STS,
            ALERT_STATUS, SUMNOMINAL_A, SUMNOMINAL_B, SUMNOMINAL_L, SUMNOMINAL_R, SUMNOMINAL,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_DTQ_DTCTD_EQPMNT
    ) AS src
    ON tgt.DTCTD_EQPMNT_ID = src.DTCTD_EQPMNT_ID
    
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
        UPDATE SET
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.DTCTD_TRAIN_ID = src.DTCTD_TRAIN_ID,
            tgt.EQPMNT_SQNC_NBR = src.EQPMNT_SQNC_NBR,
            tgt.RPRTD_MARK_CD = src.RPRTD_MARK_CD,
            tgt.RPRTD_EQPUN_NBR = src.RPRTD_EQPUN_NBR,
            tgt.RPRTD_AEI_RAW_DATA_TXT = src.RPRTD_AEI_RAW_DATA_TXT,
            tgt.RPRTD_AXLE_QTY = src.RPRTD_AXLE_QTY,
            tgt.RPRTD_EQPMNT_CTGRY_TXT = src.RPRTD_EQPMNT_CTGRY_TXT,
            tgt.RPRTD_EQPMNT_TYPE_CD = src.RPRTD_EQPMNT_TYPE_CD,
            tgt.RPRTD_EQPMNT_ORNTN_CD = src.RPRTD_EQPMNT_ORNTN_CD,
            tgt.RPRTD_SPEED_QTY = src.RPRTD_SPEED_QTY,
            tgt.RPRTD_WEIGHT_QTY = src.RPRTD_WEIGHT_QTY,
            tgt.CNFDNC_NBR = src.CNFDNC_NBR,
            tgt.GROSS_WEIGHT_QTY = src.GROSS_WEIGHT_QTY,
            tgt.TRUCK_QTY = src.TRUCK_QTY,
            tgt.TARE_WEIGHT_QTY = src.TARE_WEIGHT_QTY,
            tgt.VRFD_EQPMNT_ID = src.VRFD_EQPMNT_ID,
            tgt.VRFD_EQPMNT_ORNTN_CD = src.VRFD_EQPMNT_ORNTN_CD,
            tgt.VRFD_ORIGIN_SCAC_CD = src.VRFD_ORIGIN_SCAC_CD,
            tgt.VRFD_ORIGIN_FSAC_CD = src.VRFD_ORIGIN_FSAC_CD,
            tgt.VRFD_ORIGIN_TRSTN_NM = src.VRFD_ORIGIN_TRSTN_NM,
            tgt.VRFD_DSTNTN_SCAC_CD = src.VRFD_DSTNTN_SCAC_CD,
            tgt.VRFD_DSTNTN_FSAC_CD = src.VRFD_DSTNTN_FSAC_CD,
            tgt.VRFD_DSTNTN_TRSTN_NM = src.VRFD_DSTNTN_TRSTN_NM,
            tgt.VRFD_LOAD_EMPTY_CD = src.VRFD_LOAD_EMPTY_CD,
            tgt.VRFD_NET_WEIGHT_QTY = src.VRFD_NET_WEIGHT_QTY,
            tgt.WEIGHT_UOM_BASIS_CD = src.WEIGHT_UOM_BASIS_CD,
            tgt.DTCTD_EQPMNT_CTGRY_CD = src.DTCTD_EQPMNT_CTGRY_CD,
            tgt.RPRTD_TRUCK_QTY = src.RPRTD_TRUCK_QTY,
            tgt.CPR_EQPMNT_POOL_ID = src.CPR_EQPMNT_POOL_ID,
            tgt.OWNER_MARK_CD = src.OWNER_MARK_CD,
            tgt.MNTNC_RSPNSB_PARTY_CD = src.MNTNC_RSPNSB_PARTY_CD,
            tgt.STCC_CD = src.STCC_CD,
            tgt.CAR_OVERLOAD = src.CAR_OVERLOAD,
            tgt.RATIO_ETE = src.RATIO_ETE,
            tgt.RATIO_STS = src.RATIO_STS,
            tgt.ALERT_STATUS = src.ALERT_STATUS,
            tgt.SUMNOMINAL_A = src.SUMNOMINAL_A,
            tgt.SUMNOMINAL_B = src.SUMNOMINAL_B,
            tgt.SUMNOMINAL_L = src.SUMNOMINAL_L,
            tgt.SUMNOMINAL_R = src.SUMNOMINAL_R,
            tgt.SUMNOMINAL = src.SUMNOMINAL,
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
            tgt.DTCTD_TRAIN_ID = src.DTCTD_TRAIN_ID,
            tgt.EQPMNT_SQNC_NBR = src.EQPMNT_SQNC_NBR,
            tgt.RPRTD_MARK_CD = src.RPRTD_MARK_CD,
            tgt.RPRTD_EQPUN_NBR = src.RPRTD_EQPUN_NBR,
            tgt.RPRTD_AEI_RAW_DATA_TXT = src.RPRTD_AEI_RAW_DATA_TXT,
            tgt.RPRTD_AXLE_QTY = src.RPRTD_AXLE_QTY,
            tgt.RPRTD_EQPMNT_CTGRY_TXT = src.RPRTD_EQPMNT_CTGRY_TXT,
            tgt.RPRTD_EQPMNT_TYPE_CD = src.RPRTD_EQPMNT_TYPE_CD,
            tgt.RPRTD_EQPMNT_ORNTN_CD = src.RPRTD_EQPMNT_ORNTN_CD,
            tgt.RPRTD_SPEED_QTY = src.RPRTD_SPEED_QTY,
            tgt.RPRTD_WEIGHT_QTY = src.RPRTD_WEIGHT_QTY,
            tgt.CNFDNC_NBR = src.CNFDNC_NBR,
            tgt.GROSS_WEIGHT_QTY = src.GROSS_WEIGHT_QTY,
            tgt.TRUCK_QTY = src.TRUCK_QTY,
            tgt.TARE_WEIGHT_QTY = src.TARE_WEIGHT_QTY,
            tgt.VRFD_EQPMNT_ID = src.VRFD_EQPMNT_ID,
            tgt.VRFD_EQPMNT_ORNTN_CD = src.VRFD_EQPMNT_ORNTN_CD,
            tgt.VRFD_ORIGIN_SCAC_CD = src.VRFD_ORIGIN_SCAC_CD,
            tgt.VRFD_ORIGIN_FSAC_CD = src.VRFD_ORIGIN_FSAC_CD,
            tgt.VRFD_ORIGIN_TRSTN_NM = src.VRFD_ORIGIN_TRSTN_NM,
            tgt.VRFD_DSTNTN_SCAC_CD = src.VRFD_DSTNTN_SCAC_CD,
            tgt.VRFD_DSTNTN_FSAC_CD = src.VRFD_DSTNTN_FSAC_CD,
            tgt.VRFD_DSTNTN_TRSTN_NM = src.VRFD_DSTNTN_TRSTN_NM,
            tgt.VRFD_LOAD_EMPTY_CD = src.VRFD_LOAD_EMPTY_CD,
            tgt.VRFD_NET_WEIGHT_QTY = src.VRFD_NET_WEIGHT_QTY,
            tgt.WEIGHT_UOM_BASIS_CD = src.WEIGHT_UOM_BASIS_CD,
            tgt.DTCTD_EQPMNT_CTGRY_CD = src.DTCTD_EQPMNT_CTGRY_CD,
            tgt.RPRTD_TRUCK_QTY = src.RPRTD_TRUCK_QTY,
            tgt.CPR_EQPMNT_POOL_ID = src.CPR_EQPMNT_POOL_ID,
            tgt.OWNER_MARK_CD = src.OWNER_MARK_CD,
            tgt.MNTNC_RSPNSB_PARTY_CD = src.MNTNC_RSPNSB_PARTY_CD,
            tgt.STCC_CD = src.STCC_CD,
            tgt.CAR_OVERLOAD = src.CAR_OVERLOAD,
            tgt.RATIO_ETE = src.RATIO_ETE,
            tgt.RATIO_STS = src.RATIO_STS,
            tgt.ALERT_STATUS = src.ALERT_STATUS,
            tgt.SUMNOMINAL_A = src.SUMNOMINAL_A,
            tgt.SUMNOMINAL_B = src.SUMNOMINAL_B,
            tgt.SUMNOMINAL_L = src.SUMNOMINAL_L,
            tgt.SUMNOMINAL_R = src.SUMNOMINAL_R,
            tgt.SUMNOMINAL = src.SUMNOMINAL,
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
            DTCTD_EQPMNT_ID, CREATE_USER_ID, RECORD_CREATE_TMS, RECORD_UPDATE_TMS, UPDATE_USER_ID,
            DTCTD_TRAIN_ID, EQPMNT_SQNC_NBR, RPRTD_MARK_CD, RPRTD_EQPUN_NBR, RPRTD_AEI_RAW_DATA_TXT,
            RPRTD_AXLE_QTY, RPRTD_EQPMNT_CTGRY_TXT, RPRTD_EQPMNT_TYPE_CD, RPRTD_EQPMNT_ORNTN_CD, RPRTD_SPEED_QTY,
            RPRTD_WEIGHT_QTY, CNFDNC_NBR, GROSS_WEIGHT_QTY, TRUCK_QTY, TARE_WEIGHT_QTY,
            VRFD_EQPMNT_ID, VRFD_EQPMNT_ORNTN_CD, VRFD_ORIGIN_SCAC_CD, VRFD_ORIGIN_FSAC_CD, VRFD_ORIGIN_TRSTN_NM,
            VRFD_DSTNTN_SCAC_CD, VRFD_DSTNTN_FSAC_CD, VRFD_DSTNTN_TRSTN_NM, VRFD_LOAD_EMPTY_CD, VRFD_NET_WEIGHT_QTY,
            WEIGHT_UOM_BASIS_CD, DTCTD_EQPMNT_CTGRY_CD, RPRTD_TRUCK_QTY, CPR_EQPMNT_POOL_ID, OWNER_MARK_CD,
            MNTNC_RSPNSB_PARTY_CD, STCC_CD, CAR_OVERLOAD, RATIO_ETE, RATIO_STS,
            ALERT_STATUS, SUMNOMINAL_A, SUMNOMINAL_B, SUMNOMINAL_L, SUMNOMINAL_R, SUMNOMINAL,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.DTCTD_EQPMNT_ID, src.CREATE_USER_ID, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS, src.UPDATE_USER_ID,
            src.DTCTD_TRAIN_ID, src.EQPMNT_SQNC_NBR, src.RPRTD_MARK_CD, src.RPRTD_EQPUN_NBR, src.RPRTD_AEI_RAW_DATA_TXT,
            src.RPRTD_AXLE_QTY, src.RPRTD_EQPMNT_CTGRY_TXT, src.RPRTD_EQPMNT_TYPE_CD, src.RPRTD_EQPMNT_ORNTN_CD, src.RPRTD_SPEED_QTY,
            src.RPRTD_WEIGHT_QTY, src.CNFDNC_NBR, src.GROSS_WEIGHT_QTY, src.TRUCK_QTY, src.TARE_WEIGHT_QTY,
            src.VRFD_EQPMNT_ID, src.VRFD_EQPMNT_ORNTN_CD, src.VRFD_ORIGIN_SCAC_CD, src.VRFD_ORIGIN_FSAC_CD, src.VRFD_ORIGIN_TRSTN_NM,
            src.VRFD_DSTNTN_SCAC_CD, src.VRFD_DSTNTN_FSAC_CD, src.VRFD_DSTNTN_TRSTN_NM, src.VRFD_LOAD_EMPTY_CD, src.VRFD_NET_WEIGHT_QTY,
            src.WEIGHT_UOM_BASIS_CD, src.DTCTD_EQPMNT_CTGRY_CD, src.RPRTD_TRUCK_QTY, src.CPR_EQPMNT_POOL_ID, src.OWNER_MARK_CD,
            src.MNTNC_RSPNSB_PARTY_CD, src.STCC_CD, src.CAR_OVERLOAD, src.RATIO_ETE, src.RATIO_STS,
            src.ALERT_STATUS, src.SUMNOMINAL_A, src.SUMNOMINAL_B, src.SUMNOMINAL_L, src.SUMNOMINAL_R, src.SUMNOMINAL,
            src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    v_end_time := CURRENT_TIMESTAMP();
    DROP TABLE IF EXISTS _CDC_STAGING_DTQ_DTCTD_EQPMNT;
    
    INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME, ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ERROR_MESSAGE, CREATED_AT) VALUES ('DTQ_DTCTD_EQPMNT', :v_batch_id, 'SUCCESS', :v_start_time, :v_end_time, :v_rows_merged, :v_rows_inserted, :v_rows_updated, :v_rows_deleted, NULL, CURRENT_TIMESTAMP());
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes (I:' || v_rows_inserted || ' U:' || v_rows_updated || ' D:' || v_rows_deleted || '). Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        v_end_time := CURRENT_TIMESTAMP();
        v_error_msg := SQLERRM;
        DROP TABLE IF EXISTS _CDC_STAGING_DTQ_DTCTD_EQPMNT;
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME, ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ERROR_MESSAGE, CREATED_AT) VALUES ('DTQ_DTCTD_EQPMNT', :v_batch_id, 'ERROR', :v_start_time, :v_end_time, 0, 0, 0, 0, :v_error_msg, CURRENT_TIMESTAMP());
        RETURN 'ERROR: ' || v_error_msg || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.EHMS.TASK_SP_PROCESS_DTQ_DTCTD_EQPMNT
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process DTQ_DTCTD_EQPMNT_BASE CDC changes into data preservation table'
AS
    CALL D_RAW.EHMS.SP_PROCESS_DTQ_DTCTD_EQPMNT();

ALTER TASK D_RAW.EHMS.TASK_SP_PROCESS_DTQ_DTCTD_EQPMNT RESUME;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'DTQ_DTCTD_EQPMNT%' IN SCHEMA D_BRONZE.EHMS;
-- SHOW STREAMS LIKE 'DTQ_DTCTD_EQPMNT%' IN SCHEMA D_RAW.EHMS;
-- SHOW TASKS LIKE 'TASK_SP_PROCESS_DTQ_DTCTD_EQPMNT%' IN SCHEMA D_RAW.EHMS;
-- CALL D_RAW.EHMS.SP_PROCESS_DTQ_DTCTD_EQPMNT();
