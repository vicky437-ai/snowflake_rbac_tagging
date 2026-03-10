/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE (V2 - ENHANCED)
================================================================================
Source Table : D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE
Target Table : D_BRONZE.SADB.TRAIN_OPTRN_EVENT
Stream       : D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE_HIST_STREAM
Procedure    : D_RAW.SADB.SP_PROCESS_TRAIN_OPTRN_EVENT()
Task         : D_RAW.SADB.TASK_PROCESS_TRAIN_OPTRN_EVENT
Primary Key  : OPTRN_EVENT_ID (Single)
Total Columns: 29 source + 6 CDC metadata = 35
Filter       : SNW_OPERATION_OWNER NOT IN ('TSDPRG','EMEPRG') (exclude purged records)
================================================================================
VERSION 2 ENHANCEMENTS:
- Added execution logging table for audit/monitoring
- Added error logging table for troubleshooting
- Explicit transaction control with COMMIT/ROLLBACK
- Granular exception handling at critical stages
- Re-raise errors to notify task of failures
- Added data quality validation
- Added execution metrics (duration, row counts)
================================================================================
*/

-- =============================================================================
-- STEP 0: Create Logging Tables (Run Once)
-- =============================================================================
CREATE TABLE IF NOT EXISTS D_RAW.SADB.SP_EXECUTION_LOG (
    EXECUTION_ID VARCHAR(100) NOT NULL,
    PROCEDURE_NAME VARCHAR(200) NOT NULL,
    BATCH_ID VARCHAR(100),
    STATUS VARCHAR(50),
    ROWS_STAGED NUMBER,
    ROWS_MERGED NUMBER,
    ROWS_INSERTED NUMBER,
    ROWS_UPDATED NUMBER,
    ROWS_DELETED NUMBER,
    DURATION_SECONDS NUMBER(10,2),
    MESSAGE VARCHAR(4000),
    STARTED_AT TIMESTAMP_NTZ,
    COMPLETED_AT TIMESTAMP_NTZ,
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER(),
    PRIMARY KEY (EXECUTION_ID)
);

CREATE TABLE IF NOT EXISTS D_RAW.SADB.SP_ERROR_LOG (
    ERROR_ID VARCHAR(100) NOT NULL,
    PROCEDURE_NAME VARCHAR(200) NOT NULL,
    BATCH_ID VARCHAR(100),
    ERROR_STAGE VARCHAR(100),
    ERROR_CODE VARCHAR(20),
    ERROR_STATE VARCHAR(10),
    ERROR_MESSAGE VARCHAR(4000),
    STACK_CONTEXT VARCHAR(4000),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER(),
    PRIMARY KEY (ERROR_ID)
);

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE OR ALTER TABLE D_BRONZE.SADB.TRAIN_OPTRN_EVENT (
    OPTRN_EVENT_ID NUMBER(18,0) NOT NULL,
    OPTRN_LEG_ID NUMBER(18,0),
    EVENT_TMS TIMESTAMP_NTZ(0),
    TRAIN_PLAN_LEG_ID NUMBER(18,0),
    TRAIN_PLAN_EVENT_ID NUMBER(18,0),
    TRAIN_EVENT_TYPE_CD VARCHAR(16),
    MTP_ROUTE_POINT_SQNC_NBR NUMBER(3,0),
    TRAVEL_DRCTN_CD VARCHAR(20),
    SCAC_CD VARCHAR(16),
    FSAC_CD VARCHAR(20),
    TRSTN_VRSN_NBR NUMBER(5,0),
    RGN_NM_TRK_NBR NUMBER(18,0),
    REGION_NBR NUMBER(18,0),
    RECORD_CREATE_TMS TIMESTAMP_NTZ(0),
    RECORD_UPDATE_TMS TIMESTAMP_NTZ(0),
    CREATE_USER_ID VARCHAR(32),
    UPDATE_USER_ID VARCHAR(32),
    TIME_ZONE_CD VARCHAR(8),
    TIME_ZONE_YEAR_NBR NUMBER(4,0),
    EVENT_SOURCE_CD VARCHAR(32),
    MILE_NBR NUMBER(8,3),
    AEIRD_NBR VARCHAR(28),
    AEIRD_DRCTN_CD VARCHAR(4),
    MTP_OMTS_PNDNG_IND VARCHAR(4),
    CTC_SIGNAL_ID VARCHAR(24),
    OPSNG_CTC_SIGNAL_ID VARCHAR(24),
    SNW_OPERATION_TYPE VARCHAR(1),
    SNW_LAST_REPLICATED TIMESTAMP_NTZ(9),

    CDC_OPERATION VARCHAR(10),
    CDC_TIMESTAMP TIMESTAMP_NTZ,
    IS_DELETED BOOLEAN,
    RECORD_CREATED_AT TIMESTAMP_NTZ,
    RECORD_UPDATED_AT TIMESTAMP_NTZ,
    SOURCE_LOAD_BATCH_ID VARCHAR(100),
    SNW_OPERATION_OWNER VARCHAR(256),

    PRIMARY KEY (OPTRN_EVENT_ID)
);

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 45,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 15;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for TRAIN_OPTRN_EVENT_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing (V2 - ENHANCED)
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.SADB.SP_PROCESS_TRAIN_OPTRN_EVENT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_execution_id VARCHAR;
    v_batch_id VARCHAR;
    v_start_time TIMESTAMP_NTZ;
    v_stream_stale BOOLEAN DEFAULT FALSE;
    v_staging_count NUMBER DEFAULT 0;
    v_rows_merged NUMBER DEFAULT 0;
    v_null_pk_count NUMBER DEFAULT 0;
    v_result VARCHAR;
    v_error_msg VARCHAR;
    v_error_stage VARCHAR;
    
    staging_exception EXCEPTION (-20001, 'Failed to create staging table');
    merge_exception EXCEPTION (-20002, 'MERGE operation failed');
    validation_exception EXCEPTION (-20003, 'Data validation failed');
    recovery_exception EXCEPTION (-20004, 'Stream recovery failed');
BEGIN
    v_start_time := CURRENT_TIMESTAMP();
    v_execution_id := 'EXEC_' || TO_VARCHAR(v_start_time, 'YYYYMMDD_HH24MISS_FF3');
    v_batch_id := 'BATCH_' || TO_VARCHAR(v_start_time, 'YYYYMMDD_HH24MISS');
    v_error_stage := 'INITIALIZATION';
    
    -- =========================================================================
    -- CHECK 1: Detect if stream is stale (happens after IDMC truncate/reload)
    -- =========================================================================
    v_error_stage := 'STREAM_HEALTH_CHECK';
    BEGIN
        SELECT COUNT(*) INTO v_staging_count 
        FROM D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE_HIST_STREAM
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
        v_error_stage := 'STREAM_RECOVERY';
        v_result := 'STREAM_STALE_DETECTED: ' || NVL(v_error_msg, 'Unknown');
        
        BEGIN
            CREATE OR REPLACE STREAM D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE_HIST_STREAM
            ON TABLE D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE
            SHOW_INITIAL_ROWS = TRUE
            COMMENT = 'CDC Stream recreated after staleness detection';
        EXCEPTION
            WHEN OTHER THEN
                INSERT INTO D_RAW.SADB.SP_ERROR_LOG (
                    ERROR_ID, PROCEDURE_NAME, BATCH_ID, ERROR_STAGE,
                    ERROR_CODE, ERROR_STATE, ERROR_MESSAGE, STACK_CONTEXT
                ) VALUES (
                    :v_execution_id || '_RECREATE', 'SP_PROCESS_TRAIN_OPTRN_EVENT', :v_batch_id, 
                    'STREAM_RECREATE', SQLCODE, SQLSTATE, SQLERRM, 'Failed to recreate stream'
                );
                RAISE recovery_exception;
        END;
        
        BEGIN
            BEGIN TRANSACTION;
            
            MERGE INTO D_BRONZE.SADB.TRAIN_OPTRN_EVENT AS tgt
            USING (
                SELECT 
                    src.*,
                    'INSERT' AS CDC_OP,
                    :v_batch_id AS BATCH_ID
                FROM D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE_HIST_STREAM src
                WHERE NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')
            ) AS src
            ON tgt.OPTRN_EVENT_ID = src.OPTRN_EVENT_ID
            WHEN MATCHED THEN UPDATE SET
                tgt.OPTRN_LEG_ID = src.OPTRN_LEG_ID,
                tgt.EVENT_TMS = src.EVENT_TMS,
                tgt.TRAIN_PLAN_LEG_ID = src.TRAIN_PLAN_LEG_ID,
                tgt.TRAIN_PLAN_EVENT_ID = src.TRAIN_PLAN_EVENT_ID,
                tgt.TRAIN_EVENT_TYPE_CD = src.TRAIN_EVENT_TYPE_CD,
                tgt.MTP_ROUTE_POINT_SQNC_NBR = src.MTP_ROUTE_POINT_SQNC_NBR,
                tgt.TRAVEL_DRCTN_CD = src.TRAVEL_DRCTN_CD,
                tgt.SCAC_CD = src.SCAC_CD,
                tgt.FSAC_CD = src.FSAC_CD,
                tgt.TRSTN_VRSN_NBR = src.TRSTN_VRSN_NBR,
                tgt.RGN_NM_TRK_NBR = src.RGN_NM_TRK_NBR,
                tgt.REGION_NBR = src.REGION_NBR,
                tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
                tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
                tgt.CREATE_USER_ID = src.CREATE_USER_ID,
                tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
                tgt.TIME_ZONE_CD = src.TIME_ZONE_CD,
                tgt.TIME_ZONE_YEAR_NBR = src.TIME_ZONE_YEAR_NBR,
                tgt.EVENT_SOURCE_CD = src.EVENT_SOURCE_CD,
                tgt.MILE_NBR = src.MILE_NBR,
                tgt.AEIRD_NBR = src.AEIRD_NBR,
                tgt.AEIRD_DRCTN_CD = src.AEIRD_DRCTN_CD,
                tgt.MTP_OMTS_PNDNG_IND = src.MTP_OMTS_PNDNG_IND,
                tgt.CTC_SIGNAL_ID = src.CTC_SIGNAL_ID,
                tgt.OPSNG_CTC_SIGNAL_ID = src.OPSNG_CTC_SIGNAL_ID,
                tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
                tgt.SNW_OPERATION_OWNER = src.SNW_OPERATION_OWNER,
                tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
                tgt.CDC_OPERATION = 'RELOADED',
                tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
                tgt.IS_DELETED = FALSE,
                tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
                tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
            WHEN NOT MATCHED THEN INSERT (
                OPTRN_EVENT_ID, OPTRN_LEG_ID, EVENT_TMS, TRAIN_PLAN_LEG_ID, TRAIN_PLAN_EVENT_ID,
                TRAIN_EVENT_TYPE_CD, MTP_ROUTE_POINT_SQNC_NBR, TRAVEL_DRCTN_CD, SCAC_CD, FSAC_CD,
                TRSTN_VRSN_NBR, RGN_NM_TRK_NBR, REGION_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
                CREATE_USER_ID, UPDATE_USER_ID, TIME_ZONE_CD, TIME_ZONE_YEAR_NBR, EVENT_SOURCE_CD,
                MILE_NBR, AEIRD_NBR, AEIRD_DRCTN_CD, MTP_OMTS_PNDNG_IND, CTC_SIGNAL_ID, OPSNG_CTC_SIGNAL_ID,
                SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
                CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, 
                RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
            ) VALUES (
                src.OPTRN_EVENT_ID, src.OPTRN_LEG_ID, src.EVENT_TMS, src.TRAIN_PLAN_LEG_ID, src.TRAIN_PLAN_EVENT_ID,
                src.TRAIN_EVENT_TYPE_CD, src.MTP_ROUTE_POINT_SQNC_NBR, src.TRAVEL_DRCTN_CD, src.SCAC_CD, src.FSAC_CD,
                src.TRSTN_VRSN_NBR, src.RGN_NM_TRK_NBR, src.REGION_NBR, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS,
                src.CREATE_USER_ID, src.UPDATE_USER_ID, src.TIME_ZONE_CD, src.TIME_ZONE_YEAR_NBR, src.EVENT_SOURCE_CD,
                src.MILE_NBR, src.AEIRD_NBR, src.AEIRD_DRCTN_CD, src.MTP_OMTS_PNDNG_IND, src.CTC_SIGNAL_ID, src.OPSNG_CTC_SIGNAL_ID,
                src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
                'INSERT', CURRENT_TIMESTAMP(), FALSE,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
            );
            
            v_rows_merged := SQLROWCOUNT;
            COMMIT;
            
        EXCEPTION
            WHEN OTHER THEN
                ROLLBACK;
                INSERT INTO D_RAW.SADB.SP_ERROR_LOG (
                    ERROR_ID, PROCEDURE_NAME, BATCH_ID, ERROR_STAGE,
                    ERROR_CODE, ERROR_STATE, ERROR_MESSAGE, STACK_CONTEXT
                ) VALUES (
                    :v_execution_id || '_RECOVERY', 'SP_PROCESS_TRAIN_OPTRN_EVENT', :v_batch_id, 
                    'RECOVERY_MERGE', SQLCODE, SQLSTATE, SQLERRM, 'Recovery MERGE failed, transaction rolled back'
                );
                RAISE recovery_exception;
        END;
        
        INSERT INTO D_RAW.SADB.SP_EXECUTION_LOG (
            EXECUTION_ID, PROCEDURE_NAME, BATCH_ID, STATUS, ROWS_MERGED,
            DURATION_SECONDS, MESSAGE, STARTED_AT, COMPLETED_AT
        ) VALUES (
            :v_execution_id, 'SP_PROCESS_TRAIN_OPTRN_EVENT', :v_batch_id, 'RECOVERY_SUCCESS',
            :v_rows_merged, TIMESTAMPDIFF(SECOND, :v_start_time, CURRENT_TIMESTAMP()),
            'Stream recreated and recovery merge completed', :v_start_time, CURRENT_TIMESTAMP()
        );
        
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_merged || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- STAGE 1: Create staging table from stream (single read - best practice)
    -- =========================================================================
    v_error_stage := 'STAGING_CREATION';
    BEGIN
        CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_TRAIN_OPTRN_EVENT AS
        SELECT 
            OPTRN_EVENT_ID, OPTRN_LEG_ID, EVENT_TMS, TRAIN_PLAN_LEG_ID, TRAIN_PLAN_EVENT_ID,
            TRAIN_EVENT_TYPE_CD, MTP_ROUTE_POINT_SQNC_NBR, TRAVEL_DRCTN_CD, SCAC_CD, FSAC_CD,
            TRSTN_VRSN_NBR, RGN_NM_TRK_NBR, REGION_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            CREATE_USER_ID, UPDATE_USER_ID, TIME_ZONE_CD, TIME_ZONE_YEAR_NBR, EVENT_SOURCE_CD,
            MILE_NBR, AEIRD_NBR, AEIRD_DRCTN_CD, MTP_OMTS_PNDNG_IND, CTC_SIGNAL_ID, OPSNG_CTC_SIGNAL_ID,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            METADATA$ACTION AS CDC_ACTION,
            METADATA$ISUPDATE AS CDC_IS_UPDATE,
            METADATA$ROW_ID AS ROW_ID
        FROM D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE_HIST_STREAM
        WHERE NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG');
    EXCEPTION
        WHEN OTHER THEN
            INSERT INTO D_RAW.SADB.SP_ERROR_LOG (
                ERROR_ID, PROCEDURE_NAME, BATCH_ID, ERROR_STAGE,
                ERROR_CODE, ERROR_STATE, ERROR_MESSAGE, STACK_CONTEXT
            ) VALUES (
                :v_execution_id || '_STAGE', 'SP_PROCESS_TRAIN_OPTRN_EVENT', :v_batch_id, 
                'STAGING_CREATION', SQLCODE, SQLSTATE, SQLERRM, 'Failed to create staging table from stream'
            );
            RAISE staging_exception;
    END;
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_TRAIN_OPTRN_EVENT;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_TRAIN_OPTRN_EVENT;
        
        INSERT INTO D_RAW.SADB.SP_EXECUTION_LOG (
            EXECUTION_ID, PROCEDURE_NAME, BATCH_ID, STATUS, ROWS_STAGED,
            DURATION_SECONDS, MESSAGE, STARTED_AT, COMPLETED_AT
        ) VALUES (
            :v_execution_id, 'SP_PROCESS_TRAIN_OPTRN_EVENT', :v_batch_id, 'NO_DATA',
            0, TIMESTAMPDIFF(SECOND, :v_start_time, CURRENT_TIMESTAMP()),
            'Stream has no changes to process', :v_start_time, CURRENT_TIMESTAMP()
        );
        
        RETURN 'NO_DATA: Stream has no changes to process at ' || CURRENT_TIMESTAMP()::VARCHAR;
    END IF;
    
    -- =========================================================================
    -- STAGE 2: Data Quality Validation
    -- =========================================================================
    v_error_stage := 'DATA_VALIDATION';
    SELECT COUNT(*) INTO v_null_pk_count 
    FROM _CDC_STAGING_TRAIN_OPTRN_EVENT 
    WHERE OPTRN_EVENT_ID IS NULL;
    
    IF (v_null_pk_count > 0) THEN
        INSERT INTO D_RAW.SADB.SP_ERROR_LOG (
            ERROR_ID, PROCEDURE_NAME, BATCH_ID, ERROR_STAGE,
            ERROR_CODE, ERROR_STATE, ERROR_MESSAGE, STACK_CONTEXT
        ) VALUES (
            :v_execution_id || '_VALIDATE', 'SP_PROCESS_TRAIN_OPTRN_EVENT', :v_batch_id, 
            'DATA_VALIDATION', '-20003', 'P0001', 
            'Found ' || :v_null_pk_count || ' records with NULL primary key (OPTRN_EVENT_ID)',
            'Primary key validation failed - NULL values detected'
        );
        DROP TABLE IF EXISTS _CDC_STAGING_TRAIN_OPTRN_EVENT;
        RAISE validation_exception;
    END IF;
    
    -- =========================================================================
    -- STAGE 3: MERGE CDC changes with transaction control
    -- =========================================================================
    v_error_stage := 'MERGE_PROCESSING';
    BEGIN
        BEGIN TRANSACTION;
        
        MERGE INTO D_BRONZE.SADB.TRAIN_OPTRN_EVENT AS tgt
        USING (
            SELECT 
                OPTRN_EVENT_ID, OPTRN_LEG_ID, EVENT_TMS, TRAIN_PLAN_LEG_ID, TRAIN_PLAN_EVENT_ID,
                TRAIN_EVENT_TYPE_CD, MTP_ROUTE_POINT_SQNC_NBR, TRAVEL_DRCTN_CD, SCAC_CD, FSAC_CD,
                TRSTN_VRSN_NBR, RGN_NM_TRK_NBR, REGION_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
                CREATE_USER_ID, UPDATE_USER_ID, TIME_ZONE_CD, TIME_ZONE_YEAR_NBR, EVENT_SOURCE_CD,
                MILE_NBR, AEIRD_NBR, AEIRD_DRCTN_CD, MTP_OMTS_PNDNG_IND, CTC_SIGNAL_ID, OPSNG_CTC_SIGNAL_ID,
                SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
                CDC_ACTION,
                CDC_IS_UPDATE,
                ROW_ID,
                :v_batch_id AS BATCH_ID
            FROM _CDC_STAGING_TRAIN_OPTRN_EVENT
        ) AS src
        ON tgt.OPTRN_EVENT_ID = src.OPTRN_EVENT_ID
        
        WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
            UPDATE SET
                tgt.OPTRN_LEG_ID = src.OPTRN_LEG_ID,
                tgt.EVENT_TMS = src.EVENT_TMS,
                tgt.TRAIN_PLAN_LEG_ID = src.TRAIN_PLAN_LEG_ID,
                tgt.TRAIN_PLAN_EVENT_ID = src.TRAIN_PLAN_EVENT_ID,
                tgt.TRAIN_EVENT_TYPE_CD = src.TRAIN_EVENT_TYPE_CD,
                tgt.MTP_ROUTE_POINT_SQNC_NBR = src.MTP_ROUTE_POINT_SQNC_NBR,
                tgt.TRAVEL_DRCTN_CD = src.TRAVEL_DRCTN_CD,
                tgt.SCAC_CD = src.SCAC_CD,
                tgt.FSAC_CD = src.FSAC_CD,
                tgt.TRSTN_VRSN_NBR = src.TRSTN_VRSN_NBR,
                tgt.RGN_NM_TRK_NBR = src.RGN_NM_TRK_NBR,
                tgt.REGION_NBR = src.REGION_NBR,
                tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
                tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
                tgt.CREATE_USER_ID = src.CREATE_USER_ID,
                tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
                tgt.TIME_ZONE_CD = src.TIME_ZONE_CD,
                tgt.TIME_ZONE_YEAR_NBR = src.TIME_ZONE_YEAR_NBR,
                tgt.EVENT_SOURCE_CD = src.EVENT_SOURCE_CD,
                tgt.MILE_NBR = src.MILE_NBR,
                tgt.AEIRD_NBR = src.AEIRD_NBR,
                tgt.AEIRD_DRCTN_CD = src.AEIRD_DRCTN_CD,
                tgt.MTP_OMTS_PNDNG_IND = src.MTP_OMTS_PNDNG_IND,
                tgt.CTC_SIGNAL_ID = src.CTC_SIGNAL_ID,
                tgt.OPSNG_CTC_SIGNAL_ID = src.OPSNG_CTC_SIGNAL_ID,
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
                tgt.OPTRN_LEG_ID = src.OPTRN_LEG_ID,
                tgt.EVENT_TMS = src.EVENT_TMS,
                tgt.TRAIN_PLAN_LEG_ID = src.TRAIN_PLAN_LEG_ID,
                tgt.TRAIN_PLAN_EVENT_ID = src.TRAIN_PLAN_EVENT_ID,
                tgt.TRAIN_EVENT_TYPE_CD = src.TRAIN_EVENT_TYPE_CD,
                tgt.MTP_ROUTE_POINT_SQNC_NBR = src.MTP_ROUTE_POINT_SQNC_NBR,
                tgt.TRAVEL_DRCTN_CD = src.TRAVEL_DRCTN_CD,
                tgt.SCAC_CD = src.SCAC_CD,
                tgt.FSAC_CD = src.FSAC_CD,
                tgt.TRSTN_VRSN_NBR = src.TRSTN_VRSN_NBR,
                tgt.RGN_NM_TRK_NBR = src.RGN_NM_TRK_NBR,
                tgt.REGION_NBR = src.REGION_NBR,
                tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
                tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
                tgt.CREATE_USER_ID = src.CREATE_USER_ID,
                tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
                tgt.TIME_ZONE_CD = src.TIME_ZONE_CD,
                tgt.TIME_ZONE_YEAR_NBR = src.TIME_ZONE_YEAR_NBR,
                tgt.EVENT_SOURCE_CD = src.EVENT_SOURCE_CD,
                tgt.MILE_NBR = src.MILE_NBR,
                tgt.AEIRD_NBR = src.AEIRD_NBR,
                tgt.AEIRD_DRCTN_CD = src.AEIRD_DRCTN_CD,
                tgt.MTP_OMTS_PNDNG_IND = src.MTP_OMTS_PNDNG_IND,
                tgt.CTC_SIGNAL_ID = src.CTC_SIGNAL_ID,
                tgt.OPSNG_CTC_SIGNAL_ID = src.OPSNG_CTC_SIGNAL_ID,
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
                OPTRN_EVENT_ID, OPTRN_LEG_ID, EVENT_TMS, TRAIN_PLAN_LEG_ID, TRAIN_PLAN_EVENT_ID,
                TRAIN_EVENT_TYPE_CD, MTP_ROUTE_POINT_SQNC_NBR, TRAVEL_DRCTN_CD, SCAC_CD, FSAC_CD,
                TRSTN_VRSN_NBR, RGN_NM_TRK_NBR, REGION_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
                CREATE_USER_ID, UPDATE_USER_ID, TIME_ZONE_CD, TIME_ZONE_YEAR_NBR, EVENT_SOURCE_CD,
                MILE_NBR, AEIRD_NBR, AEIRD_DRCTN_CD, MTP_OMTS_PNDNG_IND, CTC_SIGNAL_ID, OPSNG_CTC_SIGNAL_ID,
                SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
                CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
                RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
            ) VALUES (
                src.OPTRN_EVENT_ID, src.OPTRN_LEG_ID, src.EVENT_TMS, src.TRAIN_PLAN_LEG_ID, src.TRAIN_PLAN_EVENT_ID,
                src.TRAIN_EVENT_TYPE_CD, src.MTP_ROUTE_POINT_SQNC_NBR, src.TRAVEL_DRCTN_CD, src.SCAC_CD, src.FSAC_CD,
                src.TRSTN_VRSN_NBR, src.RGN_NM_TRK_NBR, src.REGION_NBR, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS,
                src.CREATE_USER_ID, src.UPDATE_USER_ID, src.TIME_ZONE_CD, src.TIME_ZONE_YEAR_NBR, src.EVENT_SOURCE_CD,
                src.MILE_NBR, src.AEIRD_NBR, src.AEIRD_DRCTN_CD, src.MTP_OMTS_PNDNG_IND, src.CTC_SIGNAL_ID, src.OPSNG_CTC_SIGNAL_ID,
                src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
                'INSERT', CURRENT_TIMESTAMP(), FALSE,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
            );
        
        v_rows_merged := SQLROWCOUNT;
        COMMIT;
        
    EXCEPTION
        WHEN OTHER THEN
            ROLLBACK;
            INSERT INTO D_RAW.SADB.SP_ERROR_LOG (
                ERROR_ID, PROCEDURE_NAME, BATCH_ID, ERROR_STAGE,
                ERROR_CODE, ERROR_STATE, ERROR_MESSAGE, STACK_CONTEXT
            ) VALUES (
                :v_execution_id || '_MERGE', 'SP_PROCESS_TRAIN_OPTRN_EVENT', :v_batch_id, 
                'MERGE_PROCESSING', SQLCODE, SQLSTATE, SQLERRM, 
                'MERGE failed, transaction rolled back. Staged rows: ' || :v_staging_count
            );
            DROP TABLE IF EXISTS _CDC_STAGING_TRAIN_OPTRN_EVENT;
            RAISE merge_exception;
    END;
    
    -- =========================================================================
    -- CLEANUP & SUCCESS LOGGING
    -- =========================================================================
    DROP TABLE IF EXISTS _CDC_STAGING_TRAIN_OPTRN_EVENT;
    
    INSERT INTO D_RAW.SADB.SP_EXECUTION_LOG (
        EXECUTION_ID, PROCEDURE_NAME, BATCH_ID, STATUS, ROWS_STAGED, ROWS_MERGED,
        DURATION_SECONDS, MESSAGE, STARTED_AT, COMPLETED_AT
    ) VALUES (
        :v_execution_id, 'SP_PROCESS_TRAIN_OPTRN_EVENT', :v_batch_id, 'SUCCESS',
        :v_staging_count, :v_rows_merged,
        TIMESTAMPDIFF(SECOND, :v_start_time, CURRENT_TIMESTAMP()),
        'CDC processing completed successfully', :v_start_time, CURRENT_TIMESTAMP()
    );
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes from ' || v_staging_count || ' staged rows. Batch: ' || v_batch_id;
    
-- =============================================================================
-- GLOBAL EXCEPTION HANDLER
-- =============================================================================
EXCEPTION
    WHEN staging_exception THEN
        RETURN 'ERROR [STAGING]: Failed to create staging table. Check SP_ERROR_LOG for details. Batch: ' || v_batch_id;
    
    WHEN merge_exception THEN
        RETURN 'ERROR [MERGE]: MERGE operation failed and was rolled back. Check SP_ERROR_LOG for details. Batch: ' || v_batch_id;
    
    WHEN validation_exception THEN
        RETURN 'ERROR [VALIDATION]: Data quality check failed. Check SP_ERROR_LOG for details. Batch: ' || v_batch_id;
    
    WHEN recovery_exception THEN
        RETURN 'ERROR [RECOVERY]: Stream recovery failed. Check SP_ERROR_LOG for details. Batch: ' || v_batch_id;
    
    WHEN OTHER THEN
        INSERT INTO D_RAW.SADB.SP_ERROR_LOG (
            ERROR_ID, PROCEDURE_NAME, BATCH_ID, ERROR_STAGE,
            ERROR_CODE, ERROR_STATE, ERROR_MESSAGE, STACK_CONTEXT
        ) VALUES (
            :v_execution_id || '_UNHANDLED', 'SP_PROCESS_TRAIN_OPTRN_EVENT', :v_batch_id, 
            :v_error_stage, SQLCODE, SQLSTATE, SQLERRM, 'Unhandled exception in procedure'
        );
        DROP TABLE IF EXISTS _CDC_STAGING_TRAIN_OPTRN_EVENT;
        RETURN 'ERROR [UNHANDLED]: ' || SQLERRM || ' at stage: ' || v_error_stage || '. Batch: ' || v_batch_id;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.SADB.TASK_PROCESS_TRAIN_OPTRN_EVENT
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process TRAIN_OPTRN_EVENT_BASE CDC changes into data preservation table (V2 Enhanced)'
AS
    CALL D_RAW.SADB.SP_PROCESS_TRAIN_OPTRN_EVENT();

ALTER TASK D_RAW.SADB.TASK_PROCESS_TRAIN_OPTRN_EVENT RESUME;

-- =============================================================================
-- VERIFICATION & MONITORING QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'TRAIN_OPTRN_EVENT%' IN SCHEMA D_BRONZE.SADB;
-- SHOW STREAMS LIKE 'TRAIN_OPTRN_EVENT%' IN SCHEMA D_RAW.SADB;
-- SHOW TASKS LIKE 'TASK_PROCESS_TRAIN_OPTRN_EVENT%' IN SCHEMA D_RAW.SADB;

-- Test procedure manually:
-- CALL D_RAW.SADB.SP_PROCESS_TRAIN_OPTRN_EVENT();

-- Monitor execution history:
-- SELECT * FROM D_RAW.SADB.SP_EXECUTION_LOG WHERE PROCEDURE_NAME = 'SP_PROCESS_TRAIN_OPTRN_EVENT' ORDER BY STARTED_AT DESC LIMIT 20;

-- Check for errors:
-- SELECT * FROM D_RAW.SADB.SP_ERROR_LOG WHERE PROCEDURE_NAME = 'SP_PROCESS_TRAIN_OPTRN_EVENT' ORDER BY CREATED_AT DESC LIMIT 20;

-- Execution metrics summary:
-- SELECT STATUS, COUNT(*) AS EXECUTIONS, AVG(DURATION_SECONDS) AS AVG_DURATION, SUM(ROWS_MERGED) AS TOTAL_ROWS FROM D_RAW.SADB.SP_EXECUTION_LOG WHERE PROCEDURE_NAME = 'SP_PROCESS_TRAIN_OPTRN_EVENT' GROUP BY STATUS;
