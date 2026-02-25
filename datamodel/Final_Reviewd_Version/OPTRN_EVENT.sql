/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_RAW.SADB.OPTRN_EVENT_BASE
================================================================================
Source Table : D_RAW.SADB.OPTRN_EVENT_BASE
Target Table : D_BRONZE.SADB.OPTRN_EVENT
Stream       : D_RAW.SADB.OPTRN_EVENT_BASE_HIST_STREAM
Procedure    : D_RAW.SADB.SP_PROCESS_OPTRN_EVENT()
Task         : D_RAW.SADB.TASK_PROCESS_OPTRN_EVENT
Primary Key  : OPTRN_EVENT_ID (Single)
Total Columns: 28 source + 6 CDC metadata = 34
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE TABLE IF NOT EXISTS D_BRONZE.SADB.OPTRN_EVENT (
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

    -- CDC Metadata columns for data preservation
    CDC_OPERATION VARCHAR(10),  
    CDC_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IS_DELETED BOOLEAN DEFAULT FALSE,    -- Soft delete flag
    RECORD_CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RECORD_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_LOAD_BATCH_ID VARCHAR(100), 

    PRIMARY KEY (OPTRN_EVENT_ID)
);

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_RAW.SADB.OPTRN_EVENT_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 45,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 15;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_RAW.SADB.OPTRN_EVENT_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.OPTRN_EVENT_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for OPTRN_EVENT_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.SADB.SP_PROCESS_OPTRN_EVENT()
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
        FROM D_RAW.SADB.OPTRN_EVENT_BASE_HIST_STREAM
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
        
        CREATE OR REPLACE STREAM D_RAW.SADB.OPTRN_EVENT_BASE_HIST_STREAM
        ON TABLE D_RAW.SADB.OPTRN_EVENT_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection';
        
        MERGE INTO D_BRONZE.SADB.OPTRN_EVENT AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_RAW.SADB.OPTRN_EVENT_BASE src
            LEFT JOIN D_BRONZE.SADB.OPTRN_EVENT tgt 
                ON src.OPTRN_EVENT_ID = tgt.OPTRN_EVENT_ID
            WHERE tgt.OPTRN_EVENT_ID IS NULL
               OR tgt.IS_DELETED = TRUE
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
            SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, 
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.OPTRN_EVENT_ID, src.OPTRN_LEG_ID, src.EVENT_TMS, src.TRAIN_PLAN_LEG_ID, src.TRAIN_PLAN_EVENT_ID,
            src.TRAIN_EVENT_TYPE_CD, src.MTP_ROUTE_POINT_SQNC_NBR, src.TRAVEL_DRCTN_CD, src.SCAC_CD, src.FSAC_CD,
            src.TRSTN_VRSN_NBR, src.RGN_NM_TRK_NBR, src.REGION_NBR, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS,
            src.CREATE_USER_ID, src.UPDATE_USER_ID, src.TIME_ZONE_CD, src.TIME_ZONE_YEAR_NBR, src.EVENT_SOURCE_CD,
            src.MILE_NBR, src.AEIRD_NBR, src.AEIRD_DRCTN_CD, src.MTP_OMTS_PNDNG_IND, src.CTC_SIGNAL_ID, src.OPSNG_CTC_SIGNAL_ID,
            src.SNW_OPERATION_TYPE, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
        
        v_rows_merged := SQLROWCOUNT;
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_merged || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- CHECK 2: Stage stream data into temp table (BEST PRACTICE - single read)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_OPTRN_EVENT AS
    SELECT 
        OPTRN_EVENT_ID, OPTRN_LEG_ID, EVENT_TMS, TRAIN_PLAN_LEG_ID, TRAIN_PLAN_EVENT_ID,
        TRAIN_EVENT_TYPE_CD, MTP_ROUTE_POINT_SQNC_NBR, TRAVEL_DRCTN_CD, SCAC_CD, FSAC_CD,
        TRSTN_VRSN_NBR, RGN_NM_TRK_NBR, REGION_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
        CREATE_USER_ID, UPDATE_USER_ID, TIME_ZONE_CD, TIME_ZONE_YEAR_NBR, EVENT_SOURCE_CD,
        MILE_NBR, AEIRD_NBR, AEIRD_DRCTN_CD, MTP_OMTS_PNDNG_IND, CTC_SIGNAL_ID, OPSNG_CTC_SIGNAL_ID,
        SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_RAW.SADB.OPTRN_EVENT_BASE_HIST_STREAM;
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_OPTRN_EVENT;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_OPTRN_EVENT;
        RETURN 'NO_DATA: Stream has no changes to process at ' || CURRENT_TIMESTAMP()::VARCHAR;
    END IF;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.SADB.OPTRN_EVENT AS tgt
    USING (
        SELECT 
            OPTRN_EVENT_ID, OPTRN_LEG_ID, EVENT_TMS, TRAIN_PLAN_LEG_ID, TRAIN_PLAN_EVENT_ID,
            TRAIN_EVENT_TYPE_CD, MTP_ROUTE_POINT_SQNC_NBR, TRAVEL_DRCTN_CD, SCAC_CD, FSAC_CD,
            TRSTN_VRSN_NBR, RGN_NM_TRK_NBR, REGION_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            CREATE_USER_ID, UPDATE_USER_ID, TIME_ZONE_CD, TIME_ZONE_YEAR_NBR, EVENT_SOURCE_CD,
            MILE_NBR, AEIRD_NBR, AEIRD_DRCTN_CD, MTP_OMTS_PNDNG_IND, CTC_SIGNAL_ID, OPSNG_CTC_SIGNAL_ID,
            SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_OPTRN_EVENT
    ) AS src
    ON tgt.OPTRN_EVENT_ID = src.OPTRN_EVENT_ID
    
    -- UPDATE scenario (METADATA$ACTION='INSERT' AND METADATA$ISUPDATE=TRUE)
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
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'INSERT',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    -- NEW INSERT scenario
    WHEN NOT MATCHED AND src.CDC_ACTION = 'INSERT' THEN 
        INSERT (
            OPTRN_EVENT_ID, OPTRN_LEG_ID, EVENT_TMS, TRAIN_PLAN_LEG_ID, TRAIN_PLAN_EVENT_ID,
            TRAIN_EVENT_TYPE_CD, MTP_ROUTE_POINT_SQNC_NBR, TRAVEL_DRCTN_CD, SCAC_CD, FSAC_CD,
            TRSTN_VRSN_NBR, RGN_NM_TRK_NBR, REGION_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            CREATE_USER_ID, UPDATE_USER_ID, TIME_ZONE_CD, TIME_ZONE_YEAR_NBR, EVENT_SOURCE_CD,
            MILE_NBR, AEIRD_NBR, AEIRD_DRCTN_CD, MTP_OMTS_PNDNG_IND, CTC_SIGNAL_ID, OPSNG_CTC_SIGNAL_ID,
            SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.OPTRN_EVENT_ID, src.OPTRN_LEG_ID, src.EVENT_TMS, src.TRAIN_PLAN_LEG_ID, src.TRAIN_PLAN_EVENT_ID,
            src.TRAIN_EVENT_TYPE_CD, src.MTP_ROUTE_POINT_SQNC_NBR, src.TRAVEL_DRCTN_CD, src.SCAC_CD, src.FSAC_CD,
            src.TRSTN_VRSN_NBR, src.RGN_NM_TRK_NBR, src.REGION_NBR, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS,
            src.CREATE_USER_ID, src.UPDATE_USER_ID, src.TIME_ZONE_CD, src.TIME_ZONE_YEAR_NBR, src.EVENT_SOURCE_CD,
            src.MILE_NBR, src.AEIRD_NBR, src.AEIRD_DRCTN_CD, src.MTP_OMTS_PNDNG_IND, src.CTC_SIGNAL_ID, src.OPSNG_CTC_SIGNAL_ID,
            src.SNW_OPERATION_TYPE, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    DROP TABLE IF EXISTS _CDC_STAGING_OPTRN_EVENT;
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes. Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS _CDC_STAGING_OPTRN_EVENT;
        RETURN 'ERROR: ' || SQLERRM || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.SADB.TASK_PROCESS_OPTRN_EVENT
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process OPTRN_EVENT_BASE CDC changes into data preservation table'
WHEN
    SYSTEM$STREAM_HAS_DATA('D_RAW.SADB.OPTRN_EVENT_BASE_HIST_STREAM')
AS
    CALL D_RAW.SADB.SP_PROCESS_OPTRN_EVENT();

ALTER TASK D_RAW.SADB.TASK_PROCESS_OPTRN_EVENT RESUME;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'OPTRN_EVENT%' IN SCHEMA D_BRONZE.SADB;
-- SHOW STREAMS LIKE 'OPTRN_EVENT%' IN SCHEMA D_RAW.SADB;
-- SHOW TASKS LIKE 'TASK_PROCESS_OPTRN_EVENT%' IN SCHEMA D_RAW.SADB;
-- CALL D_RAW.SADB.SP_PROCESS_OPTRN_EVENT();
