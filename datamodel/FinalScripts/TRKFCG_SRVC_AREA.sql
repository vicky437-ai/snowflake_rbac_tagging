/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_RAW.SADB.TRKFCG_SRVC_AREA_BASE
================================================================================
Source Table : D_RAW.SADB.TRKFCG_SRVC_AREA_BASE
Target Table : D_BRONZE.SADB.TRKFCG_SRVC_AREA
Stream       : D_RAW.SADB.TRKFCG_SRVC_AREA_BASE_HIST_STREAM
Procedure    : D_RAW.SADB.SP_PROCESS_TRKFCG_SRVC_AREA()
Task         : D_RAW.SADB.TASK_PROCESS_TRKFCG_SRVC_AREA
Primary Key  : GRPHC_OBJECT_VRSN_ID (Single)
Total Columns: 25 source + 6 CDC metadata = 31
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE TABLE IF NOT EXISTS D_BRONZE.SADB.TRKFCG_SRVC_AREA (
    GRPHC_OBJECT_VRSN_ID NUMBER(18,0) NOT NULL,
    VRSN_CREATE_TMS TIMESTAMP_NTZ(0),
    VRSN_USER_ID VARCHAR(32),
    FIRST_GRPHC_OBJECT_VRSN_ID NUMBER(18,0),
    PRVS_GRPHC_OBJECT_VRSN_ID NUMBER(18,0),
    GRPHC_OBJECT_MDFCTN_CD VARCHAR(36),
    GRPHC_OBJECT_STATUS_CD VARCHAR(32),
    GRPHC_TRNSCT_ID NUMBER(18,0),
    SRVC_AREA_ID NUMBER(18,0),
    RECORD_CREATE_TMS TIMESTAMP_NTZ(0),
    RECORD_UPDATE_TMS TIMESTAMP_NTZ(0),
    CREATE_USER_ID VARCHAR(32),
    UPDATE_USER_ID VARCHAR(32),
    SRVC_AREA_CD VARCHAR(8),
    EFCTV_TMS TIMESTAMP_NTZ(0),
    LONG_ENGLSH_NM VARCHAR(320),
    SHORT_ENGLSH_NM VARCHAR(40),
    EXPIRY_TMS TIMESTAMP_NTZ(0),
    GRPHC_BSNS_AREA_NM VARCHAR(32),
    LONG_FRENCH_NM VARCHAR(320),
    SHORT_FRENCH_NM VARCHAR(40),
    SRVC_AREA_NOTE_TXT VARCHAR(320),
    SRVC_AREA_ST VARCHAR(160),
    SNW_OPERATION_TYPE VARCHAR(1),
    SNW_LAST_REPLICATED TIMESTAMP_NTZ(9),

    CDC_OPERATION VARCHAR(10),  
    CDC_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IS_DELETED BOOLEAN DEFAULT FALSE,
    RECORD_CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RECORD_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_LOAD_BATCH_ID VARCHAR(100), 

    PRIMARY KEY (GRPHC_OBJECT_VRSN_ID)
);

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_RAW.SADB.TRKFCG_SRVC_AREA_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 45,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 15;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_RAW.SADB.TRKFCG_SRVC_AREA_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.TRKFCG_SRVC_AREA_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for TRKFCG_SRVC_AREA_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.SADB.SP_PROCESS_TRKFCG_SRVC_AREA()
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
        FROM D_RAW.SADB.TRKFCG_SRVC_AREA_BASE_HIST_STREAM
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
        
        CREATE OR REPLACE STREAM D_RAW.SADB.TRKFCG_SRVC_AREA_BASE_HIST_STREAM
        ON TABLE D_RAW.SADB.TRKFCG_SRVC_AREA_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection';
        
        MERGE INTO D_BRONZE.SADB.TRKFCG_SRVC_AREA AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_RAW.SADB.TRKFCG_SRVC_AREA_BASE src
            LEFT JOIN D_BRONZE.SADB.TRKFCG_SRVC_AREA tgt 
                ON src.GRPHC_OBJECT_VRSN_ID = tgt.GRPHC_OBJECT_VRSN_ID
            WHERE tgt.GRPHC_OBJECT_VRSN_ID IS NULL
               OR tgt.IS_DELETED = TRUE
        ) AS src
        ON tgt.GRPHC_OBJECT_VRSN_ID = src.GRPHC_OBJECT_VRSN_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.VRSN_CREATE_TMS = src.VRSN_CREATE_TMS,
            tgt.VRSN_USER_ID = src.VRSN_USER_ID,
            tgt.FIRST_GRPHC_OBJECT_VRSN_ID = src.FIRST_GRPHC_OBJECT_VRSN_ID,
            tgt.PRVS_GRPHC_OBJECT_VRSN_ID = src.PRVS_GRPHC_OBJECT_VRSN_ID,
            tgt.GRPHC_OBJECT_MDFCTN_CD = src.GRPHC_OBJECT_MDFCTN_CD,
            tgt.GRPHC_OBJECT_STATUS_CD = src.GRPHC_OBJECT_STATUS_CD,
            tgt.GRPHC_TRNSCT_ID = src.GRPHC_TRNSCT_ID,
            tgt.SRVC_AREA_ID = src.SRVC_AREA_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.SRVC_AREA_CD = src.SRVC_AREA_CD,
            tgt.EFCTV_TMS = src.EFCTV_TMS,
            tgt.LONG_ENGLSH_NM = src.LONG_ENGLSH_NM,
            tgt.SHORT_ENGLSH_NM = src.SHORT_ENGLSH_NM,
            tgt.EXPIRY_TMS = src.EXPIRY_TMS,
            tgt.GRPHC_BSNS_AREA_NM = src.GRPHC_BSNS_AREA_NM,
            tgt.LONG_FRENCH_NM = src.LONG_FRENCH_NM,
            tgt.SHORT_FRENCH_NM = src.SHORT_FRENCH_NM,
            tgt.SRVC_AREA_NOTE_TXT = src.SRVC_AREA_NOTE_TXT,
            tgt.SRVC_AREA_ST = src.SRVC_AREA_ST,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'RELOADED',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        WHEN NOT MATCHED THEN INSERT (
            GRPHC_OBJECT_VRSN_ID, VRSN_CREATE_TMS, VRSN_USER_ID, FIRST_GRPHC_OBJECT_VRSN_ID, PRVS_GRPHC_OBJECT_VRSN_ID,
            GRPHC_OBJECT_MDFCTN_CD, GRPHC_OBJECT_STATUS_CD, GRPHC_TRNSCT_ID, SRVC_AREA_ID, RECORD_CREATE_TMS,
            RECORD_UPDATE_TMS, CREATE_USER_ID, UPDATE_USER_ID, SRVC_AREA_CD, EFCTV_TMS,
            LONG_ENGLSH_NM, SHORT_ENGLSH_NM, EXPIRY_TMS, GRPHC_BSNS_AREA_NM, LONG_FRENCH_NM,
            SHORT_FRENCH_NM, SRVC_AREA_NOTE_TXT, SRVC_AREA_ST, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.GRPHC_OBJECT_VRSN_ID, src.VRSN_CREATE_TMS, src.VRSN_USER_ID, src.FIRST_GRPHC_OBJECT_VRSN_ID, src.PRVS_GRPHC_OBJECT_VRSN_ID,
            src.GRPHC_OBJECT_MDFCTN_CD, src.GRPHC_OBJECT_STATUS_CD, src.GRPHC_TRNSCT_ID, src.SRVC_AREA_ID, src.RECORD_CREATE_TMS,
            src.RECORD_UPDATE_TMS, src.CREATE_USER_ID, src.UPDATE_USER_ID, src.SRVC_AREA_CD, src.EFCTV_TMS,
            src.LONG_ENGLSH_NM, src.SHORT_ENGLSH_NM, src.EXPIRY_TMS, src.GRPHC_BSNS_AREA_NM, src.LONG_FRENCH_NM,
            src.SHORT_FRENCH_NM, src.SRVC_AREA_NOTE_TXT, src.SRVC_AREA_ST, src.SNW_OPERATION_TYPE, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
        
        v_rows_merged := SQLROWCOUNT;
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_merged || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- CHECK 2: Stage stream data into temp table (BEST PRACTICE - single read)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_TRKFCG_SRVC_AREA AS
    SELECT 
        GRPHC_OBJECT_VRSN_ID, VRSN_CREATE_TMS, VRSN_USER_ID, FIRST_GRPHC_OBJECT_VRSN_ID, PRVS_GRPHC_OBJECT_VRSN_ID,
        GRPHC_OBJECT_MDFCTN_CD, GRPHC_OBJECT_STATUS_CD, GRPHC_TRNSCT_ID, SRVC_AREA_ID, RECORD_CREATE_TMS,
        RECORD_UPDATE_TMS, CREATE_USER_ID, UPDATE_USER_ID, SRVC_AREA_CD, EFCTV_TMS,
        LONG_ENGLSH_NM, SHORT_ENGLSH_NM, EXPIRY_TMS, GRPHC_BSNS_AREA_NM, LONG_FRENCH_NM,
        SHORT_FRENCH_NM, SRVC_AREA_NOTE_TXT, SRVC_AREA_ST, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_RAW.SADB.TRKFCG_SRVC_AREA_BASE_HIST_STREAM;
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_TRKFCG_SRVC_AREA;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_TRKFCG_SRVC_AREA;
        RETURN 'NO_DATA: Stream has no changes to process at ' || CURRENT_TIMESTAMP()::VARCHAR;
    END IF;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.SADB.TRKFCG_SRVC_AREA AS tgt
    USING (
        SELECT 
            GRPHC_OBJECT_VRSN_ID, VRSN_CREATE_TMS, VRSN_USER_ID, FIRST_GRPHC_OBJECT_VRSN_ID, PRVS_GRPHC_OBJECT_VRSN_ID,
            GRPHC_OBJECT_MDFCTN_CD, GRPHC_OBJECT_STATUS_CD, GRPHC_TRNSCT_ID, SRVC_AREA_ID, RECORD_CREATE_TMS,
            RECORD_UPDATE_TMS, CREATE_USER_ID, UPDATE_USER_ID, SRVC_AREA_CD, EFCTV_TMS,
            LONG_ENGLSH_NM, SHORT_ENGLSH_NM, EXPIRY_TMS, GRPHC_BSNS_AREA_NM, LONG_FRENCH_NM,
            SHORT_FRENCH_NM, SRVC_AREA_NOTE_TXT, SRVC_AREA_ST, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_TRKFCG_SRVC_AREA
    ) AS src
    ON tgt.GRPHC_OBJECT_VRSN_ID = src.GRPHC_OBJECT_VRSN_ID
    
    -- UPDATE scenario (METADATA$ACTION='INSERT' AND METADATA$ISUPDATE=TRUE)
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
        UPDATE SET
            tgt.VRSN_CREATE_TMS = src.VRSN_CREATE_TMS,
            tgt.VRSN_USER_ID = src.VRSN_USER_ID,
            tgt.FIRST_GRPHC_OBJECT_VRSN_ID = src.FIRST_GRPHC_OBJECT_VRSN_ID,
            tgt.PRVS_GRPHC_OBJECT_VRSN_ID = src.PRVS_GRPHC_OBJECT_VRSN_ID,
            tgt.GRPHC_OBJECT_MDFCTN_CD = src.GRPHC_OBJECT_MDFCTN_CD,
            tgt.GRPHC_OBJECT_STATUS_CD = src.GRPHC_OBJECT_STATUS_CD,
            tgt.GRPHC_TRNSCT_ID = src.GRPHC_TRNSCT_ID,
            tgt.SRVC_AREA_ID = src.SRVC_AREA_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.SRVC_AREA_CD = src.SRVC_AREA_CD,
            tgt.EFCTV_TMS = src.EFCTV_TMS,
            tgt.LONG_ENGLSH_NM = src.LONG_ENGLSH_NM,
            tgt.SHORT_ENGLSH_NM = src.SHORT_ENGLSH_NM,
            tgt.EXPIRY_TMS = src.EXPIRY_TMS,
            tgt.GRPHC_BSNS_AREA_NM = src.GRPHC_BSNS_AREA_NM,
            tgt.LONG_FRENCH_NM = src.LONG_FRENCH_NM,
            tgt.SHORT_FRENCH_NM = src.SHORT_FRENCH_NM,
            tgt.SRVC_AREA_NOTE_TXT = src.SRVC_AREA_NOTE_TXT,
            tgt.SRVC_AREA_ST = src.SRVC_AREA_ST,
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
            tgt.VRSN_CREATE_TMS = src.VRSN_CREATE_TMS,
            tgt.VRSN_USER_ID = src.VRSN_USER_ID,
            tgt.FIRST_GRPHC_OBJECT_VRSN_ID = src.FIRST_GRPHC_OBJECT_VRSN_ID,
            tgt.PRVS_GRPHC_OBJECT_VRSN_ID = src.PRVS_GRPHC_OBJECT_VRSN_ID,
            tgt.GRPHC_OBJECT_MDFCTN_CD = src.GRPHC_OBJECT_MDFCTN_CD,
            tgt.GRPHC_OBJECT_STATUS_CD = src.GRPHC_OBJECT_STATUS_CD,
            tgt.GRPHC_TRNSCT_ID = src.GRPHC_TRNSCT_ID,
            tgt.SRVC_AREA_ID = src.SRVC_AREA_ID,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.SRVC_AREA_CD = src.SRVC_AREA_CD,
            tgt.EFCTV_TMS = src.EFCTV_TMS,
            tgt.LONG_ENGLSH_NM = src.LONG_ENGLSH_NM,
            tgt.SHORT_ENGLSH_NM = src.SHORT_ENGLSH_NM,
            tgt.EXPIRY_TMS = src.EXPIRY_TMS,
            tgt.GRPHC_BSNS_AREA_NM = src.GRPHC_BSNS_AREA_NM,
            tgt.LONG_FRENCH_NM = src.LONG_FRENCH_NM,
            tgt.SHORT_FRENCH_NM = src.SHORT_FRENCH_NM,
            tgt.SRVC_AREA_NOTE_TXT = src.SRVC_AREA_NOTE_TXT,
            tgt.SRVC_AREA_ST = src.SRVC_AREA_ST,
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
            GRPHC_OBJECT_VRSN_ID, VRSN_CREATE_TMS, VRSN_USER_ID, FIRST_GRPHC_OBJECT_VRSN_ID, PRVS_GRPHC_OBJECT_VRSN_ID,
            GRPHC_OBJECT_MDFCTN_CD, GRPHC_OBJECT_STATUS_CD, GRPHC_TRNSCT_ID, SRVC_AREA_ID, RECORD_CREATE_TMS,
            RECORD_UPDATE_TMS, CREATE_USER_ID, UPDATE_USER_ID, SRVC_AREA_CD, EFCTV_TMS,
            LONG_ENGLSH_NM, SHORT_ENGLSH_NM, EXPIRY_TMS, GRPHC_BSNS_AREA_NM, LONG_FRENCH_NM,
            SHORT_FRENCH_NM, SRVC_AREA_NOTE_TXT, SRVC_AREA_ST, SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.GRPHC_OBJECT_VRSN_ID, src.VRSN_CREATE_TMS, src.VRSN_USER_ID, src.FIRST_GRPHC_OBJECT_VRSN_ID, src.PRVS_GRPHC_OBJECT_VRSN_ID,
            src.GRPHC_OBJECT_MDFCTN_CD, src.GRPHC_OBJECT_STATUS_CD, src.GRPHC_TRNSCT_ID, src.SRVC_AREA_ID, src.RECORD_CREATE_TMS,
            src.RECORD_UPDATE_TMS, src.CREATE_USER_ID, src.UPDATE_USER_ID, src.SRVC_AREA_CD, src.EFCTV_TMS,
            src.LONG_ENGLSH_NM, src.SHORT_ENGLSH_NM, src.EXPIRY_TMS, src.GRPHC_BSNS_AREA_NM, src.LONG_FRENCH_NM,
            src.SHORT_FRENCH_NM, src.SRVC_AREA_NOTE_TXT, src.SRVC_AREA_ST, src.SNW_OPERATION_TYPE, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    DROP TABLE IF EXISTS _CDC_STAGING_TRKFCG_SRVC_AREA;
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes. Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS _CDC_STAGING_TRKFCG_SRVC_AREA;
        RETURN 'ERROR: ' || SQLERRM || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.SADB.TASK_PROCESS_TRKFCG_SRVC_AREA
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process TRKFCG_SRVC_AREA_BASE CDC changes into data preservation table'
WHEN
    SYSTEM$STREAM_HAS_DATA('D_RAW.SADB.TRKFCG_SRVC_AREA_BASE_HIST_STREAM')
AS
    CALL D_RAW.SADB.SP_PROCESS_TRKFCG_SRVC_AREA();

ALTER TASK D_RAW.SADB.TASK_PROCESS_TRKFCG_SRVC_AREA RESUME;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'TRKFCG_SRVC_AREA%' IN SCHEMA D_BRONZE.SADB;
-- SHOW STREAMS LIKE 'TRKFCG_SRVC_AREA%' IN SCHEMA D_RAW.SADB;
-- SHOW TASKS LIKE 'TASK_PROCESS_TRKFCG_SRVC_AREA%' IN SCHEMA D_RAW.SADB;
-- CALL D_RAW.SADB.SP_PROCESS_TRKFCG_SRVC_AREA();
