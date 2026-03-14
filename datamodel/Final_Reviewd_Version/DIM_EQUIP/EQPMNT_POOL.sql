/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_RAW.SADB.EQPMNT_POOL_BASE
================================================================================
Source Table : D_RAW.SADB.EQPMNT_POOL_BASE
Target Table : D_BRONZE.SADB.EQPMNT_POOL
Stream       : D_RAW.SADB.EQPMNT_POOL_BASE_HIST_STREAM
Procedure    : D_RAW.SADB.SP_PROCESS_EQPMNT_POOL()
Task         : D_RAW.SADB.TASK_PROCESS_EQPMNT_POOL
Primary Key  : EQPMNT_POOL_ID (Single)
Total Columns: 29 source + 3 SNW metadata + 6 CDC metadata = 38
Filter       : SNW_OPERATION_OWNER NOT IN ('TSDPRG','EMEPRG') (exclude purged records)
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE OR ALTER TABLE D_BRONZE.SADB.EQPMNT_POOL (
    EQPMNT_POOL_ID NUMBER(18,0) NOT NULL,
    RECORD_CREATE_TMS TIMESTAMP_NTZ(0),
    CREATE_USER_ID VARCHAR(32),
    RECORD_UPDATE_TMS TIMESTAMP_NTZ(0),
    UPDATE_USER_ID VARCHAR(32),
    AAR_EQPMNT_POOL_ID VARCHAR(28),
    CPR_EQPMNT_POOL_ID VARCHAR(28),
    CMDTY_NM VARCHAR(40),
    CMNT_TXT VARCHAR(720),
    DSCRPT_TXT VARCHAR(80),
    EXTND_DSCRPT_TXT VARCHAR(320),
    EQPMNT_POOL_CD VARCHAR(4),
    EQPMNT_POOL_MNTNC_CD VARCHAR(4),
    HELD_LCTN_SCAC_CD VARCHAR(16),
    HELD_LCTN_FSAC_CD VARCHAR(20),
    HELD_LCTN_NM VARCHAR(76),
    HELD_LCTN_STPRV_CD VARCHAR(8),
    LDNG_STN_SCAC_CD VARCHAR(16),
    LDNG_STN_FSAC_CD VARCHAR(20),
    LDNG_STN_NM VARCHAR(76),
    LDNG_STN_STPRV_CD VARCHAR(8),
    LGLNT_LCTN_STN_RFRNC_ID NUMBER(18,0),
    EQPMNT_POOL_STATUS_CD VARCHAR(60),
    CREATE_PARTY_RLTNSH_USER_ID NUMBER(18,0),
    SOURCE_CREATE_TMS TIMESTAMP_NTZ(0),
    UPDATE_PARTY_RLTNSH_USER_ID NUMBER(18,0),
    SOURCE_UPDATE_TMS TIMESTAMP_NTZ(0),
    SOURCE_CREATE_USER_ID VARCHAR(32),
    SOURCE_UPDATE_USER_ID VARCHAR(32),
    SNW_OPERATION_TYPE VARCHAR(1),
    SNW_LAST_REPLICATED TIMESTAMP_NTZ(9),

    CDC_OPERATION VARCHAR(10),
    CDC_TIMESTAMP TIMESTAMP_NTZ,
    IS_DELETED BOOLEAN,
    RECORD_CREATED_AT TIMESTAMP_NTZ,
    RECORD_UPDATED_AT TIMESTAMP_NTZ,
    SOURCE_LOAD_BATCH_ID VARCHAR(100),
    SNW_OPERATION_OWNER VARCHAR(256),

    PRIMARY KEY (EQPMNT_POOL_ID)
);

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_RAW.SADB.EQPMNT_POOL_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 45,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 15;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_RAW.SADB.EQPMNT_POOL_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.EQPMNT_POOL_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for EQPMNT_POOL_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.SADB.SP_PROCESS_EQPMNT_POOL()
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
        FROM D_RAW.SADB.EQPMNT_POOL_BASE_HIST_STREAM
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
        
        CREATE OR REPLACE STREAM D_RAW.SADB.EQPMNT_POOL_BASE_HIST_STREAM
        ON TABLE D_RAW.SADB.EQPMNT_POOL_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection';
        
        MERGE INTO D_BRONZE.SADB.EQPMNT_POOL AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_RAW.SADB.EQPMNT_POOL_BASE_HIST_STREAM src
            WHERE NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')
        ) AS src
        ON tgt.EQPMNT_POOL_ID = src.EQPMNT_POOL_ID
        WHEN MATCHED THEN UPDATE SET
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.AAR_EQPMNT_POOL_ID = src.AAR_EQPMNT_POOL_ID,
            tgt.CPR_EQPMNT_POOL_ID = src.CPR_EQPMNT_POOL_ID,
            tgt.CMDTY_NM = src.CMDTY_NM,
            tgt.CMNT_TXT = src.CMNT_TXT,
            tgt.DSCRPT_TXT = src.DSCRPT_TXT,
            tgt.EXTND_DSCRPT_TXT = src.EXTND_DSCRPT_TXT,
            tgt.EQPMNT_POOL_CD = src.EQPMNT_POOL_CD,
            tgt.EQPMNT_POOL_MNTNC_CD = src.EQPMNT_POOL_MNTNC_CD,
            tgt.HELD_LCTN_SCAC_CD = src.HELD_LCTN_SCAC_CD,
            tgt.HELD_LCTN_FSAC_CD = src.HELD_LCTN_FSAC_CD,
            tgt.HELD_LCTN_NM = src.HELD_LCTN_NM,
            tgt.HELD_LCTN_STPRV_CD = src.HELD_LCTN_STPRV_CD,
            tgt.LDNG_STN_SCAC_CD = src.LDNG_STN_SCAC_CD,
            tgt.LDNG_STN_FSAC_CD = src.LDNG_STN_FSAC_CD,
            tgt.LDNG_STN_NM = src.LDNG_STN_NM,
            tgt.LDNG_STN_STPRV_CD = src.LDNG_STN_STPRV_CD,
            tgt.LGLNT_LCTN_STN_RFRNC_ID = src.LGLNT_LCTN_STN_RFRNC_ID,
            tgt.EQPMNT_POOL_STATUS_CD = src.EQPMNT_POOL_STATUS_CD,
            tgt.CREATE_PARTY_RLTNSH_USER_ID = src.CREATE_PARTY_RLTNSH_USER_ID,
            tgt.SOURCE_CREATE_TMS = src.SOURCE_CREATE_TMS,
            tgt.UPDATE_PARTY_RLTNSH_USER_ID = src.UPDATE_PARTY_RLTNSH_USER_ID,
            tgt.SOURCE_UPDATE_TMS = src.SOURCE_UPDATE_TMS,
            tgt.SOURCE_CREATE_USER_ID = src.SOURCE_CREATE_USER_ID,
            tgt.SOURCE_UPDATE_USER_ID = src.SOURCE_UPDATE_USER_ID,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_OPERATION_OWNER = src.SNW_OPERATION_OWNER,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'RELOADED',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        WHEN NOT MATCHED THEN INSERT (
            EQPMNT_POOL_ID, RECORD_CREATE_TMS, CREATE_USER_ID, RECORD_UPDATE_TMS, UPDATE_USER_ID,
            AAR_EQPMNT_POOL_ID, CPR_EQPMNT_POOL_ID, CMDTY_NM, CMNT_TXT, DSCRPT_TXT, EXTND_DSCRPT_TXT,
            EQPMNT_POOL_CD, EQPMNT_POOL_MNTNC_CD,
            HELD_LCTN_SCAC_CD, HELD_LCTN_FSAC_CD, HELD_LCTN_NM, HELD_LCTN_STPRV_CD,
            LDNG_STN_SCAC_CD, LDNG_STN_FSAC_CD, LDNG_STN_NM, LDNG_STN_STPRV_CD,
            LGLNT_LCTN_STN_RFRNC_ID, EQPMNT_POOL_STATUS_CD,
            CREATE_PARTY_RLTNSH_USER_ID, SOURCE_CREATE_TMS, UPDATE_PARTY_RLTNSH_USER_ID, SOURCE_UPDATE_TMS,
            SOURCE_CREATE_USER_ID, SOURCE_UPDATE_USER_ID,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.EQPMNT_POOL_ID, src.RECORD_CREATE_TMS, src.CREATE_USER_ID, src.RECORD_UPDATE_TMS, src.UPDATE_USER_ID,
            src.AAR_EQPMNT_POOL_ID, src.CPR_EQPMNT_POOL_ID, src.CMDTY_NM, src.CMNT_TXT, src.DSCRPT_TXT, src.EXTND_DSCRPT_TXT,
            src.EQPMNT_POOL_CD, src.EQPMNT_POOL_MNTNC_CD,
            src.HELD_LCTN_SCAC_CD, src.HELD_LCTN_FSAC_CD, src.HELD_LCTN_NM, src.HELD_LCTN_STPRV_CD,
            src.LDNG_STN_SCAC_CD, src.LDNG_STN_FSAC_CD, src.LDNG_STN_NM, src.LDNG_STN_STPRV_CD,
            src.LGLNT_LCTN_STN_RFRNC_ID, src.EQPMNT_POOL_STATUS_CD,
            src.CREATE_PARTY_RLTNSH_USER_ID, src.SOURCE_CREATE_TMS, src.UPDATE_PARTY_RLTNSH_USER_ID, src.SOURCE_UPDATE_TMS,
            src.SOURCE_CREATE_USER_ID, src.SOURCE_UPDATE_USER_ID,
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
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_EQPMNT_POOL AS
    SELECT 
        EQPMNT_POOL_ID, RECORD_CREATE_TMS, CREATE_USER_ID, RECORD_UPDATE_TMS, UPDATE_USER_ID,
        AAR_EQPMNT_POOL_ID, CPR_EQPMNT_POOL_ID, CMDTY_NM, CMNT_TXT, DSCRPT_TXT, EXTND_DSCRPT_TXT,
        EQPMNT_POOL_CD, EQPMNT_POOL_MNTNC_CD,
        HELD_LCTN_SCAC_CD, HELD_LCTN_FSAC_CD, HELD_LCTN_NM, HELD_LCTN_STPRV_CD,
        LDNG_STN_SCAC_CD, LDNG_STN_FSAC_CD, LDNG_STN_NM, LDNG_STN_STPRV_CD,
        LGLNT_LCTN_STN_RFRNC_ID, EQPMNT_POOL_STATUS_CD,
        CREATE_PARTY_RLTNSH_USER_ID, SOURCE_CREATE_TMS, UPDATE_PARTY_RLTNSH_USER_ID, SOURCE_UPDATE_TMS,
        SOURCE_CREATE_USER_ID, SOURCE_UPDATE_USER_ID,
        SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_RAW.SADB.EQPMNT_POOL_BASE_HIST_STREAM
    WHERE NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG');
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_EQPMNT_POOL;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_EQPMNT_POOL;
        RETURN 'NO_DATA: Stream has no changes to process at ' || CURRENT_TIMESTAMP()::VARCHAR;
    END IF;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.SADB.EQPMNT_POOL AS tgt
    USING (
        SELECT 
            EQPMNT_POOL_ID, RECORD_CREATE_TMS, CREATE_USER_ID, RECORD_UPDATE_TMS, UPDATE_USER_ID,
            AAR_EQPMNT_POOL_ID, CPR_EQPMNT_POOL_ID, CMDTY_NM, CMNT_TXT, DSCRPT_TXT, EXTND_DSCRPT_TXT,
            EQPMNT_POOL_CD, EQPMNT_POOL_MNTNC_CD,
            HELD_LCTN_SCAC_CD, HELD_LCTN_FSAC_CD, HELD_LCTN_NM, HELD_LCTN_STPRV_CD,
            LDNG_STN_SCAC_CD, LDNG_STN_FSAC_CD, LDNG_STN_NM, LDNG_STN_STPRV_CD,
            LGLNT_LCTN_STN_RFRNC_ID, EQPMNT_POOL_STATUS_CD,
            CREATE_PARTY_RLTNSH_USER_ID, SOURCE_CREATE_TMS, UPDATE_PARTY_RLTNSH_USER_ID, SOURCE_UPDATE_TMS,
            SOURCE_CREATE_USER_ID, SOURCE_UPDATE_USER_ID,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_EQPMNT_POOL
    ) AS src
    ON tgt.EQPMNT_POOL_ID = src.EQPMNT_POOL_ID
    
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
        UPDATE SET
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.AAR_EQPMNT_POOL_ID = src.AAR_EQPMNT_POOL_ID,
            tgt.CPR_EQPMNT_POOL_ID = src.CPR_EQPMNT_POOL_ID,
            tgt.CMDTY_NM = src.CMDTY_NM,
            tgt.CMNT_TXT = src.CMNT_TXT,
            tgt.DSCRPT_TXT = src.DSCRPT_TXT,
            tgt.EXTND_DSCRPT_TXT = src.EXTND_DSCRPT_TXT,
            tgt.EQPMNT_POOL_CD = src.EQPMNT_POOL_CD,
            tgt.EQPMNT_POOL_MNTNC_CD = src.EQPMNT_POOL_MNTNC_CD,
            tgt.HELD_LCTN_SCAC_CD = src.HELD_LCTN_SCAC_CD,
            tgt.HELD_LCTN_FSAC_CD = src.HELD_LCTN_FSAC_CD,
            tgt.HELD_LCTN_NM = src.HELD_LCTN_NM,
            tgt.HELD_LCTN_STPRV_CD = src.HELD_LCTN_STPRV_CD,
            tgt.LDNG_STN_SCAC_CD = src.LDNG_STN_SCAC_CD,
            tgt.LDNG_STN_FSAC_CD = src.LDNG_STN_FSAC_CD,
            tgt.LDNG_STN_NM = src.LDNG_STN_NM,
            tgt.LDNG_STN_STPRV_CD = src.LDNG_STN_STPRV_CD,
            tgt.LGLNT_LCTN_STN_RFRNC_ID = src.LGLNT_LCTN_STN_RFRNC_ID,
            tgt.EQPMNT_POOL_STATUS_CD = src.EQPMNT_POOL_STATUS_CD,
            tgt.CREATE_PARTY_RLTNSH_USER_ID = src.CREATE_PARTY_RLTNSH_USER_ID,
            tgt.SOURCE_CREATE_TMS = src.SOURCE_CREATE_TMS,
            tgt.UPDATE_PARTY_RLTNSH_USER_ID = src.UPDATE_PARTY_RLTNSH_USER_ID,
            tgt.SOURCE_UPDATE_TMS = src.SOURCE_UPDATE_TMS,
            tgt.SOURCE_CREATE_USER_ID = src.SOURCE_CREATE_USER_ID,
            tgt.SOURCE_UPDATE_USER_ID = src.SOURCE_UPDATE_USER_ID,
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
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.AAR_EQPMNT_POOL_ID = src.AAR_EQPMNT_POOL_ID,
            tgt.CPR_EQPMNT_POOL_ID = src.CPR_EQPMNT_POOL_ID,
            tgt.CMDTY_NM = src.CMDTY_NM,
            tgt.CMNT_TXT = src.CMNT_TXT,
            tgt.DSCRPT_TXT = src.DSCRPT_TXT,
            tgt.EXTND_DSCRPT_TXT = src.EXTND_DSCRPT_TXT,
            tgt.EQPMNT_POOL_CD = src.EQPMNT_POOL_CD,
            tgt.EQPMNT_POOL_MNTNC_CD = src.EQPMNT_POOL_MNTNC_CD,
            tgt.HELD_LCTN_SCAC_CD = src.HELD_LCTN_SCAC_CD,
            tgt.HELD_LCTN_FSAC_CD = src.HELD_LCTN_FSAC_CD,
            tgt.HELD_LCTN_NM = src.HELD_LCTN_NM,
            tgt.HELD_LCTN_STPRV_CD = src.HELD_LCTN_STPRV_CD,
            tgt.LDNG_STN_SCAC_CD = src.LDNG_STN_SCAC_CD,
            tgt.LDNG_STN_FSAC_CD = src.LDNG_STN_FSAC_CD,
            tgt.LDNG_STN_NM = src.LDNG_STN_NM,
            tgt.LDNG_STN_STPRV_CD = src.LDNG_STN_STPRV_CD,
            tgt.LGLNT_LCTN_STN_RFRNC_ID = src.LGLNT_LCTN_STN_RFRNC_ID,
            tgt.EQPMNT_POOL_STATUS_CD = src.EQPMNT_POOL_STATUS_CD,
            tgt.CREATE_PARTY_RLTNSH_USER_ID = src.CREATE_PARTY_RLTNSH_USER_ID,
            tgt.SOURCE_CREATE_TMS = src.SOURCE_CREATE_TMS,
            tgt.UPDATE_PARTY_RLTNSH_USER_ID = src.UPDATE_PARTY_RLTNSH_USER_ID,
            tgt.SOURCE_UPDATE_TMS = src.SOURCE_UPDATE_TMS,
            tgt.SOURCE_CREATE_USER_ID = src.SOURCE_CREATE_USER_ID,
            tgt.SOURCE_UPDATE_USER_ID = src.SOURCE_UPDATE_USER_ID,
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
            EQPMNT_POOL_ID, RECORD_CREATE_TMS, CREATE_USER_ID, RECORD_UPDATE_TMS, UPDATE_USER_ID,
            AAR_EQPMNT_POOL_ID, CPR_EQPMNT_POOL_ID, CMDTY_NM, CMNT_TXT, DSCRPT_TXT, EXTND_DSCRPT_TXT,
            EQPMNT_POOL_CD, EQPMNT_POOL_MNTNC_CD,
            HELD_LCTN_SCAC_CD, HELD_LCTN_FSAC_CD, HELD_LCTN_NM, HELD_LCTN_STPRV_CD,
            LDNG_STN_SCAC_CD, LDNG_STN_FSAC_CD, LDNG_STN_NM, LDNG_STN_STPRV_CD,
            LGLNT_LCTN_STN_RFRNC_ID, EQPMNT_POOL_STATUS_CD,
            CREATE_PARTY_RLTNSH_USER_ID, SOURCE_CREATE_TMS, UPDATE_PARTY_RLTNSH_USER_ID, SOURCE_UPDATE_TMS,
            SOURCE_CREATE_USER_ID, SOURCE_UPDATE_USER_ID,
            SNW_OPERATION_TYPE, SNW_OPERATION_OWNER, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.EQPMNT_POOL_ID, src.RECORD_CREATE_TMS, src.CREATE_USER_ID, src.RECORD_UPDATE_TMS, src.UPDATE_USER_ID,
            src.AAR_EQPMNT_POOL_ID, src.CPR_EQPMNT_POOL_ID, src.CMDTY_NM, src.CMNT_TXT, src.DSCRPT_TXT, src.EXTND_DSCRPT_TXT,
            src.EQPMNT_POOL_CD, src.EQPMNT_POOL_MNTNC_CD,
            src.HELD_LCTN_SCAC_CD, src.HELD_LCTN_FSAC_CD, src.HELD_LCTN_NM, src.HELD_LCTN_STPRV_CD,
            src.LDNG_STN_SCAC_CD, src.LDNG_STN_FSAC_CD, src.LDNG_STN_NM, src.LDNG_STN_STPRV_CD,
            src.LGLNT_LCTN_STN_RFRNC_ID, src.EQPMNT_POOL_STATUS_CD,
            src.CREATE_PARTY_RLTNSH_USER_ID, src.SOURCE_CREATE_TMS, src.UPDATE_PARTY_RLTNSH_USER_ID, src.SOURCE_UPDATE_TMS,
            src.SOURCE_CREATE_USER_ID, src.SOURCE_UPDATE_USER_ID,
            src.SNW_OPERATION_TYPE, src.SNW_OPERATION_OWNER, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    DROP TABLE IF EXISTS _CDC_STAGING_EQPMNT_POOL;
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes. Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS _CDC_STAGING_EQPMNT_POOL;
        RETURN 'ERROR: ' || SQLERRM || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.SADB.TASK_PROCESS_EQPMNT_POOL
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process EQPMNT_POOL_BASE CDC changes into data preservation table'
AS
    CALL D_RAW.SADB.SP_PROCESS_EQPMNT_POOL();

ALTER TASK D_RAW.SADB.TASK_PROCESS_EQPMNT_POOL RESUME;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'EQPMNT_POOL%' IN SCHEMA D_BRONZE.SADB;
-- SHOW STREAMS LIKE 'EQPMNT_POOL_BASE_HIST_STREAM%' IN SCHEMA D_RAW.SADB;
-- SHOW TASKS LIKE 'TASK_PROCESS_EQPMNT_POOL%' IN SCHEMA D_RAW.SADB;
-- CALL D_RAW.SADB.SP_PROCESS_EQPMNT_POOL();
