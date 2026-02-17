/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_BRONZE.SADB.TRKFC_TRSTN_BASE
================================================================================
Source Table : D_BRONZE.SADB.TRKFC_TRSTN_BASE
Target Table : D_BRONZE.SADB.TRKFC_TRSTN_V1
Stream       : D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM
Procedure    : D_BRONZE.SADB.SP_PROCESS_TRKFC_TRSTN_CDC()
Task         : D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC
Primary Key  : COMPOSITE (SCAC_CD, FSAC_CD)
Total Columns: 40 source + 6 CDC metadata = 46
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Target Data Preservation Table
-- =============================================================================
CREATE TABLE IF NOT EXISTS D_BRONZE.SADB.TRKFC_TRSTN_V1 (
    SCAC_CD VARCHAR(16) NOT NULL,
    FSAC_CD VARCHAR(20) NOT NULL,
    VRSN_NBR NUMBER(5,0),
    RECORD_CREATE_TMS TIMESTAMP_NTZ(0),
    RECORD_UPDATE_TMS TIMESTAMP_NTZ(0),
    CREATE_USER_ID VARCHAR(32),
    UPDATE_USER_ID VARCHAR(32),
    EFCTV_DT TIMESTAMP_NTZ(0),
    DYLGHT_SVNGS_TIME_IND VARCHAR(4),
    IRF_CREATE_DT TIMESTAMP_NTZ(0),
    IRF_UPDATE_DT TIMESTAMP_NTZ(0),
    CNTRY_CD VARCHAR(8),
    AAR_LAST_MNTND_DT TIMESTAMP_NTZ(0),
    ALTD_QTY NUMBER(8,3),
    BEA_CD VARCHAR(12),
    CNTY_ID VARCHAR(24),
    DELETE_REASON_CD VARCHAR(4),
    EXPIRY_DT TIMESTAMP_NTZ(0),
    GPLTCL_NM VARCHAR(120),
    GPLTCL_SPLC_CD VARCHAR(24),
    GPLTCL_SPLC_SUFFIX_CD VARCHAR(12),
    LNGTD_NBR NUMBER(9,6),
    LTD_NBR NUMBER(9,6),
    MSA_CD VARCHAR(16),
    POSTAL_ZIP_CD VARCHAR(36),
    RATE_POSTAL_ZIP_CD VARCHAR(36),
    RATE_ZIP_EFCTV_DT TIMESTAMP_NTZ(0),
    RELOAD_ABRVTN_TXT VARCHAR(20),
    SPLC_CD VARCHAR(24),
    SPLC_SUFFIX_CD VARCHAR(12),
    STN_STATUS_CD VARCHAR(4),
    STPRV_CD VARCHAR(8),
    TIME_ZONE_CD VARCHAR(8),
    TRNSCN_NBR VARCHAR(36),
    TRSTN_NM VARCHAR(120),
    TRSTN_SEARCH_NM VARCHAR(120),
    PARTY_FCLTY_VRSN_ID NUMBER(18,0),
    PARTY_FCLTY_ID NUMBER(18,0),
    SNW_OPERATION_TYPE VARCHAR(1),
    SNW_LAST_REPLICATED TIMESTAMP_NTZ(9),

    -- CDC Metadata columns for data preservation
    CDC_OPERATION VARCHAR(10),  
    CDC_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IS_DELETED BOOLEAN DEFAULT FALSE,    -- Soft delete flag
    RECORD_CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RECORD_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_LOAD_BATCH_ID VARCHAR(100), 

    PRIMARY KEY (SCAC_CD, FSAC_CD)
);

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_BRONZE.SADB.TRKFC_TRSTN_BASE 
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 14,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 14;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM
ON TABLE D_BRONZE.SADB.TRKFC_TRSTN_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for TRKFC_TRSTN_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_BRONZE.SADB.SP_PROCESS_TRKFC_TRSTN_CDC()
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
        FROM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM
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
        
        CREATE OR REPLACE STREAM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM
        ON TABLE D_BRONZE.SADB.TRKFC_TRSTN_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection';
        
        MERGE INTO D_BRONZE.SADB.TRKFC_TRSTN_V1 AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_BRONZE.SADB.TRKFC_TRSTN_BASE src
            LEFT JOIN D_BRONZE.SADB.TRKFC_TRSTN_V1 tgt 
                ON src.SCAC_CD = tgt.SCAC_CD
                AND src.FSAC_CD = tgt.FSAC_CD
            WHERE tgt.SCAC_CD IS NULL
               OR tgt.IS_DELETED = TRUE
        ) AS src
        ON tgt.SCAC_CD = src.SCAC_CD
            AND tgt.FSAC_CD = src.FSAC_CD
        WHEN MATCHED THEN UPDATE SET
            tgt.VRSN_NBR = src.VRSN_NBR,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.EFCTV_DT = src.EFCTV_DT,
            tgt.DYLGHT_SVNGS_TIME_IND = src.DYLGHT_SVNGS_TIME_IND,
            tgt.IRF_CREATE_DT = src.IRF_CREATE_DT,
            tgt.IRF_UPDATE_DT = src.IRF_UPDATE_DT,
            tgt.CNTRY_CD = src.CNTRY_CD,
            tgt.AAR_LAST_MNTND_DT = src.AAR_LAST_MNTND_DT,
            tgt.ALTD_QTY = src.ALTD_QTY,
            tgt.BEA_CD = src.BEA_CD,
            tgt.CNTY_ID = src.CNTY_ID,
            tgt.DELETE_REASON_CD = src.DELETE_REASON_CD,
            tgt.EXPIRY_DT = src.EXPIRY_DT,
            tgt.GPLTCL_NM = src.GPLTCL_NM,
            tgt.GPLTCL_SPLC_CD = src.GPLTCL_SPLC_CD,
            tgt.GPLTCL_SPLC_SUFFIX_CD = src.GPLTCL_SPLC_SUFFIX_CD,
            tgt.LNGTD_NBR = src.LNGTD_NBR,
            tgt.LTD_NBR = src.LTD_NBR,
            tgt.MSA_CD = src.MSA_CD,
            tgt.POSTAL_ZIP_CD = src.POSTAL_ZIP_CD,
            tgt.RATE_POSTAL_ZIP_CD = src.RATE_POSTAL_ZIP_CD,
            tgt.RATE_ZIP_EFCTV_DT = src.RATE_ZIP_EFCTV_DT,
            tgt.RELOAD_ABRVTN_TXT = src.RELOAD_ABRVTN_TXT,
            tgt.SPLC_CD = src.SPLC_CD,
            tgt.SPLC_SUFFIX_CD = src.SPLC_SUFFIX_CD,
            tgt.STN_STATUS_CD = src.STN_STATUS_CD,
            tgt.STPRV_CD = src.STPRV_CD,
            tgt.TIME_ZONE_CD = src.TIME_ZONE_CD,
            tgt.TRNSCN_NBR = src.TRNSCN_NBR,
            tgt.TRSTN_NM = src.TRSTN_NM,
            tgt.TRSTN_SEARCH_NM = src.TRSTN_SEARCH_NM,
            tgt.PARTY_FCLTY_VRSN_ID = src.PARTY_FCLTY_VRSN_ID,
            tgt.PARTY_FCLTY_ID = src.PARTY_FCLTY_ID,
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'RELOADED',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        WHEN NOT MATCHED THEN INSERT (
            SCAC_CD, FSAC_CD, VRSN_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            CREATE_USER_ID, UPDATE_USER_ID, EFCTV_DT, DYLGHT_SVNGS_TIME_IND,
            IRF_CREATE_DT, IRF_UPDATE_DT, CNTRY_CD, AAR_LAST_MNTND_DT, ALTD_QTY,
            BEA_CD, CNTY_ID, DELETE_REASON_CD, EXPIRY_DT, GPLTCL_NM,
            GPLTCL_SPLC_CD, GPLTCL_SPLC_SUFFIX_CD, LNGTD_NBR, LTD_NBR, MSA_CD,
            POSTAL_ZIP_CD, RATE_POSTAL_ZIP_CD, RATE_ZIP_EFCTV_DT, RELOAD_ABRVTN_TXT,
            SPLC_CD, SPLC_SUFFIX_CD, STN_STATUS_CD, STPRV_CD, TIME_ZONE_CD,
            TRNSCN_NBR, TRSTN_NM, TRSTN_SEARCH_NM, PARTY_FCLTY_VRSN_ID, PARTY_FCLTY_ID,
            SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.SCAC_CD, src.FSAC_CD, src.VRSN_NBR, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS,
            src.CREATE_USER_ID, src.UPDATE_USER_ID, src.EFCTV_DT, src.DYLGHT_SVNGS_TIME_IND,
            src.IRF_CREATE_DT, src.IRF_UPDATE_DT, src.CNTRY_CD, src.AAR_LAST_MNTND_DT, src.ALTD_QTY,
            src.BEA_CD, src.CNTY_ID, src.DELETE_REASON_CD, src.EXPIRY_DT, src.GPLTCL_NM,
            src.GPLTCL_SPLC_CD, src.GPLTCL_SPLC_SUFFIX_CD, src.LNGTD_NBR, src.LTD_NBR, src.MSA_CD,
            src.POSTAL_ZIP_CD, src.RATE_POSTAL_ZIP_CD, src.RATE_ZIP_EFCTV_DT, src.RELOAD_ABRVTN_TXT,
            src.SPLC_CD, src.SPLC_SUFFIX_CD, src.STN_STATUS_CD, src.STPRV_CD, src.TIME_ZONE_CD,
            src.TRNSCN_NBR, src.TRSTN_NM, src.TRSTN_SEARCH_NM, src.PARTY_FCLTY_VRSN_ID, src.PARTY_FCLTY_ID,
            src.SNW_OPERATION_TYPE, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
        
        v_rows_merged := SQLROWCOUNT;
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_merged || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- CHECK 2: Stage stream data into temp table (BEST PRACTICE - single read)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_TRKFC_TRSTN AS
    SELECT 
        SCAC_CD, FSAC_CD, VRSN_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
        CREATE_USER_ID, UPDATE_USER_ID, EFCTV_DT, DYLGHT_SVNGS_TIME_IND,
        IRF_CREATE_DT, IRF_UPDATE_DT, CNTRY_CD, AAR_LAST_MNTND_DT, ALTD_QTY,
        BEA_CD, CNTY_ID, DELETE_REASON_CD, EXPIRY_DT, GPLTCL_NM,
        GPLTCL_SPLC_CD, GPLTCL_SPLC_SUFFIX_CD, LNGTD_NBR, LTD_NBR, MSA_CD,
        POSTAL_ZIP_CD, RATE_POSTAL_ZIP_CD, RATE_ZIP_EFCTV_DT, RELOAD_ABRVTN_TXT,
        SPLC_CD, SPLC_SUFFIX_CD, STN_STATUS_CD, STPRV_CD, TIME_ZONE_CD,
        TRNSCN_NBR, TRSTN_NM, TRSTN_SEARCH_NM, PARTY_FCLTY_VRSN_ID, PARTY_FCLTY_ID,
        SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM;
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_TRKFC_TRSTN;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_TRKFC_TRSTN;
        RETURN 'NO_DATA: Stream has no changes to process at ' || CURRENT_TIMESTAMP()::VARCHAR;
    END IF;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.SADB.TRKFC_TRSTN_V1 AS tgt
    USING (
        SELECT 
            SCAC_CD, FSAC_CD, VRSN_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            CREATE_USER_ID, UPDATE_USER_ID, EFCTV_DT, DYLGHT_SVNGS_TIME_IND,
            IRF_CREATE_DT, IRF_UPDATE_DT, CNTRY_CD, AAR_LAST_MNTND_DT, ALTD_QTY,
            BEA_CD, CNTY_ID, DELETE_REASON_CD, EXPIRY_DT, GPLTCL_NM,
            GPLTCL_SPLC_CD, GPLTCL_SPLC_SUFFIX_CD, LNGTD_NBR, LTD_NBR, MSA_CD,
            POSTAL_ZIP_CD, RATE_POSTAL_ZIP_CD, RATE_ZIP_EFCTV_DT, RELOAD_ABRVTN_TXT,
            SPLC_CD, SPLC_SUFFIX_CD, STN_STATUS_CD, STPRV_CD, TIME_ZONE_CD,
            TRNSCN_NBR, TRSTN_NM, TRSTN_SEARCH_NM, PARTY_FCLTY_VRSN_ID, PARTY_FCLTY_ID,
            SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_TRKFC_TRSTN
    ) AS src
    ON tgt.SCAC_CD = src.SCAC_CD
        AND tgt.FSAC_CD = src.FSAC_CD
    
    -- UPDATE scenario (METADATA$ACTION='INSERT' AND METADATA$ISUPDATE=TRUE)
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
        UPDATE SET
            tgt.VRSN_NBR = src.VRSN_NBR,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.EFCTV_DT = src.EFCTV_DT,
            tgt.DYLGHT_SVNGS_TIME_IND = src.DYLGHT_SVNGS_TIME_IND,
            tgt.IRF_CREATE_DT = src.IRF_CREATE_DT,
            tgt.IRF_UPDATE_DT = src.IRF_UPDATE_DT,
            tgt.CNTRY_CD = src.CNTRY_CD,
            tgt.AAR_LAST_MNTND_DT = src.AAR_LAST_MNTND_DT,
            tgt.ALTD_QTY = src.ALTD_QTY,
            tgt.BEA_CD = src.BEA_CD,
            tgt.CNTY_ID = src.CNTY_ID,
            tgt.DELETE_REASON_CD = src.DELETE_REASON_CD,
            tgt.EXPIRY_DT = src.EXPIRY_DT,
            tgt.GPLTCL_NM = src.GPLTCL_NM,
            tgt.GPLTCL_SPLC_CD = src.GPLTCL_SPLC_CD,
            tgt.GPLTCL_SPLC_SUFFIX_CD = src.GPLTCL_SPLC_SUFFIX_CD,
            tgt.LNGTD_NBR = src.LNGTD_NBR,
            tgt.LTD_NBR = src.LTD_NBR,
            tgt.MSA_CD = src.MSA_CD,
            tgt.POSTAL_ZIP_CD = src.POSTAL_ZIP_CD,
            tgt.RATE_POSTAL_ZIP_CD = src.RATE_POSTAL_ZIP_CD,
            tgt.RATE_ZIP_EFCTV_DT = src.RATE_ZIP_EFCTV_DT,
            tgt.RELOAD_ABRVTN_TXT = src.RELOAD_ABRVTN_TXT,
            tgt.SPLC_CD = src.SPLC_CD,
            tgt.SPLC_SUFFIX_CD = src.SPLC_SUFFIX_CD,
            tgt.STN_STATUS_CD = src.STN_STATUS_CD,
            tgt.STPRV_CD = src.STPRV_CD,
            tgt.TIME_ZONE_CD = src.TIME_ZONE_CD,
            tgt.TRNSCN_NBR = src.TRNSCN_NBR,
            tgt.TRSTN_NM = src.TRSTN_NM,
            tgt.TRSTN_SEARCH_NM = src.TRSTN_SEARCH_NM,
            tgt.PARTY_FCLTY_VRSN_ID = src.PARTY_FCLTY_VRSN_ID,
            tgt.PARTY_FCLTY_ID = src.PARTY_FCLTY_ID,
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
            tgt.VRSN_NBR = src.VRSN_NBR,
            tgt.RECORD_CREATE_TMS = src.RECORD_CREATE_TMS,
            tgt.RECORD_UPDATE_TMS = src.RECORD_UPDATE_TMS,
            tgt.CREATE_USER_ID = src.CREATE_USER_ID,
            tgt.UPDATE_USER_ID = src.UPDATE_USER_ID,
            tgt.EFCTV_DT = src.EFCTV_DT,
            tgt.DYLGHT_SVNGS_TIME_IND = src.DYLGHT_SVNGS_TIME_IND,
            tgt.IRF_CREATE_DT = src.IRF_CREATE_DT,
            tgt.IRF_UPDATE_DT = src.IRF_UPDATE_DT,
            tgt.CNTRY_CD = src.CNTRY_CD,
            tgt.AAR_LAST_MNTND_DT = src.AAR_LAST_MNTND_DT,
            tgt.ALTD_QTY = src.ALTD_QTY,
            tgt.BEA_CD = src.BEA_CD,
            tgt.CNTY_ID = src.CNTY_ID,
            tgt.DELETE_REASON_CD = src.DELETE_REASON_CD,
            tgt.EXPIRY_DT = src.EXPIRY_DT,
            tgt.GPLTCL_NM = src.GPLTCL_NM,
            tgt.GPLTCL_SPLC_CD = src.GPLTCL_SPLC_CD,
            tgt.GPLTCL_SPLC_SUFFIX_CD = src.GPLTCL_SPLC_SUFFIX_CD,
            tgt.LNGTD_NBR = src.LNGTD_NBR,
            tgt.LTD_NBR = src.LTD_NBR,
            tgt.MSA_CD = src.MSA_CD,
            tgt.POSTAL_ZIP_CD = src.POSTAL_ZIP_CD,
            tgt.RATE_POSTAL_ZIP_CD = src.RATE_POSTAL_ZIP_CD,
            tgt.RATE_ZIP_EFCTV_DT = src.RATE_ZIP_EFCTV_DT,
            tgt.RELOAD_ABRVTN_TXT = src.RELOAD_ABRVTN_TXT,
            tgt.SPLC_CD = src.SPLC_CD,
            tgt.SPLC_SUFFIX_CD = src.SPLC_SUFFIX_CD,
            tgt.STN_STATUS_CD = src.STN_STATUS_CD,
            tgt.STPRV_CD = src.STPRV_CD,
            tgt.TIME_ZONE_CD = src.TIME_ZONE_CD,
            tgt.TRNSCN_NBR = src.TRNSCN_NBR,
            tgt.TRSTN_NM = src.TRSTN_NM,
            tgt.TRSTN_SEARCH_NM = src.TRSTN_SEARCH_NM,
            tgt.PARTY_FCLTY_VRSN_ID = src.PARTY_FCLTY_VRSN_ID,
            tgt.PARTY_FCLTY_ID = src.PARTY_FCLTY_ID,
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
            SCAC_CD, FSAC_CD, VRSN_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS,
            CREATE_USER_ID, UPDATE_USER_ID, EFCTV_DT, DYLGHT_SVNGS_TIME_IND,
            IRF_CREATE_DT, IRF_UPDATE_DT, CNTRY_CD, AAR_LAST_MNTND_DT, ALTD_QTY,
            BEA_CD, CNTY_ID, DELETE_REASON_CD, EXPIRY_DT, GPLTCL_NM,
            GPLTCL_SPLC_CD, GPLTCL_SPLC_SUFFIX_CD, LNGTD_NBR, LTD_NBR, MSA_CD,
            POSTAL_ZIP_CD, RATE_POSTAL_ZIP_CD, RATE_ZIP_EFCTV_DT, RELOAD_ABRVTN_TXT,
            SPLC_CD, SPLC_SUFFIX_CD, STN_STATUS_CD, STPRV_CD, TIME_ZONE_CD,
            TRNSCN_NBR, TRSTN_NM, TRSTN_SEARCH_NM, PARTY_FCLTY_VRSN_ID, PARTY_FCLTY_ID,
            SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src.SCAC_CD, src.FSAC_CD, src.VRSN_NBR, src.RECORD_CREATE_TMS, src.RECORD_UPDATE_TMS,
            src.CREATE_USER_ID, src.UPDATE_USER_ID, src.EFCTV_DT, src.DYLGHT_SVNGS_TIME_IND,
            src.IRF_CREATE_DT, src.IRF_UPDATE_DT, src.CNTRY_CD, src.AAR_LAST_MNTND_DT, src.ALTD_QTY,
            src.BEA_CD, src.CNTY_ID, src.DELETE_REASON_CD, src.EXPIRY_DT, src.GPLTCL_NM,
            src.GPLTCL_SPLC_CD, src.GPLTCL_SPLC_SUFFIX_CD, src.LNGTD_NBR, src.LTD_NBR, src.MSA_CD,
            src.POSTAL_ZIP_CD, src.RATE_POSTAL_ZIP_CD, src.RATE_ZIP_EFCTV_DT, src.RELOAD_ABRVTN_TXT,
            src.SPLC_CD, src.SPLC_SUFFIX_CD, src.STN_STATUS_CD, src.STPRV_CD, src.TIME_ZONE_CD,
            src.TRNSCN_NBR, src.TRSTN_NM, src.TRSTN_SEARCH_NM, src.PARTY_FCLTY_VRSN_ID, src.PARTY_FCLTY_ID,
            src.SNW_OPERATION_TYPE, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    DROP TABLE IF EXISTS _CDC_STAGING_TRKFC_TRSTN;
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes. Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS _CDC_STAGING_TRKFC_TRSTN;
        RETURN 'ERROR: ' || SQLERRM || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process TRKFC_TRSTN_BASE CDC changes into data preservation table'
WHEN
    SYSTEM$STREAM_HAS_DATA('D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM')
AS
    CALL D_BRONZE.SADB.SP_PROCESS_TRKFC_TRSTN_CDC();

ALTER TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC RESUME;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'TRKFC_TRSTN%' IN SCHEMA D_BRONZE.SADB;
-- SHOW STREAMS LIKE 'TRKFC_TRSTN%' IN SCHEMA D_BRONZE.SADB;
-- SHOW TASKS LIKE 'TASK_PROCESS_TRKFC_TRSTN%' IN SCHEMA D_BRONZE.SADB;
-- CALL D_BRONZE.SADB.SP_PROCESS_TRKFC_TRSTN_CDC();
