/*
================================================================================
UPDATED CDC PROCEDURE FOR D_RAW + D_BRONZE ARCHITECTURE
================================================================================
Version      : 2.0.0
Purpose      : CDC processing after database layer reorganization
Source       : D_RAW.SADB.TRKFC_TRSTN_BASE (raw data from IDMC)
Target       : D_BRONZE.SADB.TRKFC_TRSTN_V1 (data preservation)
Stream       : D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM

ARCHITECTURE:
  D_RAW (IDMC Ingestion)          D_BRONZE (Data Preservation)
  ├── SADB                        ├── SADB
  │   └── TRKFC_TRSTN_BASE  ───►  │   ├── TRKFC_TRSTN_BASE_HIST_STREAM
  │       (source table)          │   │       (CDC stream on D_RAW source)
  │                               │   └── TRKFC_TRSTN_V1
  │                               │       (preservation table)
================================================================================
*/

-- =============================================================================
-- STEP 1: Enable change tracking on D_RAW source table
-- =============================================================================
ALTER TABLE D_RAW.SADB.TRKFC_TRSTN_BASE
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 14;

-- =============================================================================
-- STEP 2: Create CDC stream in D_BRONZE pointing to D_RAW source
-- =============================================================================
CREATE OR REPLACE STREAM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.TRKFC_TRSTN_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC stream capturing changes from D_RAW source table';

-- =============================================================================
-- STEP 3: Create updated stored procedure
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_BRONZE.SADB.SP_PROCESS_TRKFC_TRSTN_CDC()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Processes CDC from D_RAW.SADB.TRKFC_TRSTN_BASE into D_BRONZE.SADB.TRKFC_TRSTN_V1'
AS
$$
DECLARE
    v_batch_id VARCHAR;
    v_rows_processed NUMBER DEFAULT 0;
    v_stream_name VARCHAR DEFAULT 'D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM';
    v_source_table VARCHAR DEFAULT 'D_RAW.SADB.TRKFC_TRSTN_BASE';
    v_target_table VARCHAR DEFAULT 'D_BRONZE.SADB.TRKFC_TRSTN_V1';
BEGIN
    -- Generate batch ID
    v_batch_id := 'BATCH_' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    
    -- Check if stream has data
    IF (NOT SYSTEM$STREAM_HAS_DATA(:v_stream_name)) THEN
        RETURN 'NO_DATA: Stream has no new changes to process';
    END IF;
    
    -- MERGE operation: Process CDC changes from stream
    MERGE INTO D_BRONZE.SADB.TRKFC_TRSTN_V1 AS target
    USING (
        SELECT 
            SCAC_CD,
            FSAC_CD,
            VRSN_NBR,
            CREAT_TS,
            CREAT_USER_ID,
            UPD_TS,
            UPD_USER_ID,
            STRT_EFF_DT,
            END_EFF_DT,
            XFR_TYP,
            OPRATN_TYP,
            SEG_TYP_CD,
            HZMT_IN,
            PLLT_IN,
            CRTG_IN,
            TARP_IN,
            MAX_LEN_NBR,
            MAX_WT_NBR,
            XPDT_DLVR_DAY_CNT,
            XPDT_PCKG_DAY_CNT,
            ONLN_AVBL_IN,
            PPD_ALOW_IN,
            COL_ALOW_IN,
            TP_ALOW_IN,
            SHP_SITE_RGST_IN,
            CON_SITE_RGST_IN,
            MIN_PCKG_CHG_AMT,
            MIN_DLVR_CHG_AMT,
            DFLT_TRSTN_TYP,
            DFLT_MUL_STOP_IND,
            DFLT_INTR_IND,
            DFLT_OBSZ_IND,
            ASGN_SHP_SITE_IN,
            ASGN_CON_SITE_IN,
            SLCT_VIA_PTS_IN,
            WT_UPLD_ALWD_IN,
            METADATA$ACTION AS CDC_ACTION,
            METADATA$ISUPDATE AS CDC_IS_UPDATE,
            CURRENT_TIMESTAMP() AS CDC_TIMESTAMP
        FROM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM
    ) AS source
    ON target.SCAC_CD = source.SCAC_CD 
       AND target.FSAC_CD = source.FSAC_CD
    
    -- Handle DELETE: Soft delete (mark IS_DELETED = TRUE)
    WHEN MATCHED AND source.CDC_ACTION = 'DELETE' THEN
        UPDATE SET
            target.IS_DELETED = TRUE,
            target.CDC_OPERATION = 'DELETE',
            target.CDC_TIMESTAMP = source.CDC_TIMESTAMP,
            target.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            target.SOURCE_LOAD_BATCH_ID = :v_batch_id
    
    -- Handle UPDATE: Update existing record
    WHEN MATCHED AND source.CDC_ACTION = 'INSERT' AND source.CDC_IS_UPDATE = TRUE THEN
        UPDATE SET
            target.VRSN_NBR = source.VRSN_NBR,
            target.CREAT_TS = source.CREAT_TS,
            target.CREAT_USER_ID = source.CREAT_USER_ID,
            target.UPD_TS = source.UPD_TS,
            target.UPD_USER_ID = source.UPD_USER_ID,
            target.STRT_EFF_DT = source.STRT_EFF_DT,
            target.END_EFF_DT = source.END_EFF_DT,
            target.XFR_TYP = source.XFR_TYP,
            target.OPRATN_TYP = source.OPRATN_TYP,
            target.SEG_TYP_CD = source.SEG_TYP_CD,
            target.HZMT_IN = source.HZMT_IN,
            target.PLLT_IN = source.PLLT_IN,
            target.CRTG_IN = source.CRTG_IN,
            target.TARP_IN = source.TARP_IN,
            target.MAX_LEN_NBR = source.MAX_LEN_NBR,
            target.MAX_WT_NBR = source.MAX_WT_NBR,
            target.XPDT_DLVR_DAY_CNT = source.XPDT_DLVR_DAY_CNT,
            target.XPDT_PCKG_DAY_CNT = source.XPDT_PCKG_DAY_CNT,
            target.ONLN_AVBL_IN = source.ONLN_AVBL_IN,
            target.PPD_ALOW_IN = source.PPD_ALOW_IN,
            target.COL_ALOW_IN = source.COL_ALOW_IN,
            target.TP_ALOW_IN = source.TP_ALOW_IN,
            target.SHP_SITE_RGST_IN = source.SHP_SITE_RGST_IN,
            target.CON_SITE_RGST_IN = source.CON_SITE_RGST_IN,
            target.MIN_PCKG_CHG_AMT = source.MIN_PCKG_CHG_AMT,
            target.MIN_DLVR_CHG_AMT = source.MIN_DLVR_CHG_AMT,
            target.DFLT_TRSTN_TYP = source.DFLT_TRSTN_TYP,
            target.DFLT_MUL_STOP_IND = source.DFLT_MUL_STOP_IND,
            target.DFLT_INTR_IND = source.DFLT_INTR_IND,
            target.DFLT_OBSZ_IND = source.DFLT_OBSZ_IND,
            target.ASGN_SHP_SITE_IN = source.ASGN_SHP_SITE_IN,
            target.ASGN_CON_SITE_IN = source.ASGN_CON_SITE_IN,
            target.SLCT_VIA_PTS_IN = source.SLCT_VIA_PTS_IN,
            target.WT_UPLD_ALWD_IN = source.WT_UPLD_ALWD_IN,
            target.IS_DELETED = FALSE,
            target.CDC_OPERATION = 'UPDATE',
            target.CDC_TIMESTAMP = source.CDC_TIMESTAMP,
            target.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            target.SOURCE_LOAD_BATCH_ID = :v_batch_id
    
    -- Handle INSERT: New record
    WHEN NOT MATCHED AND source.CDC_ACTION = 'INSERT' THEN
        INSERT (
            SCAC_CD, FSAC_CD, VRSN_NBR, CREAT_TS, CREAT_USER_ID,
            UPD_TS, UPD_USER_ID, STRT_EFF_DT, END_EFF_DT, XFR_TYP,
            OPRATN_TYP, SEG_TYP_CD, HZMT_IN, PLLT_IN, CRTG_IN,
            TARP_IN, MAX_LEN_NBR, MAX_WT_NBR, XPDT_DLVR_DAY_CNT,
            XPDT_PCKG_DAY_CNT, ONLN_AVBL_IN, PPD_ALOW_IN, COL_ALOW_IN,
            TP_ALOW_IN, SHP_SITE_RGST_IN, CON_SITE_RGST_IN, MIN_PCKG_CHG_AMT,
            MIN_DLVR_CHG_AMT, DFLT_TRSTN_TYP, DFLT_MUL_STOP_IND, DFLT_INTR_IND,
            DFLT_OBSZ_IND, ASGN_SHP_SITE_IN, ASGN_CON_SITE_IN, SLCT_VIA_PTS_IN,
            WT_UPLD_ALWD_IN, IS_DELETED, CDC_OPERATION, CDC_TIMESTAMP,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        )
        VALUES (
            source.SCAC_CD, source.FSAC_CD, source.VRSN_NBR, source.CREAT_TS,
            source.CREAT_USER_ID, source.UPD_TS, source.UPD_USER_ID,
            source.STRT_EFF_DT, source.END_EFF_DT, source.XFR_TYP,
            source.OPRATN_TYP, source.SEG_TYP_CD, source.HZMT_IN, source.PLLT_IN,
            source.CRTG_IN, source.TARP_IN, source.MAX_LEN_NBR, source.MAX_WT_NBR,
            source.XPDT_DLVR_DAY_CNT, source.XPDT_PCKG_DAY_CNT, source.ONLN_AVBL_IN,
            source.PPD_ALOW_IN, source.COL_ALOW_IN, source.TP_ALOW_IN,
            source.SHP_SITE_RGST_IN, source.CON_SITE_RGST_IN, source.MIN_PCKG_CHG_AMT,
            source.MIN_DLVR_CHG_AMT, source.DFLT_TRSTN_TYP, source.DFLT_MUL_STOP_IND,
            source.DFLT_INTR_IND, source.DFLT_OBSZ_IND, source.ASGN_SHP_SITE_IN,
            source.ASGN_CON_SITE_IN, source.SLCT_VIA_PTS_IN, source.WT_UPLD_ALWD_IN,
            FALSE, 'INSERT', source.CDC_TIMESTAMP,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), :v_batch_id
        );
    
    -- Get row count
    SELECT COUNT(*) INTO v_rows_processed FROM D_BRONZE.SADB.TRKFC_TRSTN_V1
    WHERE SOURCE_LOAD_BATCH_ID = :v_batch_id;
    
    RETURN 'SUCCESS: Processed ' || v_rows_processed || ' CDC changes. Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE;
END;
$$;

-- =============================================================================
-- STEP 4: Create/update task
-- =============================================================================
CREATE OR REPLACE TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'CDC task processing D_RAW source changes into D_BRONZE preservation'
WHEN
    SYSTEM$STREAM_HAS_DATA('D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM')
AS
    CALL D_BRONZE.SADB.SP_PROCESS_TRKFC_TRSTN_CDC();

-- Start the task
ALTER TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC RESUME;

-- =============================================================================
-- VERIFICATION
-- =============================================================================
SELECT '=== VERIFICATION ===' AS STATUS;

-- Check stream
SHOW STREAMS LIKE 'TRKFC_TRSTN_BASE_HIST_STREAM' IN SCHEMA D_BRONZE.SADB;

-- Check procedure
SHOW PROCEDURES LIKE 'SP_PROCESS_TRKFC_TRSTN_CDC' IN SCHEMA D_BRONZE.SADB;

-- Check task
SHOW TASKS LIKE 'TASK_PROCESS_TRKFC_TRSTN_CDC' IN SCHEMA D_BRONZE.SADB;

-- Test procedure
-- CALL D_BRONZE.SADB.SP_PROCESS_TRKFC_TRSTN_CDC();
