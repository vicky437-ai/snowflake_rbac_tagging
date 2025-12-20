-- Silver layer curated table for BASE records
-- This stores the historical snapshot from _BASE
CREATE OR REPLACE TABLE D_SILVER.SADB.TRAIN_PLAN_LEG (
    -- Primary Key
    TRAIN_PLAN_LEG_ID          NUMBER(18,0)        NOT NULL,
    
    -- Business Columns (from your BASE table)
    TRAIN_PLAN_ID              NUMBER(18,0),
    TRAIN_DRCTN_CD             VARCHAR(20),
    TRAIN_PLAN_LEG_NM          VARCHAR(32),
    MTP_TITAN_NBR              NUMBER(6,0),
    RECORD_CREATE_TMS          TIMESTAMP_NTZ(0),
    RECORD_UPDATE_TMS          TIMESTAMP_NTZ(0),
    CREATE_USER_ID             VARCHAR(32),
    UPDATE_USER_ID             VARCHAR(32),
    TURN_LEG_SQNC_NBR          NUMBER(1,0),
    TYES_TRAIN_ID              NUMBER(18,0),
    MTP_TOTAL_RTPNT_SENT_QTY   NUMBER(4,0),
    MTP_ROUTE_CMPLT_CD         VARCHAR(4),
    MTP_TRAIN_STATE_CD         VARCHAR(4),
    
    -- CDC Tracking Columns
    IS_DELETED                 BOOLEAN             DEFAULT FALSE,
    EFFECTIVE_TS               TIMESTAMP_NTZ(9)    NOT NULL,
    LOAD_TS                    TIMESTAMP_NTZ(9)    DEFAULT CURRENT_TIMESTAMP(),
    RECORD_SOURCE              VARCHAR(10)         NOT NULL  -- 'BASE' or 'CDC'
)
CLUSTER BY (TRAIN_PLAN_LEG_ID)
DATA_RETENTION_TIME_IN_DAYS = 7
CHANGE_TRACKING = TRUE
COMMENT = 'Silver layer TRAIN_PLAN_LEG with BASE snapshot and CDC tracking';


-- ONE-TIME BASE LOAD

INSERT INTO D_SILVER.SADB.TRAIN_PLAN_LEG (
    TRAIN_PLAN_LEG_ID,
    TRAIN_PLAN_ID,
    TRAIN_DRCTN_CD,
    TRAIN_PLAN_LEG_NM,
    MTP_TITAN_NBR,
    RECORD_CREATE_TMS,
    RECORD_UPDATE_TMS,
    CREATE_USER_ID,
    UPDATE_USER_ID,
    TURN_LEG_SQNC_NBR,
    TYES_TRAIN_ID,
    MTP_TOTAL_RTPNT_SENT_QTY,
    MTP_ROUTE_CMPLT_CD,
    MTP_TRAIN_STATE_CD,
    IS_DELETED,
    EFFECTIVE_TS,
    LOAD_TS,
    RECORD_SOURCE
)
SELECT
    TRAIN_PLAN_LEG_ID,
    TRAIN_PLAN_ID,
    TRAIN_DRCTN_CD,
    TRAIN_PLAN_LEG_NM,
    MTP_TITAN_NBR,
    RECORD_CREATE_TMS,
    RECORD_UPDATE_TMS,
    CREATE_USER_ID,
    UPDATE_USER_ID,
    TURN_LEG_SQNC_NBR,
    TYES_TRAIN_ID,
    MTP_TOTAL_RTPNT_SENT_QTY,
    MTP_ROUTE_CMPLT_CD,
    MTP_TRAIN_STATE_CD,
    FALSE                               AS IS_DELETED,
    COALESCE(SNW_LAST_REPLICATED, RECORD_UPDATE_TMS, RECORD_CREATE_TMS) AS EFFECTIVE_TS,
    CURRENT_TIMESTAMP()                 AS LOAD_TS,
    'BASE'                              AS RECORD_SOURCE
FROM D_BRONZE.SADB.TRAIN_PLAN_LEG_BASE;

-- Verify load
SELECT 'BASE Load Complete' AS STATUS,
       COUNT(*) AS ROW_COUNT,
       MIN(EFFECTIVE_TS) AS MIN_EFFECTIVE_TS,
       MAX(EFFECTIVE_TS) AS MAX_EFFECTIVE_TS
FROM D_SILVER.SADB.TRAIN_PLAN_LEG
WHERE RECORD_SOURCE = 'BASE';


CDC Dynamic Table

-- CDC Dynamic Table on _LOG table
-- INCREMENTAL mode works because _LOG is a regular table
-- IMMUTABLE WHERE optimizes by skipping old (immutable) rows

CREATE OR REPLACE DYNAMIC TABLE D_SILVER.SADB.TRAIN_PLAN_LEG_CDC_DT
    TARGET_LAG = '5 minutes'
    WAREHOUSE = CDC_WH_XS
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    DATA_RETENTION_TIME_IN_DAYS = 7
    
    -- CRITICAL: IMMUTABLE WHERE optimization
    -- Tells Snowflake: rows older than 1 day will never change
    -- Snowflake skips scanning these rows during incremental refresh
    IMMUTABLE WHERE (OP_LAST_REPLICATED < CURRENT_TIMESTAMP() - INTERVAL '1 day')
    
    COMMENT = 'CDC changes from _LOG table with IMMUTABLE WHERE optimization'
AS
SELECT
    -- Use _NEW columns for current values (handles I and U)
    -- For DELETE (OP_CODE='D'), _NEW may be null, so COALESCE with _OLD
    COALESCE(TRAIN_PLAN_LEG_ID_NEW, TRAIN_PLAN_LEG_ID_OLD)   AS TRAIN_PLAN_LEG_ID,
    TRAIN_PLAN_ID_NEW                                        AS TRAIN_PLAN_ID,
    TRAIN_DRCTN_CD_NEW                                       AS TRAIN_DRCTN_CD,
    TRAIN_PLAN_LEG_NM_NEW                                    AS TRAIN_PLAN_LEG_NM,
    MTP_TITAN_NBR_NEW                                        AS MTP_TITAN_NBR,
    RECORD_CREATE_TMS_NEW                                    AS RECORD_CREATE_TMS,
    RECORD_UPDATE_TMS_NEW                                    AS RECORD_UPDATE_TMS,
    CREATE_USER_ID_NEW                                       AS CREATE_USER_ID,
    UPDATE_USER_ID_NEW                                       AS UPDATE_USER_ID,
    TURN_LEG_SQNC_NBR_NEW                                    AS TURN_LEG_SQNC_NBR,
    TYES_TRAIN_ID_NEW                                        AS TYES_TRAIN_ID,
    MTP_TOTAL_RTPNT_SENT_QTY_NEW                             AS MTP_TOTAL_RTPNT_SENT_QTY,
    MTP_ROUTE_CMPLT_CD_NEW                                   AS MTP_ROUTE_CMPLT_CD,
    MTP_TRAIN_STATE_CD_NEW                                   AS MTP_TRAIN_STATE_CD,
    
    -- Delete handling
    CASE WHEN OP_CODE = 'D' THEN TRUE ELSE FALSE END         AS IS_DELETED,
    
    -- Timestamps
    OP_LAST_REPLICATED                                       AS EFFECTIVE_TS,
    OP_LAST_REPLICATED                                       AS LOAD_TS,
    'CDC'                                                    AS RECORD_SOURCE
    
FROM D_BRONZE.SADB.TRAIN_PLAN_LEG_BASE_LOG;  -- _LOG table, NOT Stream!


TRAIN_PLAN_LEG_CURR_DT Dynamic Table

-- CURRENT STATE DYNAMIC TABLE
-- One row per TRAIN_PLAN_LEG_ID with latest values
-- This is what downstream analytics should query

-- Current State Dynamic Table
-- UNIONs BASE table + CDC_DT, deduplicates by PK
-- Latest EFFECTIVE_TS wins

CREATE OR REPLACE DYNAMIC TABLE D_SILVER.SADB.TRAIN_PLAN_LEG_CURR_DT
    TARGET_LAG = '5 minutes'
    WAREHOUSE = INFA_INGEST_WH
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Current state of TRAIN_PLAN_LEG - deduplicates BASE + CDC'
AS
SELECT
    TRAIN_PLAN_LEG_ID,
    TRAIN_PLAN_ID,
    TRAIN_DRCTN_CD,
    TRAIN_PLAN_LEG_NM,
    MTP_TITAN_NBR,
    RECORD_CREATE_TMS,
    RECORD_UPDATE_TMS,
    CREATE_USER_ID,
    UPDATE_USER_ID,
    TURN_LEG_SQNC_NBR,
    TYES_TRAIN_ID,
    MTP_TOTAL_RTPNT_SENT_QTY,
    MTP_ROUTE_CMPLT_CD,
    MTP_TRAIN_STATE_CD,
    IS_DELETED,
    EFFECTIVE_TS,
    LOAD_TS
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRAIN_PLAN_LEG_ID
            ORDER BY EFFECTIVE_TS DESC, LOAD_TS DESC
        ) AS RN
    FROM (
        -- BASE snapshot
        SELECT
            TRAIN_PLAN_LEG_ID, TRAIN_PLAN_ID, TRAIN_DRCTN_CD, TRAIN_PLAN_LEG_NM,
            MTP_TITAN_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS, CREATE_USER_ID,
            UPDATE_USER_ID, TURN_LEG_SQNC_NBR, TYES_TRAIN_ID, MTP_TOTAL_RTPNT_SENT_QTY,
            MTP_ROUTE_CMPLT_CD, MTP_TRAIN_STATE_CD,
            IS_DELETED, EFFECTIVE_TS, LOAD_TS
        FROM D_SILVER.SADB.TRAIN_PLAN_LEG
        WHERE RECORD_SOURCE = 'BASE'
        
        UNION ALL
        
        -- CDC records from CDC_DT
        SELECT
            TRAIN_PLAN_LEG_ID, TRAIN_PLAN_ID, TRAIN_DRCTN_CD, TRAIN_PLAN_LEG_NM,
            MTP_TITAN_NBR, RECORD_CREATE_TMS, RECORD_UPDATE_TMS, CREATE_USER_ID,
            UPDATE_USER_ID, TURN_LEG_SQNC_NBR, TYES_TRAIN_ID, MTP_TOTAL_RTPNT_SENT_QTY,
            MTP_ROUTE_CMPLT_CD, MTP_TRAIN_STATE_CD,
            IS_DELETED, EFFECTIVE_TS, LOAD_TS
        FROM D_SILVER.SADB.TRAIN_PLAN_LEG_CDC_DT
    )
)
WHERE RN = 1;  -- Latest version per PK


Full Load:

sql--------------------------------------------------------------------------------
-- FULL LOAD PATTERN: CURRENT_TIMESTAMP() is ALLOWED with REFRESH_MODE = FULL
--------------------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE D_SILVER.SADB.TRAIN_PRODUCTIVITY_CPKC_GTM_TRN_FACT_DT
    TARGET_LAG = '10 hour'
    WAREHOUSE = INFA_INGEST_WH
    REFRESH_MODE = FULL                  
    INITIALIZE = ON_CREATE
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Silver layer - Train Productivity GTM Fact (Full Load pattern)'
AS
SELECT
    TITAN_NBR,
    SRC_SYS,
    TRAIN_NAME,
    ORG_STN_TRN_SCHD_CURR_NBR,
    DST_STN_TRN_SCHD_CURR_NBR,
    DEPART_FSAC,
    ARRIVE_FSAC,
    RECEIVING_DT,
    EST_ARRIVAL_TMSTMP,
    ACTL_ARR_DT,
    EST_DEP_TMSTMP,
    ACTL_DEP_DT,
    RLCAR_EMPTY_CNT,
    RLCAR_LOAD_CNT,
    LIVE_LOCO_CNT,
    DH_LOCO_CNT,
    EQP_CONTENT_WGT,
    DH_LOCO_TARE_TNS,
    LIVE_LOCO_TARE_TNS,
    EQP_GROSS_WGT,
    MILES,
    TRN_LGTH,
    LIVE_LOCO_FT,
    DH_LOCO_FT,
    LIVE_LOCO_HP,
    DH_LOCO_HP,
    ORG_STN_IND,
    DEST_STN_IND,
    ORG_STN_TRN_SCHD_SEQ_NBR,
    DST_STN_TRN_SCHD_SEQ_NBR,
    ETL_CREATED_TIME,
    ETL_UPDATED_TIME,
    
    CURRENT_TIMESTAMP()   AS LOAD_TS  

FROM D_BRONZE.AZURE.TRAIN_PRODUCTIVITY_CPKC_GTM_TRN_FACT;

