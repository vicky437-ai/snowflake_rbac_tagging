/*
================================================================================
DATA PRESERVATION SCRIPT FOR D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE
================================================================================
Source Table : D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE
Target Table : D_BRONZE.AWS.EVENTS_TRAIN_MVMNT_EVENT
Stream       : D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE_HIST_STREAM
Procedure    : D_RAW.AWS.SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT()
Task         : D_RAW.AWS.TASK_SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT
Primary Key  : event_id, event_timestamp_utc (COMPOSITE - 2 Columns)
Total Columns: 27 source + 6 CDC metadata = 33

================================================================================
*/

-- =============================================================================
-- STEP 1: Create or Alter Target Data Preservation Table
-- =============================================================================
CREATE OR ALTER TABLE D_BRONZE.AWS.EVENTS_TRAIN_MVMNT_EVENT (
    "event_id" NUMBER(38,0) NOT NULL,
    "event" VARCHAR(80),
    "event_timestamp_utc" TIMESTAMP_NTZ(6) NOT NULL,
    "train_plan_leg_id" NUMBER(18,0),
    "source_system" VARCHAR(80),
    "direction" VARCHAR(8),
    "train_name" VARCHAR(60),
    "fsac" VARCHAR(20),
    "latitude" NUMBER(11,8),
    "longitude" NUMBER(11,8),
    "mile_km_number" NUMBER(10,2),
    "subdivision_name" VARCHAR(200),
    "station_name" VARCHAR(200),
    "scac" VARCHAR(16),
    "division" VARCHAR(200),
    "region" VARCHAR(200),
    "district" VARCHAR(200),
    "state_province" VARCHAR(12),
    "country" VARCHAR(8),
    "subdivision_id" NUMBER(18,0),
    "record_update_timestamp_utc" TIMESTAMP_NTZ(6),
    "updated_from_loco_id" VARCHAR(56),
    "lead_locomotive" VARCHAR(56),
    "train_consist_summary_id" NUMBER(18,0),
    "train_consist_summary_version_number" NUMBER(5,0),
    SNW_OPERATION_TYPE VARCHAR(1),

    CDC_OPERATION VARCHAR(10),
    CDC_TIMESTAMP TIMESTAMP_NTZ,
    IS_DELETED BOOLEAN,
    RECORD_CREATED_AT TIMESTAMP_NTZ,
    RECORD_UPDATED_AT TIMESTAMP_NTZ,
    SOURCE_LOAD_BATCH_ID VARCHAR(100),
    SNW_LAST_REPLICATED TIMESTAMP_NTZ(9),

    PRIMARY KEY ("event_id", "event_timestamp_utc")
)
COMMENT = 'Bronze data preservation layer for Events Train Movement Event.
Source: AWS PostgreSQL (events_train_mvmnt_event) replicated via CDC into D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE.
Data Source Tables: D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE (27 source columns + 2 SNW metadata).
Transformations: No purge filter (AWS schema). Six CDC metadata columns added (CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID).
Pipeline Objects: Stream D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE_HIST_STREAM | Procedure D_RAW.AWS.SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT() | Task D_RAW.AWS.TASK_SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT.
Refresh Frequency: Every 5 minutes via Snowflake Task (incremental CDC MERGE).
Primary Key: Composite (event_id, event_timestamp_utc).';

-- =============================================================================
-- STEP 2: Enable Change Tracking on Source Table
-- =============================================================================
ALTER TABLE D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE
SET CHANGE_TRACKING = TRUE,
    DATA_RETENTION_TIME_IN_DAYS = 45,
    MAX_DATA_EXTENSION_TIME_IN_DAYS = 15;

-- =============================================================================
-- STEP 3: Create Stream with SHOW_INITIAL_ROWS for Initial Load
-- =============================================================================
CREATE OR REPLACE STREAM D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE_HIST_STREAM
ON TABLE D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'CDC Stream for EVENTS_TRAIN_MVMNT_EVENT_BASE data preservation. SHOW_INITIAL_ROWS=TRUE for initial load.';

-- =============================================================================
-- STEP 4: Create Stored Procedure for CDC Processing
-- =============================================================================
CREATE OR REPLACE PROCEDURE D_RAW.AWS.SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT()
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
        FROM D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE_HIST_STREAM
        WHERE 1=0;
        
        v_stream_stale := FALSE;
        
    EXCEPTION
        WHEN OTHER THEN
            v_stream_stale := TRUE;
            v_error_msg := SQLERRM;
    END;
    
    -- =========================================================================
    -- RECOVERY: If stream is stale, recreate it and load from stream
    -- =========================================================================
    IF (v_stream_stale = TRUE) THEN
        v_result := 'STREAM_STALE_DETECTED: ' || NVL(v_error_msg, 'Unknown') || ' - Initiating recovery at ' || CURRENT_TIMESTAMP()::VARCHAR;
        v_execution_status := 'RECOVERY';
        
        CREATE OR REPLACE STREAM D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE_HIST_STREAM
        ON TABLE D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE
        SHOW_INITIAL_ROWS = TRUE
        COMMENT = 'CDC Stream recreated after staleness detection';
        
        MERGE INTO D_BRONZE.AWS.EVENTS_TRAIN_MVMNT_EVENT AS tgt
        USING (
            SELECT 
                src.*,
                'INSERT' AS CDC_OP,
                :v_batch_id AS BATCH_ID
            FROM D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE_HIST_STREAM src
        ) AS src
        ON tgt."event_id" = src."event_id"
           AND tgt."event_timestamp_utc" = src."event_timestamp_utc"
        WHEN MATCHED THEN UPDATE SET
            tgt."event" = src."event",
            tgt."train_plan_leg_id" = src."train_plan_leg_id",
            tgt."source_system" = src."source_system",
            tgt."direction" = src."direction",
            tgt."train_name" = src."train_name",
            tgt."fsac" = src."fsac",
            tgt."latitude" = src."latitude",
            tgt."longitude" = src."longitude",
            tgt."mile_km_number" = src."mile_km_number",
            tgt."subdivision_name" = src."subdivision_name",
            tgt."station_name" = src."station_name",
            tgt."scac" = src."scac",
            tgt."division" = src."division",
            tgt."region" = src."region",
            tgt."district" = src."district",
            tgt."state_province" = src."state_province",
            tgt."country" = src."country",
            tgt."subdivision_id" = src."subdivision_id",
            tgt."record_update_timestamp_utc" = src."record_update_timestamp_utc",
            tgt."updated_from_loco_id" = src."updated_from_loco_id",
            tgt."lead_locomotive" = src."lead_locomotive",
            tgt."train_consist_summary_id" = src."train_consist_summary_id",
            tgt."train_consist_summary_version_number" = src."train_consist_summary_version_number",
            tgt.SNW_OPERATION_TYPE = src.SNW_OPERATION_TYPE,
            tgt.SNW_LAST_REPLICATED = src.SNW_LAST_REPLICATED,
            tgt.CDC_OPERATION = 'RELOADED',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        WHEN NOT MATCHED THEN INSERT (
            "event_id", "event", "event_timestamp_utc", "train_plan_leg_id", "source_system",
            "direction", "train_name", "fsac", "latitude", "longitude",
            "mile_km_number", "subdivision_name", "station_name", "scac", "division",
            "region", "district", "state_province", "country", "subdivision_id",
            "record_update_timestamp_utc", "updated_from_loco_id", "lead_locomotive",
            "train_consist_summary_id", "train_consist_summary_version_number",
            SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src."event_id", src."event", src."event_timestamp_utc", src."train_plan_leg_id", src."source_system",
            src."direction", src."train_name", src."fsac", src."latitude", src."longitude",
            src."mile_km_number", src."subdivision_name", src."station_name", src."scac", src."division",
            src."region", src."district", src."state_province", src."country", src."subdivision_id",
            src."record_update_timestamp_utc", src."updated_from_loco_id", src."lead_locomotive",
            src."train_consist_summary_id", src."train_consist_summary_version_number",
            src.SNW_OPERATION_TYPE, src.SNW_LAST_REPLICATED,
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
            'EVENTS_TRAIN_MVMNT_EVENT', :v_batch_id, 'RECOVERY', :v_start_time, :v_end_time,
            :v_rows_merged, :v_rows_merged, 0, 0,
            :v_error_msg, CURRENT_TIMESTAMP()
        );
        
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_merged || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- CHECK 2: Stage stream data into temp table (BEST PRACTICE - single read)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_EVENTS_TRAIN_MVMNT_EVENT AS
    SELECT 
        "event_id", "event", "event_timestamp_utc", "train_plan_leg_id", "source_system",
        "direction", "train_name", "fsac", "latitude", "longitude",
        "mile_km_number", "subdivision_name", "station_name", "scac", "division",
        "region", "district", "state_province", "country", "subdivision_id",
        "record_update_timestamp_utc", "updated_from_loco_id", "lead_locomotive",
        "train_consist_summary_id", "train_consist_summary_version_number",
        SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE,
        METADATA$ROW_ID AS ROW_ID
    FROM D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE_HIST_STREAM;
    
    SELECT COUNT(*) INTO v_staging_count FROM _CDC_STAGING_EVENTS_TRAIN_MVMNT_EVENT;
    
    IF (v_staging_count = 0) THEN
        DROP TABLE IF EXISTS _CDC_STAGING_EVENTS_TRAIN_MVMNT_EVENT;
        v_end_time := CURRENT_TIMESTAMP();
        
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
            TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME,
            ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED,
            ERROR_MESSAGE, CREATED_AT
        ) VALUES (
            'EVENTS_TRAIN_MVMNT_EVENT', :v_batch_id, 'NO_DATA', :v_start_time, :v_end_time,
            0, 0, 0, 0, NULL, CURRENT_TIMESTAMP()
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
    FROM _CDC_STAGING_EVENTS_TRAIN_MVMNT_EVENT;
    
    -- =========================================================================
    -- MAIN PROCESSING: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    MERGE INTO D_BRONZE.AWS.EVENTS_TRAIN_MVMNT_EVENT AS tgt
    USING (
        SELECT 
            "event_id", "event", "event_timestamp_utc", "train_plan_leg_id", "source_system",
            "direction", "train_name", "fsac", "latitude", "longitude",
            "mile_km_number", "subdivision_name", "station_name", "scac", "division",
            "region", "district", "state_province", "country", "subdivision_id",
            "record_update_timestamp_utc", "updated_from_loco_id", "lead_locomotive",
            "train_consist_summary_id", "train_consist_summary_version_number",
            SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_ACTION,
            CDC_IS_UPDATE,
            ROW_ID,
            :v_batch_id AS BATCH_ID
        FROM _CDC_STAGING_EVENTS_TRAIN_MVMNT_EVENT
    ) AS src
    ON tgt."event_id" = src."event_id"
       AND tgt."event_timestamp_utc" = src."event_timestamp_utc"
    
    -- UPDATE scenario (METADATA$ACTION='INSERT' AND METADATA$ISUPDATE=TRUE)
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN 
        UPDATE SET
            tgt."event" = src."event",
            tgt."train_plan_leg_id" = src."train_plan_leg_id",
            tgt."source_system" = src."source_system",
            tgt."direction" = src."direction",
            tgt."train_name" = src."train_name",
            tgt."fsac" = src."fsac",
            tgt."latitude" = src."latitude",
            tgt."longitude" = src."longitude",
            tgt."mile_km_number" = src."mile_km_number",
            tgt."subdivision_name" = src."subdivision_name",
            tgt."station_name" = src."station_name",
            tgt."scac" = src."scac",
            tgt."division" = src."division",
            tgt."region" = src."region",
            tgt."district" = src."district",
            tgt."state_province" = src."state_province",
            tgt."country" = src."country",
            tgt."subdivision_id" = src."subdivision_id",
            tgt."record_update_timestamp_utc" = src."record_update_timestamp_utc",
            tgt."updated_from_loco_id" = src."updated_from_loco_id",
            tgt."lead_locomotive" = src."lead_locomotive",
            tgt."train_consist_summary_id" = src."train_consist_summary_id",
            tgt."train_consist_summary_version_number" = src."train_consist_summary_version_number",
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
    
    -- RE-INSERT scenario (record exists but being re-inserted after delete)
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = FALSE THEN
        UPDATE SET
            tgt."event" = src."event",
            tgt."train_plan_leg_id" = src."train_plan_leg_id",
            tgt."source_system" = src."source_system",
            tgt."direction" = src."direction",
            tgt."train_name" = src."train_name",
            tgt."fsac" = src."fsac",
            tgt."latitude" = src."latitude",
            tgt."longitude" = src."longitude",
            tgt."mile_km_number" = src."mile_km_number",
            tgt."subdivision_name" = src."subdivision_name",
            tgt."station_name" = src."station_name",
            tgt."scac" = src."scac",
            tgt."division" = src."division",
            tgt."region" = src."region",
            tgt."district" = src."district",
            tgt."state_province" = src."state_province",
            tgt."country" = src."country",
            tgt."subdivision_id" = src."subdivision_id",
            tgt."record_update_timestamp_utc" = src."record_update_timestamp_utc",
            tgt."updated_from_loco_id" = src."updated_from_loco_id",
            tgt."lead_locomotive" = src."lead_locomotive",
            tgt."train_consist_summary_id" = src."train_consist_summary_id",
            tgt."train_consist_summary_version_number" = src."train_consist_summary_version_number",
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
            "event_id", "event", "event_timestamp_utc", "train_plan_leg_id", "source_system",
            "direction", "train_name", "fsac", "latitude", "longitude",
            "mile_km_number", "subdivision_name", "station_name", "scac", "division",
            "region", "district", "state_province", "country", "subdivision_id",
            "record_update_timestamp_utc", "updated_from_loco_id", "lead_locomotive",
            "train_consist_summary_id", "train_consist_summary_version_number",
            SNW_OPERATION_TYPE, SNW_LAST_REPLICATED,
            CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED,
            RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID
        ) VALUES (
            src."event_id", src."event", src."event_timestamp_utc", src."train_plan_leg_id", src."source_system",
            src."direction", src."train_name", src."fsac", src."latitude", src."longitude",
            src."mile_km_number", src."subdivision_name", src."station_name", src."scac", src."division",
            src."region", src."district", src."state_province", src."country", src."subdivision_id",
            src."record_update_timestamp_utc", src."updated_from_loco_id", src."lead_locomotive",
            src."train_consist_summary_id", src."train_consist_summary_version_number",
            src.SNW_OPERATION_TYPE, src.SNW_LAST_REPLICATED,
            'INSERT', CURRENT_TIMESTAMP(), FALSE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID
        );
    
    v_rows_merged := SQLROWCOUNT;
    v_end_time := CURRENT_TIMESTAMP();
    
    DROP TABLE IF EXISTS _CDC_STAGING_EVENTS_TRAIN_MVMNT_EVENT;
    
    -- =========================================================================
    -- EXECUTION LOGGING: Write metrics to CDC_EXECUTION_LOG
    -- =========================================================================
    INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
        TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME,
        ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED,
        ERROR_MESSAGE, CREATED_AT
    ) VALUES (
        'EVENTS_TRAIN_MVMNT_EVENT', :v_batch_id, 'SUCCESS', :v_start_time, :v_end_time,
        :v_rows_merged, :v_rows_inserted, :v_rows_updated, :v_rows_deleted,
        NULL, CURRENT_TIMESTAMP()
    );
    
    RETURN 'SUCCESS: Processed ' || v_rows_merged || ' CDC changes (I:' || v_rows_inserted || 
           ' U:' || v_rows_updated || ' D:' || v_rows_deleted || '). Batch: ' || v_batch_id;
    
EXCEPTION
    WHEN OTHER THEN
        v_end_time := CURRENT_TIMESTAMP();
        v_error_msg := SQLERRM;
        
        DROP TABLE IF EXISTS _CDC_STAGING_EVENTS_TRAIN_MVMNT_EVENT;
        
        INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (
            TABLE_NAME, BATCH_ID, EXECUTION_STATUS, START_TIME, END_TIME,
            ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED,
            ERROR_MESSAGE, CREATED_AT
        ) VALUES (
            'EVENTS_TRAIN_MVMNT_EVENT', :v_batch_id, 'ERROR', :v_start_time, :v_end_time,
            0, 0, 0, 0,
            :v_error_msg, CURRENT_TIMESTAMP()
        );
        
        RETURN 'ERROR: ' || v_error_msg || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
END;
$$;

-- =============================================================================
-- STEP 5: Create Scheduled Task to Process CDC Data
-- =============================================================================
CREATE OR REPLACE TASK D_RAW.AWS.TASK_SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Task to process EVENTS_TRAIN_MVMNT_EVENT_BASE CDC changes into data preservation table'
AS
    CALL D_RAW.AWS.SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT();

ALTER TASK D_RAW.AWS.TASK_SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT RESUME;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- SHOW TABLES LIKE 'EVENTS_TRAIN_MVMNT_EVENT%' IN SCHEMA D_BRONZE.AWS;
-- SHOW STREAMS LIKE 'EVENTS_TRAIN_MVMNT_EVENT%' IN SCHEMA D_RAW.AWS;
-- SHOW TASKS LIKE 'TASK_PROCESS_EVENTS_TRAIN%' IN SCHEMA D_RAW.AWS;
-- CALL D_RAW.AWS.SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT();
