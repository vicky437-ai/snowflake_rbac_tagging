/*
================================================================================
METADATA-DRIVEN DATA PRESERVATION FRAMEWORK FOR INFORMATICA IDMC CDC TABLES
================================================================================
Purpose: Protect historical data from IDMC job redeployment truncation events
Pattern: _BASE Table -> Stream -> Task/SP -> Data Preservation Table

Key Features:
1. Metadata-driven: Register 20+ tables in config, auto-generates everything
2. Initial Load: SHOW_INITIAL_ROWS=TRUE for first-time capture
3. CDC Processing: INSERT, UPDATE, DELETE via stream metadata
4. Truncate Recovery: Auto-detects stale streams and recovers
5. Soft Delete: IS_DELETED flag preserves history
6. One Generic SP: Works for any table based on metadata
================================================================================
*/

-- =============================================================================
-- SECTION 1: FRAMEWORK SETUP
-- =============================================================================
USE ROLE D-SNW-DEVBI1-ETL;
USE WAREHOUSE INFA_INGEST_WH;

CREATE DATABASE IF NOT EXISTS CDC_PRESERVATION;
CREATE SCHEMA IF NOT EXISTS CDC_PRESERVATION.CONFIG;
CREATE SCHEMA IF NOT EXISTS CDC_PRESERVATION.PROCESSING;
CREATE SCHEMA IF NOT EXISTS CDC_PRESERVATION.MONITORING;

-- =============================================================================
-- SECTION 2: METADATA CONFIGURATION TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS CDC_PRESERVATION.CONFIG.TABLE_CONFIG (
    CONFIG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    
    -- Source _BASE table info
    SOURCE_DATABASE VARCHAR(255) NOT NULL,
    SOURCE_SCHEMA VARCHAR(255) NOT NULL,
    SOURCE_TABLE VARCHAR(255) NOT NULL,          -- e.g., OPTRN_LEG_BASE
    
    -- Target preservation table info
    TARGET_DATABASE VARCHAR(255) NOT NULL,
    TARGET_SCHEMA VARCHAR(255) NOT NULL,
    TARGET_TABLE VARCHAR(255) NOT NULL,          -- e.g., OPTRN_LEG
    
    -- Primary key column(s) - comma-separated for composite keys
    PRIMARY_KEY_COLUMNS VARCHAR(4000) NOT NULL,  -- e.g., 'OPTRN_LEG_ID' or 'COL1,COL2'
    
    -- Auto-generated names (populated by setup SP)
    STREAM_NAME VARCHAR(255),
    TASK_NAME VARCHAR(255),
    
    -- Task configuration
    TASK_WAREHOUSE VARCHAR(255) DEFAULT 'INFA_INGEST_WH',
    TASK_SCHEDULE VARCHAR(100) DEFAULT '5 MINUTE',
    TASK_TIMEOUT_MS NUMBER DEFAULT 3600000,
    TASK_MAX_FAILURES NUMBER DEFAULT 3,
    
    -- Stream configuration
    DATA_RETENTION_DAYS NUMBER DEFAULT 14,
    
    -- Status
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    PRIORITY NUMBER DEFAULT 100,
    
    -- Audit
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(255) DEFAULT CURRENT_USER(),
    NOTES VARCHAR(4000),
    
    CONSTRAINT UK_SOURCE UNIQUE (SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE)
);

-- =============================================================================
-- SECTION 3: PROCESSING LOG TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS CDC_PRESERVATION.MONITORING.PROCESSING_LOG (
    LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    CONFIG_ID NUMBER,
    BATCH_ID VARCHAR(100),
    SOURCE_TABLE_FQN VARCHAR(1000),
    TARGET_TABLE_FQN VARCHAR(1000),
    PROCESS_TYPE VARCHAR(50),               -- NORMAL, RECOVERY, INITIAL_LOAD
    PROCESS_START_TIME TIMESTAMP_LTZ,
    PROCESS_END_TIME TIMESTAMP_LTZ,
    ROWS_INSERTED NUMBER DEFAULT 0,
    ROWS_UPDATED NUMBER DEFAULT 0,
    ROWS_DELETED NUMBER DEFAULT 0,
    ROWS_PROCESSED NUMBER DEFAULT 0,
    STATUS VARCHAR(20),                     -- SUCCESS, FAILED, NO_DATA, RECOVERED
    ERROR_MESSAGE VARCHAR(16000),
    EXECUTION_TIME_SECONDS NUMBER(10,2),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- SECTION 4: STREAM STATUS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS CDC_PRESERVATION.MONITORING.STREAM_STATUS (
    STATUS_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    CONFIG_ID NUMBER NOT NULL,
    STREAM_FQN VARCHAR(1000),
    IS_STALE BOOLEAN DEFAULT FALSE,
    STALE_DETECTED_AT TIMESTAMP_LTZ,
    LAST_RECOVERY_AT TIMESTAMP_LTZ,
    RECOVERY_COUNT NUMBER DEFAULT 0,
    LAST_CHECKED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- SECTION 5: GENERIC CDC PROCESSING STORED PROCEDURE (METADATA-DRIVEN)
-- =============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(
    P_CONFIG_ID NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    -- Configuration variables
    v_source_db VARCHAR;
    v_source_schema VARCHAR;
    v_source_table VARCHAR;
    v_target_db VARCHAR;
    v_target_schema VARCHAR;
    v_target_table VARCHAR;
    v_pk_columns VARCHAR;
    v_stream_name VARCHAR;
    
    -- Fully qualified names
    v_source_fqn VARCHAR;
    v_target_fqn VARCHAR;
    v_stream_fqn VARCHAR;
    
    -- Processing variables
    v_batch_id VARCHAR;
    v_start_time TIMESTAMP_LTZ;
    v_stream_stale BOOLEAN DEFAULT FALSE;
    v_staging_count NUMBER DEFAULT 0;
    v_rows_processed NUMBER DEFAULT 0;
    v_process_type VARCHAR DEFAULT 'NORMAL';
    v_error_msg VARCHAR;
    v_exec_secs NUMBER;
    
    -- Dynamic SQL holders
    v_col_list VARCHAR;
    v_pk_join_on VARCHAR;
    v_pk_join_src VARCHAR;
    v_update_set VARCHAR;
    v_insert_cols VARCHAR;
    v_insert_vals VARCHAR;
    v_staging_sql VARCHAR;
    v_merge_sql VARCHAR;
    v_recovery_sql VARCHAR;
BEGIN
    v_batch_id := 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    v_start_time := CURRENT_TIMESTAMP();
    
    -- =========================================================================
    -- STEP 1: Load configuration from metadata table
    -- =========================================================================
    SELECT 
        SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE,
        TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE,
        PRIMARY_KEY_COLUMNS,
        COALESCE(STREAM_NAME, SOURCE_TABLE || '_STREAM')
    INTO 
        v_source_db, v_source_schema, v_source_table,
        v_target_db, v_target_schema, v_target_table,
        v_pk_columns, v_stream_name
    FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG
    WHERE CONFIG_ID = :P_CONFIG_ID AND IS_ACTIVE = TRUE;
    
    v_source_fqn := v_source_db || '.' || v_source_schema || '.' || v_source_table;
    v_target_fqn := v_target_db || '.' || v_target_schema || '.' || v_target_table;
    v_stream_fqn := v_source_db || '.' || v_source_schema || '.' || v_stream_name;
    
    -- =========================================================================
    -- STEP 2: Build dynamic column lists from source table metadata
    -- =========================================================================
    -- Get all columns from source table
    LET col_sql VARCHAR := 'SELECT LISTAGG(COLUMN_NAME, '','') WITHIN GROUP (ORDER BY ORDINAL_POSITION) 
        FROM ' || v_source_db || '.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = ''' || v_source_schema || ''' 
        AND TABLE_NAME = ''' || v_source_table || '''';
    EXECUTE IMMEDIATE col_sql;
    LET rs1 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur1 CURSOR FOR rs1;
    OPEN cur1;
    FETCH cur1 INTO v_col_list;
    CLOSE cur1;
    
    -- Build PK join condition: tgt.PK1 = src.PK1 AND tgt.PK2 = src.PK2
    LET pk_sql VARCHAR := 'SELECT LISTAGG(''tgt.'' || TRIM(VALUE) || '' = src.'' || TRIM(VALUE), '' AND '') 
        FROM TABLE(SPLIT_TO_TABLE(''' || v_pk_columns || ''', '',''))';
    EXECUTE IMMEDIATE pk_sql;
    LET rs2 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur2 CURSOR FOR rs2;
    OPEN cur2;
    FETCH cur2 INTO v_pk_join_on;
    CLOSE cur2;
    
    -- Build UPDATE SET clause (exclude PK columns and CDC metadata)
    LET upd_sql VARCHAR := 'SELECT LISTAGG(''tgt.'' || TRIM(VALUE) || '' = src.'' || TRIM(VALUE), '', '') 
        FROM TABLE(SPLIT_TO_TABLE(''' || v_col_list || ''', '','')) 
        WHERE TRIM(VALUE) NOT IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(''' || v_pk_columns || ''', '','')))';
    EXECUTE IMMEDIATE upd_sql;
    LET rs3 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur3 CURSOR FOR rs3;
    OPEN cur3;
    FETCH cur3 INTO v_update_set;
    CLOSE cur3;
    
    -- Build INSERT columns and values
    v_insert_cols := v_col_list || ', CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID';
    v_insert_vals := 'src.' || REPLACE(v_col_list, ',', ', src.') || ', ''INSERT'', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''' || v_batch_id || '''';
    
    -- =========================================================================
    -- STEP 3: Check if stream is stale (happens after IDMC truncate/reload)
    -- =========================================================================
    BEGIN
        LET stale_check VARCHAR := 'SELECT COUNT(*) FROM ' || v_stream_fqn || ' WHERE 1=0';
        EXECUTE IMMEDIATE stale_check;
        v_stream_stale := FALSE;
    EXCEPTION
        WHEN OTHER THEN
            v_stream_stale := TRUE;
            v_error_msg := SQLERRM;
    END;
    
    -- =========================================================================
    -- STEP 4: RECOVERY MODE - If stream is stale, recreate and do differential load
    -- =========================================================================
    IF (v_stream_stale = TRUE) THEN
        v_process_type := 'RECOVERY';
        
        -- Log stale detection
        UPDATE CDC_PRESERVATION.MONITORING.STREAM_STATUS
        SET IS_STALE = TRUE, STALE_DETECTED_AT = CURRENT_TIMESTAMP(), LAST_CHECKED_AT = CURRENT_TIMESTAMP()
        WHERE CONFIG_ID = :P_CONFIG_ID;
        
        -- Recreate stream
        LET recreate_stream VARCHAR := 'CREATE OR REPLACE STREAM ' || v_stream_fqn || 
            ' ON TABLE ' || v_source_fqn || 
            ' SHOW_INITIAL_ROWS = TRUE ' ||
            ' COMMENT = ''Recreated after stale detection at ' || CURRENT_TIMESTAMP()::VARCHAR || '''';
        EXECUTE IMMEDIATE recreate_stream;
        
        -- Build PK join for recovery (using src alias from subquery)
        LET pk_recovery VARCHAR := 'SELECT LISTAGG(''src.'' || TRIM(VALUE) || '' = tgt.'' || TRIM(VALUE), '' AND '') 
            FROM TABLE(SPLIT_TO_TABLE(''' || v_pk_columns || ''', '',''))';
        EXECUTE IMMEDIATE pk_recovery;
        LET rs4 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        LET cur4 CURSOR FOR rs4;
        OPEN cur4;
        FETCH cur4 INTO v_pk_join_src;
        CLOSE cur4;
        
        -- Differential MERGE: Insert new records, resurrect soft-deleted records
        v_recovery_sql := 'MERGE INTO ' || v_target_fqn || ' AS tgt
            USING (
                SELECT src.*, ''' || v_batch_id || ''' AS BATCH_ID
                FROM ' || v_source_fqn || ' src
                LEFT JOIN ' || v_target_fqn || ' tgt ON ' || v_pk_join_src || '
                WHERE tgt.' || SPLIT_PART(v_pk_columns, ',', 1) || ' IS NULL
                   OR tgt.IS_DELETED = TRUE
            ) AS src
            ON ' || v_pk_join_on || '
            WHEN MATCHED THEN UPDATE SET
                ' || v_update_set || ',
                tgt.CDC_OPERATION = ''RELOADED'',
                tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
                tgt.IS_DELETED = FALSE,
                tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
                tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
            WHEN NOT MATCHED THEN INSERT (' || v_insert_cols || ')
                VALUES (' || v_insert_vals || ')';
        
        EXECUTE IMMEDIATE v_recovery_sql;
        v_rows_processed := SQLROWCOUNT;
        
        -- Update stream status
        MERGE INTO CDC_PRESERVATION.MONITORING.STREAM_STATUS ss
        USING (SELECT :P_CONFIG_ID AS CID) src ON ss.CONFIG_ID = src.CID
        WHEN MATCHED THEN UPDATE SET 
            IS_STALE = FALSE, 
            LAST_RECOVERY_AT = CURRENT_TIMESTAMP(), 
            RECOVERY_COUNT = RECOVERY_COUNT + 1,
            LAST_CHECKED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (CONFIG_ID, STREAM_FQN, LAST_RECOVERY_AT, RECOVERY_COUNT, LAST_CHECKED_AT)
            VALUES (src.CID, v_stream_fqn, CURRENT_TIMESTAMP(), 1, CURRENT_TIMESTAMP());
        
        v_exec_secs := DATEDIFF('SECOND', v_start_time, CURRENT_TIMESTAMP());
        
        -- Log recovery
        INSERT INTO CDC_PRESERVATION.MONITORING.PROCESSING_LOG 
            (CONFIG_ID, BATCH_ID, SOURCE_TABLE_FQN, TARGET_TABLE_FQN, PROCESS_TYPE,
             PROCESS_START_TIME, PROCESS_END_TIME, ROWS_PROCESSED, STATUS, EXECUTION_TIME_SECONDS)
        VALUES (:P_CONFIG_ID, :v_batch_id, :v_source_fqn, :v_target_fqn, 'RECOVERY',
                :v_start_time, CURRENT_TIMESTAMP(), :v_rows_processed, 'RECOVERED', :v_exec_secs);
        
        RETURN 'RECOVERY_COMPLETE: Stream recreated, ' || v_rows_processed || ' rows merged. Batch: ' || v_batch_id;
    END IF;
    
    -- =========================================================================
    -- STEP 5: NORMAL MODE - Stage stream data into temp table
    -- =========================================================================
    v_staging_sql := 'CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_' || P_CONFIG_ID || ' AS
        SELECT ' || v_col_list || ',
               METADATA$ACTION AS CDC_ACTION,
               METADATA$ISUPDATE AS CDC_IS_UPDATE,
               METADATA$ROW_ID AS ROW_ID
        FROM ' || v_stream_fqn;
    
    EXECUTE IMMEDIATE v_staging_sql;
    
    LET cnt_sql VARCHAR := 'SELECT COUNT(*) FROM _CDC_STAGING_' || P_CONFIG_ID;
    EXECUTE IMMEDIATE cnt_sql;
    LET cnt_rs RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cnt_cur CURSOR FOR cnt_rs;
    OPEN cnt_cur;
    FETCH cnt_cur INTO v_staging_count;
    CLOSE cnt_cur;
    
    IF (v_staging_count = 0) THEN
        EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS _CDC_STAGING_' || P_CONFIG_ID;
        RETURN 'NO_DATA: Stream has no changes. Table: ' || v_source_fqn;
    END IF;
    
    -- =========================================================================
    -- STEP 6: MERGE CDC changes from staging into Data Preservation table
    -- =========================================================================
    v_merge_sql := 'MERGE INTO ' || v_target_fqn || ' AS tgt
        USING (
            SELECT *, ''' || v_batch_id || ''' AS BATCH_ID
            FROM _CDC_STAGING_' || P_CONFIG_ID || '
        ) AS src
        ON ' || v_pk_join_on || '
        
        -- UPDATE: METADATA$ACTION=INSERT with METADATA$ISUPDATE=TRUE
        WHEN MATCHED AND src.CDC_ACTION = ''INSERT'' AND src.CDC_IS_UPDATE = TRUE THEN 
            UPDATE SET
                ' || v_update_set || ',
                tgt.CDC_OPERATION = ''UPDATE'',
                tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
                tgt.IS_DELETED = FALSE,
                tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
                tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        
        -- DELETE: METADATA$ACTION=DELETE with METADATA$ISUPDATE=FALSE (soft delete)
        WHEN MATCHED AND src.CDC_ACTION = ''DELETE'' AND src.CDC_IS_UPDATE = FALSE THEN 
            UPDATE SET
                tgt.CDC_OPERATION = ''DELETE'',
                tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
                tgt.IS_DELETED = TRUE,
                tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
                tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        
        -- RE-INSERT: Record exists but new INSERT came (upsert behavior)
        WHEN MATCHED AND src.CDC_ACTION = ''INSERT'' AND src.CDC_IS_UPDATE = FALSE THEN
            UPDATE SET
                ' || v_update_set || ',
                tgt.CDC_OPERATION = ''INSERT'',
                tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
                tgt.IS_DELETED = FALSE,
                tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
                tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
        
        -- NEW INSERT: Record does not exist
        WHEN NOT MATCHED AND src.CDC_ACTION = ''INSERT'' THEN 
            INSERT (' || v_insert_cols || ')
            VALUES (' || v_insert_vals || ')';
    
    EXECUTE IMMEDIATE v_merge_sql;
    v_rows_processed := SQLROWCOUNT;
    
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS _CDC_STAGING_' || P_CONFIG_ID;
    
    v_exec_secs := DATEDIFF('SECOND', v_start_time, CURRENT_TIMESTAMP());
    
    -- Log success
    INSERT INTO CDC_PRESERVATION.MONITORING.PROCESSING_LOG 
        (CONFIG_ID, BATCH_ID, SOURCE_TABLE_FQN, TARGET_TABLE_FQN, PROCESS_TYPE,
         PROCESS_START_TIME, PROCESS_END_TIME, ROWS_PROCESSED, STATUS, EXECUTION_TIME_SECONDS)
    VALUES (:P_CONFIG_ID, :v_batch_id, :v_source_fqn, :v_target_fqn, 'NORMAL',
            :v_start_time, CURRENT_TIMESTAMP(), :v_rows_processed, 'SUCCESS', :v_exec_secs);
    
    RETURN 'SUCCESS: Processed ' || v_rows_processed || ' CDC changes. Table: ' || v_source_table || '. Batch: ' || v_batch_id;

EXCEPTION
    WHEN OTHER THEN
        v_error_msg := SQLERRM;
        v_exec_secs := DATEDIFF('SECOND', v_start_time, CURRENT_TIMESTAMP());
        
        INSERT INTO CDC_PRESERVATION.MONITORING.PROCESSING_LOG 
            (CONFIG_ID, BATCH_ID, SOURCE_TABLE_FQN, TARGET_TABLE_FQN, PROCESS_TYPE,
             PROCESS_START_TIME, PROCESS_END_TIME, STATUS, ERROR_MESSAGE, EXECUTION_TIME_SECONDS)
        VALUES (:P_CONFIG_ID, :v_batch_id, :v_source_fqn, :v_target_fqn, :v_process_type,
                :v_start_time, CURRENT_TIMESTAMP(), 'FAILED', :v_error_msg, :v_exec_secs);
        
        RETURN 'ERROR: ' || v_error_msg || ' | Table: ' || v_source_fqn;
END;
$$;

-- =============================================================================
-- SECTION 6: TARGET TABLE CREATION PROCEDURE
-- =============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_CREATE_TARGET_TABLE(
    P_CONFIG_ID NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_source_fqn VARCHAR;
    v_target_fqn VARCHAR;
    v_target_db VARCHAR;
    v_target_schema VARCHAR;
    v_pk_columns VARCHAR;
    v_col_defs VARCHAR;
    v_pk_constraint VARCHAR;
    v_create_sql VARCHAR;
BEGIN
    SELECT 
        SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE,
        TARGET_DATABASE || '.' || TARGET_SCHEMA || '.' || TARGET_TABLE,
        TARGET_DATABASE,
        TARGET_SCHEMA,
        PRIMARY_KEY_COLUMNS
    INTO v_source_fqn, v_target_fqn, v_target_db, v_target_schema, v_pk_columns
    FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG
    WHERE CONFIG_ID = :P_CONFIG_ID;
    
    -- Create target schema if not exists
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || v_target_db || '.' || v_target_schema;
    
    -- Get column definitions from source table
    LET col_def_sql VARCHAR := 'SELECT LISTAGG(COLUMN_NAME || '' '' || DATA_TYPE || 
        CASE 
            WHEN DATA_TYPE IN (''NUMBER'', ''DECIMAL'', ''NUMERIC'') AND NUMERIC_PRECISION IS NOT NULL 
                THEN ''('' || NUMERIC_PRECISION || '','' || COALESCE(NUMERIC_SCALE, 0) || '')''
            WHEN DATA_TYPE IN (''VARCHAR'', ''CHAR'', ''STRING'') AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL 
                THEN ''('' || CHARACTER_MAXIMUM_LENGTH || '')''
            ELSE ''''
        END, '', '') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
        FROM ' || SPLIT_PART(v_source_fqn, '.', 1) || '.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = ''' || SPLIT_PART(v_source_fqn, '.', 2) || ''' 
        AND TABLE_NAME = ''' || SPLIT_PART(v_source_fqn, '.', 3) || '''';
    
    EXECUTE IMMEDIATE col_def_sql;
    LET rs RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur CURSOR FOR rs;
    OPEN cur;
    FETCH cur INTO v_col_defs;
    CLOSE cur;
    
    -- Build primary key constraint
    v_pk_constraint := 'PRIMARY KEY (' || v_pk_columns || ')';
    
    -- Create target table with CDC metadata columns
    v_create_sql := 'CREATE TABLE IF NOT EXISTS ' || v_target_fqn || ' (
        ' || v_col_defs || ',
        CDC_OPERATION VARCHAR(10),
        CDC_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        IS_DELETED BOOLEAN DEFAULT FALSE,
        RECORD_CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        RECORD_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        SOURCE_LOAD_BATCH_ID VARCHAR(100),
        ' || v_pk_constraint || '
    )';
    
    EXECUTE IMMEDIATE v_create_sql;
    
    -- Add comment
    EXECUTE IMMEDIATE 'COMMENT ON TABLE ' || v_target_fqn || ' IS ''Data Preservation table for ' || 
        SPLIT_PART(v_source_fqn, '.', 3) || ' - Auto-generated by CDC Framework''';
    
    RETURN 'SUCCESS: Target table created - ' || v_target_fqn;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- SECTION 7: STREAM CREATION PROCEDURE
-- =============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_CREATE_STREAM(
    P_CONFIG_ID NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_source_fqn VARCHAR;
    v_stream_name VARCHAR;
    v_stream_fqn VARCHAR;
    v_source_db VARCHAR;
    v_source_schema VARCHAR;
    v_source_table VARCHAR;
    v_retention_days NUMBER;
BEGIN
    SELECT 
        SOURCE_DATABASE,
        SOURCE_SCHEMA,
        SOURCE_TABLE,
        SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE,
        COALESCE(STREAM_NAME, SOURCE_TABLE || '_STREAM'),
        COALESCE(DATA_RETENTION_DAYS, 14)
    INTO v_source_db, v_source_schema, v_source_table, v_source_fqn, v_stream_name, v_retention_days
    FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG
    WHERE CONFIG_ID = :P_CONFIG_ID;
    
    v_stream_fqn := v_source_db || '.' || v_source_schema || '.' || v_stream_name;
    
    -- Enable change tracking on source table
    EXECUTE IMMEDIATE 'ALTER TABLE ' || v_source_fqn || ' SET CHANGE_TRACKING = TRUE, DATA_RETENTION_TIME_IN_DAYS = ' || v_retention_days;
    
    -- Create stream with SHOW_INITIAL_ROWS for initial load
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM ' || v_stream_fqn || 
        ' ON TABLE ' || v_source_fqn || 
        ' SHOW_INITIAL_ROWS = TRUE' ||
        ' COMMENT = ''CDC Stream for data preservation - Config ID: ' || P_CONFIG_ID || '''';
    
    -- Update config with stream name
    UPDATE CDC_PRESERVATION.CONFIG.TABLE_CONFIG 
    SET STREAM_NAME = :v_stream_name, UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE CONFIG_ID = :P_CONFIG_ID;
    
    -- Initialize stream status
    MERGE INTO CDC_PRESERVATION.MONITORING.STREAM_STATUS ss
    USING (SELECT :P_CONFIG_ID AS CID, :v_stream_fqn AS SFQN) src ON ss.CONFIG_ID = src.CID
    WHEN NOT MATCHED THEN INSERT (CONFIG_ID, STREAM_FQN, LAST_CHECKED_AT)
        VALUES (src.CID, src.SFQN, CURRENT_TIMESTAMP());
    
    RETURN 'SUCCESS: Stream created - ' || v_stream_fqn;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- SECTION 8: TASK CREATION PROCEDURE (ONE TASK PER TABLE)
-- =============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_CREATE_TASK(
    P_CONFIG_ID NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_task_name VARCHAR;
    v_task_fqn VARCHAR;
    v_stream_name VARCHAR;
    v_stream_fqn VARCHAR;
    v_source_db VARCHAR;
    v_source_schema VARCHAR;
    v_source_table VARCHAR;
    v_warehouse VARCHAR;
    v_schedule VARCHAR;
    v_timeout_ms NUMBER;
    v_max_failures NUMBER;
BEGIN
    SELECT 
        SOURCE_DATABASE,
        SOURCE_SCHEMA,
        SOURCE_TABLE,
        COALESCE(TASK_NAME, 'TASK_' || SOURCE_TABLE || '_CDC'),
        COALESCE(STREAM_NAME, SOURCE_TABLE || '_STREAM'),
        COALESCE(TASK_WAREHOUSE, 'INFA_INGEST_WH'),
        COALESCE(TASK_SCHEDULE, '5 MINUTE'),
        COALESCE(TASK_TIMEOUT_MS, 3600000),
        COALESCE(TASK_MAX_FAILURES, 3)
    INTO v_source_db, v_source_schema, v_source_table, v_task_name, v_stream_name, 
         v_warehouse, v_schedule, v_timeout_ms, v_max_failures
    FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG
    WHERE CONFIG_ID = :P_CONFIG_ID;
    
    v_task_fqn := v_source_db || '.' || v_source_schema || '.' || v_task_name;
    v_stream_fqn := v_source_db || '.' || v_source_schema || '.' || v_stream_name;
    
    -- Create task
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TASK ' || v_task_fqn || '
            WAREHOUSE = ' || v_warehouse || '
            SCHEDULE = ''' || v_schedule || '''
            ALLOW_OVERLAPPING_EXECUTION = FALSE
            SUSPEND_TASK_AFTER_NUM_FAILURES = ' || v_max_failures || '
            USER_TASK_TIMEOUT_MS = ' || v_timeout_ms || '
            COMMENT = ''CDC Task for ' || v_source_table || ' data preservation - Config ID: ' || P_CONFIG_ID || '''
        WHEN
            SYSTEM$STREAM_HAS_DATA(''' || v_stream_fqn || ''')
        AS
            CALL CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(' || P_CONFIG_ID || ')';
    
    -- Update config with task name
    UPDATE CDC_PRESERVATION.CONFIG.TABLE_CONFIG 
    SET TASK_NAME = :v_task_name, UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE CONFIG_ID = :P_CONFIG_ID;
    
    RETURN 'SUCCESS: Task created (SUSPENDED) - ' || v_task_fqn;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- SECTION 9: TASK MANAGEMENT PROCEDURES
-- =============================================================================

-- Resume single task
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_RESUME_TASK(P_CONFIG_ID NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_task_fqn VARCHAR;
BEGIN
    SELECT SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || COALESCE(TASK_NAME, 'TASK_' || SOURCE_TABLE || '_CDC')
    INTO v_task_fqn
    FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG
    WHERE CONFIG_ID = :P_CONFIG_ID;
    
    EXECUTE IMMEDIATE 'ALTER TASK ' || v_task_fqn || ' RESUME';
    RETURN 'SUCCESS: Task resumed - ' || v_task_fqn;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- Suspend single task
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_SUSPEND_TASK(P_CONFIG_ID NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_task_fqn VARCHAR;
BEGIN
    SELECT SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || COALESCE(TASK_NAME, 'TASK_' || SOURCE_TABLE || '_CDC')
    INTO v_task_fqn
    FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG
    WHERE CONFIG_ID = :P_CONFIG_ID;
    
    EXECUTE IMMEDIATE 'ALTER TASK ' || v_task_fqn || ' SUSPEND';
    RETURN 'SUCCESS: Task suspended - ' || v_task_fqn;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- Resume all tasks
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_RESUME_ALL_TASKS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_results ARRAY := ARRAY_CONSTRUCT();
    c_configs CURSOR FOR SELECT CONFIG_ID, SOURCE_TABLE FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE IS_ACTIVE = TRUE;
BEGIN
    FOR rec IN c_configs DO
        BEGIN
            CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_TASK(rec.CONFIG_ID);
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('config_id', rec.CONFIG_ID, 'table', rec.SOURCE_TABLE, 'status', 'RESUMED'));
        EXCEPTION
            WHEN OTHER THEN
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('config_id', rec.CONFIG_ID, 'table', rec.SOURCE_TABLE, 'status', 'FAILED', 'error', SQLERRM));
        END;
    END FOR;
    RETURN OBJECT_CONSTRUCT('tasks_processed', ARRAY_SIZE(v_results), 'details', v_results);
END;
$$;

-- Suspend all tasks
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_SUSPEND_ALL_TASKS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_results ARRAY := ARRAY_CONSTRUCT();
    c_configs CURSOR FOR SELECT CONFIG_ID, SOURCE_TABLE FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE IS_ACTIVE = TRUE;
BEGIN
    FOR rec IN c_configs DO
        BEGIN
            CALL CDC_PRESERVATION.PROCESSING.SP_SUSPEND_TASK(rec.CONFIG_ID);
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('config_id', rec.CONFIG_ID, 'table', rec.SOURCE_TABLE, 'status', 'SUSPENDED'));
        EXCEPTION
            WHEN OTHER THEN
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('config_id', rec.CONFIG_ID, 'table', rec.SOURCE_TABLE, 'status', 'FAILED', 'error', SQLERRM));
        END;
    END FOR;
    RETURN OBJECT_CONSTRUCT('tasks_processed', ARRAY_SIZE(v_results), 'details', v_results);
END;
$$;

-- =============================================================================
-- SECTION 10: FULL PIPELINE SETUP (ONE-CLICK)
-- =============================================================================

-- Setup single table pipeline
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_SETUP_PIPELINE(
    P_CONFIG_ID NUMBER,
    P_AUTO_START BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_target_result VARCHAR;
    v_stream_result VARCHAR;
    v_task_result VARCHAR;
    v_source_table VARCHAR;
BEGIN
    SELECT SOURCE_TABLE INTO v_source_table
    FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE CONFIG_ID = :P_CONFIG_ID;
    
    -- Step 1: Create target table
    CALL CDC_PRESERVATION.PROCESSING.SP_CREATE_TARGET_TABLE(:P_CONFIG_ID);
    
    -- Step 2: Create stream
    CALL CDC_PRESERVATION.PROCESSING.SP_CREATE_STREAM(:P_CONFIG_ID);
    
    -- Step 3: Create task
    CALL CDC_PRESERVATION.PROCESSING.SP_CREATE_TASK(:P_CONFIG_ID);
    
    -- Step 4: Optionally start task
    IF (P_AUTO_START) THEN
        CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_TASK(:P_CONFIG_ID);
    END IF;
    
    RETURN OBJECT_CONSTRUCT(
        'config_id', P_CONFIG_ID,
        'source_table', v_source_table,
        'target_table', 'CREATED',
        'stream', 'CREATED',
        'task', IFF(P_AUTO_START, 'RUNNING', 'SUSPENDED'),
        'status', 'SUCCESS'
    );
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT(
            'config_id', P_CONFIG_ID,
            'status', 'FAILED',
            'error', SQLERRM
        );
END;
$$;

-- Setup ALL table pipelines
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_SETUP_ALL_PIPELINES(
    P_AUTO_START BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_results ARRAY := ARRAY_CONSTRUCT();
    v_pipeline_result VARIANT;
    c_configs CURSOR FOR SELECT CONFIG_ID, SOURCE_TABLE FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE IS_ACTIVE = TRUE ORDER BY PRIORITY;
BEGIN
    FOR rec IN c_configs DO
        CALL CDC_PRESERVATION.PROCESSING.SP_SETUP_PIPELINE(rec.CONFIG_ID, :P_AUTO_START);
        v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT(
            'config_id', rec.CONFIG_ID,
            'table', rec.SOURCE_TABLE,
            'status', 'SETUP_COMPLETE'
        ));
    END FOR;
    
    RETURN OBJECT_CONSTRUCT(
        'total_pipelines', ARRAY_SIZE(v_results),
        'auto_start', P_AUTO_START,
        'details', v_results
    );
END;
$$;

-- =============================================================================
-- SECTION 11: MONITORING VIEWS
-- =============================================================================

-- Pipeline status dashboard
CREATE OR REPLACE VIEW CDC_PRESERVATION.MONITORING.V_PIPELINE_STATUS AS
SELECT 
    c.CONFIG_ID,
    c.SOURCE_DATABASE || '.' || c.SOURCE_SCHEMA || '.' || c.SOURCE_TABLE AS SOURCE_TABLE,
    c.TARGET_DATABASE || '.' || c.TARGET_SCHEMA || '.' || c.TARGET_TABLE AS TARGET_TABLE,
    c.SOURCE_DATABASE || '.' || c.SOURCE_SCHEMA || '.' || COALESCE(c.STREAM_NAME, c.SOURCE_TABLE || '_STREAM') AS STREAM_FQN,
    c.SOURCE_DATABASE || '.' || c.SOURCE_SCHEMA || '.' || COALESCE(c.TASK_NAME, 'TASK_' || c.SOURCE_TABLE || '_CDC') AS TASK_FQN,
    c.PRIMARY_KEY_COLUMNS,
    c.TASK_SCHEDULE,
    c.IS_ACTIVE,
    ss.IS_STALE AS STREAM_STALE,
    ss.RECOVERY_COUNT,
    ss.LAST_RECOVERY_AT,
    l.LAST_RUN,
    l.LAST_STATUS,
    l.LAST_ROWS_PROCESSED
FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG c
LEFT JOIN CDC_PRESERVATION.MONITORING.STREAM_STATUS ss ON c.CONFIG_ID = ss.CONFIG_ID
LEFT JOIN (
    SELECT CONFIG_ID, 
           MAX(PROCESS_END_TIME) AS LAST_RUN,
           MAX_BY(STATUS, LOG_ID) AS LAST_STATUS,
           MAX_BY(ROWS_PROCESSED, LOG_ID) AS LAST_ROWS_PROCESSED
    FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG
    GROUP BY CONFIG_ID
) l ON c.CONFIG_ID = l.CONFIG_ID;

-- Processing statistics
CREATE OR REPLACE VIEW CDC_PRESERVATION.MONITORING.V_PROCESSING_STATS AS
SELECT 
    c.SOURCE_TABLE,
    COUNT(*) AS TOTAL_RUNS,
    SUM(CASE WHEN p.STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESS_COUNT,
    SUM(CASE WHEN p.STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_COUNT,
    SUM(CASE WHEN p.STATUS = 'RECOVERED' THEN 1 ELSE 0 END) AS RECOVERY_COUNT,
    SUM(p.ROWS_PROCESSED) AS TOTAL_ROWS_PROCESSED,
    ROUND(AVG(p.EXECUTION_TIME_SECONDS), 2) AS AVG_EXEC_SECS,
    MAX(p.PROCESS_END_TIME) AS LAST_RUN
FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG c
JOIN CDC_PRESERVATION.MONITORING.PROCESSING_LOG p ON c.CONFIG_ID = p.CONFIG_ID
GROUP BY c.SOURCE_TABLE;

-- Recent errors
CREATE OR REPLACE VIEW CDC_PRESERVATION.MONITORING.V_RECENT_ERRORS AS
SELECT 
    c.SOURCE_TABLE,
    p.BATCH_ID,
    p.ERROR_MESSAGE,
    p.PROCESS_START_TIME,
    p.PROCESS_END_TIME
FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG p
JOIN CDC_PRESERVATION.CONFIG.TABLE_CONFIG c ON p.CONFIG_ID = c.CONFIG_ID
WHERE p.STATUS = 'FAILED'
ORDER BY p.CREATED_AT DESC
LIMIT 50;

-- =============================================================================
-- SECTION 12: EXAMPLE - REGISTER YOUR 20+ TABLES
-- =============================================================================

/*
================================================================================
USAGE INSTRUCTIONS FOR 20+ TABLES
================================================================================

-- STEP 1: Register all your _BASE tables in the config
-- Adjust SOURCE/TARGET database/schema as needed

INSERT INTO CDC_PRESERVATION.CONFIG.TABLE_CONFIG 
(SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, PRIMARY_KEY_COLUMNS, TASK_SCHEDULE)
VALUES
-- SADB Tables
('D_BRONZE', 'SADB', 'OPTRN_LEG_BASE', 'D_BRONZE', 'SADB', 'OPTRN_LEG', 'OPTRN_LEG_ID', '5 MINUTE'),
('D_BRONZE', 'SADB', 'OPTRN_BASE', 'D_BRONZE', 'SADB', 'OPTRN', 'OPTRN_ID', '5 MINUTE'),
('D_BRONZE', 'SADB', 'TRADE_BASE', 'D_BRONZE', 'SADB', 'TRADE', 'TRADE_ID', '5 MINUTE'),
('D_BRONZE', 'SADB', 'POSITION_BASE', 'D_BRONZE', 'SADB', 'POSITION', 'POSITION_ID', '5 MINUTE'),
('D_BRONZE', 'SADB', 'INSTRUMENT_BASE', 'D_BRONZE', 'SADB', 'INSTRUMENT', 'INSTRUMENT_ID', '10 MINUTE'),
('D_BRONZE', 'SADB', 'COUNTERPARTY_BASE', 'D_BRONZE', 'SADB', 'COUNTERPARTY', 'COUNTERPARTY_ID', '10 MINUTE'),
('D_BRONZE', 'SADB', 'PORTFOLIO_BASE', 'D_BRONZE', 'SADB', 'PORTFOLIO', 'PORTFOLIO_ID', '15 MINUTE'),
('D_BRONZE', 'SADB', 'ACCOUNT_BASE', 'D_BRONZE', 'SADB', 'ACCOUNT', 'ACCOUNT_ID', '10 MINUTE'),
('D_BRONZE', 'SADB', 'CASHFLOW_BASE', 'D_BRONZE', 'SADB', 'CASHFLOW', 'CASHFLOW_ID', '5 MINUTE'),
('D_BRONZE', 'SADB', 'SETTLEMENT_BASE', 'D_BRONZE', 'SADB', 'SETTLEMENT', 'SETTLEMENT_ID', '5 MINUTE'),

-- Additional schemas
('D_BRONZE', 'RISK', 'VAR_RESULTS_BASE', 'D_BRONZE', 'RISK', 'VAR_RESULTS', 'VAR_ID', '15 MINUTE'),
('D_BRONZE', 'RISK', 'EXPOSURE_BASE', 'D_BRONZE', 'RISK', 'EXPOSURE', 'EXPOSURE_ID', '10 MINUTE'),
('D_BRONZE', 'MARKET', 'PRICE_BASE', 'D_BRONZE', 'MARKET', 'PRICE', 'PRICE_ID', '1 MINUTE'),
('D_BRONZE', 'MARKET', 'RATE_BASE', 'D_BRONZE', 'MARKET', 'RATE', 'RATE_ID', '5 MINUTE'),
('D_BRONZE', 'REF', 'CURRENCY_BASE', 'D_BRONZE', 'REF', 'CURRENCY', 'CURRENCY_ID', '30 MINUTE'),
('D_BRONZE', 'REF', 'CALENDAR_BASE', 'D_BRONZE', 'REF', 'CALENDAR', 'CALENDAR_ID', '1 HOUR'),

-- Composite primary key example
('D_BRONZE', 'SADB', 'ORDER_FILL_BASE', 'D_BRONZE', 'SADB', 'ORDER_FILL', 'ORDER_ID,FILL_ID', '5 MINUTE'),
('D_BRONZE', 'SADB', 'TRADE_ALLOC_BASE', 'D_BRONZE', 'SADB', 'TRADE_ALLOC', 'TRADE_ID,ALLOC_ID', '5 MINUTE');

-- Add more tables as needed...

-- STEP 2: Setup ALL pipelines (creates target tables, streams, tasks)
CALL CDC_PRESERVATION.PROCESSING.SP_SETUP_ALL_PIPELINES(FALSE);

-- STEP 3: Start ALL tasks
CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_ALL_TASKS();

-- Or start individual tables:
CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_TASK(1);  -- OPTRN_LEG
CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_TASK(2);  -- OPTRN
-- etc.

-- STEP 4: Monitor
SELECT * FROM CDC_PRESERVATION.MONITORING.V_PIPELINE_STATUS;
SELECT * FROM CDC_PRESERVATION.MONITORING.V_PROCESSING_STATS;
SELECT * FROM CDC_PRESERVATION.MONITORING.V_RECENT_ERRORS;

-- Check task execution history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP())
))
WHERE NAME LIKE 'TASK_%_CDC'
ORDER BY SCHEDULED_TIME DESC
LIMIT 50;

================================================================================
*/
