/*
================================================================================
BASE TABLE DATA CLEANUP FRAMEWORK - PRODUCTION READY
================================================================================
Purpose:     Automated cleanup of data older than N days from all _BASE tables
Pattern:     Metadata-driven, parameterized for multi-environment deployment
Schedule:    Daily execution via Snowflake Task (2 AM UTC default)
Tested:      2026-02-16 - VALIDATED with D_BRONZE.SALES schema
================================================================================

USAGE:
1. Deploy framework (run Sections 1-13)
2. Add configuration: INSERT INTO CLEANUP_CONFIG (DATABASE_NAME, SCHEMA_NAME, DATE_COLUMN) VALUES ('DB', 'SCHEMA', 'DATE_COL');
3. Preview cleanup: CALL SP_CLEANUP_DRY_RUN('DB', 'SCHEMA', 'DATE_COL', 45);
4. Execute cleanup: CALL SP_CLEANUP_SCHEMA('DB', 'SCHEMA', 'DATE_COL', 45);
5. Create task: CALL SP_CREATE_MASTER_CLEANUP_TASK();
6. Resume task: ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS RESUME;

================================================================================
*/

-- ============================================================================
-- SECTION 1: FRAMEWORK SETUP
-- ============================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE SCHEMA IF NOT EXISTS CDC_PRESERVATION.CLEANUP;

-- ============================================================================
-- SECTION 2: CONFIGURATION TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG (
    CONFIG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    DATABASE_NAME VARCHAR(255) NOT NULL,
    SCHEMA_NAME VARCHAR(255) NOT NULL,
    TABLE_PATTERN VARCHAR(255) DEFAULT '%_BASE',
    DATE_COLUMN VARCHAR(255) NOT NULL,
    RETENTION_DAYS NUMBER DEFAULT 45,
    BATCH_SIZE NUMBER DEFAULT 100000,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    TASK_SCHEDULE VARCHAR(100) DEFAULT 'USING CRON 0 2 * * * UTC',
    TASK_WAREHOUSE VARCHAR(255) DEFAULT 'COMPUTE_WH',
    LAST_CLEANUP_AT TIMESTAMP_LTZ,
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(255) DEFAULT CURRENT_USER(),
    NOTES VARCHAR(4000),
    CONSTRAINT UK_DB_SCHEMA UNIQUE (DATABASE_NAME, SCHEMA_NAME)
);

-- ============================================================================
-- SECTION 3: CLEANUP LOG TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS CDC_PRESERVATION.CLEANUP.CLEANUP_LOG (
    LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    CONFIG_ID NUMBER,
    BATCH_ID VARCHAR(100),
    DATABASE_NAME VARCHAR(255),
    SCHEMA_NAME VARCHAR(255),
    TABLE_NAME VARCHAR(255),
    DATE_COLUMN VARCHAR(255),
    RETENTION_DAYS NUMBER,
    CUTOFF_DATE DATE,
    ROWS_BEFORE NUMBER,
    ROWS_DELETED NUMBER,
    ROWS_AFTER NUMBER,
    EXECUTION_START TIMESTAMP_LTZ,
    EXECUTION_END TIMESTAMP_LTZ,
    DURATION_SECONDS NUMBER(10,2),
    STATUS VARCHAR(20),
    ERROR_MESSAGE VARCHAR(16000),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- SECTION 4: EXCLUSION TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS CDC_PRESERVATION.CLEANUP.CLEANUP_EXCLUSIONS (
    EXCLUSION_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    DATABASE_NAME VARCHAR(255) NOT NULL,
    SCHEMA_NAME VARCHAR(255) NOT NULL,
    TABLE_NAME VARCHAR(255) NOT NULL,
    EXCLUSION_REASON VARCHAR(1000),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(255) DEFAULT CURRENT_USER(),
    CONSTRAINT UK_EXCLUSION UNIQUE (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME)
);

-- ============================================================================
-- SECTION 5: CORE CLEANUP PROCEDURE (Single Table)
-- ============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.CLEANUP.SP_CLEANUP_BASE_TABLE(
    P_DATABASE_NAME VARCHAR,
    P_SCHEMA_NAME VARCHAR,
    P_TABLE_NAME VARCHAR,
    P_DATE_COLUMN VARCHAR,
    P_RETENTION_DAYS NUMBER,
    P_BATCH_SIZE NUMBER,
    P_BATCH_ID VARCHAR,
    P_CONFIG_ID NUMBER DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_table_fqn VARCHAR;
    v_cutoff_date DATE;
    v_rows_before NUMBER;
    v_rows_deleted NUMBER := 0;
    v_rows_after NUMBER;
    v_start_time TIMESTAMP_LTZ;
    v_end_time TIMESTAMP_LTZ;
    v_duration NUMBER;
    v_error_msg VARCHAR;
    v_db VARCHAR;
    v_sch VARCHAR;
    v_tbl VARCHAR;
    v_col VARCHAR;
    v_ret NUMBER;
    v_batch VARCHAR;
    v_cfg NUMBER;
BEGIN
    v_start_time := CURRENT_TIMESTAMP();
    v_db := P_DATABASE_NAME;
    v_sch := P_SCHEMA_NAME;
    v_tbl := P_TABLE_NAME;
    v_col := P_DATE_COLUMN;
    v_ret := P_RETENTION_DAYS;
    v_batch := P_BATCH_ID;
    v_cfg := P_CONFIG_ID;
    v_table_fqn := v_db || '.' || v_sch || '.' || v_tbl;
    v_cutoff_date := DATEADD('day', -v_ret, CURRENT_DATE());
    
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_table_fqn;
    LET rs1 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur1 CURSOR FOR rs1;
    OPEN cur1; FETCH cur1 INTO v_rows_before; CLOSE cur1;
    
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_db || '.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''' || v_sch || ''' AND TABLE_NAME = ''' || v_tbl || ''' AND UPPER(COLUMN_NAME) = ''' || UPPER(v_col) || '''';
    LET rs_col RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur_col CURSOR FOR rs_col;
    LET col_exists NUMBER;
    OPEN cur_col; FETCH cur_col INTO col_exists; CLOSE cur_col;
    
    IF (col_exists = 0) THEN
        INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_LOG (CONFIG_ID, BATCH_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, DATE_COLUMN, RETENTION_DAYS, CUTOFF_DATE, ROWS_BEFORE, ROWS_DELETED, ROWS_AFTER, EXECUTION_START, EXECUTION_END, DURATION_SECONDS, STATUS, ERROR_MESSAGE)
        VALUES (:v_cfg, :v_batch, :v_db, :v_sch, :v_tbl, :v_col, :v_ret, :v_cutoff_date, :v_rows_before, 0, :v_rows_before, :v_start_time, CURRENT_TIMESTAMP(), 0, 'SKIPPED', 'Date column not found');
        RETURN OBJECT_CONSTRUCT('table', v_table_fqn, 'status', 'SKIPPED', 'reason', 'Date column not found');
    END IF;
    
    EXECUTE IMMEDIATE 'DELETE FROM ' || v_table_fqn || ' WHERE ' || v_col || ' < ''' || v_cutoff_date || '''';
    v_rows_deleted := SQLROWCOUNT;
    
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_table_fqn;
    LET rs2 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur2 CURSOR FOR rs2;
    OPEN cur2; FETCH cur2 INTO v_rows_after; CLOSE cur2;
    
    v_end_time := CURRENT_TIMESTAMP();
    v_duration := DATEDIFF('SECOND', v_start_time, v_end_time);
    
    INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_LOG (CONFIG_ID, BATCH_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, DATE_COLUMN, RETENTION_DAYS, CUTOFF_DATE, ROWS_BEFORE, ROWS_DELETED, ROWS_AFTER, EXECUTION_START, EXECUTION_END, DURATION_SECONDS, STATUS)
    VALUES (:v_cfg, :v_batch, :v_db, :v_sch, :v_tbl, :v_col, :v_ret, :v_cutoff_date, :v_rows_before, :v_rows_deleted, :v_rows_after, :v_start_time, :v_end_time, :v_duration, 'SUCCESS');
    
    RETURN OBJECT_CONSTRUCT('table', v_table_fqn, 'cutoff_date', v_cutoff_date, 'rows_before', v_rows_before, 'rows_deleted', v_rows_deleted, 'rows_after', v_rows_after, 'duration_seconds', v_duration, 'status', 'SUCCESS');
    
EXCEPTION
    WHEN OTHER THEN
        v_error_msg := SQLERRM;
        v_end_time := CURRENT_TIMESTAMP();
        v_duration := DATEDIFF('SECOND', v_start_time, v_end_time);
        INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_LOG (CONFIG_ID, BATCH_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, DATE_COLUMN, RETENTION_DAYS, CUTOFF_DATE, ROWS_BEFORE, ROWS_DELETED, ROWS_AFTER, EXECUTION_START, EXECUTION_END, DURATION_SECONDS, STATUS, ERROR_MESSAGE)
        VALUES (:v_cfg, :v_batch, :v_db, :v_sch, :v_tbl, :v_col, :v_ret, :v_cutoff_date, :v_rows_before, 0, NULL, :v_start_time, :v_end_time, :v_duration, 'FAILED', :v_error_msg);
        RETURN OBJECT_CONSTRUCT('table', v_table_fqn, 'status', 'FAILED', 'error', v_error_msg);
END;
$$;

-- ============================================================================
-- SECTION 6: SCHEMA-LEVEL CLEANUP PROCEDURE
-- ============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.CLEANUP.SP_CLEANUP_SCHEMA(
    P_DATABASE_NAME VARCHAR,
    P_SCHEMA_NAME VARCHAR,
    P_DATE_COLUMN VARCHAR DEFAULT 'CREATED_DATE',
    P_RETENTION_DAYS NUMBER DEFAULT 45,
    P_BATCH_SIZE NUMBER DEFAULT 100000,
    P_TABLE_PATTERN VARCHAR DEFAULT '%_BASE'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_batch_id VARCHAR;
    v_results ARRAY := ARRAY_CONSTRUCT();
    v_tables_processed NUMBER := 0;
    v_tables_skipped NUMBER := 0;
    v_tables_failed NUMBER := 0;
    v_total_rows_deleted NUMBER := 0;
    v_start_time TIMESTAMP_LTZ;
    v_table_name VARCHAR;
    v_table_result VARIANT;
    v_is_excluded NUMBER;
    v_db VARCHAR;
    v_schema VARCHAR;
    v_date_col VARCHAR;
    v_ret_days NUMBER;
    v_batch_sz NUMBER;
    v_pattern VARCHAR;
BEGIN
    v_batch_id := 'CLEANUP_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    v_start_time := CURRENT_TIMESTAMP();
    v_db := P_DATABASE_NAME;
    v_schema := P_SCHEMA_NAME;
    v_date_col := P_DATE_COLUMN;
    v_ret_days := P_RETENTION_DAYS;
    v_batch_sz := P_BATCH_SIZE;
    v_pattern := P_TABLE_PATTERN;
    
    LET query VARCHAR := 'SELECT TABLE_NAME FROM ' || v_db || '.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''' || v_schema || ''' AND TABLE_NAME LIKE ''' || v_pattern || ''' AND TABLE_TYPE = ''BASE TABLE'' ORDER BY TABLE_NAME';
    LET rs_tables RESULTSET := (EXECUTE IMMEDIATE :query);
    LET cur_tables CURSOR FOR rs_tables;
    
    FOR rec IN cur_tables DO
        v_table_name := rec.TABLE_NAME;
        
        SELECT COUNT(*) INTO v_is_excluded FROM CDC_PRESERVATION.CLEANUP.CLEANUP_EXCLUSIONS
        WHERE DATABASE_NAME = :v_db AND SCHEMA_NAME = :v_schema AND TABLE_NAME = :v_table_name;
        
        IF (v_is_excluded > 0) THEN
            v_tables_skipped := v_tables_skipped + 1;
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('table', v_table_name, 'status', 'EXCLUDED'));
            CONTINUE;
        END IF;
        
        BEGIN
            CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_BASE_TABLE(:v_db, :v_schema, :v_table_name, :v_date_col, :v_ret_days, :v_batch_sz, :v_batch_id, NULL);
            LET rs RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
            LET cur CURSOR FOR rs;
            OPEN cur; FETCH cur INTO v_table_result; CLOSE cur;
            
            v_results := ARRAY_APPEND(v_results, v_table_result);
            IF (v_table_result:status::VARCHAR = 'SUCCESS') THEN
                v_tables_processed := v_tables_processed + 1;
                v_total_rows_deleted := v_total_rows_deleted + COALESCE(v_table_result:rows_deleted::NUMBER, 0);
            ELSEIF (v_table_result:status::VARCHAR = 'SKIPPED') THEN
                v_tables_skipped := v_tables_skipped + 1;
            ELSE
                v_tables_failed := v_tables_failed + 1;
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_tables_failed := v_tables_failed + 1;
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('table', v_table_name, 'status', 'FAILED', 'error', SQLERRM));
        END;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT(
        'batch_id', v_batch_id, 
        'database', v_db, 
        'schema', v_schema, 
        'table_pattern', v_pattern, 
        'retention_days', v_ret_days, 
        'cutoff_date', DATEADD('day', -v_ret_days, CURRENT_DATE()), 
        'tables_processed', v_tables_processed, 
        'tables_skipped', v_tables_skipped, 
        'tables_failed', v_tables_failed, 
        'total_rows_deleted', v_total_rows_deleted, 
        'duration_seconds', DATEDIFF('SECOND', v_start_time, CURRENT_TIMESTAMP()), 
        'details', v_results
    );
END;
$$;

-- ============================================================================
-- SECTION 7: DRY RUN PROCEDURE (Preview)
-- ============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.CLEANUP.SP_CLEANUP_DRY_RUN(
    P_DATABASE_NAME VARCHAR,
    P_SCHEMA_NAME VARCHAR,
    P_DATE_COLUMN VARCHAR DEFAULT 'CREATED_DATE',
    P_RETENTION_DAYS NUMBER DEFAULT 45,
    P_TABLE_PATTERN VARCHAR DEFAULT '%_BASE'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_cutoff_date DATE;
    v_results ARRAY := ARRAY_CONSTRUCT();
    v_table_name VARCHAR;
    v_total_count NUMBER;
    v_delete_count NUMBER;
    v_total_to_delete NUMBER := 0;
    v_db VARCHAR;
    v_sch VARCHAR;
    v_col VARCHAR;
    v_ret NUMBER;
    v_pat VARCHAR;
BEGIN
    v_db := P_DATABASE_NAME;
    v_sch := P_SCHEMA_NAME;
    v_col := P_DATE_COLUMN;
    v_ret := P_RETENTION_DAYS;
    v_pat := P_TABLE_PATTERN;
    v_cutoff_date := DATEADD('day', -v_ret, CURRENT_DATE());
    
    LET query VARCHAR := 'SELECT TABLE_NAME FROM ' || v_db || '.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''' || v_sch || ''' AND TABLE_NAME LIKE ''' || v_pat || ''' AND TABLE_TYPE = ''BASE TABLE'' ORDER BY TABLE_NAME';
    LET rs_tables RESULTSET := (EXECUTE IMMEDIATE :query);
    LET cur_tables CURSOR FOR rs_tables;
    
    FOR rec IN cur_tables DO
        v_table_name := rec.TABLE_NAME;
        
        BEGIN
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_db || '.' || v_sch || '.' || v_table_name;
            LET rs1 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
            LET cur1 CURSOR FOR rs1;
            OPEN cur1; FETCH cur1 INTO v_total_count; CLOSE cur1;
            
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_db || '.' || v_sch || '.' || v_table_name || ' WHERE ' || v_col || ' < ''' || v_cutoff_date || '''';
            LET rs2 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
            LET cur2 CURSOR FOR rs2;
            OPEN cur2; FETCH cur2 INTO v_delete_count; CLOSE cur2;
            
            v_total_to_delete := v_total_to_delete + v_delete_count;
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('table', v_table_name, 'total_rows', v_total_count, 'rows_to_delete', v_delete_count, 'rows_to_keep', v_total_count - v_delete_count, 'delete_pct', ROUND(100.0 * v_delete_count / NULLIF(v_total_count, 0), 2)));
        EXCEPTION
            WHEN OTHER THEN
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('table', v_table_name, 'status', 'ERROR', 'error', SQLERRM));
        END;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT('mode', 'DRY_RUN', 'database', v_db, 'schema', v_sch, 'table_pattern', v_pat, 'date_column', v_col, 'retention_days', v_ret, 'cutoff_date', v_cutoff_date, 'total_rows_to_delete', v_total_to_delete, 'table_count', ARRAY_SIZE(v_results), 'details', v_results);
END;
$$;

-- ============================================================================
-- SECTION 8: CONFIG-DRIVEN PROCEDURES
-- ============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.CLEANUP.SP_CLEANUP_BY_CONFIG(P_CONFIG_ID NUMBER)
RETURNS VARIANT LANGUAGE SQL EXECUTE AS CALLER AS
$$
DECLARE
    v_database_name VARCHAR; v_schema_name VARCHAR; v_table_pattern VARCHAR;
    v_date_column VARCHAR; v_retention_days NUMBER; v_batch_size NUMBER; v_result VARIANT;
BEGIN
    SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_PATTERN, DATE_COLUMN, RETENTION_DAYS, BATCH_SIZE
    INTO v_database_name, v_schema_name, v_table_pattern, v_date_column, v_retention_days, v_batch_size
    FROM CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG WHERE CONFIG_ID = :P_CONFIG_ID AND IS_ACTIVE = TRUE;
    
    CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_SCHEMA(:v_database_name, :v_schema_name, :v_date_column, :v_retention_days, :v_batch_size, :v_table_pattern);
    LET rs RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur CURSOR FOR rs; OPEN cur; FETCH cur INTO v_result; CLOSE cur;
    
    UPDATE CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG SET LAST_CLEANUP_AT = CURRENT_TIMESTAMP(), UPDATED_AT = CURRENT_TIMESTAMP() WHERE CONFIG_ID = :P_CONFIG_ID;
    RETURN v_result;
EXCEPTION
    WHEN OTHER THEN RETURN OBJECT_CONSTRUCT('config_id', P_CONFIG_ID, 'status', 'FAILED', 'error', SQLERRM);
END;
$$;

CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.CLEANUP.SP_CLEANUP_ALL_CONFIGS()
RETURNS VARIANT LANGUAGE SQL EXECUTE AS CALLER AS
$$
DECLARE
    v_results ARRAY := ARRAY_CONSTRUCT();
    v_config_id NUMBER; v_db_name VARCHAR; v_schema_name VARCHAR; v_result VARIANT;
    v_total_deleted NUMBER := 0;
    c_configs CURSOR FOR SELECT CONFIG_ID, DATABASE_NAME, SCHEMA_NAME FROM CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG WHERE IS_ACTIVE = TRUE ORDER BY CONFIG_ID;
BEGIN
    FOR rec IN c_configs DO
        v_config_id := rec.CONFIG_ID; v_db_name := rec.DATABASE_NAME; v_schema_name := rec.SCHEMA_NAME;
        BEGIN
            CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_BY_CONFIG(:v_config_id);
            LET rs RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
            LET cur CURSOR FOR rs; OPEN cur; FETCH cur INTO v_result; CLOSE cur;
            v_results := ARRAY_APPEND(v_results, v_result);
            v_total_deleted := v_total_deleted + COALESCE(v_result:total_rows_deleted::NUMBER, 0);
        EXCEPTION
            WHEN OTHER THEN
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('config_id', v_config_id, 'database', v_db_name, 'schema', v_schema_name, 'status', 'FAILED', 'error', SQLERRM));
        END;
    END FOR;
    RETURN OBJECT_CONSTRUCT('execution_time', CURRENT_TIMESTAMP(), 'configs_processed', ARRAY_SIZE(v_results), 'total_rows_deleted', v_total_deleted, 'results', v_results);
END;
$$;

-- ============================================================================
-- SECTION 9: TASK CREATION
-- ============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.CLEANUP.SP_CREATE_MASTER_CLEANUP_TASK(
    P_WAREHOUSE VARCHAR DEFAULT 'COMPUTE_WH',
    P_SCHEDULE VARCHAR DEFAULT 'USING CRON 0 2 * * * UTC'
)
RETURNS VARCHAR LANGUAGE SQL EXECUTE AS CALLER AS
$$
BEGIN
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS WAREHOUSE = ' || P_WAREHOUSE || ' SCHEDULE = ''' || P_SCHEDULE || ''' ALLOW_OVERLAPPING_EXECUTION = FALSE USER_TASK_TIMEOUT_MS = 14400000 COMMENT = ''Daily master cleanup task for all configured schemas'' AS CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_ALL_CONFIGS()';
    RETURN 'SUCCESS: Master task created. Run: ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS RESUME;';
EXCEPTION
    WHEN OTHER THEN RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- ============================================================================
-- SECTION 10: MONITORING VIEWS
-- ============================================================================
CREATE OR REPLACE VIEW CDC_PRESERVATION.CLEANUP.V_CLEANUP_SUMMARY AS
SELECT DATE(CREATED_AT) AS CLEANUP_DATE, DATABASE_NAME, SCHEMA_NAME,
    COUNT(DISTINCT TABLE_NAME) AS TABLES_PROCESSED,
    SUM(CASE WHEN STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESS_COUNT,
    SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_COUNT,
    SUM(ROWS_DELETED) AS TOTAL_ROWS_DELETED,
    ROUND(AVG(DURATION_SECONDS), 2) AS AVG_DURATION_SEC
FROM CDC_PRESERVATION.CLEANUP.CLEANUP_LOG
GROUP BY DATE(CREATED_AT), DATABASE_NAME, SCHEMA_NAME
ORDER BY CLEANUP_DATE DESC;

CREATE OR REPLACE VIEW CDC_PRESERVATION.CLEANUP.V_CONFIG_STATUS AS
SELECT c.CONFIG_ID, c.DATABASE_NAME, c.SCHEMA_NAME, c.TABLE_PATTERN, c.DATE_COLUMN,
    c.RETENTION_DAYS, c.IS_ACTIVE, c.LAST_CLEANUP_AT, c.TASK_SCHEDULE
FROM CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG c;

CREATE OR REPLACE VIEW CDC_PRESERVATION.CLEANUP.V_RECENT_CLEANUPS AS
SELECT BATCH_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CUTOFF_DATE,
    ROWS_BEFORE, ROWS_DELETED, ROWS_AFTER, DURATION_SECONDS, STATUS, ERROR_MESSAGE, CREATED_AT
FROM CDC_PRESERVATION.CLEANUP.CLEANUP_LOG
WHERE CREATED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY CREATED_AT DESC;

CREATE OR REPLACE VIEW CDC_PRESERVATION.CLEANUP.V_FAILED_CLEANUPS AS
SELECT BATCH_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, ERROR_MESSAGE, CREATED_AT
FROM CDC_PRESERVATION.CLEANUP.CLEANUP_LOG
WHERE STATUS = 'FAILED' AND CREATED_AT > DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY CREATED_AT DESC;

-- ============================================================================
-- SECTION 11: UTILITY PROCEDURES
-- ============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.CLEANUP.SP_RESUME_CLEANUP_TASK(P_TASK_NAME VARCHAR DEFAULT 'TASK_CLEANUP_ALL_SCHEMAS')
RETURNS VARCHAR LANGUAGE SQL AS
$$
BEGIN
    EXECUTE IMMEDIATE 'ALTER TASK CDC_PRESERVATION.CLEANUP.' || P_TASK_NAME || ' RESUME';
    RETURN 'SUCCESS: Task ' || P_TASK_NAME || ' resumed';
EXCEPTION WHEN OTHER THEN RETURN 'ERROR: ' || SQLERRM;
END;
$$;

CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.CLEANUP.SP_SUSPEND_CLEANUP_TASK(P_TASK_NAME VARCHAR DEFAULT 'TASK_CLEANUP_ALL_SCHEMAS')
RETURNS VARCHAR LANGUAGE SQL AS
$$
BEGIN
    EXECUTE IMMEDIATE 'ALTER TASK CDC_PRESERVATION.CLEANUP.' || P_TASK_NAME || ' SUSPEND';
    RETURN 'SUCCESS: Task ' || P_TASK_NAME || ' suspended';
EXCEPTION WHEN OTHER THEN RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- ============================================================================
-- SECTION 12: SAMPLE CONFIGURATION (Modify for your environment)
-- ============================================================================
/*
-- Add your database/schema configurations here:
INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG 
(DATABASE_NAME, SCHEMA_NAME, TABLE_PATTERN, DATE_COLUMN, RETENTION_DAYS, NOTES)
VALUES 
('D_BRONZE', 'SALES', '%_BASE', 'CREATED_DATE', 45, 'Production cleanup for SALES _BASE tables'),
('D_BRONZE', 'ORDERS', '%_BASE', 'CREATED_DATE', 45, 'Production cleanup for ORDERS _BASE tables');

-- Create and start the task:
CALL CDC_PRESERVATION.CLEANUP.SP_CREATE_MASTER_CLEANUP_TASK('COMPUTE_WH', 'USING CRON 0 2 * * * UTC');
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS RESUME;

-- Exclude specific tables from cleanup:
INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_EXCLUSIONS 
(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, EXCLUSION_REASON)
VALUES ('D_BRONZE', 'SALES', 'AUDIT_LOG_BASE', 'Audit retention required for compliance');
*/

-- ============================================================================
-- END OF FRAMEWORK
-- ============================================================================
