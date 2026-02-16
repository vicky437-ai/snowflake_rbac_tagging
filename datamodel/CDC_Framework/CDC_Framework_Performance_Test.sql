/*
================================================================================
CDC DATA PRESERVATION FRAMEWORK - PERFORMANCE BASELINE TEST SCRIPT
================================================================================
Purpose:     Measure CDC latency and throughput with sample data loads
Metrics:     - End-to-end latency (ms)
             - Throughput (rows/second)
             - Batch processing time
             - Stream consumption time
             - MERGE operation time
================================================================================
*/

-- ============================================================================
-- SECTION 1: SETUP PERFORMANCE TEST ENVIRONMENT
-- ============================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CDC_PRESERVATION;

-- Create performance monitoring schema
CREATE SCHEMA IF NOT EXISTS CDC_PRESERVATION.PERFORMANCE;

-- Performance results table
CREATE OR REPLACE TABLE CDC_PRESERVATION.PERFORMANCE.TEST_RESULTS (
    TEST_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    TEST_RUN_ID VARCHAR(100),
    TEST_NAME VARCHAR(255),
    TEST_CATEGORY VARCHAR(50),
    BATCH_SIZE NUMBER,
    OPERATION_TYPE VARCHAR(20),
    START_TIME TIMESTAMP_LTZ,
    END_TIME TIMESTAMP_LTZ,
    DURATION_MS NUMBER,
    ROWS_PROCESSED NUMBER,
    THROUGHPUT_ROWS_PER_SEC NUMBER(10,2),
    LATENCY_AVG_MS NUMBER(10,2),
    LATENCY_P50_MS NUMBER(10,2),
    LATENCY_P95_MS NUMBER(10,2),
    LATENCY_P99_MS NUMBER(10,2),
    STREAM_STAGE_MS NUMBER,
    MERGE_EXECUTE_MS NUMBER,
    WAREHOUSE_SIZE VARCHAR(20),
    STATUS VARCHAR(20),
    NOTES VARCHAR(4000),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create test source table (simulates _BASE table)
CREATE OR REPLACE TABLE CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE (
    RECORD_ID NUMBER PRIMARY KEY,
    RECORD_KEY VARCHAR(100),
    STRING_COL_1 VARCHAR(500),
    STRING_COL_2 VARCHAR(500),
    STRING_COL_3 VARCHAR(1000),
    NUMBER_COL_1 NUMBER(18,4),
    NUMBER_COL_2 NUMBER(18,4),
    DATE_COL TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    MODIFIED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Enable change tracking
ALTER TABLE CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE SET CHANGE_TRACKING = TRUE;

-- Create stream on test table
CREATE OR REPLACE STREAM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_STREAM
    ON TABLE CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
    SHOW_INITIAL_ROWS = FALSE;

-- Create preserved target table
CREATE OR REPLACE TABLE CDC_PRESERVATION.PERFORMANCE.PERF_TEST_PRESERVED (
    RECORD_ID NUMBER PRIMARY KEY,
    RECORD_KEY VARCHAR(100),
    STRING_COL_1 VARCHAR(500),
    STRING_COL_2 VARCHAR(500),
    STRING_COL_3 VARCHAR(1000),
    NUMBER_COL_1 NUMBER(18,4),
    NUMBER_COL_2 NUMBER(18,4),
    DATE_COL TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ,
    MODIFIED_AT TIMESTAMP_NTZ,
    -- CDC Metadata
    CDC_OPERATION VARCHAR(10),
    CDC_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IS_DELETED BOOLEAN DEFAULT FALSE,
    RECORD_CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RECORD_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_LOAD_BATCH_ID VARCHAR(100)
);

-- ============================================================================
-- SECTION 2: PERFORMANCE TEST STORED PROCEDURE
-- ============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST(
    P_TEST_RUN_ID VARCHAR,
    P_BATCH_SIZE NUMBER,
    P_OPERATION_TYPE VARCHAR  -- 'INSERT', 'UPDATE', 'DELETE', 'MIXED'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start_time TIMESTAMP_LTZ;
    v_end_time TIMESTAMP_LTZ;
    v_stage_start TIMESTAMP_LTZ;
    v_stage_end TIMESTAMP_LTZ;
    v_merge_start TIMESTAMP_LTZ;
    v_merge_end TIMESTAMP_LTZ;
    v_duration_ms NUMBER;
    v_stream_stage_ms NUMBER;
    v_merge_ms NUMBER;
    v_rows_processed NUMBER := 0;
    v_throughput NUMBER;
    v_batch_id VARCHAR;
    v_test_name VARCHAR;
    v_warehouse_size VARCHAR;
BEGIN
    v_batch_id := 'PERF_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    v_test_name := P_OPERATION_TYPE || '_' || P_BATCH_SIZE || '_ROWS';
    
    -- Get warehouse size
    SELECT CURRENT_WAREHOUSE() INTO v_warehouse_size;
    
    -- ========================================================================
    -- STEP 1: Generate Test Data Based on Operation Type
    -- ========================================================================
    v_start_time := CURRENT_TIMESTAMP();
    
    IF (P_OPERATION_TYPE = 'INSERT') THEN
        -- Clear existing data
        TRUNCATE TABLE CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE;
        TRUNCATE TABLE CDC_PRESERVATION.PERFORMANCE.PERF_TEST_PRESERVED;
        
        -- Recreate stream (to clear it)
        CREATE OR REPLACE STREAM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_STREAM
            ON TABLE CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
            SHOW_INITIAL_ROWS = FALSE;
        
        -- Generate INSERT test data
        INSERT INTO CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
        SELECT 
            SEQ4() AS RECORD_ID,
            'KEY_' || SEQ4() AS RECORD_KEY,
            RANDSTR(100, RANDOM()) AS STRING_COL_1,
            RANDSTR(100, RANDOM()) AS STRING_COL_2,
            RANDSTR(500, RANDOM()) AS STRING_COL_3,
            UNIFORM(0, 1000000, RANDOM())::NUMBER(18,4) AS NUMBER_COL_1,
            UNIFORM(0, 1000000, RANDOM())::NUMBER(18,4) AS NUMBER_COL_2,
            DATEADD('day', -UNIFORM(0, 365, RANDOM()), CURRENT_TIMESTAMP()) AS DATE_COL,
            CURRENT_TIMESTAMP() AS CREATED_AT,
            CURRENT_TIMESTAMP() AS MODIFIED_AT
        FROM TABLE(GENERATOR(ROWCOUNT => :P_BATCH_SIZE));
        
    ELSEIF (P_OPERATION_TYPE = 'UPDATE') THEN
        -- Ensure we have data to update
        LET existing_count NUMBER := (SELECT COUNT(*) FROM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE);
        IF (existing_count < P_BATCH_SIZE) THEN
            -- Insert base data first
            INSERT INTO CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
            SELECT 
                SEQ4() AS RECORD_ID,
                'KEY_' || SEQ4() AS RECORD_KEY,
                RANDSTR(100, RANDOM()) AS STRING_COL_1,
                RANDSTR(100, RANDOM()) AS STRING_COL_2,
                RANDSTR(500, RANDOM()) AS STRING_COL_3,
                UNIFORM(0, 1000000, RANDOM())::NUMBER(18,4) AS NUMBER_COL_1,
                UNIFORM(0, 1000000, RANDOM())::NUMBER(18,4) AS NUMBER_COL_2,
                DATEADD('day', -UNIFORM(0, 365, RANDOM()), CURRENT_TIMESTAMP()) AS DATE_COL,
                CURRENT_TIMESTAMP() AS CREATED_AT,
                CURRENT_TIMESTAMP() AS MODIFIED_AT
            FROM TABLE(GENERATOR(ROWCOUNT => :P_BATCH_SIZE));
            
            -- Process initial load
            MERGE INTO CDC_PRESERVATION.PERFORMANCE.PERF_TEST_PRESERVED AS tgt
            USING (SELECT *, 'INIT' AS BATCH FROM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_STREAM) AS src
            ON tgt.RECORD_ID = src.RECORD_ID
            WHEN NOT MATCHED AND src.METADATA$ACTION = 'INSERT' THEN
                INSERT (RECORD_ID, RECORD_KEY, STRING_COL_1, STRING_COL_2, STRING_COL_3, 
                       NUMBER_COL_1, NUMBER_COL_2, DATE_COL, CREATED_AT, MODIFIED_AT,
                       CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID)
                VALUES (src.RECORD_ID, src.RECORD_KEY, src.STRING_COL_1, src.STRING_COL_2, src.STRING_COL_3,
                       src.NUMBER_COL_1, src.NUMBER_COL_2, src.DATE_COL, src.CREATED_AT, src.MODIFIED_AT,
                       'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH);
        END IF;
        
        -- Recreate stream for update test
        CREATE OR REPLACE STREAM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_STREAM
            ON TABLE CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
            SHOW_INITIAL_ROWS = FALSE;
        
        -- Perform updates
        UPDATE CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
        SET STRING_COL_1 = RANDSTR(100, RANDOM()),
            NUMBER_COL_1 = UNIFORM(0, 1000000, RANDOM())::NUMBER(18,4),
            MODIFIED_AT = CURRENT_TIMESTAMP()
        WHERE RECORD_ID <= :P_BATCH_SIZE;
        
    ELSEIF (P_OPERATION_TYPE = 'DELETE') THEN
        -- Ensure we have data to delete
        LET existing_count NUMBER := (SELECT COUNT(*) FROM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE);
        IF (existing_count < P_BATCH_SIZE) THEN
            RETURN OBJECT_CONSTRUCT('status', 'ERROR', 'message', 'Not enough data to delete. Run INSERT test first.');
        END IF;
        
        -- Recreate stream for delete test
        CREATE OR REPLACE STREAM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_STREAM
            ON TABLE CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
            SHOW_INITIAL_ROWS = FALSE;
        
        -- Perform deletes
        DELETE FROM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
        WHERE RECORD_ID <= :P_BATCH_SIZE;
        
    ELSEIF (P_OPERATION_TYPE = 'MIXED') THEN
        -- Mixed workload: 50% INSERT, 30% UPDATE, 20% DELETE
        LET insert_count NUMBER := FLOOR(P_BATCH_SIZE * 0.5);
        LET update_count NUMBER := FLOOR(P_BATCH_SIZE * 0.3);
        LET delete_count NUMBER := P_BATCH_SIZE - insert_count - update_count;
        
        -- Recreate stream
        CREATE OR REPLACE STREAM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_STREAM
            ON TABLE CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
            SHOW_INITIAL_ROWS = FALSE;
        
        -- Get max ID
        LET max_id NUMBER := (SELECT COALESCE(MAX(RECORD_ID), 0) FROM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE);
        
        -- INSERTs
        INSERT INTO CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
        SELECT 
            :max_id + SEQ4() + 1 AS RECORD_ID,
            'KEY_' || (:max_id + SEQ4() + 1) AS RECORD_KEY,
            RANDSTR(100, RANDOM()) AS STRING_COL_1,
            RANDSTR(100, RANDOM()) AS STRING_COL_2,
            RANDSTR(500, RANDOM()) AS STRING_COL_3,
            UNIFORM(0, 1000000, RANDOM())::NUMBER(18,4) AS NUMBER_COL_1,
            UNIFORM(0, 1000000, RANDOM())::NUMBER(18,4) AS NUMBER_COL_2,
            DATEADD('day', -UNIFORM(0, 365, RANDOM()), CURRENT_TIMESTAMP()) AS DATE_COL,
            CURRENT_TIMESTAMP() AS CREATED_AT,
            CURRENT_TIMESTAMP() AS MODIFIED_AT
        FROM TABLE(GENERATOR(ROWCOUNT => :insert_count));
        
        -- UPDATEs (on existing records)
        UPDATE CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
        SET STRING_COL_1 = RANDSTR(100, RANDOM()),
            MODIFIED_AT = CURRENT_TIMESTAMP()
        WHERE RECORD_ID <= :update_count;
        
        -- DELETEs (on different records)
        DELETE FROM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
        WHERE RECORD_ID > :update_count AND RECORD_ID <= :update_count + :delete_count;
    END IF;
    
    -- ========================================================================
    -- STEP 2: Stage Stream Data (Measure Stream Consumption Time)
    -- ========================================================================
    v_stage_start := CURRENT_TIMESTAMP();
    
    CREATE OR REPLACE TEMPORARY TABLE _PERF_STAGING AS
    SELECT 
        RECORD_ID, RECORD_KEY, STRING_COL_1, STRING_COL_2, STRING_COL_3,
        NUMBER_COL_1, NUMBER_COL_2, DATE_COL, CREATED_AT, MODIFIED_AT,
        METADATA$ACTION AS CDC_ACTION,
        METADATA$ISUPDATE AS CDC_IS_UPDATE
    FROM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_STREAM;
    
    v_stage_end := CURRENT_TIMESTAMP();
    v_stream_stage_ms := DATEDIFF('MILLISECOND', v_stage_start, v_stage_end);
    
    -- Get staged row count
    SELECT COUNT(*) INTO v_rows_processed FROM _PERF_STAGING;
    
    -- ========================================================================
    -- STEP 3: Execute MERGE (Measure MERGE Time)
    -- ========================================================================
    v_merge_start := CURRENT_TIMESTAMP();
    
    MERGE INTO CDC_PRESERVATION.PERFORMANCE.PERF_TEST_PRESERVED AS tgt
    USING (SELECT *, :v_batch_id AS BATCH_ID FROM _PERF_STAGING) AS src
    ON tgt.RECORD_ID = src.RECORD_ID
    
    -- UPDATE (existing record modified)
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = TRUE THEN
        UPDATE SET
            tgt.RECORD_KEY = src.RECORD_KEY,
            tgt.STRING_COL_1 = src.STRING_COL_1,
            tgt.STRING_COL_2 = src.STRING_COL_2,
            tgt.STRING_COL_3 = src.STRING_COL_3,
            tgt.NUMBER_COL_1 = src.NUMBER_COL_1,
            tgt.NUMBER_COL_2 = src.NUMBER_COL_2,
            tgt.DATE_COL = src.DATE_COL,
            tgt.MODIFIED_AT = src.MODIFIED_AT,
            tgt.CDC_OPERATION = 'UPDATE',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    -- DELETE (soft delete)
    WHEN MATCHED AND src.CDC_ACTION = 'DELETE' AND src.CDC_IS_UPDATE = FALSE THEN
        UPDATE SET
            tgt.CDC_OPERATION = 'DELETE',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = TRUE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    -- RE-INSERT (record reappeared)
    WHEN MATCHED AND src.CDC_ACTION = 'INSERT' AND src.CDC_IS_UPDATE = FALSE THEN
        UPDATE SET
            tgt.RECORD_KEY = src.RECORD_KEY,
            tgt.STRING_COL_1 = src.STRING_COL_1,
            tgt.STRING_COL_2 = src.STRING_COL_2,
            tgt.STRING_COL_3 = src.STRING_COL_3,
            tgt.NUMBER_COL_1 = src.NUMBER_COL_1,
            tgt.NUMBER_COL_2 = src.NUMBER_COL_2,
            tgt.DATE_COL = src.DATE_COL,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.MODIFIED_AT = src.MODIFIED_AT,
            tgt.CDC_OPERATION = 'INSERT',
            tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.IS_DELETED = FALSE,
            tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(),
            tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID
    
    -- NEW INSERT
    WHEN NOT MATCHED AND src.CDC_ACTION = 'INSERT' THEN
        INSERT (RECORD_ID, RECORD_KEY, STRING_COL_1, STRING_COL_2, STRING_COL_3,
               NUMBER_COL_1, NUMBER_COL_2, DATE_COL, CREATED_AT, MODIFIED_AT,
               CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID)
        VALUES (src.RECORD_ID, src.RECORD_KEY, src.STRING_COL_1, src.STRING_COL_2, src.STRING_COL_3,
               src.NUMBER_COL_1, src.NUMBER_COL_2, src.DATE_COL, src.CREATED_AT, src.MODIFIED_AT,
               'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID);
    
    v_merge_end := CURRENT_TIMESTAMP();
    v_merge_ms := DATEDIFF('MILLISECOND', v_merge_start, v_merge_end);
    
    -- ========================================================================
    -- STEP 4: Calculate Metrics
    -- ========================================================================
    v_end_time := CURRENT_TIMESTAMP();
    v_duration_ms := DATEDIFF('MILLISECOND', v_start_time, v_end_time);
    
    IF (v_rows_processed > 0 AND v_duration_ms > 0) THEN
        v_throughput := (v_rows_processed * 1000.0) / v_duration_ms;
    ELSE
        v_throughput := 0;
    END IF;
    
    -- ========================================================================
    -- STEP 5: Record Results
    -- ========================================================================
    INSERT INTO CDC_PRESERVATION.PERFORMANCE.TEST_RESULTS (
        TEST_RUN_ID, TEST_NAME, TEST_CATEGORY, BATCH_SIZE, OPERATION_TYPE,
        START_TIME, END_TIME, DURATION_MS, ROWS_PROCESSED, THROUGHPUT_ROWS_PER_SEC,
        LATENCY_AVG_MS, STREAM_STAGE_MS, MERGE_EXECUTE_MS, WAREHOUSE_SIZE, STATUS
    ) VALUES (
        :P_TEST_RUN_ID, :v_test_name, 'CDC_PROCESSING', :P_BATCH_SIZE, :P_OPERATION_TYPE,
        :v_start_time, :v_end_time, :v_duration_ms, :v_rows_processed, :v_throughput,
        CASE WHEN v_rows_processed > 0 THEN v_duration_ms / v_rows_processed ELSE 0 END,
        :v_stream_stage_ms, :v_merge_ms, :v_warehouse_size, 'SUCCESS'
    );
    
    -- Cleanup
    DROP TABLE IF EXISTS _PERF_STAGING;
    
    RETURN OBJECT_CONSTRUCT(
        'test_run_id', P_TEST_RUN_ID,
        'test_name', v_test_name,
        'operation_type', P_OPERATION_TYPE,
        'batch_size', P_BATCH_SIZE,
        'rows_processed', v_rows_processed,
        'total_duration_ms', v_duration_ms,
        'stream_stage_ms', v_stream_stage_ms,
        'merge_execute_ms', v_merge_ms,
        'throughput_rows_per_sec', ROUND(v_throughput, 2),
        'avg_latency_ms', ROUND(CASE WHEN v_rows_processed > 0 THEN v_duration_ms / v_rows_processed ELSE 0 END, 2),
        'warehouse_size', v_warehouse_size,
        'status', 'SUCCESS'
    );
EXCEPTION
    WHEN OTHER THEN
        INSERT INTO CDC_PRESERVATION.PERFORMANCE.TEST_RESULTS (
            TEST_RUN_ID, TEST_NAME, BATCH_SIZE, OPERATION_TYPE, STATUS, NOTES
        ) VALUES (
            :P_TEST_RUN_ID, :v_test_name, :P_BATCH_SIZE, :P_OPERATION_TYPE, 'FAILED', SQLERRM
        );
        RETURN OBJECT_CONSTRUCT('status', 'FAILED', 'error', SQLERRM);
END;
$$;

-- ============================================================================
-- SECTION 3: FULL PERFORMANCE TEST SUITE
-- ============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PERFORMANCE.SP_RUN_FULL_TEST_SUITE(
    P_TEST_RUN_ID VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_test_run_id VARCHAR;
    v_results ARRAY := ARRAY_CONSTRUCT();
    v_batch_sizes ARRAY := ARRAY_CONSTRUCT(100, 1000, 5000, 10000, 50000, 100000);
    v_operations ARRAY := ARRAY_CONSTRUCT('INSERT', 'UPDATE', 'DELETE', 'MIXED');
    v_batch_size NUMBER;
    v_operation VARCHAR;
    v_test_result VARIANT;
BEGIN
    v_test_run_id := COALESCE(P_TEST_RUN_ID, 'RUN_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS'));
    
    -- Run tests for each batch size and operation type
    FOR i IN 0 TO ARRAY_SIZE(v_batch_sizes) - 1 DO
        v_batch_size := v_batch_sizes[i]::NUMBER;
        
        FOR j IN 0 TO ARRAY_SIZE(v_operations) - 1 DO
            v_operation := v_operations[j]::VARCHAR;
            
            -- Skip large DELETE tests (need data first)
            IF (v_operation = 'DELETE' AND v_batch_size > 10000) THEN
                CONTINUE;
            END IF;
            
            BEGIN
                CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST(:v_test_run_id, :v_batch_size, :v_operation);
                LET rs RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
                LET cur CURSOR FOR rs;
                OPEN cur; FETCH cur INTO v_test_result; CLOSE cur;
                
                v_results := ARRAY_APPEND(v_results, v_test_result);
            EXCEPTION
                WHEN OTHER THEN
                    v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT(
                        'batch_size', v_batch_size,
                        'operation', v_operation,
                        'status', 'FAILED',
                        'error', SQLERRM
                    ));
            END;
        END FOR;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT(
        'test_run_id', v_test_run_id,
        'tests_executed', ARRAY_SIZE(v_results),
        'results', v_results
    );
END;
$$;

-- ============================================================================
-- SECTION 4: QUICK PERFORMANCE TEST (Smaller Scale)
-- ============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PERFORMANCE.SP_RUN_QUICK_TEST()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_test_run_id VARCHAR := 'QUICK_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    v_results ARRAY := ARRAY_CONSTRUCT();
    v_test_result VARIANT;
BEGIN
    -- Test 1: 1,000 INSERTs
    CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST(:v_test_run_id, 1000, 'INSERT');
    LET rs1 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur1 CURSOR FOR rs1; OPEN cur1; FETCH cur1 INTO v_test_result; CLOSE cur1;
    v_results := ARRAY_APPEND(v_results, v_test_result);
    
    -- Test 2: 1,000 UPDATEs
    CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST(:v_test_run_id, 1000, 'UPDATE');
    LET rs2 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur2 CURSOR FOR rs2; OPEN cur2; FETCH cur2 INTO v_test_result; CLOSE cur2;
    v_results := ARRAY_APPEND(v_results, v_test_result);
    
    -- Test 3: 500 DELETEs
    CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST(:v_test_run_id, 500, 'DELETE');
    LET rs3 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur3 CURSOR FOR rs3; OPEN cur3; FETCH cur3 INTO v_test_result; CLOSE cur3;
    v_results := ARRAY_APPEND(v_results, v_test_result);
    
    -- Test 4: 5,000 INSERTs (larger batch)
    CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST(:v_test_run_id, 5000, 'INSERT');
    LET rs4 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur4 CURSOR FOR rs4; OPEN cur4; FETCH cur4 INTO v_test_result; CLOSE cur4;
    v_results := ARRAY_APPEND(v_results, v_test_result);
    
    -- Test 5: 10,000 INSERTs (production-like)
    CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST(:v_test_run_id, 10000, 'INSERT');
    LET rs5 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur5 CURSOR FOR rs5; OPEN cur5; FETCH cur5 INTO v_test_result; CLOSE cur5;
    v_results := ARRAY_APPEND(v_results, v_test_result);
    
    RETURN OBJECT_CONSTRUCT(
        'test_run_id', v_test_run_id,
        'tests_executed', ARRAY_SIZE(v_results),
        'summary', (
            SELECT OBJECT_CONSTRUCT(
                'total_rows_processed', SUM(ROWS_PROCESSED),
                'avg_throughput_rows_per_sec', ROUND(AVG(THROUGHPUT_ROWS_PER_SEC), 2),
                'avg_latency_ms', ROUND(AVG(LATENCY_AVG_MS), 4),
                'total_duration_sec', ROUND(SUM(DURATION_MS) / 1000.0, 2)
            )
            FROM CDC_PRESERVATION.PERFORMANCE.TEST_RESULTS
            WHERE TEST_RUN_ID = :v_test_run_id
        ),
        'results', v_results
    );
END;
$$;

-- ============================================================================
-- SECTION 5: PERFORMANCE REPORTING VIEWS
-- ============================================================================

-- Summary by operation type
CREATE OR REPLACE VIEW CDC_PRESERVATION.PERFORMANCE.V_PERF_BY_OPERATION AS
SELECT 
    OPERATION_TYPE,
    COUNT(*) AS TEST_COUNT,
    SUM(ROWS_PROCESSED) AS TOTAL_ROWS,
    ROUND(AVG(THROUGHPUT_ROWS_PER_SEC), 2) AS AVG_THROUGHPUT,
    ROUND(MIN(THROUGHPUT_ROWS_PER_SEC), 2) AS MIN_THROUGHPUT,
    ROUND(MAX(THROUGHPUT_ROWS_PER_SEC), 2) AS MAX_THROUGHPUT,
    ROUND(AVG(LATENCY_AVG_MS), 4) AS AVG_LATENCY_MS,
    ROUND(AVG(STREAM_STAGE_MS), 2) AS AVG_STREAM_MS,
    ROUND(AVG(MERGE_EXECUTE_MS), 2) AS AVG_MERGE_MS
FROM CDC_PRESERVATION.PERFORMANCE.TEST_RESULTS
WHERE STATUS = 'SUCCESS'
GROUP BY OPERATION_TYPE
ORDER BY OPERATION_TYPE;

-- Summary by batch size
CREATE OR REPLACE VIEW CDC_PRESERVATION.PERFORMANCE.V_PERF_BY_BATCH_SIZE AS
SELECT 
    BATCH_SIZE,
    COUNT(*) AS TEST_COUNT,
    ROUND(AVG(THROUGHPUT_ROWS_PER_SEC), 2) AS AVG_THROUGHPUT,
    ROUND(AVG(DURATION_MS), 2) AS AVG_DURATION_MS,
    ROUND(AVG(LATENCY_AVG_MS), 4) AS AVG_LATENCY_MS,
    ROUND(AVG(STREAM_STAGE_MS), 2) AS AVG_STREAM_MS,
    ROUND(AVG(MERGE_EXECUTE_MS), 2) AS AVG_MERGE_MS
FROM CDC_PRESERVATION.PERFORMANCE.TEST_RESULTS
WHERE STATUS = 'SUCCESS'
GROUP BY BATCH_SIZE
ORDER BY BATCH_SIZE;

-- Latest test run results
CREATE OR REPLACE VIEW CDC_PRESERVATION.PERFORMANCE.V_LATEST_TEST_RUN AS
SELECT 
    TEST_RUN_ID,
    TEST_NAME,
    OPERATION_TYPE,
    BATCH_SIZE,
    ROWS_PROCESSED,
    DURATION_MS,
    THROUGHPUT_ROWS_PER_SEC,
    LATENCY_AVG_MS,
    STREAM_STAGE_MS,
    MERGE_EXECUTE_MS,
    WAREHOUSE_SIZE,
    STATUS,
    CREATED_AT
FROM CDC_PRESERVATION.PERFORMANCE.TEST_RESULTS
WHERE TEST_RUN_ID = (SELECT MAX(TEST_RUN_ID) FROM CDC_PRESERVATION.PERFORMANCE.TEST_RESULTS)
ORDER BY CREATED_AT;

-- Performance baseline summary
CREATE OR REPLACE VIEW CDC_PRESERVATION.PERFORMANCE.V_PERFORMANCE_BASELINE AS
SELECT
    'CDC Framework Performance Baseline' AS REPORT_TITLE,
    COUNT(*) AS TOTAL_TESTS,
    SUM(ROWS_PROCESSED) AS TOTAL_ROWS_PROCESSED,
    ROUND(AVG(THROUGHPUT_ROWS_PER_SEC), 2) AS AVG_THROUGHPUT_ROWS_PER_SEC,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY THROUGHPUT_ROWS_PER_SEC), 2) AS MEDIAN_THROUGHPUT,
    ROUND(MIN(THROUGHPUT_ROWS_PER_SEC), 2) AS MIN_THROUGHPUT,
    ROUND(MAX(THROUGHPUT_ROWS_PER_SEC), 2) AS MAX_THROUGHPUT,
    ROUND(AVG(LATENCY_AVG_MS), 4) AS AVG_LATENCY_MS,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY LATENCY_AVG_MS), 4) AS P95_LATENCY_MS,
    ROUND(AVG(STREAM_STAGE_MS), 2) AS AVG_STREAM_STAGE_MS,
    ROUND(AVG(MERGE_EXECUTE_MS), 2) AS AVG_MERGE_MS,
    MIN(CREATED_AT) AS FIRST_TEST,
    MAX(CREATED_AT) AS LAST_TEST
FROM CDC_PRESERVATION.PERFORMANCE.TEST_RESULTS
WHERE STATUS = 'SUCCESS';

-- ============================================================================
-- SECTION 6: RUN QUICK PERFORMANCE TEST
-- ============================================================================

-- Execute quick test
-- CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_QUICK_TEST();

-- ============================================================================
-- SECTION 7: SAMPLE MANUAL TEST EXECUTION
-- ============================================================================
/*
-- Run individual tests:
CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST('MANUAL_TEST', 1000, 'INSERT');
CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST('MANUAL_TEST', 1000, 'UPDATE');
CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST('MANUAL_TEST', 500, 'DELETE');
CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST('MANUAL_TEST', 2000, 'MIXED');

-- Run full test suite (takes longer):
CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_FULL_TEST_SUITE();

-- View results:
SELECT * FROM CDC_PRESERVATION.PERFORMANCE.V_LATEST_TEST_RUN;
SELECT * FROM CDC_PRESERVATION.PERFORMANCE.V_PERF_BY_OPERATION;
SELECT * FROM CDC_PRESERVATION.PERFORMANCE.V_PERF_BY_BATCH_SIZE;
SELECT * FROM CDC_PRESERVATION.PERFORMANCE.V_PERFORMANCE_BASELINE;
*/

-- ============================================================================
-- SECTION 8: LATENCY MEASUREMENT (END-TO-END)
-- ============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PERFORMANCE.SP_MEASURE_E2E_LATENCY(
    P_NUM_RECORDS NUMBER DEFAULT 100
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_test_id VARCHAR := 'LATENCY_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    v_insert_time TIMESTAMP_LTZ;
    v_process_time TIMESTAMP_LTZ;
    v_complete_time TIMESTAMP_LTZ;
    v_source_to_stream_ms NUMBER;
    v_stream_to_target_ms NUMBER;
    v_total_latency_ms NUMBER;
    v_max_id NUMBER;
BEGIN
    -- Get starting point
    SELECT COALESCE(MAX(RECORD_ID), 0) INTO v_max_id FROM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE;
    
    -- Record insert time
    v_insert_time := CURRENT_TIMESTAMP();
    
    -- Insert test records with timestamp
    INSERT INTO CDC_PRESERVATION.PERFORMANCE.PERF_TEST_SOURCE
    SELECT 
        :v_max_id + SEQ4() + 1 AS RECORD_ID,
        :v_test_id || '_' || SEQ4() AS RECORD_KEY,
        'LATENCY_TEST' AS STRING_COL_1,
        TO_VARCHAR(:v_insert_time) AS STRING_COL_2,
        RANDSTR(100, RANDOM()) AS STRING_COL_3,
        UNIFORM(0, 1000, RANDOM())::NUMBER(18,4) AS NUMBER_COL_1,
        UNIFORM(0, 1000, RANDOM())::NUMBER(18,4) AS NUMBER_COL_2,
        CURRENT_TIMESTAMP() AS DATE_COL,
        CURRENT_TIMESTAMP() AS CREATED_AT,
        CURRENT_TIMESTAMP() AS MODIFIED_AT
    FROM TABLE(GENERATOR(ROWCOUNT => :P_NUM_RECORDS));
    
    -- Process immediately (simulating task execution)
    v_process_time := CURRENT_TIMESTAMP();
    
    -- Stage and merge
    CREATE OR REPLACE TEMPORARY TABLE _LATENCY_STAGING AS
    SELECT *, METADATA$ACTION AS CDC_ACTION, METADATA$ISUPDATE AS CDC_IS_UPDATE
    FROM CDC_PRESERVATION.PERFORMANCE.PERF_TEST_STREAM
    WHERE RECORD_KEY LIKE :v_test_id || '%';
    
    MERGE INTO CDC_PRESERVATION.PERFORMANCE.PERF_TEST_PRESERVED AS tgt
    USING (SELECT *, :v_test_id AS BATCH_ID FROM _LATENCY_STAGING) AS src
    ON tgt.RECORD_ID = src.RECORD_ID
    WHEN NOT MATCHED AND src.CDC_ACTION = 'INSERT' THEN
        INSERT (RECORD_ID, RECORD_KEY, STRING_COL_1, STRING_COL_2, STRING_COL_3,
               NUMBER_COL_1, NUMBER_COL_2, DATE_COL, CREATED_AT, MODIFIED_AT,
               CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID)
        VALUES (src.RECORD_ID, src.RECORD_KEY, src.STRING_COL_1, src.STRING_COL_2, src.STRING_COL_3,
               src.NUMBER_COL_1, src.NUMBER_COL_2, src.DATE_COL, src.CREATED_AT, src.MODIFIED_AT,
               'INSERT', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.BATCH_ID);
    
    v_complete_time := CURRENT_TIMESTAMP();
    
    -- Calculate latencies
    v_source_to_stream_ms := DATEDIFF('MILLISECOND', v_insert_time, v_process_time);
    v_stream_to_target_ms := DATEDIFF('MILLISECOND', v_process_time, v_complete_time);
    v_total_latency_ms := DATEDIFF('MILLISECOND', v_insert_time, v_complete_time);
    
    -- Record results
    INSERT INTO CDC_PRESERVATION.PERFORMANCE.TEST_RESULTS (
        TEST_RUN_ID, TEST_NAME, TEST_CATEGORY, BATCH_SIZE, OPERATION_TYPE,
        START_TIME, END_TIME, DURATION_MS, ROWS_PROCESSED, 
        LATENCY_AVG_MS, STREAM_STAGE_MS, MERGE_EXECUTE_MS, STATUS
    ) VALUES (
        :v_test_id, 'E2E_LATENCY_TEST', 'LATENCY', :P_NUM_RECORDS, 'INSERT',
        :v_insert_time, :v_complete_time, :v_total_latency_ms, :P_NUM_RECORDS,
        :v_total_latency_ms / :P_NUM_RECORDS, :v_source_to_stream_ms, :v_stream_to_target_ms, 'SUCCESS'
    );
    
    DROP TABLE IF EXISTS _LATENCY_STAGING;
    
    RETURN OBJECT_CONSTRUCT(
        'test_id', v_test_id,
        'records_processed', P_NUM_RECORDS,
        'insert_time', v_insert_time,
        'process_time', v_process_time,
        'complete_time', v_complete_time,
        'latency_breakdown', OBJECT_CONSTRUCT(
            'source_to_process_ms', v_source_to_stream_ms,
            'process_to_target_ms', v_stream_to_target_ms,
            'total_e2e_latency_ms', v_total_latency_ms,
            'avg_per_record_ms', ROUND(v_total_latency_ms / P_NUM_RECORDS, 4)
        ),
        'status', 'SUCCESS'
    );
END;
$$;

-- ============================================================================
-- SECTION 9: GENERATE PERFORMANCE REPORT
-- ============================================================================
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PERFORMANCE.SP_GENERATE_REPORT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_report VARCHAR := '';
BEGIN
    v_report := '
================================================================================
           CDC DATA PRESERVATION FRAMEWORK - PERFORMANCE REPORT
================================================================================

BASELINE METRICS:
';
    
    SELECT v_report || '
Total Tests Run:           ' || TOTAL_TESTS || '
Total Rows Processed:      ' || TOTAL_ROWS_PROCESSED || '
Avg Throughput:            ' || AVG_THROUGHPUT_ROWS_PER_SEC || ' rows/sec
Median Throughput:         ' || MEDIAN_THROUGHPUT || ' rows/sec
Min Throughput:            ' || MIN_THROUGHPUT || ' rows/sec
Max Throughput:            ' || MAX_THROUGHPUT || ' rows/sec
Avg Latency:               ' || AVG_LATENCY_MS || ' ms/row
P95 Latency:               ' || P95_LATENCY_MS || ' ms/row
Avg Stream Stage Time:     ' || AVG_STREAM_STAGE_MS || ' ms
Avg MERGE Time:            ' || AVG_MERGE_MS || ' ms
'
    INTO v_report
    FROM CDC_PRESERVATION.PERFORMANCE.V_PERFORMANCE_BASELINE;
    
    v_report := v_report || '
--------------------------------------------------------------------------------
PERFORMANCE BY OPERATION TYPE:
--------------------------------------------------------------------------------
';
    
    FOR rec IN (SELECT * FROM CDC_PRESERVATION.PERFORMANCE.V_PERF_BY_OPERATION) DO
        v_report := v_report || '
' || rec.OPERATION_TYPE || ':
  - Test Count:     ' || rec.TEST_COUNT || '
  - Total Rows:     ' || rec.TOTAL_ROWS || '
  - Avg Throughput: ' || rec.AVG_THROUGHPUT || ' rows/sec
  - Avg Latency:    ' || rec.AVG_LATENCY_MS || ' ms/row
  - Avg Stream:     ' || rec.AVG_STREAM_MS || ' ms
  - Avg MERGE:      ' || rec.AVG_MERGE_MS || ' ms
';
    END FOR;
    
    v_report := v_report || '
--------------------------------------------------------------------------------
PERFORMANCE BY BATCH SIZE:
--------------------------------------------------------------------------------
';
    
    FOR rec IN (SELECT * FROM CDC_PRESERVATION.PERFORMANCE.V_PERF_BY_BATCH_SIZE) DO
        v_report := v_report || '
Batch Size ' || rec.BATCH_SIZE || ':
  - Test Count:     ' || rec.TEST_COUNT || '
  - Avg Throughput: ' || rec.AVG_THROUGHPUT || ' rows/sec
  - Avg Duration:   ' || rec.AVG_DURATION_MS || ' ms
  - Avg Latency:    ' || rec.AVG_LATENCY_MS || ' ms/row
';
    END FOR;
    
    v_report := v_report || '
================================================================================
                              END OF REPORT
================================================================================
';
    
    RETURN v_report;
END;
$$;

-- ============================================================================
-- END OF PERFORMANCE TEST SCRIPT
-- ============================================================================
/*
USAGE:
================================================================================

1. Quick Test (5 tests, ~30 seconds):
   CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_QUICK_TEST();

2. Individual Tests:
   CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST('MY_TEST', 1000, 'INSERT');
   CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST('MY_TEST', 1000, 'UPDATE');
   CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST('MY_TEST', 500, 'DELETE');
   CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_PERFORMANCE_TEST('MY_TEST', 2000, 'MIXED');

3. End-to-End Latency Test:
   CALL CDC_PRESERVATION.PERFORMANCE.SP_MEASURE_E2E_LATENCY(100);

4. Full Test Suite (comprehensive, takes 5-10 minutes):
   CALL CDC_PRESERVATION.PERFORMANCE.SP_RUN_FULL_TEST_SUITE();

5. View Results:
   SELECT * FROM CDC_PRESERVATION.PERFORMANCE.V_LATEST_TEST_RUN;
   SELECT * FROM CDC_PRESERVATION.PERFORMANCE.V_PERF_BY_OPERATION;
   SELECT * FROM CDC_PRESERVATION.PERFORMANCE.V_PERF_BY_BATCH_SIZE;
   SELECT * FROM CDC_PRESERVATION.PERFORMANCE.V_PERFORMANCE_BASELINE;

6. Generate Text Report:
   CALL CDC_PRESERVATION.PERFORMANCE.SP_GENERATE_REPORT();

EXPECTED BASELINE (X-Small Warehouse):
================================================================================
| Batch Size | Throughput (rows/sec) | Latency (ms/row) |
|------------|----------------------|------------------|
| 1,000      | 2,000 - 5,000       | 0.2 - 0.5        |
| 10,000     | 5,000 - 15,000      | 0.07 - 0.2       |
| 100,000    | 10,000 - 30,000     | 0.03 - 0.1       |
================================================================================
*/
