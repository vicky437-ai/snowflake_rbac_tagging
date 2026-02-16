# CDC Data Preservation Framework
## Production Implementation Runbook

---

# Table of Contents

1. [Pre-Implementation Checklist](#1-pre-implementation-checklist)
2. [Environment Setup](#2-environment-setup)
3. [Framework Deployment](#3-framework-deployment)
4. [Table Registration](#4-table-registration)
5. [Validation & Testing](#5-validation--testing)
6. [Go-Live Procedure](#6-go-live-procedure)
7. [Monitoring & Operations](#7-monitoring--operations)
8. [Troubleshooting Guide](#8-troubleshooting-guide)
9. [Rollback Procedures](#9-rollback-procedures)
10. [Maintenance Procedures](#10-maintenance-procedures)

---

# 1. Pre-Implementation Checklist

## 1.1 Prerequisites

| Requirement | Description | Status |
|-------------|-------------|--------|
| Snowflake Role | Role with CREATE DATABASE, SCHEMA, TABLE, STREAM, TASK privileges | ☐ |
| Warehouse | Dedicated warehouse for CDC processing (recommended: XSMALL) | ☐ |
| Source Tables | List of all _BASE tables to protect | ☐ |
| Primary Keys | Documented primary key columns for each table | ☐ |
| Change Tracking | Source tables must support CHANGE_TRACKING | ☐ |
| Network Access | Snowflake account accessible from deployment environment | ☐ |

## 1.2 Information Gathering

```sql
-- Run this to identify all _BASE tables in your environment
SHOW TABLES LIKE '%_BASE' IN DATABASE <YOUR_DATABASE>;

-- Check if tables support change tracking
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, 
       IS_TRANSIENT, RETENTION_TIME
FROM <YOUR_DATABASE>.INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE '%_BASE';

-- Verify primary key constraints exist
SELECT tc.TABLE_NAME, kcu.COLUMN_NAME
FROM <YOUR_DATABASE>.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN <YOUR_DATABASE>.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
  AND tc.TABLE_NAME LIKE '%_BASE';
```

## 1.3 Sign-Off Requirements

| Approver | Role | Signature | Date |
|----------|------|-----------|------|
| Data Engineering Lead | Technical Approval | | |
| DBA | Database Approval | | |
| Security | Security Review | | |
| Business Owner | Business Approval | | |

---

# 2. Environment Setup

## 2.1 Role and Privileges Setup

```sql
-- STEP 2.1.1: Create dedicated role for CDC Framework
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS CDC_FRAMEWORK_ADMIN;
CREATE ROLE IF NOT EXISTS CDC_FRAMEWORK_OPERATOR;

-- Grant hierarchy
GRANT ROLE CDC_FRAMEWORK_OPERATOR TO ROLE CDC_FRAMEWORK_ADMIN;
GRANT ROLE CDC_FRAMEWORK_ADMIN TO ROLE SYSADMIN;

-- STEP 2.1.2: Grant necessary privileges
USE ROLE ACCOUNTADMIN;

-- Database creation privilege
GRANT CREATE DATABASE ON ACCOUNT TO ROLE CDC_FRAMEWORK_ADMIN;

-- Warehouse usage
GRANT USAGE ON WAREHOUSE <CDC_WAREHOUSE> TO ROLE CDC_FRAMEWORK_ADMIN;
GRANT USAGE ON WAREHOUSE <CDC_WAREHOUSE> TO ROLE CDC_FRAMEWORK_OPERATOR;
GRANT OPERATE ON WAREHOUSE <CDC_WAREHOUSE> TO ROLE CDC_FRAMEWORK_ADMIN;

-- Source database access
GRANT USAGE ON DATABASE <SOURCE_DATABASE> TO ROLE CDC_FRAMEWORK_ADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE <SOURCE_DATABASE> TO ROLE CDC_FRAMEWORK_ADMIN;
GRANT SELECT ON ALL TABLES IN DATABASE <SOURCE_DATABASE> TO ROLE CDC_FRAMEWORK_ADMIN;
GRANT CREATE STREAM ON ALL SCHEMAS IN DATABASE <SOURCE_DATABASE> TO ROLE CDC_FRAMEWORK_ADMIN;

-- Task execution privileges
GRANT EXECUTE TASK ON ACCOUNT TO ROLE CDC_FRAMEWORK_ADMIN;
```

## 2.2 Warehouse Configuration

```sql
-- STEP 2.2.1: Create dedicated warehouse (if not exists)
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS CDC_PROCESSING_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Dedicated warehouse for CDC Data Preservation Framework';

-- STEP 2.2.2: Configure resource monitor (recommended)
CREATE RESOURCE MONITOR IF NOT EXISTS CDC_MONITOR
    WITH CREDIT_QUOTA = 100
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE CDC_PROCESSING_WH SET RESOURCE_MONITOR = CDC_MONITOR;
```

---

# 3. Framework Deployment

## 3.1 Database and Schema Creation

```sql
-- STEP 3.1.1: Set context
USE ROLE CDC_FRAMEWORK_ADMIN;
USE WAREHOUSE CDC_PROCESSING_WH;

-- STEP 3.1.2: Create framework database
CREATE DATABASE IF NOT EXISTS CDC_PRESERVATION
    COMMENT = 'CDC Data Preservation Framework - Metadata-driven CDC processing';

-- STEP 3.1.3: Create schemas
CREATE SCHEMA IF NOT EXISTS CDC_PRESERVATION.CONFIG
    COMMENT = 'Configuration tables for CDC processing';
    
CREATE SCHEMA IF NOT EXISTS CDC_PRESERVATION.PROCESSING
    COMMENT = 'Stored procedures and processing logic';
    
CREATE SCHEMA IF NOT EXISTS CDC_PRESERVATION.MONITORING
    COMMENT = 'Logging and monitoring tables/views';
```

## 3.2 Configuration Tables

```sql
-- STEP 3.2.1: Create TABLE_CONFIG
CREATE TABLE IF NOT EXISTS CDC_PRESERVATION.CONFIG.TABLE_CONFIG (
    CONFIG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    SOURCE_DATABASE VARCHAR(255) NOT NULL,
    SOURCE_SCHEMA VARCHAR(255) NOT NULL,
    SOURCE_TABLE VARCHAR(255) NOT NULL,
    TARGET_DATABASE VARCHAR(255) NOT NULL,
    TARGET_SCHEMA VARCHAR(255) NOT NULL,
    TARGET_TABLE VARCHAR(255) NOT NULL,
    PRIMARY_KEY_COLUMNS VARCHAR(4000) NOT NULL,
    STREAM_NAME VARCHAR(255),
    TASK_NAME VARCHAR(255),
    TASK_WAREHOUSE VARCHAR(255) DEFAULT 'CDC_PROCESSING_WH',
    TASK_SCHEDULE VARCHAR(100) DEFAULT '5 MINUTE',
    TASK_TIMEOUT_MS NUMBER DEFAULT 3600000,
    TASK_MAX_FAILURES NUMBER DEFAULT 3,
    DATA_RETENTION_DAYS NUMBER DEFAULT 14,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    PRIORITY NUMBER DEFAULT 100,
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(255) DEFAULT CURRENT_USER(),
    NOTES VARCHAR(4000),
    CONSTRAINT UK_SOURCE UNIQUE (SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE)
);

-- STEP 3.2.2: Create PROCESSING_LOG
CREATE TABLE IF NOT EXISTS CDC_PRESERVATION.MONITORING.PROCESSING_LOG (
    LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    CONFIG_ID NUMBER,
    BATCH_ID VARCHAR(100),
    SOURCE_TABLE_FQN VARCHAR(1000),
    TARGET_TABLE_FQN VARCHAR(1000),
    PROCESS_TYPE VARCHAR(50),
    PROCESS_START_TIME TIMESTAMP_LTZ,
    PROCESS_END_TIME TIMESTAMP_LTZ,
    ROWS_INSERTED NUMBER DEFAULT 0,
    ROWS_UPDATED NUMBER DEFAULT 0,
    ROWS_DELETED NUMBER DEFAULT 0,
    ROWS_PROCESSED NUMBER DEFAULT 0,
    STATUS VARCHAR(20),
    ERROR_MESSAGE VARCHAR(16000),
    EXECUTION_TIME_SECONDS NUMBER(10,2),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- STEP 3.2.3: Create STREAM_STATUS
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
```

## 3.3 Deploy Stored Procedures

Deploy all 10 stored procedures in the following order:

### 3.3.1 SP_PROCESS_CDC_GENERIC (Core Processing)

```sql
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(
    P_CONFIG_ID NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_source_db VARCHAR; v_source_schema VARCHAR; v_source_table VARCHAR;
    v_target_db VARCHAR; v_target_schema VARCHAR; v_target_table VARCHAR;
    v_pk_columns VARCHAR; v_stream_name VARCHAR;
    v_source_fqn VARCHAR; v_target_fqn VARCHAR; v_stream_fqn VARCHAR;
    v_batch_id VARCHAR; v_start_time TIMESTAMP_LTZ;
    v_stream_stale BOOLEAN DEFAULT FALSE;
    v_staging_count NUMBER DEFAULT 0; v_rows_processed NUMBER DEFAULT 0;
    v_process_type VARCHAR DEFAULT 'NORMAL';
    v_error_msg VARCHAR; v_exec_secs NUMBER;
    v_col_list VARCHAR; v_pk_join_on VARCHAR; v_update_set VARCHAR;
    v_insert_cols VARCHAR; v_insert_vals VARCHAR;
BEGIN
    v_batch_id := 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    v_start_time := CURRENT_TIMESTAMP();
    
    -- Load configuration
    SELECT SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE,
           PRIMARY_KEY_COLUMNS, COALESCE(STREAM_NAME, SOURCE_TABLE || '_STREAM')
    INTO v_source_db, v_source_schema, v_source_table, v_target_db, v_target_schema, v_target_table,
         v_pk_columns, v_stream_name
    FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE CONFIG_ID = :P_CONFIG_ID AND IS_ACTIVE = TRUE;
    
    v_source_fqn := v_source_db || '.' || v_source_schema || '.' || v_source_table;
    v_target_fqn := v_target_db || '.' || v_target_schema || '.' || v_target_table;
    v_stream_fqn := v_source_db || '.' || v_source_schema || '.' || v_stream_name;
    
    -- Build dynamic column lists
    LET col_sql VARCHAR := 'SELECT LISTAGG(COLUMN_NAME, '','') WITHIN GROUP (ORDER BY ORDINAL_POSITION) 
        FROM ' || v_source_db || '.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''' || v_source_schema || ''' AND TABLE_NAME = ''' || v_source_table || '''';
    EXECUTE IMMEDIATE col_sql;
    LET rs1 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur1 CURSOR FOR rs1; OPEN cur1; FETCH cur1 INTO v_col_list; CLOSE cur1;
    
    -- Build PK join and UPDATE SET clauses
    LET pk_sql VARCHAR := 'SELECT LISTAGG(''tgt.'' || TRIM(VALUE) || '' = src.'' || TRIM(VALUE), '' AND '') FROM TABLE(SPLIT_TO_TABLE(''' || v_pk_columns || ''', '',''))';
    EXECUTE IMMEDIATE pk_sql;
    LET rs2 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur2 CURSOR FOR rs2; OPEN cur2; FETCH cur2 INTO v_pk_join_on; CLOSE cur2;
    
    LET upd_sql VARCHAR := 'SELECT LISTAGG(''tgt.'' || TRIM(VALUE) || '' = src.'' || TRIM(VALUE), '', '') FROM TABLE(SPLIT_TO_TABLE(''' || v_col_list || ''', '','')) WHERE TRIM(VALUE) NOT IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(''' || v_pk_columns || ''', '','')))'; 
    EXECUTE IMMEDIATE upd_sql;
    LET rs3 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur3 CURSOR FOR rs3; OPEN cur3; FETCH cur3 INTO v_update_set; CLOSE cur3;
    
    v_insert_cols := v_col_list || ', CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID';
    v_insert_vals := 'src.' || REPLACE(v_col_list, ',', ', src.') || ', ''INSERT'', CURRENT_TIMESTAMP(), FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''' || v_batch_id || '''';
    
    -- Check stream health
    BEGIN
        LET stale_check VARCHAR := 'SELECT COUNT(*) FROM ' || v_stream_fqn || ' WHERE 1=0';
        EXECUTE IMMEDIATE stale_check;
        v_stream_stale := FALSE;
    EXCEPTION
        WHEN OTHER THEN v_stream_stale := TRUE; v_error_msg := SQLERRM;
    END;
    
    -- RECOVERY MODE if stream stale
    IF (v_stream_stale = TRUE) THEN
        v_process_type := 'RECOVERY';
        EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM ' || v_stream_fqn || ' ON TABLE ' || v_source_fqn || ' SHOW_INITIAL_ROWS = TRUE';
        
        LET pk_recovery VARCHAR := 'SELECT LISTAGG(''src.'' || TRIM(VALUE) || '' = tgt.'' || TRIM(VALUE), '' AND '') FROM TABLE(SPLIT_TO_TABLE(''' || v_pk_columns || ''', '',''))';
        EXECUTE IMMEDIATE pk_recovery;
        LET rs4 RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        LET cur4 CURSOR FOR rs4;
        LET v_pk_join_src VARCHAR;
        OPEN cur4; FETCH cur4 INTO v_pk_join_src; CLOSE cur4;
        
        LET recovery_sql VARCHAR := 'MERGE INTO ' || v_target_fqn || ' AS tgt USING (SELECT src.*, ''' || v_batch_id || ''' AS BATCH_ID FROM ' || v_source_fqn || ' src LEFT JOIN ' || v_target_fqn || ' tgt ON ' || v_pk_join_src || ' WHERE tgt.' || SPLIT_PART(v_pk_columns, ',', 1) || ' IS NULL OR tgt.IS_DELETED = TRUE) AS src ON ' || v_pk_join_on || ' WHEN MATCHED THEN UPDATE SET ' || v_update_set || ', tgt.CDC_OPERATION = ''RELOADED'', tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(), tgt.IS_DELETED = FALSE, tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(), tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID WHEN NOT MATCHED THEN INSERT (' || v_insert_cols || ') VALUES (' || v_insert_vals || ')';
        EXECUTE IMMEDIATE recovery_sql;
        v_rows_processed := SQLROWCOUNT;
        
        UPDATE CDC_PRESERVATION.MONITORING.STREAM_STATUS SET IS_STALE = FALSE, LAST_RECOVERY_AT = CURRENT_TIMESTAMP(), RECOVERY_COUNT = RECOVERY_COUNT + 1 WHERE CONFIG_ID = :P_CONFIG_ID;
        v_exec_secs := DATEDIFF('SECOND', v_start_time, CURRENT_TIMESTAMP());
        INSERT INTO CDC_PRESERVATION.MONITORING.PROCESSING_LOG (CONFIG_ID, BATCH_ID, SOURCE_TABLE_FQN, TARGET_TABLE_FQN, PROCESS_TYPE, PROCESS_START_TIME, PROCESS_END_TIME, ROWS_PROCESSED, STATUS, EXECUTION_TIME_SECONDS) VALUES (:P_CONFIG_ID, :v_batch_id, :v_source_fqn, :v_target_fqn, 'RECOVERY', :v_start_time, CURRENT_TIMESTAMP(), :v_rows_processed, 'RECOVERED', :v_exec_secs);
        RETURN 'RECOVERY_COMPLETE: ' || v_rows_processed || ' rows. Batch: ' || v_batch_id;
    END IF;
    
    -- NORMAL MODE - Stage and MERGE
    LET staging_sql VARCHAR := 'CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_' || P_CONFIG_ID || ' AS SELECT ' || v_col_list || ', METADATA$ACTION AS CDC_ACTION, METADATA$ISUPDATE AS CDC_IS_UPDATE FROM ' || v_stream_fqn;
    EXECUTE IMMEDIATE staging_sql;
    
    LET cnt_sql VARCHAR := 'SELECT COUNT(*) FROM _CDC_STAGING_' || P_CONFIG_ID;
    EXECUTE IMMEDIATE cnt_sql;
    LET cnt_rs RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cnt_cur CURSOR FOR cnt_rs; OPEN cnt_cur; FETCH cnt_cur INTO v_staging_count; CLOSE cnt_cur;
    
    IF (v_staging_count = 0) THEN
        EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS _CDC_STAGING_' || P_CONFIG_ID;
        RETURN 'NO_DATA: Stream empty for ' || v_source_table;
    END IF;
    
    LET merge_sql VARCHAR := 'MERGE INTO ' || v_target_fqn || ' AS tgt USING (SELECT *, ''' || v_batch_id || ''' AS BATCH_ID FROM _CDC_STAGING_' || P_CONFIG_ID || ') AS src ON ' || v_pk_join_on || ' WHEN MATCHED AND src.CDC_ACTION = ''INSERT'' AND src.CDC_IS_UPDATE = TRUE THEN UPDATE SET ' || v_update_set || ', tgt.CDC_OPERATION = ''UPDATE'', tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(), tgt.IS_DELETED = FALSE, tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(), tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID WHEN MATCHED AND src.CDC_ACTION = ''DELETE'' AND src.CDC_IS_UPDATE = FALSE THEN UPDATE SET tgt.CDC_OPERATION = ''DELETE'', tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(), tgt.IS_DELETED = TRUE, tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(), tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID WHEN MATCHED AND src.CDC_ACTION = ''INSERT'' AND src.CDC_IS_UPDATE = FALSE THEN UPDATE SET ' || v_update_set || ', tgt.CDC_OPERATION = ''INSERT'', tgt.CDC_TIMESTAMP = CURRENT_TIMESTAMP(), tgt.IS_DELETED = FALSE, tgt.RECORD_UPDATED_AT = CURRENT_TIMESTAMP(), tgt.SOURCE_LOAD_BATCH_ID = src.BATCH_ID WHEN NOT MATCHED AND src.CDC_ACTION = ''INSERT'' THEN INSERT (' || v_insert_cols || ') VALUES (' || v_insert_vals || ')';
    EXECUTE IMMEDIATE merge_sql;
    v_rows_processed := SQLROWCOUNT;
    
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS _CDC_STAGING_' || P_CONFIG_ID;
    v_exec_secs := DATEDIFF('SECOND', v_start_time, CURRENT_TIMESTAMP());
    INSERT INTO CDC_PRESERVATION.MONITORING.PROCESSING_LOG (CONFIG_ID, BATCH_ID, SOURCE_TABLE_FQN, TARGET_TABLE_FQN, PROCESS_TYPE, PROCESS_START_TIME, PROCESS_END_TIME, ROWS_PROCESSED, STATUS, EXECUTION_TIME_SECONDS) VALUES (:P_CONFIG_ID, :v_batch_id, :v_source_fqn, :v_target_fqn, 'NORMAL', :v_start_time, CURRENT_TIMESTAMP(), :v_rows_processed, 'SUCCESS', :v_exec_secs);
    RETURN 'SUCCESS: ' || v_rows_processed || ' rows. Table: ' || v_source_table || '. Batch: ' || v_batch_id;
EXCEPTION
    WHEN OTHER THEN
        v_error_msg := SQLERRM; v_exec_secs := DATEDIFF('SECOND', v_start_time, CURRENT_TIMESTAMP());
        INSERT INTO CDC_PRESERVATION.MONITORING.PROCESSING_LOG (CONFIG_ID, BATCH_ID, SOURCE_TABLE_FQN, TARGET_TABLE_FQN, PROCESS_TYPE, PROCESS_START_TIME, PROCESS_END_TIME, STATUS, ERROR_MESSAGE, EXECUTION_TIME_SECONDS) VALUES (:P_CONFIG_ID, :v_batch_id, :v_source_fqn, :v_target_fqn, :v_process_type, :v_start_time, CURRENT_TIMESTAMP(), 'FAILED', :v_error_msg, :v_exec_secs);
        RETURN 'ERROR: ' || v_error_msg;
END;
$$;
```

### 3.3.2 SP_CREATE_TARGET_TABLE

```sql
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_CREATE_TARGET_TABLE(P_CONFIG_ID NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_source_fqn VARCHAR; v_target_fqn VARCHAR; v_target_db VARCHAR;
    v_target_schema VARCHAR; v_pk_columns VARCHAR; v_col_defs VARCHAR;
BEGIN
    SELECT SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE,
           TARGET_DATABASE || '.' || TARGET_SCHEMA || '.' || TARGET_TABLE,
           TARGET_DATABASE, TARGET_SCHEMA, PRIMARY_KEY_COLUMNS
    INTO v_source_fqn, v_target_fqn, v_target_db, v_target_schema, v_pk_columns
    FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE CONFIG_ID = :P_CONFIG_ID;
    
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || v_target_db || '.' || v_target_schema;
    
    LET col_def_sql VARCHAR := 'SELECT LISTAGG(COLUMN_NAME || '' '' || DATA_TYPE || 
        CASE WHEN DATA_TYPE IN (''NUMBER'', ''DECIMAL'', ''NUMERIC'') AND NUMERIC_PRECISION IS NOT NULL 
                THEN ''('' || NUMERIC_PRECISION || '','' || COALESCE(NUMERIC_SCALE, 0) || '')''
             WHEN DATA_TYPE IN (''VARCHAR'', ''CHAR'', ''STRING'', ''TEXT'') AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL 
                THEN ''('' || CHARACTER_MAXIMUM_LENGTH || '')''
             ELSE '''' END, '', '') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
        FROM ' || SPLIT_PART(v_source_fqn, '.', 1) || '.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = ''' || SPLIT_PART(v_source_fqn, '.', 2) || ''' AND TABLE_NAME = ''' || SPLIT_PART(v_source_fqn, '.', 3) || '''';
    
    EXECUTE IMMEDIATE col_def_sql;
    LET rs RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET cur CURSOR FOR rs; OPEN cur; FETCH cur INTO v_col_defs; CLOSE cur;
    
    LET create_sql VARCHAR := 'CREATE TABLE IF NOT EXISTS ' || v_target_fqn || ' (
        ' || v_col_defs || ',
        CDC_OPERATION VARCHAR(10), CDC_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        IS_DELETED BOOLEAN DEFAULT FALSE, RECORD_CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        RECORD_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), SOURCE_LOAD_BATCH_ID VARCHAR(100),
        PRIMARY KEY (' || v_pk_columns || '))';
    
    EXECUTE IMMEDIATE create_sql;
    RETURN 'SUCCESS: Target table created - ' || v_target_fqn;
EXCEPTION
    WHEN OTHER THEN RETURN 'ERROR: ' || SQLERRM;
END;
$$;
```

### 3.3.3 SP_CREATE_STREAM

```sql
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_CREATE_STREAM(P_CONFIG_ID NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_source_fqn VARCHAR; v_stream_name VARCHAR; v_stream_fqn VARCHAR;
    v_source_db VARCHAR; v_source_schema VARCHAR; v_source_table VARCHAR;
BEGIN
    SELECT SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE,
           SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE,
           COALESCE(STREAM_NAME, SOURCE_TABLE || '_STREAM')
    INTO v_source_db, v_source_schema, v_source_table, v_source_fqn, v_stream_name
    FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE CONFIG_ID = :P_CONFIG_ID;
    
    v_stream_fqn := v_source_db || '.' || v_source_schema || '.' || v_stream_name;
    
    EXECUTE IMMEDIATE 'ALTER TABLE ' || v_source_fqn || ' SET CHANGE_TRACKING = TRUE';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM ' || v_stream_fqn || ' ON TABLE ' || v_source_fqn || 
        ' SHOW_INITIAL_ROWS = TRUE COMMENT = ''CDC Stream - Config ID: ' || P_CONFIG_ID || '''';
    
    UPDATE CDC_PRESERVATION.CONFIG.TABLE_CONFIG SET STREAM_NAME = :v_stream_name, UPDATED_AT = CURRENT_TIMESTAMP() WHERE CONFIG_ID = :P_CONFIG_ID;
    
    MERGE INTO CDC_PRESERVATION.MONITORING.STREAM_STATUS ss
    USING (SELECT :P_CONFIG_ID AS CID, :v_stream_fqn AS SFQN) src ON ss.CONFIG_ID = src.CID
    WHEN NOT MATCHED THEN INSERT (CONFIG_ID, STREAM_FQN, LAST_CHECKED_AT) VALUES (src.CID, src.SFQN, CURRENT_TIMESTAMP());
    
    RETURN 'SUCCESS: Stream created - ' || v_stream_fqn;
EXCEPTION
    WHEN OTHER THEN RETURN 'ERROR: ' || SQLERRM;
END;
$$;
```

### 3.3.4 SP_CREATE_TASK

```sql
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_CREATE_TASK(P_CONFIG_ID NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_task_name VARCHAR; v_task_fqn VARCHAR; v_stream_name VARCHAR; v_stream_fqn VARCHAR;
    v_source_db VARCHAR; v_source_schema VARCHAR; v_source_table VARCHAR;
    v_warehouse VARCHAR; v_schedule VARCHAR; v_timeout_ms NUMBER; v_max_failures NUMBER;
BEGIN
    SELECT SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE,
           COALESCE(TASK_NAME, 'TASK_' || SOURCE_TABLE || '_CDC'),
           COALESCE(STREAM_NAME, SOURCE_TABLE || '_STREAM'),
           COALESCE(TASK_WAREHOUSE, 'COMPUTE_WH'),
           COALESCE(TASK_SCHEDULE, '5 MINUTE'),
           COALESCE(TASK_TIMEOUT_MS, 3600000),
           COALESCE(TASK_MAX_FAILURES, 3)
    INTO v_source_db, v_source_schema, v_source_table, v_task_name, v_stream_name, 
         v_warehouse, v_schedule, v_timeout_ms, v_max_failures
    FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE CONFIG_ID = :P_CONFIG_ID;
    
    v_task_fqn := v_source_db || '.' || v_source_schema || '.' || v_task_name;
    v_stream_fqn := v_source_db || '.' || v_source_schema || '.' || v_stream_name;
    
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TASK ' || v_task_fqn || '
            WAREHOUSE = ' || v_warehouse || '
            SCHEDULE = ''' || v_schedule || '''
            ALLOW_OVERLAPPING_EXECUTION = FALSE
            SUSPEND_TASK_AFTER_NUM_FAILURES = ' || v_max_failures || '
            USER_TASK_TIMEOUT_MS = ' || v_timeout_ms || '
            COMMENT = ''CDC Task for ' || v_source_table || ' - Config ID: ' || P_CONFIG_ID || '''
        WHEN
            SYSTEM$STREAM_HAS_DATA(''' || v_stream_fqn || ''')
        AS
            CALL CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(' || P_CONFIG_ID || ')';
    
    UPDATE CDC_PRESERVATION.CONFIG.TABLE_CONFIG SET TASK_NAME = :v_task_name, UPDATED_AT = CURRENT_TIMESTAMP() WHERE CONFIG_ID = :P_CONFIG_ID;
    
    RETURN 'SUCCESS: Task created (SUSPENDED) - ' || v_task_fqn;
EXCEPTION
    WHEN OTHER THEN RETURN 'ERROR: ' || SQLERRM;
END;
$$;
```

### 3.3.5 SP_RESUME_TASK & SP_SUSPEND_TASK

```sql
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
    INTO v_task_fqn FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE CONFIG_ID = :P_CONFIG_ID;
    
    EXECUTE IMMEDIATE 'ALTER TASK ' || v_task_fqn || ' RESUME';
    RETURN 'SUCCESS: Task resumed - ' || v_task_fqn;
EXCEPTION
    WHEN OTHER THEN RETURN 'ERROR: ' || SQLERRM;
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
    INTO v_task_fqn FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE CONFIG_ID = :P_CONFIG_ID;
    
    EXECUTE IMMEDIATE 'ALTER TASK ' || v_task_fqn || ' SUSPEND';
    RETURN 'SUCCESS: Task suspended - ' || v_task_fqn;
EXCEPTION
    WHEN OTHER THEN RETURN 'ERROR: ' || SQLERRM;
END;
$$;
```

### 3.3.6 SP_RESUME_ALL_TASKS & SP_SUSPEND_ALL_TASKS

```sql
-- Resume all tasks
CREATE OR REPLACE PROCEDURE CDC_PRESERVATION.PROCESSING.SP_RESUME_ALL_TASKS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_results ARRAY := ARRAY_CONSTRUCT();
    v_config_id NUMBER; v_source_table VARCHAR;
    c_configs CURSOR FOR SELECT CONFIG_ID, SOURCE_TABLE FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE IS_ACTIVE = TRUE;
BEGIN
    FOR rec IN c_configs DO
        v_config_id := rec.CONFIG_ID; v_source_table := rec.SOURCE_TABLE;
        BEGIN
            CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_TASK(:v_config_id);
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('config_id', v_config_id, 'table', v_source_table, 'status', 'RESUMED'));
        EXCEPTION
            WHEN OTHER THEN
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('config_id', v_config_id, 'table', v_source_table, 'status', 'FAILED', 'error', SQLERRM));
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
    v_config_id NUMBER; v_source_table VARCHAR;
    c_configs CURSOR FOR SELECT CONFIG_ID, SOURCE_TABLE FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE IS_ACTIVE = TRUE;
BEGIN
    FOR rec IN c_configs DO
        v_config_id := rec.CONFIG_ID; v_source_table := rec.SOURCE_TABLE;
        BEGIN
            CALL CDC_PRESERVATION.PROCESSING.SP_SUSPEND_TASK(:v_config_id);
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('config_id', v_config_id, 'table', v_source_table, 'status', 'SUSPENDED'));
        EXCEPTION
            WHEN OTHER THEN
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('config_id', v_config_id, 'table', v_source_table, 'status', 'FAILED', 'error', SQLERRM));
        END;
    END FOR;
    RETURN OBJECT_CONSTRUCT('tasks_processed', ARRAY_SIZE(v_results), 'details', v_results);
END;
$$;
```

### 3.3.7 SP_SETUP_PIPELINE & SP_SETUP_ALL_PIPELINES

```sql
-- Setup single pipeline (creates target table, stream, task)
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
    v_source_table VARCHAR;
BEGIN
    SELECT SOURCE_TABLE INTO v_source_table FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE CONFIG_ID = :P_CONFIG_ID;
    
    CALL CDC_PRESERVATION.PROCESSING.SP_CREATE_TARGET_TABLE(:P_CONFIG_ID);
    CALL CDC_PRESERVATION.PROCESSING.SP_CREATE_STREAM(:P_CONFIG_ID);
    CALL CDC_PRESERVATION.PROCESSING.SP_CREATE_TASK(:P_CONFIG_ID);
    
    IF (P_AUTO_START) THEN
        CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_TASK(:P_CONFIG_ID);
    END IF;
    
    RETURN OBJECT_CONSTRUCT('config_id', P_CONFIG_ID, 'source_table', v_source_table, 
        'target_table', 'CREATED', 'stream', 'CREATED', 'task', IFF(P_AUTO_START, 'RUNNING', 'SUSPENDED'), 'status', 'SUCCESS');
EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT('config_id', P_CONFIG_ID, 'status', 'FAILED', 'error', SQLERRM);
END;
$$;

-- Setup ALL pipelines
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
    v_config_id NUMBER; v_source_table VARCHAR;
    v_auto_start BOOLEAN := P_AUTO_START;
    c_configs CURSOR FOR SELECT CONFIG_ID, SOURCE_TABLE FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG WHERE IS_ACTIVE = TRUE ORDER BY PRIORITY;
BEGIN
    FOR rec IN c_configs DO
        v_config_id := rec.CONFIG_ID; v_source_table := rec.SOURCE_TABLE;
        BEGIN
            CALL CDC_PRESERVATION.PROCESSING.SP_SETUP_PIPELINE(:v_config_id, :v_auto_start);
            v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('config_id', v_config_id, 'table', v_source_table, 'status', 'SUCCESS'));
        EXCEPTION
            WHEN OTHER THEN
                v_results := ARRAY_APPEND(v_results, OBJECT_CONSTRUCT('config_id', v_config_id, 'table', v_source_table, 'status', 'FAILED', 'error', SQLERRM));
        END;
    END FOR;
    RETURN OBJECT_CONSTRUCT('total_pipelines', ARRAY_SIZE(v_results), 'auto_start', v_auto_start, 'details', v_results);
END;
$$;
```

### 3.3.8 Verification

```sql
-- Verify all 10 procedures created
SHOW PROCEDURES IN SCHEMA CDC_PRESERVATION.PROCESSING;

-- Expected output:
-- SP_PROCESS_CDC_GENERIC
-- SP_CREATE_TARGET_TABLE
-- SP_CREATE_STREAM
-- SP_CREATE_TASK
-- SP_RESUME_TASK
-- SP_SUSPEND_TASK
-- SP_RESUME_ALL_TASKS
-- SP_SUSPEND_ALL_TASKS
-- SP_SETUP_PIPELINE
-- SP_SETUP_ALL_PIPELINES
```

## 3.4 Deploy Monitoring Views

```sql
-- STEP 3.4.1: Create monitoring views
-- Execute monitoring view creation from CDC_Data_Preservation_Framework.sql:
-- - V_PIPELINE_STATUS
-- - V_PROCESSING_STATS
-- - V_RECENT_ERRORS

-- Verification:
SHOW VIEWS IN SCHEMA CDC_PRESERVATION.MONITORING;
```

---

# 4. Table Registration

## 4.1 Register Tables in Configuration

```sql
-- STEP 4.1.1: Register your _BASE tables
-- Replace with your actual table information

INSERT INTO CDC_PRESERVATION.CONFIG.TABLE_CONFIG 
(SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, PRIMARY_KEY_COLUMNS, TASK_SCHEDULE, PRIORITY, NOTES)
VALUES
-- High-priority transaction tables (5 min schedule)
('D_BRONZE', 'SADB', 'OPTRN_LEG_BASE', 'D_BRONZE', 'SADB', 'OPTRN_LEG', 'OPTRN_LEG_ID', '5 MINUTE', 1, 'Options transaction legs'),
('D_BRONZE', 'SADB', 'OPTRN_BASE', 'D_BRONZE', 'SADB', 'OPTRN', 'OPTRN_ID', '5 MINUTE', 2, 'Options transactions'),
('D_BRONZE', 'SADB', 'TRADE_BASE', 'D_BRONZE', 'SADB', 'TRADE', 'TRADE_ID', '5 MINUTE', 3, 'Trade data'),
('D_BRONZE', 'SADB', 'POSITION_BASE', 'D_BRONZE', 'SADB', 'POSITION', 'POSITION_ID', '5 MINUTE', 4, 'Position data'),
('D_BRONZE', 'SADB', 'CASHFLOW_BASE', 'D_BRONZE', 'SADB', 'CASHFLOW', 'CASHFLOW_ID', '5 MINUTE', 5, 'Cashflow data'),
('D_BRONZE', 'SADB', 'SETTLEMENT_BASE', 'D_BRONZE', 'SADB', 'SETTLEMENT', 'SETTLEMENT_ID', '5 MINUTE', 6, 'Settlement data'),

-- Medium-priority reference tables (10 min schedule)
('D_BRONZE', 'SADB', 'INSTRUMENT_BASE', 'D_BRONZE', 'SADB', 'INSTRUMENT', 'INSTRUMENT_ID', '10 MINUTE', 10, 'Instrument master'),
('D_BRONZE', 'SADB', 'COUNTERPARTY_BASE', 'D_BRONZE', 'SADB', 'COUNTERPARTY', 'COUNTERPARTY_ID', '10 MINUTE', 11, 'Counterparty master'),
('D_BRONZE', 'SADB', 'ACCOUNT_BASE', 'D_BRONZE', 'SADB', 'ACCOUNT', 'ACCOUNT_ID', '10 MINUTE', 12, 'Account master'),

-- Low-priority static tables (15-60 min schedule)
('D_BRONZE', 'SADB', 'PORTFOLIO_BASE', 'D_BRONZE', 'SADB', 'PORTFOLIO', 'PORTFOLIO_ID', '15 MINUTE', 20, 'Portfolio master'),
('D_BRONZE', 'REF', 'CURRENCY_BASE', 'D_BRONZE', 'REF', 'CURRENCY', 'CURRENCY_ID', '30 MINUTE', 30, 'Currency reference'),
('D_BRONZE', 'REF', 'CALENDAR_BASE', 'D_BRONZE', 'REF', 'CALENDAR', 'CALENDAR_ID', '1 HOUR', 40, 'Calendar reference'),

-- Composite key tables
('D_BRONZE', 'SADB', 'ORDER_FILL_BASE', 'D_BRONZE', 'SADB', 'ORDER_FILL', 'ORDER_ID,FILL_ID', '5 MINUTE', 7, 'Order fills - composite PK'),
('D_BRONZE', 'SADB', 'TRADE_ALLOC_BASE', 'D_BRONZE', 'SADB', 'TRADE_ALLOC', 'TRADE_ID,ALLOC_ID', '5 MINUTE', 8, 'Trade allocations - composite PK');

-- STEP 4.1.2: Verify registration
SELECT CONFIG_ID, SOURCE_TABLE, TARGET_TABLE, PRIMARY_KEY_COLUMNS, TASK_SCHEDULE, IS_ACTIVE
FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG
ORDER BY PRIORITY;
```

## 4.2 Setup Pipelines

```sql
-- STEP 4.2.1: Setup ALL pipelines (creates target tables, streams, tasks)
CALL CDC_PRESERVATION.PROCESSING.SP_SETUP_ALL_PIPELINES(FALSE);

-- STEP 4.2.2: Verify setup
SELECT * FROM CDC_PRESERVATION.MONITORING.V_PIPELINE_STATUS;

-- STEP 4.2.3: Verify streams created
SHOW STREAMS IN DATABASE D_BRONZE;

-- STEP 4.2.4: Verify tasks created (all should be SUSPENDED)
SHOW TASKS LIKE 'TASK_%_CDC' IN DATABASE D_BRONZE;

-- STEP 4.2.5: Verify target tables created
SHOW TABLES LIKE '%' IN SCHEMA D_BRONZE.SADB WHERE TABLE_NAME NOT LIKE '%_BASE';
```

---

# 5. Validation & Testing

## 5.1 Unit Testing

```sql
-- TEST 5.1.1: Test single table processing
-- Pick one low-risk table for initial test
CALL CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(1);

-- Verify result
SELECT * FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG 
WHERE CONFIG_ID = 1 ORDER BY LOG_ID DESC LIMIT 1;

-- TEST 5.1.2: Verify data in preserved table
SELECT COUNT(*) AS total_rows,
       SUM(CASE WHEN IS_DELETED THEN 1 ELSE 0 END) AS deleted_rows,
       COUNT(DISTINCT CDC_OPERATION) AS operation_types
FROM D_BRONZE.SADB.OPTRN_LEG;  -- Replace with your table

-- TEST 5.1.3: Verify CDC metadata populated
SELECT OPTRN_LEG_ID, CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, SOURCE_LOAD_BATCH_ID
FROM D_BRONZE.SADB.OPTRN_LEG
ORDER BY CDC_TIMESTAMP DESC LIMIT 10;
```

## 5.2 Integration Testing

```sql
-- TEST 5.2.1: Test stale stream recovery
-- Simulate by dropping and recreating stream
DROP STREAM IF EXISTS D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM;

-- Run CDC - should auto-recover
CALL CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(1);

-- Verify recovery logged
SELECT * FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG 
WHERE CONFIG_ID = 1 AND PROCESS_TYPE = 'RECOVERY' 
ORDER BY LOG_ID DESC LIMIT 1;

-- Verify stream status updated
SELECT * FROM CDC_PRESERVATION.MONITORING.STREAM_STATUS WHERE CONFIG_ID = 1;

-- TEST 5.2.2: Test UPDATE operation
UPDATE D_BRONZE.SADB.OPTRN_LEG_BASE SET <COLUMN> = <NEW_VALUE> WHERE OPTRN_LEG_ID = <TEST_ID>;
CALL CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(1);

-- Verify UPDATE captured
SELECT * FROM D_BRONZE.SADB.OPTRN_LEG WHERE OPTRN_LEG_ID = <TEST_ID>;

-- TEST 5.2.3: Test DELETE operation (soft delete)
DELETE FROM D_BRONZE.SADB.OPTRN_LEG_BASE WHERE OPTRN_LEG_ID = <TEST_ID>;
CALL CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(1);

-- Verify soft delete (IS_DELETED = TRUE, row still exists)
SELECT * FROM D_BRONZE.SADB.OPTRN_LEG WHERE OPTRN_LEG_ID = <TEST_ID>;
```

## 5.3 Load Testing

```sql
-- TEST 5.3.1: Test with production-like volume
-- Insert test records
INSERT INTO D_BRONZE.SADB.OPTRN_LEG_BASE 
SELECT * FROM <SOURCE_TABLE> LIMIT 10000;

-- Run CDC and measure time
SET start_time = CURRENT_TIMESTAMP();
CALL CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(1);
SELECT DATEDIFF('second', $start_time, CURRENT_TIMESTAMP()) AS execution_seconds;

-- Verify all rows processed
SELECT ROWS_PROCESSED, EXECUTION_TIME_SECONDS 
FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG 
WHERE CONFIG_ID = 1 ORDER BY LOG_ID DESC LIMIT 1;
```

---

# 5.4 Understanding the 5-Minute Task Scheduler

## How Tasks Are Created and Scheduled

When you call `SP_SETUP_PIPELINE` or `SP_SETUP_ALL_PIPELINES`, the framework creates a Snowflake Task for each table with the following configuration:

```sql
-- Task created by SP_CREATE_TASK:
CREATE OR REPLACE TASK D_BRONZE.SADB.TASK_OPTRN_LEG_CDC
    WAREHOUSE = COMPUTE_WH                -- From TABLE_CONFIG.TASK_WAREHOUSE
    SCHEDULE = '5 MINUTE'                 -- From TABLE_CONFIG.TASK_SCHEDULE
    ALLOW_OVERLAPPING_EXECUTION = FALSE   -- Prevents duplicate runs
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3   -- Auto-suspend on errors
    USER_TASK_TIMEOUT_MS = 3600000        -- 1 hour max
    COMMENT = 'CDC Task for OPTRN_LEG_BASE - Config ID: 1'
WHEN
    SYSTEM$STREAM_HAS_DATA('D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM')
AS
    CALL CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(1);
```

## Task Schedule Configuration Options

| Schedule | Syntax | Use Case |
|----------|--------|----------|
| Every 5 minutes | `'5 MINUTE'` | High-frequency transaction tables |
| Every 10 minutes | `'10 MINUTE'` | Medium-frequency tables |
| Every 15 minutes | `'15 MINUTE'` | Reference data |
| Every 30 minutes | `'30 MINUTE'` | Slowly changing data |
| Every hour | `'1 HOUR'` | Static/lookup tables |
| Cron schedule | `'USING CRON 0 */5 * * * UTC'` | Custom schedules |

## How to Set Custom Schedule Per Table

```sql
-- Option 1: Set schedule during registration
INSERT INTO CDC_PRESERVATION.CONFIG.TABLE_CONFIG 
(SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, 
 PRIMARY_KEY_COLUMNS, TASK_SCHEDULE)
VALUES
('D_BRONZE', 'SADB', 'HIGH_FREQ_TABLE_BASE', 'D_BRONZE', 'SADB', 'HIGH_FREQ_TABLE', 'ID', '2 MINUTE'),
('D_BRONZE', 'SADB', 'LOW_FREQ_TABLE_BASE', 'D_BRONZE', 'SADB', 'LOW_FREQ_TABLE', 'ID', '30 MINUTE');

-- Option 2: Update existing table schedule
UPDATE CDC_PRESERVATION.CONFIG.TABLE_CONFIG
SET TASK_SCHEDULE = '10 MINUTE', UPDATED_AT = CURRENT_TIMESTAMP()
WHERE SOURCE_TABLE = 'OPTRN_LEG_BASE';

-- Then recreate task with new schedule
CALL CDC_PRESERVATION.PROCESSING.SP_SUSPEND_TASK(<CONFIG_ID>);
CALL CDC_PRESERVATION.PROCESSING.SP_CREATE_TASK(<CONFIG_ID>);
CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_TASK(<CONFIG_ID>);
```

## Key Task Behavior

1. **Stream Trigger (`WHEN` clause)**: Task checks `SYSTEM$STREAM_HAS_DATA()` every 5 minutes
   - If TRUE → Executes `SP_PROCESS_CDC_GENERIC`
   - If FALSE → Skips execution (no warehouse cost!)

2. **Task States**:
   - `suspended` → Created but not running (default after setup)
   - `started` → Active and checking stream every 5 minutes

3. **Costs**: You only pay when the task actually runs (when stream has data)

---

# 5.4 Understanding the 5-Minute Task Scheduler

## How Tasks Are Created and Scheduled

When you call `SP_SETUP_PIPELINE` or `SP_SETUP_ALL_PIPELINES`, the framework creates a Snowflake Task for each table with the following configuration:

```sql
-- Task created by SP_CREATE_TASK:
CREATE OR REPLACE TASK D_BRONZE.SADB.TASK_OPTRN_LEG_CDC
    WAREHOUSE = COMPUTE_WH                -- From TABLE_CONFIG.TASK_WAREHOUSE
    SCHEDULE = '5 MINUTE'                 -- From TABLE_CONFIG.TASK_SCHEDULE
    ALLOW_OVERLAPPING_EXECUTION = FALSE   -- Prevents duplicate runs
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3   -- Auto-suspend on errors
    USER_TASK_TIMEOUT_MS = 3600000        -- 1 hour max
    COMMENT = 'CDC Task for OPTRN_LEG_BASE - Config ID: 1'
WHEN
    SYSTEM$STREAM_HAS_DATA('D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM')
AS
    CALL CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(1);
```

## Task Schedule Configuration Options

| Schedule | Syntax | Use Case |
|----------|--------|----------|
| Every 5 minutes | `'5 MINUTE'` | High-frequency transaction tables |
| Every 10 minutes | `'10 MINUTE'` | Medium-frequency tables |
| Every 15 minutes | `'15 MINUTE'` | Reference data |
| Every 30 minutes | `'30 MINUTE'` | Slowly changing data |
| Every hour | `'1 HOUR'` | Static/lookup tables |
| Cron schedule | `'USING CRON 0 */5 * * * UTC'` | Custom schedules |

## How to Set Custom Schedule Per Table

```sql
-- Option 1: Set schedule during registration
INSERT INTO CDC_PRESERVATION.CONFIG.TABLE_CONFIG 
(SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, 
 PRIMARY_KEY_COLUMNS, TASK_SCHEDULE)
VALUES
('D_BRONZE', 'SADB', 'HIGH_FREQ_TABLE_BASE', 'D_BRONZE', 'SADB', 'HIGH_FREQ_TABLE', 'ID', '2 MINUTE'),
('D_BRONZE', 'SADB', 'LOW_FREQ_TABLE_BASE', 'D_BRONZE', 'SADB', 'LOW_FREQ_TABLE', 'ID', '30 MINUTE');

-- Option 2: Update existing table schedule
UPDATE CDC_PRESERVATION.CONFIG.TABLE_CONFIG
SET TASK_SCHEDULE = '10 MINUTE', UPDATED_AT = CURRENT_TIMESTAMP()
WHERE SOURCE_TABLE = 'OPTRN_LEG_BASE';

-- Then recreate task with new schedule
CALL CDC_PRESERVATION.PROCESSING.SP_SUSPEND_TASK(<CONFIG_ID>);
CALL CDC_PRESERVATION.PROCESSING.SP_CREATE_TASK(<CONFIG_ID>);
CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_TASK(<CONFIG_ID>);
```

## Key Task Behavior

1. **Stream Trigger (`WHEN` clause)**: Task checks `SYSTEM$STREAM_HAS_DATA()` every 5 minutes
   - If TRUE → Executes `SP_PROCESS_CDC_GENERIC`
   - If FALSE → Skips execution (no warehouse cost!)

2. **Task States**:
   - `suspended` → Created but not running (default after setup)
   - `started` → Active and checking stream every 5 minutes

3. **Costs**: You only pay when the task actually runs (when stream has data)

---

# 6. Go-Live Procedure

## 6.1 Pre-Go-Live Checklist

| Step | Description | Verified |
|------|-------------|----------|
| 1 | All tables registered in TABLE_CONFIG | ☐ |
| 2 | All pipelines setup (streams, tasks created) | ☐ |
| 3 | Unit tests passed | ☐ |
| 4 | Integration tests passed | ☐ |
| 5 | Load tests passed | ☐ |
| 6 | Monitoring views accessible | ☐ |
| 7 | Alerting configured | ☐ |
| 8 | Runbook reviewed by operations team | ☐ |
| 9 | Rollback procedure tested | ☐ |
| 10 | Business approval obtained | ☐ |

## 6.2 Go-Live Execution

```sql
-- STEP 6.2.1: Final verification before go-live
SELECT CONFIG_ID, SOURCE_TABLE, TASK_NAME, IS_ACTIVE
FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG
WHERE IS_ACTIVE = TRUE;

-- STEP 6.2.2: Resume ALL tasks (Go-Live!)
CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_ALL_TASKS();

-- STEP 6.2.3: Verify tasks are running
SHOW TASKS LIKE 'TASK_%_CDC' IN DATABASE D_BRONZE;
-- All should show state = 'started'

-- STEP 6.2.4: Monitor first executions
SELECT * FROM CDC_PRESERVATION.MONITORING.V_PIPELINE_STATUS;

-- Wait 5 minutes for first run...

-- STEP 6.2.5: Verify first successful runs
SELECT CONFIG_ID, SOURCE_TABLE_FQN, STATUS, ROWS_PROCESSED, CREATED_AT
FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG
WHERE CREATED_AT > DATEADD('minute', -10, CURRENT_TIMESTAMP())
ORDER BY CREATED_AT DESC;
```

## 6.3 Post-Go-Live Monitoring (First 24 Hours)

```sql
-- Run every hour for first 24 hours
-- Check 1: All tasks running
SHOW TASKS LIKE 'TASK_%_CDC' IN DATABASE D_BRONZE;

-- Check 2: No errors
SELECT * FROM CDC_PRESERVATION.MONITORING.V_RECENT_ERRORS;

-- Check 3: Processing statistics
SELECT * FROM CDC_PRESERVATION.MONITORING.V_PROCESSING_STATS;

-- Check 4: Stream health
SELECT * FROM CDC_PRESERVATION.MONITORING.STREAM_STATUS WHERE IS_STALE = TRUE;
```

---

# 7. Monitoring & Operations

## 7.1 Daily Health Check

```sql
-- Daily health check query
SELECT 
    'Tasks' AS check_type,
    COUNT(*) AS total,
    SUM(CASE WHEN state = 'started' THEN 1 ELSE 0 END) AS healthy,
    SUM(CASE WHEN state = 'suspended' THEN 1 ELSE 0 END) AS suspended
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE name LIKE 'TASK_%_CDC'

UNION ALL

SELECT 
    'Streams' AS check_type,
    COUNT(*) AS total,
    SUM(CASE WHEN IS_STALE = FALSE THEN 1 ELSE 0 END) AS healthy,
    SUM(CASE WHEN IS_STALE = TRUE THEN 1 ELSE 0 END) AS stale
FROM CDC_PRESERVATION.MONITORING.STREAM_STATUS

UNION ALL

SELECT 
    'Processing (24h)' AS check_type,
    COUNT(*) AS total,
    SUM(CASE WHEN STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS success,
    SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) AS failed
FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG
WHERE CREATED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP());
```

## 7.2 Alert Configuration

```sql
-- Create alert for failed processing
CREATE OR REPLACE ALERT CDC_PRESERVATION.MONITORING.ALERT_CDC_FAILURES
    WAREHOUSE = CDC_PROCESSING_WH
    SCHEDULE = '5 MINUTE'
    IF (EXISTS (
        SELECT 1 FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG
        WHERE STATUS = 'FAILED'
        AND CREATED_AT > DATEADD('minute', -10, CURRENT_TIMESTAMP())
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'cdc_alerts@company.com',
            'CDC Framework Alert: Processing Failure',
            'One or more CDC processing jobs have failed. Check V_RECENT_ERRORS for details.'
        );

-- Create alert for stale streams
CREATE OR REPLACE ALERT CDC_PRESERVATION.MONITORING.ALERT_STALE_STREAMS
    WAREHOUSE = CDC_PROCESSING_WH
    SCHEDULE = '15 MINUTE'
    IF (EXISTS (
        SELECT 1 FROM CDC_PRESERVATION.MONITORING.STREAM_STATUS
        WHERE IS_STALE = TRUE
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'cdc_alerts@company.com',
            'CDC Framework Alert: Stale Stream Detected',
            'One or more streams are stale. Auto-recovery will be attempted on next task run.'
        );

-- Resume alerts
ALTER ALERT CDC_PRESERVATION.MONITORING.ALERT_CDC_FAILURES RESUME;
ALTER ALERT CDC_PRESERVATION.MONITORING.ALERT_STALE_STREAMS RESUME;
```

## 7.3 Task History Analysis

```sql
-- View task execution history
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    RETURN_VALUE,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100
))
WHERE NAME LIKE 'TASK_%_CDC'
ORDER BY SCHEDULED_TIME DESC;
```

---

# 8. Troubleshooting Guide

## 8.1 Common Issues and Resolutions

### Issue: Task Suspended Due to Failures

```sql
-- Diagnose
SHOW TASKS LIKE 'TASK_%_CDC' IN DATABASE D_BRONZE;
-- Look for state = 'suspended'

-- Check error details
SELECT * FROM CDC_PRESERVATION.MONITORING.V_RECENT_ERRORS;

-- Resolution: Fix underlying issue, then resume
ALTER TASK D_BRONZE.SADB.TASK_<TABLE>_CDC RESUME;
```

### Issue: Stream is Stale

```sql
-- Diagnose
SELECT * FROM CDC_PRESERVATION.MONITORING.STREAM_STATUS WHERE IS_STALE = TRUE;

-- Resolution: Framework will auto-recover on next run
-- Force immediate recovery:
CALL CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(<CONFIG_ID>);
```

### Issue: Duplicate Key Error

```sql
-- Diagnose
SELECT * FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG
WHERE ERROR_MESSAGE LIKE '%Duplicate%'
ORDER BY LOG_ID DESC LIMIT 1;

-- Resolution: Check if stream has duplicate records
-- May need to consume stream manually or recreate
```

### Issue: Task Not Running

```sql
-- Diagnose
SHOW TASKS LIKE 'TASK_<TABLE>_CDC';
-- Check: Is state = 'started'?
-- Check: Is warehouse available?

-- Resolution options:
-- 1. Resume task
ALTER TASK D_BRONZE.SADB.TASK_<TABLE>_CDC RESUME;

-- 2. Check warehouse
SHOW WAREHOUSES LIKE 'CDC_PROCESSING_WH';
ALTER WAREHOUSE CDC_PROCESSING_WH RESUME;
```

## 8.2 Log Analysis Queries

```sql
-- Find all errors in last 24 hours
SELECT CONFIG_ID, BATCH_ID, SOURCE_TABLE_FQN, ERROR_MESSAGE, CREATED_AT
FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG
WHERE STATUS = 'FAILED'
AND CREATED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY CREATED_AT DESC;

-- Find tables with high failure rate
SELECT 
    c.SOURCE_TABLE,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN p.STATUS = 'FAILED' THEN 1 ELSE 0 END) AS failures,
    ROUND(100.0 * SUM(CASE WHEN p.STATUS = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_pct
FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG c
JOIN CDC_PRESERVATION.MONITORING.PROCESSING_LOG p ON c.CONFIG_ID = p.CONFIG_ID
WHERE p.CREATED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY c.SOURCE_TABLE
HAVING failure_pct > 5
ORDER BY failure_pct DESC;
```

---

# 9. Rollback Procedures

## 9.1 Rollback Single Table

```sql
-- STEP 9.1.1: Suspend task
CALL CDC_PRESERVATION.PROCESSING.SP_SUSPEND_TASK(<CONFIG_ID>);

-- STEP 9.1.2: Mark as inactive
UPDATE CDC_PRESERVATION.CONFIG.TABLE_CONFIG
SET IS_ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE CONFIG_ID = <CONFIG_ID>;

-- STEP 9.1.3: (Optional) Drop stream and task
DROP STREAM IF EXISTS D_BRONZE.SADB.<TABLE>_STREAM;
DROP TASK IF EXISTS D_BRONZE.SADB.TASK_<TABLE>_CDC;

-- STEP 9.1.4: (Optional) Keep preserved table for data recovery
-- Or drop: DROP TABLE IF EXISTS D_BRONZE.SADB.<TABLE>;
```

## 9.2 Rollback Entire Framework

```sql
-- STEP 9.2.1: Suspend all tasks
CALL CDC_PRESERVATION.PROCESSING.SP_SUSPEND_ALL_TASKS();

-- STEP 9.2.2: Verify all tasks suspended
SHOW TASKS LIKE 'TASK_%_CDC' IN DATABASE D_BRONZE;

-- STEP 9.2.3: (Optional) Drop all framework objects
-- WARNING: This will delete all configuration and logs
-- DROP DATABASE CDC_PRESERVATION;

-- STEP 9.2.4: (Optional) Drop all created streams
-- Generate drop statements:
SELECT 'DROP STREAM IF EXISTS ' || STREAM_FQN || ';'
FROM CDC_PRESERVATION.MONITORING.STREAM_STATUS;

-- STEP 9.2.5: (Optional) Drop all created tasks
-- Generate drop statements:
SELECT 'DROP TASK IF EXISTS ' || SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || TASK_NAME || ';'
FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG
WHERE TASK_NAME IS NOT NULL;
```

---

# 10. Maintenance Procedures

## 10.1 Adding New Tables

```sql
-- STEP 10.1.1: Register new table
INSERT INTO CDC_PRESERVATION.CONFIG.TABLE_CONFIG 
(SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, 
 PRIMARY_KEY_COLUMNS, TASK_SCHEDULE, PRIORITY, NOTES)
VALUES
('D_BRONZE', '<SCHEMA>', '<NEW_TABLE>_BASE', 'D_BRONZE', '<SCHEMA>', '<NEW_TABLE>',
 '<PK_COLUMN>', '5 MINUTE', 50, 'Description of new table');

-- STEP 10.1.2: Get the new CONFIG_ID
SELECT CONFIG_ID FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG 
WHERE SOURCE_TABLE = '<NEW_TABLE>_BASE';

-- STEP 10.1.3: Setup pipeline
CALL CDC_PRESERVATION.PROCESSING.SP_SETUP_PIPELINE(<NEW_CONFIG_ID>, TRUE);

-- STEP 10.1.4: Verify
SELECT * FROM CDC_PRESERVATION.MONITORING.V_PIPELINE_STATUS 
WHERE CONFIG_ID = <NEW_CONFIG_ID>;
```

## 10.2 Modifying Table Configuration

```sql
-- Example: Change schedule from 5 minutes to 10 minutes
UPDATE CDC_PRESERVATION.CONFIG.TABLE_CONFIG
SET TASK_SCHEDULE = '10 MINUTE', UPDATED_AT = CURRENT_TIMESTAMP()
WHERE CONFIG_ID = <CONFIG_ID>;

-- Recreate task with new schedule
CALL CDC_PRESERVATION.PROCESSING.SP_SUSPEND_TASK(<CONFIG_ID>);
CALL CDC_PRESERVATION.PROCESSING.SP_CREATE_TASK(<CONFIG_ID>);
CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_TASK(<CONFIG_ID>);
```

## 10.3 Log Cleanup

```sql
-- Archive old logs (older than 90 days)
CREATE TABLE IF NOT EXISTS CDC_PRESERVATION.MONITORING.PROCESSING_LOG_ARCHIVE AS
SELECT * FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG WHERE 1=0;

INSERT INTO CDC_PRESERVATION.MONITORING.PROCESSING_LOG_ARCHIVE
SELECT * FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG
WHERE CREATED_AT < DATEADD('day', -90, CURRENT_TIMESTAMP());

-- Delete archived records
DELETE FROM CDC_PRESERVATION.MONITORING.PROCESSING_LOG
WHERE CREATED_AT < DATEADD('day', -90, CURRENT_TIMESTAMP());
```

---

# Appendix A: Quick Reference Commands

```sql
-- View all configurations
SELECT * FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG;

-- View pipeline status
SELECT * FROM CDC_PRESERVATION.MONITORING.V_PIPELINE_STATUS;

-- View recent processing
SELECT * FROM CDC_PRESERVATION.MONITORING.V_PROCESSING_STATS;

-- View recent errors
SELECT * FROM CDC_PRESERVATION.MONITORING.V_RECENT_ERRORS;

-- Resume all tasks
CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_ALL_TASKS();

-- Suspend all tasks
CALL CDC_PRESERVATION.PROCESSING.SP_SUSPEND_ALL_TASKS();

-- Process single table manually
CALL CDC_PRESERVATION.PROCESSING.SP_PROCESS_CDC_GENERIC(<CONFIG_ID>);

-- Setup new pipeline
CALL CDC_PRESERVATION.PROCESSING.SP_SETUP_PIPELINE(<CONFIG_ID>, TRUE);
```

---

*Document Version: 1.0*  
*Framework: CDC Data Preservation*  
*Last Updated: February 16, 2026*
