# Data Preservation Architecture for IDMC CDC Tables

## Overview

This document describes the data preservation solution designed to protect historical data from Informatica IDMC job redeployment truncation events.

| Attribute | Value |
|-----------|-------|
| **Source Table** | `D_BRONZE.SADB.OPTRN_LEG_BASE` |
| **Target Table** | `D_BRONZE.SADB.OPTRN_LEG` |
| **Stream** | `D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM` |
| **Stored Procedure** | `D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC()` |
| **Task** | `D_BRONZE.SADB.TASK_PROCESS_OPTRN_LEG_CDC` |

---

## Architecture Diagram

```mermaid
flowchart LR
    subgraph Source["SOURCE SYSTEM"]
        SADB[(SADB Database)]
    end
    
    subgraph IDMC["INFORMATICA IDMC"]
        CDC[CDC Process]
    end
    
    subgraph Bronze["BRONZE LAYER"]
        BASE[("_BASE Table<br/>(may be truncated)")]
        STREAM[[Stream]]
        PRESERVED[("Data Preserved Table<br/>(protected)")]
    end
    
    subgraph Scheduler["SNOWFLAKE SCHEDULER"]
        TASK[Task<br/>Every 5 min]
        SP[Stored Procedure]
    end
    
    SADB -->|CDC Data| CDC
    CDC -->|Load/Merge| BASE
    BASE -->|Change Capture| STREAM
    STREAM -->|Trigger| TASK
    TASK -->|Execute| SP
    SP -->|MERGE| PRESERVED
    
    style PRESERVED fill:#90EE90,stroke:#228B22,stroke-width:3px
    style BASE fill:#FFB6C1,stroke:#DC143C,stroke-width:2px
```

---

## Problem Statement

```mermaid
flowchart TD
    subgraph Problem["PROBLEM: IDMC Redeploy Scenario"]
        A[IDMC Job Redeployed] --> B[TRUNCATE _BASE Table]
        B --> C[Historical Data Lost!]
        C --> D[Source only keeps 45 days]
        D --> E[❌ Permanent Data Loss]
    end
    
    style E fill:#FF6B6B,stroke:#C0392B,stroke-width:2px
```

---

## Solution: Data Preservation Flow

```mermaid
flowchart TD
    subgraph Solution["SOLUTION: Data Preservation"]
        A[IDMC Job Redeployed] --> B[TRUNCATE _BASE Table]
        B --> C[Stream becomes STALE]
        C --> D[SP Detects Staleness]
        D --> E[Auto-Recovery Mode]
        E --> F[Recreate Stream]
        F --> G[Differential Merge]
        G --> H[✅ Historical Data Preserved!]
    end
    
    style H fill:#90EE90,stroke:#27AE60,stroke-width:2px
```

---

## Stored Procedure Flow: `SP_PROCESS_OPTRN_LEG_CDC()`

```mermaid
flowchart TD
    START([START]) --> BATCH[Generate BATCH_ID]
    
    BATCH --> CHECK_STALE{Check Stream<br/>STALE?}
    
    CHECK_STALE -->|YES| RECOVERY[/"RECOVERY MODE"/]
    CHECK_STALE -->|NO| STAGE[/"NORMAL MODE"/]
    
    subgraph RecoveryMode["Recovery Mode (Stream Stale)"]
        RECOVERY --> RECREATE[Recreate Stream<br/>SHOW_INITIAL_ROWS=TRUE]
        RECREATE --> DIFF_MERGE[Differential MERGE<br/>_BASE → PRESERVED]
        DIFF_MERGE --> RETURN_RECOVERY[Return: RECOVERY_COMPLETE]
    end
    
    subgraph NormalMode["Normal Mode (Stream Healthy)"]
        STAGE --> CREATE_STAGING[CREATE TEMP TABLE<br/>_CDC_STAGING<br/>FROM STREAM]
        CREATE_STAGING --> CHECK_COUNT{Staging<br/>Count = 0?}
        CHECK_COUNT -->|YES| RETURN_NODATA[Return: NO_DATA]
        CHECK_COUNT -->|NO| MERGE_CDC[MERGE CDC Changes]
    end
    
    subgraph MergeLogic["MERGE Logic"]
        MERGE_CDC --> CASE1[CASE 1: UPDATE<br/>ACTION=INSERT + ISUPDATE=TRUE]
        MERGE_CDC --> CASE2[CASE 2: DELETE<br/>ACTION=DELETE + ISUPDATE=FALSE]
        MERGE_CDC --> CASE3[CASE 3: RE-INSERT<br/>ACTION=INSERT + ISUPDATE=FALSE<br/>Record Exists]
        MERGE_CDC --> CASE4[CASE 4: NEW INSERT<br/>ACTION=INSERT<br/>Record Not Exists]
        
        CASE1 --> UPDATE1[UPDATE columns<br/>CDC_OPERATION='UPDATE']
        CASE2 --> UPDATE2[SOFT DELETE<br/>IS_DELETED=TRUE<br/>CDC_OPERATION='DELETE']
        CASE3 --> UPDATE3[UPDATE columns<br/>CDC_OPERATION='INSERT']
        CASE4 --> INSERT4[INSERT new row<br/>CDC_OPERATION='INSERT']
    end
    
    UPDATE1 --> CLEANUP
    UPDATE2 --> CLEANUP
    UPDATE3 --> CLEANUP
    UPDATE4 --> CLEANUP
    INSERT4 --> CLEANUP
    
    CLEANUP[DROP _CDC_STAGING] --> RETURN_SUCCESS[Return: SUCCESS]
    
    RETURN_RECOVERY --> END_PROC([END])
    RETURN_NODATA --> END_PROC
    RETURN_SUCCESS --> END_PROC
    
    style RECOVERY fill:#FFA500,stroke:#FF8C00
    style STAGE fill:#87CEEB,stroke:#4682B4
    style UPDATE2 fill:#90EE90,stroke:#228B22,stroke-width:2px
```

---

## CDC Action Mapping

```mermaid
flowchart LR
    subgraph SourceAction["Source _BASE Action"]
        INSERT_SRC[INSERT]
        UPDATE_SRC[UPDATE]
        DELETE_SRC[DELETE]
    end
    
    subgraph StreamCapture["Stream Captures"]
        INS_STREAM["ACTION=INSERT<br/>ISUPDATE=FALSE"]
        UPD_STREAM["ACTION=DELETE (old)<br/>ACTION=INSERT (new)<br/>ISUPDATE=TRUE"]
        DEL_STREAM["ACTION=DELETE<br/>ISUPDATE=FALSE"]
    end
    
    subgraph PreservedResult["Preserved Table Result"]
        INS_RESULT["New Row Created<br/>CDC_OPERATION='INSERT'"]
        UPD_RESULT["Row Updated<br/>CDC_OPERATION='UPDATE'"]
        DEL_RESULT["Soft Delete<br/>IS_DELETED=TRUE<br/>CDC_OPERATION='DELETE'"]
    end
    
    INSERT_SRC --> INS_STREAM --> INS_RESULT
    UPDATE_SRC --> UPD_STREAM --> UPD_RESULT
    DELETE_SRC --> DEL_STREAM --> DEL_RESULT
    
    style DEL_RESULT fill:#90EE90,stroke:#228B22,stroke-width:2px
```

---

## MERGE Cases Explained

| Case | Stream Condition | Record Exists? | Action | Result |
|------|------------------|----------------|--------|--------|
| **1** | `ACTION='INSERT'` + `ISUPDATE=TRUE` | Yes | UPDATE | Update all columns, `CDC_OPERATION='UPDATE'` |
| **2** | `ACTION='DELETE'` + `ISUPDATE=FALSE` | Yes | SOFT DELETE | Set `IS_DELETED=TRUE`, preserve record |
| **3** | `ACTION='INSERT'` + `ISUPDATE=FALSE` | Yes | UPDATE | Overwrite columns, `CDC_OPERATION='INSERT'` |
| **4** | `ACTION='INSERT'` | No | INSERT | Create new row |

---

## IDMC Truncate/Reload Scenario

```mermaid
sequenceDiagram
    participant IDMC as IDMC Job
    participant BASE as _BASE Table
    participant STREAM as Stream
    participant TASK as Scheduled Task
    participant SP as Stored Procedure
    participant PRESERVED as Preserved Table
    
    Note over IDMC,PRESERVED: Normal Operation
    IDMC->>BASE: Load CDC Data
    BASE->>STREAM: Capture Changes
    TASK->>SP: Trigger (every 5 min)
    SP->>STREAM: Read Changes
    SP->>PRESERVED: MERGE Data
    
    Note over IDMC,PRESERVED: IDMC Redeploy (Truncate Scenario)
    IDMC->>BASE: TRUNCATE TABLE
    BASE--xSTREAM: Stream becomes STALE
    IDMC->>BASE: Reload Data (45 days)
    
    Note over IDMC,PRESERVED: Auto-Recovery
    TASK->>SP: Trigger
    SP->>STREAM: Check Status = STALE
    SP->>STREAM: Recreate Stream
    SP->>BASE: Read Current Data
    SP->>PRESERVED: Differential MERGE
    Note over PRESERVED: Historical Data PRESERVED!
```

---

## Data Flow Timeline

```mermaid
gantt
    title Data Preservation Timeline
    dateFormat  YYYY-MM-DD
    section Normal CDC
    Initial Load           :done, init, 2024-01-01, 1d
    Daily CDC Processing   :done, cdc1, 2024-01-02, 30d
    section IDMC Redeploy
    IDMC Truncates _BASE   :crit, trunc, 2024-02-01, 1d
    Stream Goes Stale      :crit, stale, 2024-02-01, 1d
    section Recovery
    Auto-Recovery Triggered :active, recover, 2024-02-01, 1d
    Differential Merge      :active, merge, 2024-02-01, 1d
    section Resumed
    Normal CDC Resumes     :cdc2, 2024-02-02, 28d
```

---

## Key Components

### 1. Stream Configuration
```sql
CREATE OR REPLACE STREAM D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM
ON TABLE D_BRONZE.SADB.OPTRN_LEG_BASE
SHOW_INITIAL_ROWS = TRUE;  -- Critical for initial load
```

### 2. Task Configuration
```sql
CREATE OR REPLACE TASK D_BRONZE.SADB.TASK_PROCESS_OPTRN_LEG_CDC
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
WHEN
    SYSTEM$STREAM_HAS_DATA('D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM')
AS
    CALL D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC();
```

### 3. Preserved Table Structure
```sql
CREATE TABLE D_BRONZE.SADB.OPTRN_LEG (
    -- Business columns
    OPTRN_LEG_ID NUMBER(38,0) PRIMARY KEY,
    ...
    -- CDC Metadata
    CDC_OPERATION VARCHAR(10),      -- INSERT, UPDATE, DELETE, RELOADED
    CDC_TIMESTAMP TIMESTAMP_NTZ,
    IS_DELETED BOOLEAN DEFAULT FALSE,  -- Soft delete flag
    SOURCE_LOAD_BATCH_ID VARCHAR(100)
);
```

---

## Monitoring

### Check Stream Status
```sql
SELECT * FROM D_BRONZE.SADB.VW_OPTRN_LEG_STREAM_STATUS;
```

### View CDC Statistics
```sql
SELECT * FROM D_BRONZE.SADB.VW_OPTRN_LEG_CDC_STATS;
```

### Task Execution History
```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_PROCESS_OPTRN_LEG_CDC',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;
```

---

## Summary

| Scenario | _BASE Table | Stream | Preserved Table | Data Status |
|----------|-------------|--------|-----------------|-------------|
| Normal CDC | ✅ Active | ✅ Healthy | ✅ Updated | ✅ Protected |
| IDMC Truncate | ❌ Truncated | ⚠️ Stale | ✅ Intact | ✅ Protected |
| IDMC Reload | ✅ Reloaded | ✅ Recreated | ✅ Merged | ✅ Protected |
| DELETE in source | Row removed | Captured | IS_DELETED=TRUE | ✅ Protected |

---

## Contact

For questions or issues, contact the Data Engineering team.

---

*Document Version: 1.0*  
*Last Updated: February 2026*

---

## Appendix A: Stale Stream Detection - Issue & Resolution

### Problem Identified in Production

During IDMC job redeployment, the stream became stale but the stored procedure **failed to detect it** and did not trigger auto-recovery.

### Root Cause Analysis

```mermaid
flowchart TD
    subgraph Original["ORIGINAL APPROACH (FAILED)"]
        A[SHOW STREAMS...] --> B[RESULT_SCAN]
        B --> C["SELECT 'stale' column"]
        C --> D{Detected?}
        D -->|Often NO| E[❌ Recovery NOT triggered]
    end
    
    style E fill:#FF6B6B,stroke:#C0392B
```

#### Why Original Logic Failed

| Issue | Description |
|-------|-------------|
| **RESULT_SCAN Unreliable** | Inside stored procedures, `LAST_QUERY_ID()` may reference wrong query |
| **Column Case Sensitivity** | SHOW commands return lowercase `"stale"`, but behavior varies |
| **Exception Masking** | If SELECT fails, exception caught but `v_stream_stale` stays `FALSE` |
| **SHOW Doesn't Error** | `SHOW STREAMS` doesn't throw error on stale - just shows status column |

#### Original Code (Problematic)

```sql
-- ❌ UNRELIABLE - Do not use this pattern
BEGIN
    SHOW STREAMS LIKE 'OPTRN_LEG_BASE_STREAM' IN SCHEMA D_BRONZE.SADB;
    
    SELECT "stale"::BOOLEAN INTO v_stream_stale
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "name" = 'OPTRN_LEG_BASE_STREAM';
    
EXCEPTION
    WHEN OTHER THEN
        v_stream_stale := TRUE;
END;
```

### Solution: Direct Stream Query

Per **Snowflake Documentation**: *"Querying a stale stream throws an error"*

```mermaid
flowchart TD
    subgraph New["NEW APPROACH (RELIABLE)"]
        A["SELECT FROM STREAM<br/>(with WHERE 1=0)"] --> B{Query Success?}
        B -->|YES| C[Stream Healthy<br/>v_stream_stale = FALSE]
        B -->|ERROR| D[Stream Stale<br/>v_stream_stale = TRUE]
        D --> E[✅ Recovery Triggered]
    end
    
    style E fill:#90EE90,stroke:#27AE60
```

#### Fixed Code (Reliable)

```sql
-- ✅ RELIABLE - Snowflake recommended pattern
BEGIN
    -- Attempt to query stream - stale streams throw error
    SELECT COUNT(*) INTO v_staging_count 
    FROM D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM
    WHERE 1=0;  -- No data retrieval, just validates stream
    
    v_stream_stale := FALSE;
    
EXCEPTION
    WHEN OTHER THEN
        -- Stream is stale - error message: "The stream has become stale..."
        v_stream_stale := TRUE;
        v_error_msg := SQLERRM;  -- Capture error for debugging
END;
```

### Comparison

| Aspect | Original (SHOW STREAMS) | Fixed (Direct Query) |
|--------|------------------------|----------------------|
| **Reliability** | ❌ Inconsistent | ✅ Always works |
| **Error Detection** | Indirect via column | Direct via exception |
| **Snowflake Documented** | No | Yes |
| **SP Compatible** | Issues with RESULT_SCAN | Fully compatible |
| **Error Message Captured** | No | Yes (SQLERRM) |

---

## Appendix B: Task Configuration & Best Practices

### Current Task Parameters Explained

```sql
CREATE OR REPLACE TASK D_BRONZE.SADB.TASK_PROCESS_OPTRN_LEG_CDC
    WAREHOUSE = INFA_INGEST_WH              -- Dedicated compute
    SCHEDULE = '5 MINUTE'                    -- Run frequency
    ALLOW_OVERLAPPING_EXECUTION = FALSE      -- Prevent concurrent runs
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3      -- Auto-suspend on failures
    USER_TASK_TIMEOUT_MS = 3600000           -- 1 hour timeout
    COMMENT = 'CDC processing task'
WHEN
    SYSTEM$STREAM_HAS_DATA('...')            -- Only run when data exists
AS
    CALL D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC();
```

### Task Parameters Reference

| Parameter | Value | Purpose | Best Practice |
|-----------|-------|---------|---------------|
| `WAREHOUSE` | `INFA_INGEST_WH` | Compute resource for task | ✅ Use dedicated warehouse for isolation |
| `SCHEDULE` | `'5 MINUTE'` | How often task checks to run | ✅ Balance between latency and cost |
| `ALLOW_OVERLAPPING_EXECUTION` | `FALSE` | Prevent concurrent executions | ✅ Always FALSE for CDC to prevent duplicates |
| `SUSPEND_TASK_AFTER_NUM_FAILURES` | `3` | Auto-suspend after N consecutive failures | ✅ Prevents runaway errors, alerts team |
| `USER_TASK_TIMEOUT_MS` | `3600000` | Maximum execution time (ms) | ✅ Prevents hung tasks (1 hour = 3600000) |
| `WHEN` | `SYSTEM$STREAM_HAS_DATA()` | Condition to trigger execution | ✅ Critical - saves compute costs |
| `COMMENT` | Description text | Documentation | ✅ Always document purpose |

### Task Execution Flow

```mermaid
flowchart TD
    subgraph Scheduler["SNOWFLAKE TASK SCHEDULER"]
        A[Every 5 Minutes] --> B{WHEN Condition<br/>SYSTEM$STREAM_HAS_DATA?}
        B -->|FALSE| C[Skip Execution<br/>No warehouse used]
        B -->|TRUE| D[Start Warehouse]
        D --> E[Execute SP]
        E --> F{Success?}
        F -->|YES| G[Reset Failure Counter]
        F -->|NO| H[Increment Failure Counter]
        H --> I{Failures >= 3?}
        I -->|YES| J[SUSPEND TASK<br/>Alert Required]
        I -->|NO| K[Wait for Next Schedule]
        G --> K
    end
    
    style J fill:#FF6B6B,stroke:#C0392B
    style C fill:#90EE90,stroke:#27AE60
```

### Schedule Options

| Schedule Type | Syntax | Example | Use Case |
|---------------|--------|---------|----------|
| **Minutes** | `'N MINUTE'` | `'5 MINUTE'` | Near real-time CDC |
| **Hours** | `'N HOUR'` | `'1 HOUR'` | Hourly batch processing |
| **CRON** | `'USING CRON expr TZ'` | `'USING CRON 0 9 * * * UTC'` | Specific times (9 AM daily) |

```sql
-- Examples
SCHEDULE = '5 MINUTE'                           -- Every 5 minutes
SCHEDULE = '1 HOUR'                             -- Every hour
SCHEDULE = 'USING CRON 0 */2 * * * UTC'        -- Every 2 hours
SCHEDULE = 'USING CRON 0 9 * * MON-FRI UTC'    -- Weekdays at 9 AM
```

### WHEN Clause Best Practices

```mermaid
flowchart LR
    subgraph Without["WITHOUT WHEN Clause"]
        A1[Task Scheduled] --> B1[Start Warehouse]
        B1 --> C1[Execute SP]
        C1 --> D1[SP checks: No data]
        D1 --> E1["Wasted Compute $$$"]
    end
    
    subgraph With["WITH WHEN Clause"]
        A2[Task Scheduled] --> B2{Stream Has Data?}
        B2 -->|NO| C2[Skip - No Cost]
        B2 -->|YES| D2[Start Warehouse]
        D2 --> E2[Execute SP]
        E2 --> F2["Efficient ✅"]
    end
    
    style E1 fill:#FF6B6B
    style C2 fill:#90EE90
    style F2 fill:#90EE90
```

### Cost Optimization

| Configuration | Without WHEN | With WHEN |
|---------------|--------------|-----------|
| Task runs per day | 288 (every 5 min) | Only when data exists |
| Warehouse starts | 288 times | ~10-50 times (varies) |
| Estimated cost savings | - | **70-90%** |

---

## Appendix C: Monitoring & Operations

### Task Execution History Query

```sql
-- View last 50 task executions
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    DATEDIFF('second', SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SEC,
    RETURN_VALUE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_PROCESS_OPTRN_LEG_CDC',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 50;
```

### Task State Diagram

```mermaid
stateDiagram-v2
    [*] --> SUSPENDED: Created
    SUSPENDED --> STARTED: ALTER TASK RESUME
    STARTED --> EXECUTING: Schedule + WHEN=TRUE
    EXECUTING --> SUCCEEDED: SP Returns Success
    EXECUTING --> FAILED: SP Throws Error
    SUCCEEDED --> STARTED: Wait for Next Schedule
    FAILED --> STARTED: Failures < 3
    FAILED --> SUSPENDED: Failures >= 3
    STARTED --> SUSPENDED: ALTER TASK SUSPEND
    
    note right of SUSPENDED: Manual intervention required
    note right of FAILED: Check ERROR_MESSAGE
```

### Common Task States

| State | Description | Action Required |
|-------|-------------|-----------------|
| `STARTED` | Task is active and scheduled | None - normal operation |
| `EXECUTING` | Task currently running | None - in progress |
| `SUCCEEDED` | Last run completed successfully | None |
| `FAILED` | Last run failed | Check error message |
| `SUSPENDED` | Task is paused | `ALTER TASK ... RESUME` |

### Resume Suspended Task

```sql
-- Check if task is suspended
SHOW TASKS LIKE 'TASK_PROCESS_OPTRN_LEG_CDC' IN SCHEMA D_BRONZE.SADB;

-- Resume task after fixing issues
ALTER TASK D_BRONZE.SADB.TASK_PROCESS_OPTRN_LEG_CDC RESUME;
```

### Stream Health Check

```sql
-- Check stream status
SELECT 
    STREAM_NAME,
    STALE,
    STALE_AFTER,
    MODE
FROM TABLE(INFORMATION_SCHEMA.STREAMS(
    STREAM_NAME => 'OPTRN_LEG_BASE_STREAM'
));

-- Or use the monitoring view
SELECT * FROM D_BRONZE.SADB.VW_OPTRN_LEG_STREAM_STATUS;
```

### Failure Investigation Query

```sql
-- Find failed executions with error details
SELECT 
    SCHEDULED_TIME,
    STATE,
    ERROR_CODE,
    ERROR_MESSAGE,
    RETURN_VALUE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_PROCESS_OPTRN_LEG_CDC',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -30, CURRENT_TIMESTAMP())
))
WHERE STATE = 'FAILED'
ORDER BY SCHEDULED_TIME DESC;
```

---

## Appendix D: Troubleshooting Guide

### Issue: Stream Shows Stale

```mermaid
flowchart TD
    A[Stream is Stale] --> B{Auto-Recovery<br/>Working?}
    B -->|YES| C[Wait for next task run]
    B -->|NO| D[Manual Recovery]
    D --> E["1. Check SP logic"]
    E --> F["2. Manually recreate stream"]
    F --> G["3. Verify task resumes"]
    
    style D fill:#FFA500
```

**Manual Recovery Steps:**
```sql
-- 1. Recreate stream
CREATE OR REPLACE STREAM D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM
ON TABLE D_BRONZE.SADB.OPTRN_LEG_BASE
SHOW_INITIAL_ROWS = TRUE;

-- 2. Resume task if suspended
ALTER TASK D_BRONZE.SADB.TASK_PROCESS_OPTRN_LEG_CDC RESUME;

-- 3. Manually trigger if needed
EXECUTE TASK D_BRONZE.SADB.TASK_PROCESS_OPTRN_LEG_CDC;
```

### Issue: Task Not Running

| Symptom | Possible Cause | Resolution |
|---------|---------------|------------|
| Task shows `SUSPENDED` | 3+ consecutive failures | Fix root cause, then `ALTER TASK RESUME` |
| Task shows `STARTED` but no executions | Stream has no data | Normal - WHEN clause prevents unnecessary runs |
| Task shows `STARTED` but WHEN never true | Stream was recreated | Check stream has `SHOW_INITIAL_ROWS = TRUE` |

### Issue: Duplicate Records in Preserved Table

| Cause | Prevention |
|-------|------------|
| Overlapping task executions | `ALLOW_OVERLAPPING_EXECUTION = FALSE` ✅ |
| Stream read multiple times | Stage to temp table first ✅ |
| Manual + automated runs | Avoid `EXECUTE TASK` during active schedule |

---

## Appendix E: Summary Checklist

### Pre-Production Checklist

- [ ] Stream created with `SHOW_INITIAL_ROWS = TRUE`
- [ ] SP uses direct stream query for stale detection (not SHOW STREAMS)
- [ ] SP stages stream data to temp table before processing
- [ ] Task has `WHEN SYSTEM$STREAM_HAS_DATA()` clause
- [ ] Task has `ALLOW_OVERLAPPING_EXECUTION = FALSE`
- [ ] Task has `SUSPEND_TASK_AFTER_NUM_FAILURES` set
- [ ] Monitoring views created
- [ ] Task resumed with `ALTER TASK ... RESUME`

### Operational Checklist (Daily)

- [ ] Check task execution history for failures
- [ ] Verify stream is not stale
- [ ] Review preservation stats view
- [ ] Confirm row counts match expectations
