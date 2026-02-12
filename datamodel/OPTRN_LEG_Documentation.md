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
