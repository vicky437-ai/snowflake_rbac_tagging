# CDC Data Preservation Framework
## Customer Presentation & Technical Documentation

---

# Executive Summary

## The Challenge

Organizations face critical data synchronization challenges with IDMC job redeployment:

```mermaid
flowchart LR
    subgraph Problem["COMMON CHALLENGES"]
        A[IDMC Job Redeploy] --> B[DROP/RECREATE Table]
        B --> C[Stream Becomes STALE]
        C --> D[Historical Data LOST]
        D --> E["❌ COMPLIANCE GAPS"]
    end
    
    style E fill:#FF6B6B,stroke:#C0392B,stroke-width:3px
```

**Business Impact:**
- Historical data permanently lost during IDMC redeployment
- Compliance gaps - unable to prove historical state
- Audit failures - missing records for regulatory reporting
- Downstream system failures - broken data lineage

---

## Our Solution: Metadata-Driven CDC Framework

```mermaid
flowchart LR
    subgraph Solution["METADATA-DRIVEN CDC FRAMEWORK"]
        A[Configure Once] --> B[Metadata Table]
        B --> C[Generic Processor]
        C --> D[Auto-Recovery]
        D --> E["✅ DATA PRESERVED"]
    end
    
    style E fill:#90EE90,stroke:#27AE60,stroke-width:3px
```

**Key Benefits:**
- **Zero Code Changes** - Add tables via metadata configuration
- **Auto-Recovery** - Handles IDMC redeployment automatically
- **Soft Deletes** - Historical records never lost
- **One Task per Table** - Efficient stream-triggered execution

---

# Architecture Overview

## High-Level Architecture

```mermaid
flowchart TB
    subgraph Source["SOURCE LAYER (Bronze)"]
        SRC1[(OPTRN_LEG_BASE)]
        SRC2[(TRADE_BASE)]
        SRC3[(POSITION_BASE)]
    end
    
    subgraph Framework["CDC PRESERVATION FRAMEWORK"]
        subgraph Config["CONFIGURATION"]
            CFG[(TABLE_CONFIG<br/>20+ Tables Registered)]
        end
        
        subgraph Processing["PROCESSING"]
            SP[SP_PROCESS_CDC_GENERIC<br/>Metadata-Driven]
        end
        
        subgraph Monitoring["MONITORING"]
            LOG[(PROCESSING_LOG)]
            SS[(STREAM_STATUS)]
            DASH[Dashboard Views]
        end
    end
    
    subgraph Target["PRESERVED LAYER"]
        TGT1[(OPTRN_LEG<br/>+ CDC Metadata)]
        TGT2[(TRADE<br/>+ IS_DELETED)]
        TGT3[(POSITION<br/>+ History)]
    end
    
    SRC1 --> |Stream| SP
    SRC2 --> |Stream| SP
    SRC3 --> |Stream| SP
    
    CFG --> SP
    
    SP --> TGT1
    SP --> TGT2
    SP --> TGT3
    
    SP --> LOG
    SP --> SS
    LOG --> DASH
    
    style TGT1 fill:#90EE90,stroke:#228B22
    style TGT2 fill:#90EE90,stroke:#228B22
    style TGT3 fill:#90EE90,stroke:#228B22
```

---

## Component Architecture

```mermaid
flowchart LR
    subgraph Config["CONFIGURATION"]
        C1[TABLE_CONFIG<br/>Table Definitions]
        C2[PROCESSING_LOG<br/>Audit Trail]
        C3[STREAM_STATUS<br/>Health Tracking]
    end
    
    subgraph Processing["PROCESSING (10 SPs)"]
        P1[SP_PROCESS_CDC_GENERIC<br/>Core Processor]
        P2[SP_SETUP_PIPELINE<br/>One-Click Setup]
        P3[SP_CREATE_TASK<br/>Task Generator]
        P4[SP_RESUME/SUSPEND<br/>Task Management]
    end
    
    subgraph Tasks["TASKS (Per Table)"]
        T1[TASK_TABLE1_CDC<br/>5 min Schedule]
        T2[TASK_TABLE2_CDC<br/>5 min Schedule]
        TN[TASK_TABLEN_CDC<br/>Custom Schedule]
    end
    
    C1 --> P2
    P2 --> P3
    P3 --> T1
    P3 --> T2
    P3 --> TN
    T1 --> P1
    T2 --> P1
    TN --> P1
    P1 --> C2
    P1 --> C3
```

---

## Task Architecture (One Task per Table)

```mermaid
flowchart TD
    subgraph Scheduler["SNOWFLAKE TASK SCHEDULER"]
        T1["TASK_OPTRN_LEG_CDC<br/>Schedule: 5 MINUTE<br/>Warehouse: COMPUTE_WH"]
        T2["TASK_TRADE_CDC<br/>Schedule: 5 MINUTE"]
        T3["TASK_POSITION_CDC<br/>Schedule: 10 MINUTE"]
    end
    
    subgraph Trigger["STREAM TRIGGER"]
        COND1{SYSTEM$STREAM_HAS_DATA?}
        COND2{SYSTEM$STREAM_HAS_DATA?}
        COND3{SYSTEM$STREAM_HAS_DATA?}
    end
    
    subgraph Execution["EXECUTION"]
        SP[SP_PROCESS_CDC_GENERIC]
    end
    
    T1 --> COND1
    T2 --> COND2
    T3 --> COND3
    
    COND1 -->|YES| SP
    COND2 -->|YES| SP
    COND3 -->|YES| SP
    
    COND1 -->|NO| SKIP1[Skip - No Cost]
    COND2 -->|NO| SKIP2[Skip - No Cost]
    COND3 -->|NO| SKIP3[Skip - No Cost]
    
    style SKIP1 fill:#90EE90
    style SKIP2 fill:#90EE90
    style SKIP3 fill:#90EE90
```

**Key Benefit:** Tasks only run when `SYSTEM$STREAM_HAS_DATA()` returns TRUE = **Zero wasted compute**

---

# Data Flow Diagrams

## Normal CDC Flow

```mermaid
sequenceDiagram
    participant IDMC as IDMC
    participant BASE as _BASE Table
    participant STREAM as Stream
    participant TASK as Task (5 min)
    participant SP as SP_PROCESS_CDC_GENERIC
    participant PRESERVED as Preserved Table
    participant LOG as Processing Log
    
    IDMC->>BASE: INSERT/UPDATE/DELETE
    BASE->>STREAM: Capture Changes (CDC)
    
    Note over TASK: Every 5 minutes
    TASK->>STREAM: Check SYSTEM$STREAM_HAS_DATA
    
    alt Stream Has Data
        TASK->>SP: Call SP_PROCESS_CDC_GENERIC(CONFIG_ID)
        SP->>STREAM: Stage to Temp Table
        SP->>PRESERVED: MERGE Changes
        SP->>LOG: Log SUCCESS
    else No Data
        Note over TASK: Skip - No Cost
    end
```

---

## IDMC Redeployment Recovery Flow

```mermaid
sequenceDiagram
    participant IDMC as IDMC (Redeploy)
    participant BASE as _BASE Table
    participant STREAM as Stream
    participant SP as SP_PROCESS_CDC_GENERIC
    participant PRESERVED as Preserved Table
    participant STATUS as Stream Status
    
    Note over IDMC,BASE: IDMC Job Redeployed
    IDMC->>BASE: DROP TABLE
    BASE--xSTREAM: Stream becomes STALE ❌
    IDMC->>BASE: CREATE TABLE
    IDMC->>BASE: Load 45 days data
    
    Note over SP: Next Task Run
    SP->>STREAM: Query Stream
    STREAM-->>SP: ERROR: Base table dropped
    
    Note over SP: AUTO-RECOVERY MODE
    SP->>STREAM: Recreate Stream<br/>SHOW_INITIAL_ROWS=TRUE
    SP->>PRESERVED: Differential MERGE<br/>(New records only)
    SP->>STATUS: Update RECOVERY_COUNT++
    
    Note over PRESERVED: ✅ Historical Data INTACT!<br/>✅ New Data Added
```

---

## Stream Metadata Interpretation

```mermaid
flowchart TD
    subgraph StreamMetadata["SNOWFLAKE STREAM METADATA"]
        M1["METADATA$ACTION"]
        M2["METADATA$ISUPDATE"]
    end
    
    subgraph Operations["OPERATION MAPPING"]
        OP1["INSERT<br/>ACTION=INSERT, ISUPDATE=FALSE"]
        OP2["UPDATE<br/>ACTION=INSERT, ISUPDATE=TRUE"]
        OP3["DELETE<br/>ACTION=DELETE, ISUPDATE=FALSE"]
    end
    
    subgraph Actions["PRESERVED TABLE ACTIONS"]
        A1["INSERT new row<br/>CDC_OPERATION='INSERT'"]
        A2["UPDATE existing row<br/>CDC_OPERATION='UPDATE'"]
        A3["SOFT DELETE<br/>IS_DELETED=TRUE<br/>CDC_OPERATION='DELETE'"]
    end
    
    M1 --> OP1
    M1 --> OP2
    M1 --> OP3
    M2 --> OP1
    M2 --> OP2
    M2 --> OP3
    
    OP1 --> A1
    OP2 --> A2
    OP3 --> A3
    
    style A3 fill:#FFD700
```

---

# Stored Procedure Logic

## SP_PROCESS_CDC_GENERIC Flow

```mermaid
flowchart TD
    START([START]) --> LOAD[Load Config from TABLE_CONFIG]
    LOAD --> BUILD[Build Dynamic SQL<br/>from INFORMATION_SCHEMA]
    BUILD --> CHECK{Check Stream<br/>Stale?}
    
    CHECK -->|YES - Error| RECOVER[RECOVERY MODE]
    CHECK -->|NO| STAGE[Stage Stream to Temp Table]
    
    subgraph Recovery["RECOVERY MODE"]
        RECOVER --> RECREATE[Recreate Stream<br/>SHOW_INITIAL_ROWS=TRUE]
        RECREATE --> DIFFMERGE[Differential MERGE<br/>New + Resurrected records only]
        DIFFMERGE --> LOGRECOVERY[Log RECOVERED status]
    end
    
    subgraph Normal["NORMAL MODE"]
        STAGE --> EMPTY{Staging<br/>Count = 0?}
        EMPTY -->|YES| NODATA[Return NO_DATA]
        EMPTY -->|NO| MERGE[Execute MERGE]
        MERGE --> LOGSUCCESS[Log SUCCESS status]
    end
    
    LOGRECOVERY --> END([END])
    NODATA --> END
    LOGSUCCESS --> END
```

---

## MERGE Statement Logic

```mermaid
flowchart TD
    subgraph MergeStatement["MERGE INTO preserved_table"]
        SOURCE["USING staging_table AS src"]
        JOIN["ON tgt.PK = src.PK"]
    end
    
    subgraph Cases["MERGE CASES"]
        CASE1["CASE 1: UPDATE<br/>WHEN MATCHED AND<br/>ACTION='INSERT' AND ISUPDATE=TRUE<br/>→ UPDATE columns<br/>→ CDC_OPERATION='UPDATE'"]
        
        CASE2["CASE 2: SOFT DELETE<br/>WHEN MATCHED AND<br/>ACTION='DELETE' AND ISUPDATE=FALSE<br/>→ IS_DELETED=TRUE<br/>→ CDC_OPERATION='DELETE'"]
        
        CASE3["CASE 3: RE-INSERT<br/>WHEN MATCHED AND<br/>ACTION='INSERT' AND ISUPDATE=FALSE<br/>→ UPDATE columns<br/>→ IS_DELETED=FALSE"]
        
        CASE4["CASE 4: NEW INSERT<br/>WHEN NOT MATCHED AND<br/>ACTION='INSERT'<br/>→ INSERT new row"]
    end
    
    SOURCE --> JOIN
    JOIN --> CASE1
    JOIN --> CASE2
    JOIN --> CASE3
    JOIN --> CASE4
    
    style CASE2 fill:#FFD700
```

---

# Database Schema

## Entity Relationship Diagram

```mermaid
erDiagram
    TABLE_CONFIG ||--o{ PROCESSING_LOG : generates
    TABLE_CONFIG ||--o| STREAM_STATUS : tracks
    
    TABLE_CONFIG {
        int CONFIG_ID PK
        string SOURCE_DATABASE
        string SOURCE_SCHEMA
        string SOURCE_TABLE
        string TARGET_DATABASE
        string TARGET_SCHEMA
        string TARGET_TABLE
        string PRIMARY_KEY_COLUMNS
        string STREAM_NAME
        string TASK_NAME
        string TASK_WAREHOUSE
        string TASK_SCHEDULE
        boolean IS_ACTIVE
        int PRIORITY
    }
    
    STREAM_STATUS {
        int STATUS_ID PK
        int CONFIG_ID FK
        string STREAM_FQN
        boolean IS_STALE
        timestamp STALE_DETECTED_AT
        timestamp LAST_RECOVERY_AT
        int RECOVERY_COUNT
    }
    
    PROCESSING_LOG {
        int LOG_ID PK
        int CONFIG_ID FK
        string BATCH_ID
        string SOURCE_TABLE_FQN
        string TARGET_TABLE_FQN
        string PROCESS_TYPE
        int ROWS_PROCESSED
        string STATUS
        string ERROR_MESSAGE
    }
```

---

## Preserved Table Structure

```mermaid
classDiagram
    class PreservedTable {
        +PK_COLUMN : NUMBER
        +source_column_1 : TYPE
        +source_column_2 : TYPE
        +source_column_N : TYPE
        ---CDC Metadata---
        +CDC_OPERATION : VARCHAR(10)
        +CDC_TIMESTAMP : TIMESTAMP_NTZ
        +IS_DELETED : BOOLEAN
        +RECORD_CREATED_AT : TIMESTAMP_NTZ
        +RECORD_UPDATED_AT : TIMESTAMP_NTZ
        +SOURCE_LOAD_BATCH_ID : VARCHAR(100)
    }
    
    note for PreservedTable "CDC_OPERATION: INSERT, UPDATE, DELETE, RELOADED\nIS_DELETED: Soft delete flag (history preserved)"
```

---

# Task Configuration & Scheduling

## How Tasks are Scheduled (5-Minute Example)

```mermaid
flowchart TD
    subgraph TaskDefinition["TASK DEFINITION"]
        DEF["CREATE TASK TASK_OPTRN_LEG_CDC<br/>WAREHOUSE = COMPUTE_WH<br/>SCHEDULE = '5 MINUTE'<br/>ALLOW_OVERLAPPING_EXECUTION = FALSE<br/>SUSPEND_TASK_AFTER_NUM_FAILURES = 3<br/>USER_TASK_TIMEOUT_MS = 3600000"]
    end
    
    subgraph Trigger["STREAM TRIGGER CONDITION"]
        WHEN["WHEN<br/>SYSTEM$STREAM_HAS_DATA('stream_name')"]
    end
    
    subgraph Action["ACTION"]
        AS["AS<br/>CALL SP_PROCESS_CDC_GENERIC(CONFIG_ID)"]
    end
    
    DEF --> WHEN
    WHEN --> AS
```

## Task Parameters Explained

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `WAREHOUSE` | `COMPUTE_WH` | Compute resource for execution |
| `SCHEDULE` | `'5 MINUTE'` | Check frequency (configurable per table) |
| `ALLOW_OVERLAPPING_EXECUTION` | `FALSE` | Prevent duplicate runs |
| `SUSPEND_TASK_AFTER_NUM_FAILURES` | `3` | Auto-suspend on repeated errors |
| `USER_TASK_TIMEOUT_MS` | `3600000` | 1 hour max runtime |
| `WHEN SYSTEM$STREAM_HAS_DATA()` | Stream check | **Only runs when data exists** |

## How to Start the 5-Minute Scheduler

```sql
-- Option 1: Start ALL tasks at once
CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_ALL_TASKS();

-- Option 2: Start individual table task
CALL CDC_PRESERVATION.PROCESSING.SP_RESUME_TASK(1);  -- CONFIG_ID = 1

-- Option 3: Direct SQL
ALTER TASK D_BRONZE.SADB.TASK_OPTRN_LEG_CDC RESUME;

-- Verify tasks are running
SHOW TASKS LIKE 'TASK_%_CDC' IN DATABASE D_BRONZE;
-- Look for state = 'started'
```

---

# Monitoring Dashboard

## Key Metrics Views

```mermaid
flowchart LR
    subgraph Views["MONITORING VIEWS"]
        V1[V_PIPELINE_STATUS]
        V2[V_PROCESSING_STATS]
        V3[V_RECENT_ERRORS]
    end
    
    subgraph Metrics["KEY METRICS"]
        M1[Tables Active/Inactive]
        M2[Success/Error Rates]
        M3[Rows Processed]
        M4[Stream Health]
        M5[Recovery Count]
    end
    
    V1 --> M1
    V1 --> M4
    V2 --> M2
    V2 --> M3
    V3 --> M5
```

## Sample Dashboard Queries

```sql
-- Overall Health Check
SELECT * FROM CDC_PRESERVATION.MONITORING.V_PIPELINE_STATUS;

-- Processing Statistics
SELECT * FROM CDC_PRESERVATION.MONITORING.V_PROCESSING_STATS;

-- Recent Errors
SELECT * FROM CDC_PRESERVATION.MONITORING.V_RECENT_ERRORS;

-- Task Execution History
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP())
)) WHERE NAME LIKE 'TASK_%_CDC' ORDER BY SCHEDULED_TIME DESC;
```

---

# Test Results Summary

## Test Scenarios Executed

```mermaid
flowchart LR
    subgraph Tests["TEST SCENARIOS - ALL PASSED"]
        T1["✅ DROP/RECREATE<br/>(IDMC Redeploy)"]
        T2["✅ TRUNCATE<br/>Recovery"]
        T3["✅ Stale Stream<br/>Auto-Recovery"]
        T4["✅ Stream<br/>Coalescing"]
        T5["✅ Soft Delete<br/>Preservation"]
        T6["✅ Unicode/<br/>Special Chars"]
    end
    
    style T1 fill:#90EE90
    style T2 fill:#90EE90
    style T3 fill:#90EE90
    style T4 fill:#90EE90
    style T5 fill:#90EE90
    style T6 fill:#90EE90
```

| Test | Description | Result |
|------|-------------|--------|
| **Test 1** | DROP/RECREATE table (IDMC redeployment) | ✅ PASSED - Auto-recovery |
| **Test 2** | TRUNCATE + Reload recovery | ✅ PASSED - History preserved |
| **Test 3** | Stale stream detection & recreation | ✅ PASSED - Automatic |
| **Test 4** | Multiple rapid updates (coalescing) | ✅ PASSED - Final state captured |
| **Test 5** | Soft delete preservation | ✅ PASSED - IS_DELETED=TRUE |
| **Test 6** | Unicode/special characters | ✅ PASSED - All preserved |

---

# Deployment Guide

## Deployment Steps

```mermaid
flowchart TD
    subgraph Step1["STEP 1: Deploy Framework"]
        A1[Create Database & Schemas]
        A2[Create Config Tables]
        A3[Deploy 10 Stored Procedures]
        A4[Create Monitoring Views]
    end
    
    subgraph Step2["STEP 2: Register Tables"]
        B1[INSERT into TABLE_CONFIG]
        B2[Define PKs, Schedule, Warehouse]
    end
    
    subgraph Step3["STEP 3: Setup Pipelines"]
        C1["CALL SP_SETUP_ALL_PIPELINES(FALSE)"]
        C2[Creates Target Tables]
        C3[Creates Streams]
        C4[Creates Tasks (SUSPENDED)]
    end
    
    subgraph Step4["STEP 4: Go Live"]
        D1["CALL SP_RESUME_ALL_TASKS()"]
        D2[Monitor Dashboard]
    end
    
    Step1 --> Step2 --> Step3 --> Step4
```

## Adding New Tables

```sql
-- STEP 1: Register new table in config
INSERT INTO CDC_PRESERVATION.CONFIG.TABLE_CONFIG 
(SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, 
 TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, 
 PRIMARY_KEY_COLUMNS, TASK_SCHEDULE, NOTES)
VALUES
('D_BRONZE', 'SADB', 'NEW_TABLE_BASE', 
 'D_BRONZE', 'SADB', 'NEW_TABLE',
 'NEW_TABLE_ID', '5 MINUTE', 'New table for preservation');

-- STEP 2: Get CONFIG_ID
SELECT CONFIG_ID FROM CDC_PRESERVATION.CONFIG.TABLE_CONFIG 
WHERE SOURCE_TABLE = 'NEW_TABLE_BASE';

-- STEP 3: Setup pipeline (creates target, stream, task)
CALL CDC_PRESERVATION.PROCESSING.SP_SETUP_PIPELINE(<CONFIG_ID>, TRUE);

-- Done! Task is now running on 5-minute schedule
```

---

# Summary

## Key Benefits

```mermaid
mindmap
  root((CDC Framework))
    Data Protection
      Soft Deletes
      Auto-Recovery
      Full History
      IDMC Safe
    Efficiency
      Stream-Triggered
      Zero Wasted Compute
      One Generic SP
    Maintainability
      Metadata-Driven
      Easy to Add Tables
      10 Reusable SPs
    Monitoring
      Dashboard Views
      Execution Logs
      Stream Health
      Recovery Tracking
```

## Framework Comparison

| Feature | Without Framework | With Framework |
|---------|-------------------|----------------|
| Data Loss on IDMC Redeploy | ❌ Yes | ✅ No - Auto-Recovery |
| Historical Records | ❌ Lost on DELETE | ✅ Preserved (IS_DELETED) |
| Recovery | ❌ Manual | ✅ Automatic |
| Maintenance | 20+ individual SPs | 1 generic SP |
| Adding Tables | Complex code changes | Simple INSERT |
| Monitoring | None | Complete dashboard |

---

# Files Delivered

| File | Purpose |
|------|---------|
| `CDC_Data_Preservation_Framework.sql` | Complete framework with all 10 SPs |
| `CDC_Framework_Production_Runbook.md` | Step-by-step deployment guide |
| `CDC_Framework_Customer_Presentation_v2.md` | This documentation |
| `CDC_Framework_Test_Results.md` | Comprehensive test results |
| `CDC_Framework_Best_Practices_Review.md` | Snowflake best practices assessment |

---

# Contact & Support

For questions or issues:
1. Check monitoring views for status: `V_PIPELINE_STATUS`
2. Review execution logs: `V_PROCESSING_STATS`
3. Check recent errors: `V_RECENT_ERRORS`
4. Contact Data Engineering team

---

*Document Version: 2.0*  
*Framework: CDC Data Preservation*  
*Status: Production Ready*  
*Last Updated: February 2026*
