# BASE Table Data Cleanup Framework
## Customer Presentation & Technical Documentation

---

# Executive Summary

## The Challenge

Organizations face critical storage and performance challenges with growing staging tables:

```mermaid
flowchart LR
    subgraph Problem["COMMON CHALLENGES"]
        A[_BASE Tables] --> B[Data Accumulation]
        B --> C[45+ Days Old Data]
        C --> D[Storage Costs ↑]
        D --> E["❌ PERFORMANCE DEGRADATION"]
    end
    
    style E fill:#FF6B6B,stroke:#C0392B,stroke-width:3px
```

**Business Impact:**
- Storage costs grow unbounded as staging data accumulates
- Query performance degrades with large table scans
- Manual cleanup processes are error-prone and inconsistent
- No audit trail of cleanup operations

---

## Our Solution: Automated Cleanup Framework

```mermaid
flowchart LR
    subgraph Solution["METADATA-DRIVEN CLEANUP FRAMEWORK"]
        A[Configure Once] --> B[Metadata Config]
        B --> C[Scheduled Task]
        C --> D[Auto-Cleanup]
        D --> E["✅ OPTIMIZED STORAGE"]
    end
    
    style E fill:#90EE90,stroke:#27AE60,stroke-width:3px
```

**Key Benefits:**
- **Zero Manual Intervention** - Fully automated scheduled cleanup
- **Configurable Retention** - 45 days default, adjustable per schema
- **Safe Operations** - Dry-run preview, exclusion lists, audit logs
- **Complete Visibility** - Monitoring views and execution history

---

# Architecture Overview

## High-Level Architecture

```mermaid
flowchart TB
    subgraph Source["SOURCE LAYER (Bronze _BASE Tables)"]
        SRC1[(CUSTOMERS_BASE)]
        SRC2[(ORDERS_BASE)]
        SRC3[(PRODUCTS_BASE)]
    end
    
    subgraph Framework["CLEANUP FRAMEWORK (CDC_PRESERVATION.CLEANUP)"]
        subgraph Config["CONFIGURATION"]
            CFG[(CLEANUP_CONFIG<br/>Schema Settings)]
            EXC[(CLEANUP_EXCLUSIONS<br/>Skip List)]
        end
        
        subgraph Processing["PROCESSING"]
            SP[SP_CLEANUP_SCHEMA<br/>Metadata-Driven]
        end
        
        subgraph Monitoring["MONITORING"]
            LOG[(CLEANUP_LOG)]
            V1[V_CLEANUP_SUMMARY]
            V2[V_RECENT_CLEANUPS]
        end
    end
    
    subgraph Task["SNOWFLAKE TASK"]
        TSK[TASK_CLEANUP_ALL_SCHEMAS<br/>Daily 2 AM UTC]
    end
    
    TSK --> |Triggers| SP
    CFG --> SP
    EXC --> SP
    
    SP --> |DELETE old data| SRC1
    SP --> |DELETE old data| SRC2
    SP --> |DELETE old data| SRC3
    
    SP --> LOG
    LOG --> V1
    LOG --> V2
    
    style SRC1 fill:#90EE90,stroke:#228B22
    style SRC2 fill:#90EE90,stroke:#228B22
    style SRC3 fill:#90EE90,stroke:#228B22
```

---

## Component Architecture

```mermaid
flowchart LR
    subgraph Config["CONFIGURATION (3 Tables)"]
        C1[CLEANUP_CONFIG<br/>Schema Settings]
        C2[CLEANUP_LOG<br/>Audit Trail]
        C3[CLEANUP_EXCLUSIONS<br/>Skip List]
    end
    
    subgraph Processing["PROCESSING (8 SPs)"]
        P1[SP_CLEANUP_BASE_TABLE<br/>Core Delete]
        P2[SP_CLEANUP_SCHEMA<br/>Schema Loop]
        P3[SP_CLEANUP_DRY_RUN<br/>Preview Mode]
        P4[SP_CLEANUP_ALL_CONFIGS<br/>Master Orchestrator]
    end
    
    subgraph Views["MONITORING (4 Views)"]
        V1[V_CLEANUP_SUMMARY<br/>Daily Stats]
        V2[V_RECENT_CLEANUPS<br/>Last 7 Days]
        V3[V_FAILED_CLEANUPS<br/>Errors Only]
        V4[V_CONFIG_STATUS<br/>Config Overview]
    end
    
    C1 --> P4
    P4 --> P2
    P2 --> P1
    P1 --> C2
    C2 --> V1
    C2 --> V2
    C2 --> V3
    C1 --> V4
```

---

## Task Architecture (Daily Scheduled Execution)

```mermaid
flowchart TD
    subgraph Scheduler["SNOWFLAKE TASK SCHEDULER"]
        T1["TASK_CLEANUP_ALL_SCHEMAS<br/>Schedule: CRON 0 2 * * * UTC<br/>Warehouse: COMPUTE_WH"]
    end
    
    subgraph Orchestration["ORCHESTRATION"]
        SP1[SP_CLEANUP_ALL_CONFIGS<br/>Loop through active configs]
    end
    
    subgraph Execution["EXECUTION (Per Schema)"]
        SP2[SP_CLEANUP_SCHEMA<br/>Find _BASE tables]
        SP3[SP_CLEANUP_BASE_TABLE<br/>DELETE old records]
    end
    
    subgraph Targets["TARGET _BASE TABLES"]
        TBL1[(CUSTOMERS_BASE<br/>5 rows kept)]
        TBL2[(ORDERS_BASE<br/>107 rows kept)]
        TBL3[(PRODUCTS_BASE<br/>50 rows kept)]
    end
    
    T1 --> SP1
    SP1 --> SP2
    SP2 --> SP3
    
    SP3 --> |DELETE WHERE date < cutoff| TBL1
    SP3 --> |DELETE WHERE date < cutoff| TBL2
    SP3 --> |DELETE WHERE date < cutoff| TBL3
    
    style T1 fill:#4169E1,stroke:#000080,color:#fff
    style TBL1 fill:#90EE90
    style TBL2 fill:#90EE90
    style TBL3 fill:#90EE90
```

**Key Benefit:** Task runs daily at 2 AM UTC during low-activity window = **Minimal impact on operations**

---

# Data Flow Diagrams

## Daily Cleanup Flow

```mermaid
sequenceDiagram
    participant TASK as Task (2 AM UTC)
    participant SP1 as SP_CLEANUP_ALL_CONFIGS
    participant CONFIG as CLEANUP_CONFIG
    participant SP2 as SP_CLEANUP_SCHEMA
    participant SP3 as SP_CLEANUP_BASE_TABLE
    participant BASE as _BASE Table
    participant LOG as CLEANUP_LOG
    
    Note over TASK: Daily 2 AM UTC Trigger
    TASK->>SP1: Call SP_CLEANUP_ALL_CONFIGS()
    SP1->>CONFIG: Load Active Configurations
    
    loop For Each Active Config
        SP1->>SP2: Call SP_CLEANUP_SCHEMA(db, schema, ...)
        SP2->>SP2: Find all _BASE tables
        
        loop For Each _BASE Table
            SP2->>SP3: Call SP_CLEANUP_BASE_TABLE(table, ...)
            SP3->>BASE: DELETE WHERE date < cutoff
            SP3->>LOG: Log rows_before, rows_deleted, rows_after
        end
    end
    
    SP1-->>TASK: Return Summary JSON
```

---

## Single Table Cleanup Flow

```mermaid
sequenceDiagram
    participant SP as SP_CLEANUP_BASE_TABLE
    participant EXC as CLEANUP_EXCLUSIONS
    participant INFO as INFORMATION_SCHEMA
    participant TBL as _BASE Table
    participant LOG as CLEANUP_LOG
    
    Note over SP: START Cleanup for Single Table
    SP->>EXC: Check exclusion list
    
    alt Table Excluded
        SP->>LOG: Log SKIPPED (Excluded)
        SP-->>SP: Return EXCLUDED status
    else Table Not Excluded
        SP->>INFO: Validate date column exists
        
        alt Column Not Found
            SP->>LOG: Log SKIPPED (Column missing)
            SP-->>SP: Return SKIPPED status
        else Column Found
            SP->>TBL: COUNT(*) before cleanup
            SP->>TBL: DELETE WHERE date < cutoff
            SP->>TBL: COUNT(*) after cleanup
            SP->>LOG: Log SUCCESS with metrics
            SP-->>SP: Return SUCCESS status
        end
    end
```

---

## Cutoff Date Calculation

```mermaid
flowchart LR
    subgraph Calculation["CUTOFF DATE CALCULATION"]
        TODAY["CURRENT_DATE()<br/>2026-02-16"]
        MINUS["- RETENTION_DAYS<br/>(45 days)"]
        CUTOFF["CUTOFF_DATE<br/>2026-01-02"]
    end
    
    subgraph Action["DELETE ACTION"]
        DELETE["DELETE FROM table<br/>WHERE CREATED_DATE < '2026-01-02'"]
    end
    
    subgraph Result["RESULT"]
        CLEAN["✅ Old Data Removed<br/>Recent Data Preserved"]
    end
    
    TODAY --> MINUS --> CUTOFF --> DELETE --> CLEAN
    
    style CUTOFF fill:#FF6B6B,stroke:#C0392B
    style CLEAN fill:#90EE90,stroke:#228B22
```

---

## Dry Run vs Execute Decision Flow

```mermaid
flowchart TD
    subgraph Decision["USER DECISION FLOW"]
        START([New Schema Setup]) --> DRY{Run Dry Run<br/>First?}
        
        DRY -->|"YES (Recommended)"| PREVIEW[SP_CLEANUP_DRY_RUN<br/>Preview Only - No Changes]
        DRY -->|NO| EXECUTE
        
        PREVIEW --> REVIEW{Review<br/>Results}
        
        REVIEW -->|Acceptable| EXECUTE[SP_CLEANUP_SCHEMA<br/>Execute DELETE]
        REVIEW -->|Too Many Rows| ADJUST[Adjust Retention Days<br/>or Add Exclusions]
        ADJUST --> PREVIEW
        
        EXECUTE --> LOG[Results Logged to<br/>CLEANUP_LOG]
        LOG --> MONITOR[Monitor via<br/>Dashboard Views]
    end
    
    style PREVIEW fill:#FFD700,stroke:#B8860B
    style EXECUTE fill:#90EE90,stroke:#228B22
```

---

# Stored Procedure Logic

## SP_CLEANUP_BASE_TABLE Flow

```mermaid
flowchart TD
    START([START]) --> LOAD[Load Parameters<br/>Database, Schema, Table, DateColumn]
    LOAD --> FQN[Build Fully Qualified Name<br/>DB.SCHEMA.TABLE]
    FQN --> CUTOFF[Calculate Cutoff Date<br/>CURRENT_DATE - RETENTION_DAYS]
    
    CUTOFF --> BEFORE[Get Row Count BEFORE<br/>SELECT COUNT(*)]
    BEFORE --> COLCHECK{Date Column<br/>Exists in Table?}
    
    COLCHECK -->|NO| SKIP[Log SKIPPED<br/>Column not found]
    COLCHECK -->|YES| DELETE[Execute DELETE<br/>WHERE date_column < cutoff]
    
    DELETE --> AFTER[Get Row Count AFTER<br/>SELECT COUNT(*)]
    AFTER --> LOG[Log to CLEANUP_LOG<br/>rows_before, rows_deleted, rows_after]
    
    SKIP --> RETURN[Return Result JSON]
    LOG --> RETURN
    
    RETURN --> END([END])
    
    style DELETE fill:#FF6B6B,stroke:#C0392B
    style LOG fill:#90EE90,stroke:#228B22
```

---

## SP_CLEANUP_SCHEMA Flow

```mermaid
flowchart TD
    START([START]) --> BATCH[Generate BATCH_ID<br/>CLEANUP_YYYYMMDD_HHMMSS]
    BATCH --> QUERY[Query INFORMATION_SCHEMA<br/>Find all _BASE Tables]
    
    QUERY --> LOOP{More Tables<br/>to Process?}
    
    LOOP -->|YES| CHECK{Table in<br/>Exclusion List?}
    CHECK -->|YES| SKIPEX[Add to Results<br/>status: EXCLUDED]
    CHECK -->|NO| CALL[Call SP_CLEANUP_BASE_TABLE<br/>for this table]
    
    CALL --> RESULT{Result<br/>Status?}
    RESULT -->|SUCCESS| INCPROC[tables_processed++<br/>total_deleted += rows_deleted]
    RESULT -->|SKIPPED| INCSKIP[tables_skipped++]
    RESULT -->|FAILED| INCFAIL[tables_failed++]
    
    SKIPEX --> LOOP
    INCPROC --> LOOP
    INCSKIP --> LOOP
    INCFAIL --> LOOP
    
    LOOP -->|NO| RETURN[Return Summary JSON<br/>with all results]
    RETURN --> END([END])
```

---

# Database Schema

## Entity Relationship Diagram

```mermaid
erDiagram
    CLEANUP_CONFIG ||--o{ CLEANUP_LOG : generates
    
    CLEANUP_CONFIG {
        int CONFIG_ID PK
        string DATABASE_NAME
        string SCHEMA_NAME
        string TABLE_PATTERN
        string DATE_COLUMN
        int RETENTION_DAYS
        int BATCH_SIZE
        boolean IS_ACTIVE
        string TASK_SCHEDULE
        string TASK_WAREHOUSE
        timestamp LAST_CLEANUP_AT
        timestamp CREATED_AT
        string NOTES
    }
    
    CLEANUP_LOG {
        int LOG_ID PK
        int CONFIG_ID FK
        string BATCH_ID
        string DATABASE_NAME
        string SCHEMA_NAME
        string TABLE_NAME
        string DATE_COLUMN
        int RETENTION_DAYS
        date CUTOFF_DATE
        int ROWS_BEFORE
        int ROWS_DELETED
        int ROWS_AFTER
        timestamp EXECUTION_START
        timestamp EXECUTION_END
        decimal DURATION_SECONDS
        string STATUS
        string ERROR_MESSAGE
    }
    
    CLEANUP_EXCLUSIONS {
        int EXCLUSION_ID PK
        string DATABASE_NAME
        string SCHEMA_NAME
        string TABLE_NAME
        string EXCLUSION_REASON
        timestamp CREATED_AT
        string CREATED_BY
    }
```

---

## Configuration Table Structure

```mermaid
classDiagram
    class CLEANUP_CONFIG {
        +CONFIG_ID : NUMBER (PK, Auto)
        +DATABASE_NAME : VARCHAR(255)
        +SCHEMA_NAME : VARCHAR(255)
        +TABLE_PATTERN : VARCHAR(255)
        +DATE_COLUMN : VARCHAR(255)
        +RETENTION_DAYS : NUMBER
        +BATCH_SIZE : NUMBER
        +IS_ACTIVE : BOOLEAN
        +TASK_SCHEDULE : VARCHAR(100)
        +TASK_WAREHOUSE : VARCHAR(255)
        +LAST_CLEANUP_AT : TIMESTAMP_LTZ
        +CREATED_AT : TIMESTAMP_LTZ
        +NOTES : VARCHAR(4000)
    }
    
    note for CLEANUP_CONFIG "Default Values:\n- RETENTION_DAYS: 45\n- TABLE_PATTERN: %_BASE\n- TASK_SCHEDULE: CRON 0 2 * * * UTC\n- IS_ACTIVE: TRUE"
```

---

# Task Configuration & Scheduling

## How Tasks are Scheduled (Daily 2 AM UTC)

```mermaid
flowchart TD
    subgraph TaskDefinition["TASK DEFINITION"]
        DEF["CREATE TASK TASK_CLEANUP_ALL_SCHEMAS<br/>WAREHOUSE = COMPUTE_WH<br/>SCHEDULE = 'USING CRON 0 2 * * * UTC'<br/>ALLOW_OVERLAPPING_EXECUTION = FALSE<br/>USER_TASK_TIMEOUT_MS = 14400000"]
    end
    
    subgraph Action["ACTION"]
        AS["AS<br/>CALL SP_CLEANUP_ALL_CONFIGS()"]
    end
    
    DEF --> AS
```

## Task Parameters Explained

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `WAREHOUSE` | `COMPUTE_WH` | Compute resource for execution |
| `SCHEDULE` | `CRON 0 2 * * * UTC` | Daily at 2 AM UTC (low activity) |
| `ALLOW_OVERLAPPING_EXECUTION` | `FALSE` | Prevent duplicate runs |
| `USER_TASK_TIMEOUT_MS` | `14400000` | 4 hour max runtime |

## How to Manage the Task

```sql
-- Option 1: Enable scheduled cleanup
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS RESUME;

-- Option 2: Disable scheduled cleanup
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS SUSPEND;

-- Option 3: Run immediately (manual trigger)
EXECUTE TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS;

-- Verify task status
SHOW TASKS LIKE 'TASK_CLEANUP%' IN SCHEMA CDC_PRESERVATION.CLEANUP;
-- Look for state = 'started'
```

---

# Monitoring Dashboard

## Key Metrics Views

```mermaid
flowchart LR
    subgraph Views["MONITORING VIEWS"]
        V1[V_CLEANUP_SUMMARY]
        V2[V_RECENT_CLEANUPS]
        V3[V_FAILED_CLEANUPS]
        V4[V_CONFIG_STATUS]
    end
    
    subgraph Metrics["KEY METRICS"]
        M1[Daily Rows Deleted]
        M2[Success/Fail Rates]
        M3[Tables Processed]
        M4[Execution Duration]
        M5[Config Status]
    end
    
    V1 --> M1
    V1 --> M2
    V2 --> M3
    V2 --> M4
    V3 --> M2
    V4 --> M5
```

## Sample Dashboard Queries

```sql
-- Daily Health Check
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_CLEANUP_SUMMARY;

-- Recent Cleanup Details
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_RECENT_CLEANUPS;

-- Check for Failures
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_FAILED_CLEANUPS;

-- Active Configurations
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_CONFIG_STATUS
WHERE IS_ACTIVE = TRUE;

-- Task Execution History
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_CLEANUP_ALL_SCHEMAS',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
)) ORDER BY SCHEDULED_TIME DESC;
```

---

# Test Results Summary

## Test Scenarios Executed

```mermaid
flowchart LR
    subgraph Tests["TEST SCENARIOS - ALL PASSED"]
        T1["✅ Dry Run<br/>Preview Mode"]
        T2["✅ Schema Cleanup<br/>Multiple Tables"]
        T3["✅ Exclusion List<br/>Skip Tables"]
        T4["✅ Missing Column<br/>Graceful Skip"]
        T5["✅ Task Scheduling<br/>2 AM UTC"]
        T6["✅ Audit Logging<br/>Full History"]
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
| **Test 1** | Dry run preview mode | ✅ PASSED - Shows rows to delete without changes |
| **Test 2** | Multi-table schema cleanup | ✅ PASSED - 100 rows deleted from 2 tables |
| **Test 3** | Table exclusion list | ✅ PASSED - Excluded tables skipped |
| **Test 4** | Missing date column handling | ✅ PASSED - Graceful skip with log |
| **Test 5** | Scheduled task creation | ✅ PASSED - 2 AM UTC daily schedule |
| **Test 6** | Audit log completeness | ✅ PASSED - Full metrics captured |

---

# Deployment Guide

## Deployment Steps

```mermaid
flowchart TD
    subgraph Step1["STEP 1: Deploy Framework"]
        A1[Create Schema<br/>CDC_PRESERVATION.CLEANUP]
        A2[Create 3 Config Tables]
        A3[Deploy 8 Stored Procedures]
        A4[Create 4 Monitoring Views]
    end
    
    subgraph Step2["STEP 2: Configure"]
        B1[INSERT into CLEANUP_CONFIG]
        B2[Define database, schema, date_column]
        B3[Set retention_days if not 45]
    end
    
    subgraph Step3["STEP 3: Validate"]
        C1["CALL SP_CLEANUP_DRY_RUN(...)"]
        C2[Review preview results]
        C3[Add exclusions if needed]
    end
    
    subgraph Step4["STEP 4: Go Live"]
        D1["CALL SP_CREATE_MASTER_CLEANUP_TASK()"]
        D2["ALTER TASK ... RESUME"]
        D3[Monitor V_CLEANUP_SUMMARY]
    end
    
    Step1 --> Step2 --> Step3 --> Step4
```

## Adding New Schemas

```sql
-- STEP 1: Add configuration for new schema
INSERT INTO CDC_PRESERVATION.CLEANUP.CLEANUP_CONFIG 
(DATABASE_NAME, SCHEMA_NAME, TABLE_PATTERN, DATE_COLUMN, RETENTION_DAYS, NOTES)
VALUES
('D_BRONZE', 'NEW_SCHEMA', '%_BASE', 'CREATED_DATE', 45, 
 'Cleanup for NEW_SCHEMA _BASE tables');

-- STEP 2: Run dry run to preview
CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_DRY_RUN(
    'D_BRONZE', 'NEW_SCHEMA', 'CREATED_DATE', 45, '%_BASE'
);

-- STEP 3: Execute manual test (optional)
CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_SCHEMA(
    'D_BRONZE', 'NEW_SCHEMA', 'CREATED_DATE', 45, 100000, '%_BASE'
);

-- Done! Task will include this schema on next scheduled run
```

---

# Summary

## Key Benefits

```mermaid
mindmap
  root((Cleanup Framework))
    Storage Optimization
      45-Day Retention
      Configurable per Schema
      Reduces Storage Costs
      Improves Query Performance
    Automation
      Daily Scheduled Task
      Zero Manual Intervention
      Config-Driven
    Safety
      Dry Run Preview
      Exclusion Lists
      Time Travel Recovery
    Monitoring
      4 Dashboard Views
      Complete Audit Trail
      Execution Metrics
```

## Framework Comparison

| Feature | Manual Cleanup | With Framework |
|---------|---------------|----------------|
| Execution | ❌ Manual scripts | ✅ Automated daily |
| Consistency | ❌ Varies by engineer | ✅ Standardized |
| Audit Trail | ❌ None | ✅ Complete history |
| Preview | ❌ No preview | ✅ Dry run mode |
| Exclusions | ❌ Code changes | ✅ Config table |
| Monitoring | ❌ Manual checks | ✅ Dashboard views |

---

# Files Delivered

| File | Purpose |
|------|---------|
| `BASE_Table_Cleanup_Framework.sql` | Complete framework with all 8 SPs |
| `BASE_Table_Cleanup_Framework_Runbook.md` | Step-by-step deployment guide |
| `BASE_Table_Cleanup_Framework_Documentation.md` | This customer presentation |
| `Cleanup_Framework_Deployment_Validation.sql` | Multi-environment validation script |

---

# Quick Start Commands

```sql
-- 1. Preview cleanup (DRY RUN - no changes)
CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_DRY_RUN(
    'D_BRONZE', 'SALES', 'CREATED_DATE', 45, '%_BASE'
);

-- 2. Execute manual cleanup
CALL CDC_PRESERVATION.CLEANUP.SP_CLEANUP_SCHEMA(
    'D_BRONZE', 'SALES', 'CREATED_DATE', 45, 100000, '%_BASE'
);

-- 3. Enable daily scheduled cleanup
ALTER TASK CDC_PRESERVATION.CLEANUP.TASK_CLEANUP_ALL_SCHEMAS RESUME;

-- 4. Monitor cleanup activity
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_CLEANUP_SUMMARY;
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_RECENT_CLEANUPS;
```

---

# Contact & Support

For questions or issues:
1. Check monitoring views for status: `V_CLEANUP_SUMMARY`
2. Review execution logs: `V_RECENT_CLEANUPS`
3. Check failures: `V_FAILED_CLEANUPS`
4. Contact Data Engineering team

---

*Document Version: 2.0*  
*Framework: BASE Table Data Cleanup*  
*Status: Production Ready*  
*Last Updated: February 2026*
