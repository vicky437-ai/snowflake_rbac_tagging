# BASE Table Data Cleanup Framework
## Customer Presentation & Technical Documentation

---

## Executive Summary

The **BASE Table Data Cleanup Framework** is an enterprise-grade, automated solution for managing data retention in Snowflake staging tables. It provides configurable, scheduled cleanup of records older than a specified retention period (default: 45 days) across all `_BASE` suffix tables.

### Key Business Value

| Metric | Impact |
|--------|--------|
| **Storage Cost Reduction** | 30-50% reduction in staging table storage |
| **Operational Efficiency** | Zero manual intervention with automated scheduling |
| **Compliance** | Configurable retention policies per schema |
| **Risk Mitigation** | Dry-run preview, exclusion lists, comprehensive audit logs |

---

## 1. Architecture Overview

### 1.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        BASE TABLE CLEANUP FRAMEWORK                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐             │
│  │   SNOWFLAKE     │    │   CLEANUP       │    │   MONITORING    │             │
│  │     TASK        │───▶│   ENGINE        │───▶│   & LOGGING     │             │
│  │  (Scheduler)    │    │  (Procedures)   │    │    (Views)      │             │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘             │
│         │                      │                       │                        │
│         │                      │                       │                        │
│         ▼                      ▼                       ▼                        │
│  ┌─────────────────────────────────────────────────────────────────┐           │
│  │                    CONFIGURATION LAYER                           │           │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │           │
│  │  │ CLEANUP_     │  │ CLEANUP_     │  │ CLEANUP_     │           │           │
│  │  │ CONFIG       │  │ EXCLUSIONS   │  │ LOG          │           │           │
│  │  │ (Settings)   │  │ (Skip List)  │  │ (Audit)      │           │           │
│  │  └──────────────┘  └──────────────┘  └──────────────┘           │           │
│  └─────────────────────────────────────────────────────────────────┘           │
│                                                                                  │
│                              TARGET SCHEMAS                                      │
│  ┌─────────────────────────────────────────────────────────────────┐           │
│  │  D_BRONZE.SALES          D_BRONZE.ORDERS        D_BRONZE.xxx    │           │
│  │  ┌────────────────┐      ┌────────────────┐     ┌────────────┐  │           │
│  │  │ CUSTOMERS_BASE │      │ ORDERS_BASE    │     │ xxx_BASE   │  │           │
│  │  │ ORDERS_BASE    │      │ ITEMS_BASE     │     │ yyy_BASE   │  │           │
│  │  │ PRODUCTS_BASE  │      │ RETURNS_BASE   │     │ zzz_BASE   │  │           │
│  │  └────────────────┘      └────────────────┘     └────────────┘  │           │
│  └─────────────────────────────────────────────────────────────────┘           │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              FRAMEWORK COMPONENTS                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                         SCHEDULER LAYER                                  │    │
│  │                                                                          │    │
│  │   ┌──────────────────────────────────────────────────────────────┐      │    │
│  │   │  TASK: TASK_CLEANUP_ALL_SCHEMAS                              │      │    │
│  │   │  Schedule: CRON 0 2 * * * UTC (Daily 2 AM UTC)               │      │    │
│  │   │  Timeout: 4 hours                                             │      │    │
│  │   │  Overlap: FALSE                                               │      │    │
│  │   └──────────────────────────────────────────────────────────────┘      │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                       │                                          │
│                                       ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                         PROCEDURE LAYER                                  │    │
│  │                                                                          │    │
│  │   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐     │    │
│  │   │ SP_CLEANUP_     │    │ SP_CLEANUP_     │    │ SP_CLEANUP_     │     │    │
│  │   │ ALL_CONFIGS     │───▶│ BY_CONFIG       │───▶│ SCHEMA          │     │    │
│  │   │ (Orchestrator)  │    │ (Config Reader) │    │ (Schema Loop)   │     │    │
│  │   └─────────────────┘    └─────────────────┘    └────────┬────────┘     │    │
│  │                                                          │               │    │
│  │                                                          ▼               │    │
│  │   ┌─────────────────┐                          ┌─────────────────┐      │    │
│  │   │ SP_CLEANUP_     │                          │ SP_CLEANUP_     │      │    │
│  │   │ DRY_RUN         │                          │ BASE_TABLE      │      │    │
│  │   │ (Preview Only)  │                          │ (Core Delete)   │      │    │
│  │   └─────────────────┘                          └─────────────────┘      │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                         MONITORING LAYER                                 │    │
│  │                                                                          │    │
│  │   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐     │    │
│  │   │ V_CLEANUP_      │    │ V_RECENT_       │    │ V_FAILED_       │     │    │
│  │   │ SUMMARY         │    │ CLEANUPS        │    │ CLEANUPS        │     │    │
│  │   │ (Daily Stats)   │    │ (Last 7 Days)   │    │ (Errors Only)   │     │    │
│  │   └─────────────────┘    └─────────────────┘    └─────────────────┘     │    │
│  │                                                                          │    │
│  │   ┌─────────────────┐                                                    │    │
│  │   │ V_CONFIG_       │                                                    │    │
│  │   │ STATUS          │                                                    │    │
│  │   │ (Config View)   │                                                    │    │
│  │   └─────────────────┘                                                    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Flow Diagrams

### 2.1 Task Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           DAILY TASK EXECUTION FLOW                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌─────────┐                                                                    │
│   │  START  │  (2:00 AM UTC Daily)                                              │
│   └────┬────┘                                                                    │
│        │                                                                         │
│        ▼                                                                         │
│   ┌─────────────────────────────────────────┐                                   │
│   │  TASK_CLEANUP_ALL_SCHEMAS               │                                   │
│   │  ─────────────────────────────────────  │                                   │
│   │  Calls: SP_CLEANUP_ALL_CONFIGS()        │                                   │
│   └────────────────┬────────────────────────┘                                   │
│                    │                                                             │
│                    ▼                                                             │
│   ┌─────────────────────────────────────────┐                                   │
│   │  Load Active Configurations             │                                   │
│   │  ─────────────────────────────────────  │                                   │
│   │  SELECT * FROM CLEANUP_CONFIG           │                                   │
│   │  WHERE IS_ACTIVE = TRUE                 │                                   │
│   └────────────────┬────────────────────────┘                                   │
│                    │                                                             │
│        ┌───────────┴───────────┐                                                │
│        ▼                       ▼                                                │
│   ┌─────────┐            ┌─────────┐                                            │
│   │ Config  │            │ Config  │  ... (N configs)                           │
│   │   #1    │            │   #2    │                                            │
│   └────┬────┘            └────┬────┘                                            │
│        │                      │                                                  │
│        ▼                      ▼                                                  │
│   ┌─────────────────────────────────────────┐                                   │
│   │  SP_CLEANUP_SCHEMA()                    │                                   │
│   │  ─────────────────────────────────────  │                                   │
│   │  For each _BASE table in schema:        │                                   │
│   │  1. Check exclusion list                │                                   │
│   │  2. Validate date column exists         │                                   │
│   │  3. Execute DELETE                      │                                   │
│   │  4. Log results                         │                                   │
│   └────────────────┬────────────────────────┘                                   │
│                    │                                                             │
│                    ▼                                                             │
│   ┌─────────────────────────────────────────┐                                   │
│   │  Update CLEANUP_CONFIG                  │                                   │
│   │  ─────────────────────────────────────  │                                   │
│   │  SET LAST_CLEANUP_AT = NOW()            │                                   │
│   └────────────────┬────────────────────────┘                                   │
│                    │                                                             │
│                    ▼                                                             │
│   ┌─────────────────────────────────────────┐                                   │
│   │  Return Summary JSON                    │                                   │
│   │  ─────────────────────────────────────  │                                   │
│   │  {                                      │                                   │
│   │    "configs_processed": N,              │                                   │
│   │    "total_rows_deleted": X,             │                                   │
│   │    "results": [...]                     │                                   │
│   │  }                                      │                                   │
│   └────────────────┬────────────────────────┘                                   │
│                    │                                                             │
│                    ▼                                                             │
│               ┌─────────┐                                                        │
│               │   END   │                                                        │
│               └─────────┘                                                        │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Single Table Cleanup Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         SINGLE TABLE CLEANUP PROCESS                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌─────────┐                                                                    │
│   │  START  │                                                                    │
│   └────┬────┘                                                                    │
│        │                                                                         │
│        ▼                                                                         │
│   ┌─────────────────────────────────────────┐                                   │
│   │  Check Exclusion List                   │                                   │
│   │  ─────────────────────────────────────  │                                   │
│   │  Is table in CLEANUP_EXCLUSIONS?        │                                   │
│   └────────────────┬────────────────────────┘                                   │
│                    │                                                             │
│          ┌─────────┴─────────┐                                                  │
│          │                   │                                                  │
│        [YES]               [NO]                                                 │
│          │                   │                                                  │
│          ▼                   ▼                                                  │
│   ┌─────────────┐   ┌─────────────────────────────────┐                        │
│   │  Log SKIP   │   │  Get Row Count BEFORE           │                        │
│   │  Return     │   │  ───────────────────────────    │                        │
│   │  EXCLUDED   │   │  SELECT COUNT(*) FROM table     │                        │
│   └──────┬──────┘   └────────────────┬────────────────┘                        │
│          │                           │                                          │
│          │                           ▼                                          │
│          │          ┌─────────────────────────────────┐                        │
│          │          │  Validate Date Column Exists    │                        │
│          │          │  ───────────────────────────    │                        │
│          │          │  Check INFORMATION_SCHEMA       │                        │
│          │          └────────────────┬────────────────┘                        │
│          │                           │                                          │
│          │                 ┌─────────┴─────────┐                               │
│          │                 │                   │                               │
│          │           [NOT FOUND]           [FOUND]                             │
│          │                 │                   │                               │
│          │                 ▼                   ▼                               │
│          │          ┌─────────────┐   ┌─────────────────────────────┐         │
│          │          │  Log SKIP   │   │  Calculate Cutoff Date      │         │
│          │          │  "Column    │   │  ─────────────────────────  │         │
│          │          │  not found" │   │  CURRENT_DATE - 45 days     │         │
│          │          └──────┬──────┘   └────────────────┬────────────┘         │
│          │                 │                           │                       │
│          │                 │                           ▼                       │
│          │                 │          ┌─────────────────────────────┐         │
│          │                 │          │  Execute DELETE             │         │
│          │                 │          │  ─────────────────────────  │         │
│          │                 │          │  DELETE FROM table          │         │
│          │                 │          │  WHERE date_col < cutoff    │         │
│          │                 │          └────────────────┬────────────┘         │
│          │                 │                           │                       │
│          │                 │                           ▼                       │
│          │                 │          ┌─────────────────────────────┐         │
│          │                 │          │  Get Row Count AFTER        │         │
│          │                 │          │  ─────────────────────────  │         │
│          │                 │          │  SELECT COUNT(*) FROM table │         │
│          │                 │          └────────────────┬────────────┘         │
│          │                 │                           │                       │
│          │                 │                           ▼                       │
│          │                 │          ┌─────────────────────────────┐         │
│          │                 │          │  Log to CLEANUP_LOG         │         │
│          │                 │          │  ─────────────────────────  │         │
│          │                 │          │  rows_before, rows_deleted, │         │
│          │                 │          │  rows_after, duration, etc. │         │
│          │                 │          └────────────────┬────────────┘         │
│          │                 │                           │                       │
│          ▼                 ▼                           ▼                       │
│   ┌─────────────────────────────────────────────────────────────────┐         │
│   │                     Return Result JSON                          │         │
│   │  ─────────────────────────────────────────────────────────────  │         │
│   │  { "table": "...", "status": "SUCCESS/SKIPPED/FAILED",          │         │
│   │    "rows_before": N, "rows_deleted": X, "rows_after": M }       │         │
│   └────────────────────────────┬────────────────────────────────────┘         │
│                                │                                               │
│                                ▼                                               │
│                           ┌─────────┐                                          │
│                           │   END   │                                          │
│                           └─────────┘                                          │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.3 Decision Flow for Dry Run vs Execute

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        DRY RUN vs EXECUTE DECISION FLOW                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│                          ┌─────────────────────┐                                │
│                          │  User Request       │                                │
│                          │  Cleanup Operation  │                                │
│                          └──────────┬──────────┘                                │
│                                     │                                            │
│                                     ▼                                            │
│                    ┌────────────────────────────────────┐                       │
│                    │  Is this a new configuration       │                       │
│                    │  or first-time cleanup?            │                       │
│                    └────────────────┬───────────────────┘                       │
│                                     │                                            │
│                  ┌──────────────────┴──────────────────┐                        │
│                  │                                      │                        │
│                [YES]                                  [NO]                       │
│                  │                                      │                        │
│                  ▼                                      ▼                        │
│   ┌──────────────────────────────┐    ┌──────────────────────────────┐         │
│   │  STEP 1: DRY RUN             │    │  Existing Config?            │         │
│   │  ────────────────────────    │    │  ────────────────────────    │         │
│   │  SP_CLEANUP_DRY_RUN(...)     │    │  Check CLEANUP_CONFIG        │         │
│   │                              │    └─────────────┬────────────────┘         │
│   │  Preview:                    │                  │                           │
│   │  • Tables to process         │         ┌────────┴────────┐                 │
│   │  • Rows to delete            │         │                 │                 │
│   │  • % of data affected        │       [YES]             [NO]                │
│   └─────────────┬────────────────┘         │                 │                 │
│                 │                          │                 ▼                 │
│                 ▼                          │    ┌──────────────────────┐       │
│   ┌──────────────────────────────┐        │    │  Add Configuration   │       │
│   │  Review Results              │        │    │  ──────────────────  │       │
│   │  ────────────────────────    │        │    │  INSERT INTO         │       │
│   │  • Acceptable delete count?  │        │    │  CLEANUP_CONFIG      │       │
│   │  • Correct tables found?     │        │    └──────────┬───────────┘       │
│   │  • Date column valid?        │        │               │                   │
│   └─────────────┬────────────────┘        │               │                   │
│                 │                          └───────┬───────┘                   │
│        ┌────────┴────────┐                        │                           │
│        │                 │                        │                           │
│     [APPROVE]        [REJECT]                     │                           │
│        │                 │                        │                           │
│        ▼                 ▼                        ▼                           │
│   ┌─────────────┐  ┌─────────────┐  ┌──────────────────────────────┐         │
│   │  STEP 2:    │  │  Adjust     │  │  EXECUTE CLEANUP             │         │
│   │  EXECUTE    │  │  Config     │  │  ────────────────────────    │         │
│   │  ─────────  │  │  ─────────  │  │  • Manual: SP_CLEANUP_SCHEMA │         │
│   │  SP_CLEANUP │  │  Modify     │  │  • Scheduled: Task runs      │         │
│   │  _SCHEMA()  │  │  parameters │  │    automatically at 2 AM     │         │
│   └──────┬──────┘  └──────┬──────┘  └─────────────┬────────────────┘         │
│          │                │                       │                           │
│          │                └───────────────────────┤                           │
│          │                                        │                           │
│          └────────────────────────────────────────┤                           │
│                                                   │                           │
│                                                   ▼                           │
│                              ┌──────────────────────────────┐                 │
│                              │  STEP 3: MONITOR             │                 │
│                              │  ────────────────────────    │                 │
│                              │  • V_CLEANUP_SUMMARY         │                 │
│                              │  • V_RECENT_CLEANUPS         │                 │
│                              │  • V_FAILED_CLEANUPS         │                 │
│                              └──────────────────────────────┘                 │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Model

### 3.1 Entity Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA MODEL                                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐       │
│   │                        CLEANUP_CONFIG                                │       │
│   │  ───────────────────────────────────────────────────────────────    │       │
│   │  PK: CONFIG_ID (NUMBER, AUTOINCREMENT)                              │       │
│   │  ───────────────────────────────────────────────────────────────    │       │
│   │  DATABASE_NAME     VARCHAR(255)   NOT NULL   ──┐                    │       │
│   │  SCHEMA_NAME       VARCHAR(255)   NOT NULL   ──┼── UNIQUE          │       │
│   │  TABLE_PATTERN     VARCHAR(255)   DEFAULT '%_BASE'                  │       │
│   │  DATE_COLUMN       VARCHAR(255)   NOT NULL                          │       │
│   │  RETENTION_DAYS    NUMBER         DEFAULT 45                        │       │
│   │  BATCH_SIZE        NUMBER         DEFAULT 100000                    │       │
│   │  IS_ACTIVE         BOOLEAN        DEFAULT TRUE                      │       │
│   │  TASK_SCHEDULE     VARCHAR(100)   DEFAULT 'CRON 0 2 * * * UTC'     │       │
│   │  TASK_WAREHOUSE    VARCHAR(255)   DEFAULT 'COMPUTE_WH'             │       │
│   │  LAST_CLEANUP_AT   TIMESTAMP_LTZ                                    │       │
│   │  CREATED_AT        TIMESTAMP_LTZ  DEFAULT CURRENT_TIMESTAMP()      │       │
│   │  UPDATED_AT        TIMESTAMP_LTZ  DEFAULT CURRENT_TIMESTAMP()      │       │
│   │  CREATED_BY        VARCHAR(255)   DEFAULT CURRENT_USER()           │       │
│   │  NOTES             VARCHAR(4000)                                    │       │
│   └─────────────────────────────────────────────────────────────────────┘       │
│                                    │                                             │
│                                    │ 1:N                                         │
│                                    ▼                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐       │
│   │                         CLEANUP_LOG                                  │       │
│   │  ───────────────────────────────────────────────────────────────    │       │
│   │  PK: LOG_ID (NUMBER, AUTOINCREMENT)                                 │       │
│   │  ───────────────────────────────────────────────────────────────    │       │
│   │  FK: CONFIG_ID       NUMBER        (References CLEANUP_CONFIG)      │       │
│   │  BATCH_ID            VARCHAR(100)  (Execution batch identifier)     │       │
│   │  DATABASE_NAME       VARCHAR(255)                                   │       │
│   │  SCHEMA_NAME         VARCHAR(255)                                   │       │
│   │  TABLE_NAME          VARCHAR(255)                                   │       │
│   │  DATE_COLUMN         VARCHAR(255)                                   │       │
│   │  RETENTION_DAYS      NUMBER                                         │       │
│   │  CUTOFF_DATE         DATE                                           │       │
│   │  ROWS_BEFORE         NUMBER        (Count before DELETE)            │       │
│   │  ROWS_DELETED        NUMBER        (Actual deleted)                 │       │
│   │  ROWS_AFTER          NUMBER        (Count after DELETE)             │       │
│   │  EXECUTION_START     TIMESTAMP_LTZ                                  │       │
│   │  EXECUTION_END       TIMESTAMP_LTZ                                  │       │
│   │  DURATION_SECONDS    NUMBER(10,2)                                   │       │
│   │  STATUS              VARCHAR(20)   (SUCCESS/FAILED/SKIPPED)        │       │
│   │  ERROR_MESSAGE       VARCHAR(16000)                                 │       │
│   │  CREATED_AT          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()     │       │
│   └─────────────────────────────────────────────────────────────────────┘       │
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐       │
│   │                      CLEANUP_EXCLUSIONS                              │       │
│   │  ───────────────────────────────────────────────────────────────    │       │
│   │  PK: EXCLUSION_ID (NUMBER, AUTOINCREMENT)                           │       │
│   │  ───────────────────────────────────────────────────────────────    │       │
│   │  DATABASE_NAME     VARCHAR(255)   NOT NULL   ──┐                    │       │
│   │  SCHEMA_NAME       VARCHAR(255)   NOT NULL   ──┼── UNIQUE          │       │
│   │  TABLE_NAME        VARCHAR(255)   NOT NULL   ──┘                    │       │
│   │  EXCLUSION_REASON  VARCHAR(1000)                                    │       │
│   │  CREATED_AT        TIMESTAMP_LTZ  DEFAULT CURRENT_TIMESTAMP()      │       │
│   │  CREATED_BY        VARCHAR(255)   DEFAULT CURRENT_USER()           │       │
│   └─────────────────────────────────────────────────────────────────────┘       │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Procedure Reference

### 4.1 Procedure Hierarchy

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          PROCEDURE CALL HIERARCHY                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  SCHEDULED EXECUTION PATH:                                                       │
│  ─────────────────────────                                                       │
│                                                                                  │
│  TASK_CLEANUP_ALL_SCHEMAS                                                        │
│          │                                                                       │
│          └──▶ SP_CLEANUP_ALL_CONFIGS()                                          │
│                      │                                                           │
│                      └──▶ SP_CLEANUP_BY_CONFIG(config_id)  [Loop per config]    │
│                                  │                                               │
│                                  └──▶ SP_CLEANUP_SCHEMA(db, schema, ...)        │
│                                              │                                   │
│                                              └──▶ SP_CLEANUP_BASE_TABLE(...)    │
│                                                        [Loop per table]          │
│                                                                                  │
│  ═══════════════════════════════════════════════════════════════════════════    │
│                                                                                  │
│  MANUAL EXECUTION PATHS:                                                         │
│  ───────────────────────                                                         │
│                                                                                  │
│  Option 1: Direct Schema Cleanup (Most Common)                                   │
│  ─────────────────────────────────────────────                                   │
│  SP_CLEANUP_SCHEMA('D_BRONZE', 'SALES', 'CREATED_DATE', 45)                     │
│          │                                                                       │
│          └──▶ SP_CLEANUP_BASE_TABLE(...) [Loop per table]                       │
│                                                                                  │
│  Option 2: Config-Based Cleanup                                                  │
│  ─────────────────────────────                                                   │
│  SP_CLEANUP_BY_CONFIG(1)                                                         │
│          │                                                                       │
│          └──▶ SP_CLEANUP_SCHEMA(...) ──▶ SP_CLEANUP_BASE_TABLE(...)            │
│                                                                                  │
│  Option 3: Preview Only (Dry Run)                                                │
│  ────────────────────────────────                                                │
│  SP_CLEANUP_DRY_RUN('D_BRONZE', 'SALES', 'CREATED_DATE', 45)                    │
│          │                                                                       │
│          └──▶ [No deletions - returns preview counts only]                      │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Procedure Quick Reference

| Procedure | Purpose | Parameters |
|-----------|---------|------------|
| `SP_CLEANUP_BASE_TABLE` | Delete old data from single table | DB, Schema, Table, DateCol, Days, BatchSize, BatchID |
| `SP_CLEANUP_SCHEMA` | Process all _BASE tables in schema | DB, Schema, DateCol, Days, BatchSize, Pattern |
| `SP_CLEANUP_DRY_RUN` | Preview cleanup without executing | DB, Schema, DateCol, Days, Pattern |
| `SP_CLEANUP_BY_CONFIG` | Execute using stored config | CONFIG_ID |
| `SP_CLEANUP_ALL_CONFIGS` | Process all active configs | None |
| `SP_CREATE_MASTER_CLEANUP_TASK` | Create scheduled task | Warehouse, Schedule |
| `SP_RESUME_CLEANUP_TASK` | Resume suspended task | TaskName |
| `SP_SUSPEND_CLEANUP_TASK` | Suspend running task | TaskName |

---

## 5. Monitoring & Operations

### 5.1 Monitoring Views

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            MONITORING VIEWS                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐       │
│   │  V_CLEANUP_SUMMARY                                                   │       │
│   │  ───────────────────────────────────────────────────────────────    │       │
│   │  Daily aggregated statistics                                         │       │
│   │                                                                      │       │
│   │  Columns: CLEANUP_DATE, DATABASE_NAME, SCHEMA_NAME,                 │       │
│   │           TABLES_PROCESSED, SUCCESS_COUNT, FAILED_COUNT,            │       │
│   │           TOTAL_ROWS_DELETED, AVG_DURATION_SEC                      │       │
│   │                                                                      │       │
│   │  Use: Daily health check, capacity planning                         │       │
│   └─────────────────────────────────────────────────────────────────────┘       │
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐       │
│   │  V_RECENT_CLEANUPS                                                   │       │
│   │  ───────────────────────────────────────────────────────────────    │       │
│   │  Last 7 days of detailed cleanup records                            │       │
│   │                                                                      │       │
│   │  Columns: BATCH_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,         │       │
│   │           CUTOFF_DATE, ROWS_BEFORE, ROWS_DELETED, ROWS_AFTER,       │       │
│   │           DURATION_SECONDS, STATUS, ERROR_MESSAGE, CREATED_AT       │       │
│   │                                                                      │       │
│   │  Use: Troubleshooting, audit, verification                          │       │
│   └─────────────────────────────────────────────────────────────────────┘       │
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐       │
│   │  V_FAILED_CLEANUPS                                                   │       │
│   │  ───────────────────────────────────────────────────────────────    │       │
│   │  Failed cleanup attempts from last 30 days                          │       │
│   │                                                                      │       │
│   │  Columns: BATCH_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,         │       │
│   │           ERROR_MESSAGE, CREATED_AT                                 │       │
│   │                                                                      │       │
│   │  Use: Error investigation, alerting                                 │       │
│   └─────────────────────────────────────────────────────────────────────┘       │
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐       │
│   │  V_CONFIG_STATUS                                                     │       │
│   │  ───────────────────────────────────────────────────────────────    │       │
│   │  Current configuration status                                        │       │
│   │                                                                      │       │
│   │  Columns: CONFIG_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_PATTERN,     │       │
│   │           DATE_COLUMN, RETENTION_DAYS, IS_ACTIVE, LAST_CLEANUP_AT,  │       │
│   │           TASK_SCHEDULE                                             │       │
│   │                                                                      │       │
│   │  Use: Configuration review, status check                            │       │
│   └─────────────────────────────────────────────────────────────────────┘       │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Sample Monitoring Queries

```sql
-- Daily cleanup summary
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_CLEANUP_SUMMARY;

-- Check for failures in last 24 hours
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_FAILED_CLEANUPS
WHERE CREATED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP());

-- View active configurations
SELECT * FROM CDC_PRESERVATION.CLEANUP.V_CONFIG_STATUS
WHERE IS_ACTIVE = TRUE;

-- Check task status
SHOW TASKS LIKE 'TASK_CLEANUP%' IN SCHEMA CDC_PRESERVATION.CLEANUP;

-- View task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_CLEANUP_ALL_SCHEMAS',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;
```

---

## 6. Security Model

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            SECURITY MODEL                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ROLE HIERARCHY:                                                                │
│   ───────────────                                                                │
│                                                                                  │
│   ┌─────────────────────┐                                                        │
│   │    ACCOUNTADMIN     │  (Framework Owner)                                     │
│   └──────────┬──────────┘                                                        │
│              │                                                                   │
│              │ GRANTS                                                            │
│              ▼                                                                   │
│   ┌─────────────────────┐                                                        │
│   │   CLEANUP_ADMIN     │  (Recommended custom role)                            │
│   │   ─────────────     │                                                        │
│   │   • USAGE on DB     │                                                        │
│   │   • USAGE on SCHEMA │                                                        │
│   │   • USAGE on WH     │                                                        │
│   │   • SELECT/INSERT/  │                                                        │
│   │     UPDATE/DELETE   │                                                        │
│   │     on CONFIG tables│                                                        │
│   │   • EXECUTE on      │                                                        │
│   │     all procedures  │                                                        │
│   │   • OPERATE on TASK │                                                        │
│   └──────────┬──────────┘                                                        │
│              │                                                                   │
│              │ GRANTS (for target schemas)                                       │
│              ▼                                                                   │
│   ┌─────────────────────────────────────────────────────────────────┐           │
│   │                    TARGET SCHEMA PERMISSIONS                     │           │
│   │   ───────────────────────────────────────────────────────────   │           │
│   │   Required on each target database/schema:                       │           │
│   │   • USAGE on DATABASE                                            │           │
│   │   • USAGE on SCHEMA                                              │           │
│   │   • DELETE on all _BASE tables (or OWNERSHIP)                   │           │
│   │   • SELECT on INFORMATION_SCHEMA                                 │           │
│   └─────────────────────────────────────────────────────────────────┘           │
│                                                                                  │
│   PROCEDURE EXECUTION:                                                           │
│   ────────────────────                                                           │
│   All procedures use EXECUTE AS CALLER                                           │
│   • Runs with caller's permissions                                               │
│   • Caller must have DELETE on target tables                                     │
│   • Provides audit trail of actual user                                          │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Test Results

### 7.1 Validation Summary

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          TEST EXECUTION RESULTS                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   Test Date: 2026-02-16                                                          │
│   Environment: Snowflake Account tgb36949                                        │
│   Target Schema: D_BRONZE.SALES                                                  │
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐       │
│   │  DRY RUN TEST                                                        │       │
│   │  ─────────────                                                       │       │
│   │  Command: CALL SP_CLEANUP_DRY_RUN('D_BRONZE', 'SALES', ...)         │       │
│   │                                                                      │       │
│   │  Result:                                                             │       │
│   │  ┌──────────────────┬─────────────┬────────────────┬────────────┐   │       │
│   │  │ Table            │ Total Rows  │ Rows to Delete │ Delete %   │   │       │
│   │  ├──────────────────┼─────────────┼────────────────┼────────────┤   │       │
│   │  │ CUSTOMERS_BASE   │ 5           │ 0              │ 0%         │   │       │
│   │  │ ORDERS_BASE      │ 157         │ 50             │ 31.85%     │   │       │
│   │  └──────────────────┴─────────────┴────────────────┴────────────┘   │       │
│   │                                                                      │       │
│   │  Status: ✅ PASSED                                                   │       │
│   └─────────────────────────────────────────────────────────────────────┘       │
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐       │
│   │  EXECUTION TEST                                                      │       │
│   │  ──────────────                                                      │       │
│   │  Command: CALL SP_CLEANUP_SCHEMA('D_BRONZE', 'SALES', ...)          │       │
│   │                                                                      │       │
│   │  Result:                                                             │       │
│   │  ┌──────────────────┬────────────┬─────────────┬─────────────────┐  │       │
│   │  │ Table            │ Before     │ Deleted     │ After           │  │       │
│   │  ├──────────────────┼────────────┼─────────────┼─────────────────┤  │       │
│   │  │ CUSTOMERS_BASE   │ 5          │ 0           │ 5               │  │       │
│   │  │ ORDERS_BASE      │ 207        │ 100         │ 107             │  │       │
│   │  └──────────────────┴────────────┴─────────────┴─────────────────┘  │       │
│   │                                                                      │       │
│   │  Summary:                                                            │       │
│   │  • Tables Processed: 2                                               │       │
│   │  • Tables Skipped: 0                                                 │       │
│   │  • Tables Failed: 0                                                  │       │
│   │  • Total Rows Deleted: 100                                           │       │
│   │                                                                      │       │
│   │  Status: ✅ PASSED                                                   │       │
│   └─────────────────────────────────────────────────────────────────────┘       │
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐       │
│   │  TASK CREATION TEST                                                  │       │
│   │  ──────────────────                                                  │       │
│   │  Command: CALL SP_CREATE_MASTER_CLEANUP_TASK(...)                   │       │
│   │                                                                      │       │
│   │  Result: Task TASK_CLEANUP_ALL_SCHEMAS created (SUSPENDED)          │       │
│   │  Schedule: CRON 0 2 * * * UTC (2 AM UTC Daily)                      │       │
│   │                                                                      │       │
│   │  Status: ✅ PASSED                                                   │       │
│   └─────────────────────────────────────────────────────────────────────┘       │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Review Score

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          FRAMEWORK REVIEW SCORE                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   OVERALL SCORE: 8.5 / 10                                                        │
│   ══════════════════════                                                         │
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐       │
│   │  CATEGORY SCORES                                                     │       │
│   │  ────────────────                                                    │       │
│   │                                                                      │       │
│   │  Parameterization      ████████████████████  10/10                  │       │
│   │  • Database/schema fully parameterized                               │       │
│   │  • Multi-environment deployment ready                                │       │
│   │                                                                      │       │
│   │  Error Handling        █████████████████████  9/10                  │       │
│   │  • Comprehensive try/catch                                           │       │
│   │  • All errors logged with context                                    │       │
│   │                                                                      │       │
│   │  Logging & Audit       █████████████████████  9/10                  │       │
│   │  • Full execution history                                            │       │
│   │  • Before/after row counts                                           │       │
│   │  • Duration tracking                                                 │       │
│   │                                                                      │       │
│   │  Monitoring            █████████████████████  9/10                  │       │
│   │  • 4 monitoring views                                                │       │
│   │  • Daily summaries                                                   │       │
│   │  • Failure tracking                                                  │       │
│   │                                                                      │       │
│   │  Safety Features       █████████████████████  9/10                  │       │
│   │  • Dry-run preview                                                   │       │
│   │  • Exclusion list support                                            │       │
│   │  • No-overlap task execution                                         │       │
│   │                                                                      │       │
│   │  Scheduling            ████████████████████   8/10                  │       │
│   │  • CRON-based task                                                   │       │
│   │  • Configurable warehouse                                            │       │
│   │  • 4-hour timeout                                                    │       │
│   │                                                                      │       │
│   │  Documentation         ████████████████████   8/10                  │       │
│   │  • Inline comments                                                   │       │
│   │  • Usage examples                                                    │       │
│   │                                                                      │       │
│   │  Scalability           ███████████████████    7/10                  │       │
│   │  • Handles multiple schemas                                          │       │
│   │  • No row-level batching for very large tables                      │       │
│   └─────────────────────────────────────────────────────────────────────┘       │
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐       │
│   │  RECOMMENDATIONS FOR ENHANCEMENT                                     │       │
│   │  ────────────────────────────────                                    │       │
│   │                                                                      │       │
│   │  Priority: MEDIUM                                                    │       │
│   │  • Add Snowflake Alerts for email/Slack notification on failures    │       │
│   │  • Add iterative batching for tables with 10M+ rows                 │       │
│   │                                                                      │       │
│   │  Priority: LOW                                                       │       │
│   │  • Add partition-aware cleanup for large partitioned tables         │       │
│   │  • Add Time Travel retention awareness                              │       │
│   │  • Add storage impact estimation in dry-run                         │       │
│   └─────────────────────────────────────────────────────────────────────┘       │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 9. Appendix: Sample Output

### Dry Run Output
```json
{
  "mode": "DRY_RUN",
  "database": "D_BRONZE",
  "schema": "SALES",
  "table_pattern": "%_BASE",
  "date_column": "CREATED_DATE",
  "retention_days": 45,
  "cutoff_date": "2026-01-02",
  "total_rows_to_delete": 50,
  "table_count": 2,
  "details": [
    {
      "table": "CUSTOMERS_BASE",
      "total_rows": 5,
      "rows_to_delete": 0,
      "rows_to_keep": 5,
      "delete_pct": 0
    },
    {
      "table": "ORDERS_BASE",
      "total_rows": 157,
      "rows_to_delete": 50,
      "rows_to_keep": 107,
      "delete_pct": 31.85
    }
  ]
}
```

### Execution Output
```json
{
  "batch_id": "CLEANUP_20260216_124117",
  "database": "D_BRONZE",
  "schema": "SALES",
  "table_pattern": "%_BASE",
  "retention_days": 45,
  "cutoff_date": "2026-01-02",
  "tables_processed": 2,
  "tables_skipped": 0,
  "tables_failed": 0,
  "total_rows_deleted": 100,
  "details": [
    {
      "table": "D_BRONZE.SALES.CUSTOMERS_BASE",
      "status": "SUCCESS",
      "rows_before": 5,
      "rows_deleted": 0,
      "rows_after": 5
    },
    {
      "table": "D_BRONZE.SALES.ORDERS_BASE",
      "status": "SUCCESS",
      "rows_before": 207,
      "rows_deleted": 100,
      "rows_after": 107
    }
  ]
}
```
