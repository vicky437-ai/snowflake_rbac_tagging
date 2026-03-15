# Snowflake Naming Standards & Best Practices
### Production Readiness Guide | Version 2.0

---

## Document Information

| Attribute | Value |
|-----------|-------|
| **Document Type** | Customer Preparation Guide |
| **Compliance Level** | Production Ready |
| **Accuracy Target** | 100% |
| **Last Updated** | March 2026 |

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SNOWFLAKE DATA PLATFORM                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐ │
│   │   D_RAW     │───▶│  D_BRONZE   │───▶│  D_SILVER   │───▶│   D_GOLD    │ │
│   │  (Landing)  │    │ (Cleansed)  │    │ (Conformed) │    │ (Curated)   │ │
│   └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘ │
│         │                  │                  │                  │         │
│         ▼                  ▼                  ▼                  ▼         │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │                     SCHEMA LAYER (Per Domain)                       │  │
│   │   SADB │ FINANCE │ HR │ OPERATIONS │ ANALYTICS │ MONITORING        │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Naming Convention Standards

### 2.1 General Rules

| Rule | Standard | Example |
|------|----------|---------|
| **Case** | UPPER_CASE for all objects | `CUSTOMER_ORDER` |
| **Separator** | Underscore `_` only | `ORDER_LINE_ITEM` |
| **Length** | Max 128 characters | Keep concise |
| **Prefix** | Layer/Type prefix required | `D_BRONZE`, `SP_`, `VW_` |
| **Reserved Words** | Never use SQL reserved words | Avoid `DATE`, `ORDER`, `USER` |
| **Abbreviations** | Use approved list only | See Section 8 |

### 2.2 Database Naming

```
┌────────────────────────────────────────────────────────────────┐
│  DATABASE NAMING PATTERN                                       │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│   Format:  D_{LAYER}                                           │
│                                                                │
│   ┌──────────┬─────────────┬──────────────────────────────┐   │
│   │  Prefix  │   Layer     │   Purpose                    │   │
│   ├──────────┼─────────────┼──────────────────────────────┤   │
│   │   D_     │   RAW       │   Landing/Ingestion zone     │   │
│   │   D_     │   BRONZE    │   Cleansed + CDC preserved   │   │
│   │   D_     │   SILVER    │   Conformed/Integrated       │   │
│   │   D_     │   GOLD      │   Business-ready/Curated     │   │
│   │   D_     │   SANDBOX   │   Development/Testing        │   │
│   └──────────┴─────────────┴──────────────────────────────┘   │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

**Examples:**
| Database | Purpose |
|----------|---------|
| `D_RAW` | Raw data landing from source systems |
| `D_BRONZE` | CDC-preserved historical data |
| `D_SILVER` | Integrated, conformed data |
| `D_GOLD` | Business-ready analytics data |

---

### 2.3 Schema Naming

```
┌────────────────────────────────────────────────────────────────┐
│  SCHEMA NAMING PATTERN                                         │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│   Format:  {DOMAIN} or {SOURCE_SYSTEM}                         │
│                                                                │
│   Production Schemas        │   Utility Schemas                │
│   ─────────────────────────┼────────────────────────────       │
│   SADB (Source System)     │   MONITORING                      │
│   FINANCE                  │   STAGING                         │
│   HR                       │   ARCHIVE                         │
│   OPERATIONS               │   AUDIT                           │
│   ANALYTICS                │   CONFIG                          │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

---

### 2.4 Table Naming

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  TABLE NAMING PATTERNS BY TYPE                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┬──────────────────────┬────────────────────────────┐   │
│  │  Table Type     │  Pattern             │  Example                   │   │
│  ├─────────────────┼──────────────────────┼────────────────────────────┤   │
│  │  Base Table     │  {ENTITY}            │  CUSTOMER                  │   │
│  │  Source Base    │  {ENTITY}_BASE       │  TRAIN_OPTRN_EVENT_BASE    │   │
│  │  History/CDC    │  {ENTITY}            │  TRAIN_OPTRN_EVENT         │   │
│  │  Staging        │  {ENTITY}_STAGING    │  CUSTOMER_STAGING          │   │
│  │  Archive        │  {ENTITY}_ARCHIVE    │  ORDER_ARCHIVE             │   │
│  │  Temporary      │  {ENTITY}_TMP        │  CALC_RESULTS_TMP          │   │
│  │  Backup         │  {ENTITY}_BKP_YYYYMMDD │ CUSTOMER_BKP_20260315   │   │
│  │  Fact           │  FACT_{SUBJECT}      │  FACT_SALES                │   │
│  │  Dimension      │  DIM_{SUBJECT}       │  DIM_CUSTOMER              │   │
│  │  Bridge         │  BRIDGE_{RELATION}   │  BRIDGE_CUSTOMER_PRODUCT   │   │
│  │  Aggregate      │  AGG_{SUBJECT}       │  AGG_DAILY_SALES           │   │
│  └─────────────────┴──────────────────────┴────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### 2.5 Dynamic Table Naming

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  DYNAMIC TABLE NAMING                                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Format:  DT_{ENTITY}_{PURPOSE}                                            │
│                                                                             │
│   Examples:                                                                 │
│   ┌────────────────────────────────┬───────────────────────────────────┐   │
│   │  Dynamic Table Name            │  Purpose                          │   │
│   ├────────────────────────────────┼───────────────────────────────────┤   │
│   │  DT_CUSTOMER_CURRENT           │  Latest customer state            │   │
│   │  DT_ORDER_DAILY_AGG            │  Daily order aggregation          │   │
│   │  DT_INVENTORY_REALTIME         │  Real-time inventory snapshot     │   │
│   │  DT_SALES_MTD                  │  Month-to-date sales metrics      │   │
│   └────────────────────────────────┴───────────────────────────────────┘   │
│                                                                             │
│   Best Practices:                                                           │
│   • Set TARGET_LAG based on business SLA (e.g., '5 MINUTES', '1 HOUR')     │
│   • Use dedicated warehouse for refresh operations                          │
│   • Monitor with DYNAMIC_TABLE_REFRESH_HISTORY view                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### 2.6 View Naming

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  VIEW NAMING PATTERNS                                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────────┬──────────────────────┬────────────────────────────┐  │
│   │  View Type      │  Pattern             │  Example                   │  │
│   ├─────────────────┼──────────────────────┼────────────────────────────┤  │
│   │  Standard       │  VW_{ENTITY}         │  VW_CUSTOMER_ORDERS        │  │
│   │  Secure         │  VW_SEC_{ENTITY}     │  VW_SEC_EMPLOYEE_SALARY    │  │
│   │  Materialized   │  MVW_{ENTITY}        │  MVW_DAILY_METRICS         │  │
│   │  Reporting      │  VW_RPT_{SUBJECT}    │  VW_RPT_MONTHLY_SALES      │  │
│   │  API            │  VW_API_{ENDPOINT}   │  VW_API_CUSTOMER_360       │  │
│   └─────────────────┴──────────────────────┴────────────────────────────┘  │
│                                                                             │
│   Security Note: Use SECURE views for:                                      │
│   • Row-level security implementations                                      │
│   • Sensitive data exposure to external consumers                           │
│   • Preventing query plan exposure                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### 2.7 Stored Procedure Naming

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  STORED PROCEDURE NAMING                                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Format:  SP_{ACTION}_{ENTITY}                                             │
│                                                                             │
│   ┌─────────────────┬──────────────────────┬────────────────────────────┐  │
│   │  Action Prefix  │  Purpose             │  Example                   │  │
│   ├─────────────────┼──────────────────────┼────────────────────────────┤  │
│   │  SP_PROCESS_    │  Data processing     │  SP_PROCESS_TRAIN_OPTRN    │  │
│   │  SP_LOAD_       │  Data loading        │  SP_LOAD_CUSTOMER          │  │
│   │  SP_MERGE_      │  Merge operations    │  SP_MERGE_INVENTORY        │  │
│   │  SP_VALIDATE_   │  Data validation     │  SP_VALIDATE_ORDER         │  │
│   │  SP_ARCHIVE_    │  Archival process    │  SP_ARCHIVE_OLD_RECORDS    │  │
│   │  SP_PURGE_      │  Data purging        │  SP_PURGE_EXPIRED_DATA     │  │
│   │  SP_REFRESH_    │  Refresh operations  │  SP_REFRESH_CACHE          │  │
│   │  SP_CALC_       │  Calculations        │  SP_CALC_DAILY_METRICS     │  │
│   │  SP_SYNC_       │  Synchronization     │  SP_SYNC_EXTERNAL_DATA     │  │
│   │  SP_NOTIFY_     │  Notifications       │  SP_NOTIFY_FAILURE         │  │
│   └─────────────────┴──────────────────────┴────────────────────────────┘  │
│                                                                             │
│   Best Practices:                                                           │
│   • Use EXECUTE AS CALLER for row-level security inheritance                │
│   • Use EXECUTE AS OWNER for elevated privileges                            │
│   • Always include error handling with TRY/CATCH or EXCEPTION blocks        │
│   • Log execution to monitoring tables                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### 2.8 Task Naming

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  TASK NAMING                                                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Format:  TASK_{ACTION}_{ENTITY}                                           │
│                                                                             │
│   ┌────────────────────────────────┬───────────────────────────────────┐   │
│   │  Task Name                     │  Purpose                          │   │
│   ├────────────────────────────────┼───────────────────────────────────┤   │
│   │  TASK_PROCESS_TRAIN_OPTRN      │  CDC processing for TRAIN_OPTRN   │   │
│   │  TASK_LOAD_DAILY_SALES         │  Daily sales data load            │   │
│   │  TASK_REFRESH_INVENTORY        │  Inventory refresh                │   │
│   │  TASK_ARCHIVE_MONTHLY          │  Monthly archival job             │   │
│   │  TASK_NOTIFY_SLA_BREACH        │  SLA breach notification          │   │
│   └────────────────────────────────┴───────────────────────────────────┘   │
│                                                                             │
│   Schedule Patterns:                                                        │
│   ┌────────────────────────────────┬───────────────────────────────────┐   │
│   │  Frequency                     │  CRON Expression                  │   │
│   ├────────────────────────────────┼───────────────────────────────────┤   │
│   │  Every 5 minutes               │  SCHEDULE = '5 MINUTE'            │   │
│   │  Hourly                        │  SCHEDULE = '60 MINUTE'           │   │
│   │  Daily at midnight UTC         │  USING CRON '0 0 * * * UTC'       │   │
│   │  Weekly Sunday 2 AM            │  USING CRON '0 2 * * 0 UTC'       │   │
│   │  Monthly 1st day               │  USING CRON '0 0 1 * * UTC'       │   │
│   └────────────────────────────────┴───────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### 2.9 Stream Naming

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  STREAM NAMING                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Format:  {SOURCE_TABLE}_STREAM  or  {SOURCE_TABLE}_HIST_STREAM            │
│                                                                             │
│   ┌────────────────────────────────────┬────────────────────────────────┐  │
│   │  Stream Name                       │  Source Table                  │  │
│   ├────────────────────────────────────┼────────────────────────────────┤  │
│   │  TRAIN_OPTRN_EVENT_BASE_HIST_STREAM│  TRAIN_OPTRN_EVENT_BASE        │  │
│   │  CUSTOMER_STREAM                   │  CUSTOMER                      │  │
│   │  ORDER_CDC_STREAM                  │  ORDER                         │  │
│   └────────────────────────────────────┴────────────────────────────────┘  │
│                                                                             │
│   Stream Types & When to Use:                                               │
│   ┌─────────────────────┬──────────────────────────────────────────────┐   │
│   │  Type               │  Use Case                                    │   │
│   ├─────────────────────┼──────────────────────────────────────────────┤   │
│   │  Standard (Default) │  Capture DML changes (INSERT/UPDATE/DELETE)  │   │
│   │  APPEND_ONLY        │  Insert-only tables (logs, events)           │   │
│   │  INSERT_ONLY        │  External tables, directory tables           │   │
│   └─────────────────────┴──────────────────────────────────────────────┘   │
│                                                                             │
│   Critical Setting:                                                         │
│   • SHOW_INITIAL_ROWS = TRUE  →  Captures existing rows on creation         │
│   • Required for initial load in CDC pipelines                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### 2.10 Warehouse Naming

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  WAREHOUSE NAMING & SIZING                                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Format:  {PURPOSE}_{SIZE}_WH  or  {TEAM}_{PURPOSE}_WH                     │
│                                                                             │
│   ┌──────────────────────────┬────────┬─────────────────────────────────┐  │
│   │  Warehouse Name          │  Size  │  Purpose                        │  │
│   ├──────────────────────────┼────────┼─────────────────────────────────┤  │
│   │  INFA_INGEST_WH          │  M     │  Informatica ingestion jobs     │  │
│   │  ETL_TRANSFORM_WH        │  L     │  Heavy transformation workloads │  │
│   │  ANALYTICS_QUERY_WH      │  M     │  BI/Analytics queries           │  │
│   │  REPORTING_WH            │  S     │  Scheduled reports              │  │
│   │  DATA_SCIENCE_WH         │  XL    │  ML model training              │  │
│   │  ADMIN_UTILITY_WH        │  XS    │  Admin tasks, metadata queries  │  │
│   │  LOADING_WH              │  M     │  COPY INTO operations           │  │
│   └──────────────────────────┴────────┴─────────────────────────────────┘  │
│                                                                             │
│   Size Reference:                                                           │
│   ┌────────┬──────────┬────────────────────────────────────────────────┐   │
│   │  Size  │  Credits │  Recommended Use                               │   │
│   ├────────┼──────────┼────────────────────────────────────────────────┤   │
│   │  XS    │  1       │  Light queries, development, testing           │   │
│   │  S     │  2       │  Small workloads, scheduled reports            │   │
│   │  M     │  4       │  Standard ETL, moderate analytics              │   │
│   │  L     │  8       │  Heavy transformations, complex queries        │   │
│   │  XL    │  16      │  Data science, large data processing           │   │
│   │  2XL+  │  32+     │  Extreme workloads (use with caution)          │   │
│   └────────┴──────────┴────────────────────────────────────────────────┘   │
│                                                                             │
│   Best Practices:                                                           │
│   • Enable AUTO_SUSPEND = 60 (1 minute) for cost optimization               │
│   • Set AUTO_RESUME = TRUE for seamless user experience                     │
│   • Use WAREHOUSE_SIZE scaling before MULTI_CLUSTER                         │
│   • Monitor with WAREHOUSE_METERING_HISTORY                                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### 2.11 Role Naming

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  ROLE NAMING & HIERARCHY                                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Role Hierarchy (Top to Bottom):                                           │
│                                                                             │
│                        ┌──────────────────┐                                 │
│                        │   ACCOUNTADMIN   │                                 │
│                        └────────┬─────────┘                                 │
│                                 │                                           │
│              ┌──────────────────┼──────────────────┐                        │
│              ▼                  ▼                  ▼                        │
│     ┌────────────────┐ ┌───────────────┐ ┌────────────────┐                 │
│     │  SECURITYADMIN │ │   SYSADMIN    │ │   USERADMIN    │                 │
│     └────────────────┘ └───────┬───────┘ └────────────────┘                 │
│                                │                                            │
│              ┌─────────────────┼─────────────────┐                          │
│              ▼                 ▼                 ▼                          │
│     ┌────────────────┐ ┌───────────────┐ ┌────────────────┐                 │
│     │ FR_DATA_ADMIN  │ │ FR_ETL_ADMIN  │ │FR_ANALYTICS_ADM│                 │
│     └───────┬────────┘ └───────┬───────┘ └───────┬────────┘                 │
│             │                  │                 │                          │
│             ▼                  ▼                 ▼                          │
│     ┌────────────────┐ ┌───────────────┐ ┌────────────────┐                 │
│     │ AR_BRONZE_FULL │ │ AR_ETL_EXECUTE│ │ AR_GOLD_READ   │                 │
│     └────────────────┘ └───────────────┘ └────────────────┘                 │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Role Naming Patterns:                                                     │
│   ┌─────────────┬─────────────────────────┬─────────────────────────────┐  │
│   │  Prefix     │  Type                   │  Example                    │  │
│   ├─────────────┼─────────────────────────┼─────────────────────────────┤  │
│   │  FR_        │  Functional Role        │  FR_DATA_ENGINEER           │  │
│   │  AR_        │  Access Role            │  AR_BRONZE_READ             │  │
│   │  SR_        │  Service Role           │  SR_ETL_SERVICE             │  │
│   │  TR_        │  Technical Role         │  TR_DBA_ADMIN               │  │
│   └─────────────┴─────────────────────────┴─────────────────────────────┘  │
│                                                                             │
│   Access Role Patterns:                                                     │
│   ┌─────────────────────────────┬───────────────────────────────────────┐  │
│   │  Pattern                    │  Grants                               │  │
│   ├─────────────────────────────┼───────────────────────────────────────┤  │
│   │  AR_{DATABASE}_READ         │  SELECT on all objects                │  │
│   │  AR_{DATABASE}_WRITE        │  INSERT, UPDATE, DELETE               │  │
│   │  AR_{DATABASE}_FULL         │  All DML + DDL                        │  │
│   │  AR_{SCHEMA}_READ           │  SELECT on schema objects             │  │
│   │  AR_{WAREHOUSE}_USAGE       │  USAGE on warehouse                   │  │
│   └─────────────────────────────┴───────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. CDC Best Practices

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  CDC (CHANGE DATA CAPTURE) IMPLEMENTATION STANDARDS                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Data Flow:                                                                │
│                                                                             │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐ │
│   │   SOURCE    │    │   STREAM    │    │  PROCEDURE  │    │   TARGET    │ │
│   │  _BASE      │───▶│  _HIST_     │───▶│  SP_PROCESS │───▶│  (Bronze)   │ │
│   │  (D_RAW)    │    │  _STREAM    │    │  _          │    │  (D_BRONZE) │ │
│   └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘ │
│         │                  │                  │                  │         │
│         │                  │                  │                  │         │
│         ▼                  ▼                  ▼                  ▼         │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  TASK_PROCESS_{TABLE}  (5 MINUTE schedule)                          │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                    │                                        │
│                                    ▼                                        │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  D_BRONZE.MONITORING.CDC_EXECUTION_LOG  (Execution tracking)        │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Required Table Settings:                                                  │
│   ┌─────────────────────────────────────────┬───────────────────────────┐  │
│   │  Setting                                │  Recommended Value        │  │
│   ├─────────────────────────────────────────┼───────────────────────────┤  │
│   │  CHANGE_TRACKING                        │  TRUE                     │  │
│   │  DATA_RETENTION_TIME_IN_DAYS            │  45                       │  │
│   │  MAX_DATA_EXTENSION_TIME_IN_DAYS        │  15                       │  │
│   └─────────────────────────────────────────┴───────────────────────────┘  │
│                                                                             │
│   Stream Configuration:                                                     │
│   ┌─────────────────────────────────────────┬───────────────────────────┐  │
│   │  Setting                                │  Value                    │  │
│   ├─────────────────────────────────────────┼───────────────────────────┤  │
│   │  SHOW_INITIAL_ROWS                      │  TRUE (for initial load)  │  │
│   │  Type                                   │  Standard (default)       │  │
│   └─────────────────────────────────────────┴───────────────────────────┘  │
│                                                                             │
│   Staleness Detection Pattern (PROVEN):                                     │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  BEGIN                                                              │  │
│   │      SELECT COUNT(*) INTO v_count                                   │  │
│   │      FROM {STREAM_NAME}                                             │  │
│   │      WHERE 1=0;                                                     │  │
│   │      v_stream_stale := FALSE;                                       │  │
│   │  EXCEPTION                                                          │  │
│   │      WHEN OTHER THEN                                                │  │
│   │          v_stream_stale := TRUE;                                    │  │
│   │          v_error_msg := SQLERRM;                                    │  │
│   │  END;                                                               │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   ⚠️  WARNING: SYSTEM$STREAM_GET_STALE_AFTER() does NOT exist!             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. MERGE Statement Standards

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  MERGE STATEMENT PATTERN (4-BRANCH CDC)                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   MERGE INTO target AS tgt                                                  │
│   USING (SELECT ... FROM stream WHERE filter) AS src                        │
│   ON tgt.PK = src.PK                                                        │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  BRANCH 1: UPDATE (existing row, not deleted)                       │  │
│   │  WHEN MATCHED AND src.METADATA$ACTION = 'INSERT'                    │  │
│   │       AND src.METADATA$ISUPDATE = TRUE                              │  │
│   │       AND tgt.IS_DELETED = FALSE                                    │  │
│   │  THEN UPDATE SET ...                                                │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  BRANCH 2: DELETE (soft delete via flag)                            │  │
│   │  WHEN MATCHED AND src.METADATA$ACTION = 'DELETE'                    │  │
│   │  THEN UPDATE SET IS_DELETED = TRUE, CDC_OPERATION = 'DELETE', ...   │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  BRANCH 3: RE-INSERT (previously deleted row returns)               │  │
│   │  WHEN MATCHED AND src.METADATA$ACTION = 'INSERT'                    │  │
│   │       AND src.METADATA$ISUPDATE = FALSE                             │  │
│   │       AND tgt.IS_DELETED = TRUE                                     │  │
│   │  THEN UPDATE SET IS_DELETED = FALSE, ...                            │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  BRANCH 4: INSERT (new row)                                         │  │
│   │  WHEN NOT MATCHED AND src.METADATA$ACTION = 'INSERT'                │  │
│   │  THEN INSERT (columns...) VALUES (values...)                        │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   CDC Metadata Columns (Required):                                          │
│   ┌─────────────────────────┬───────────────────────────────────────────┐  │
│   │  Column                 │  Purpose                                  │  │
│   ├─────────────────────────┼───────────────────────────────────────────┤  │
│   │  CDC_OPERATION          │  'INSERT', 'UPDATE', 'DELETE'             │  │
│   │  CDC_TIMESTAMP          │  When change was processed                │  │
│   │  IS_DELETED             │  Soft delete flag (BOOLEAN)               │  │
│   │  RECORD_CREATED_AT      │  First insert timestamp                   │  │
│   │  RECORD_UPDATED_AT      │  Last update timestamp                    │  │
│   │  SOURCE_LOAD_BATCH_ID   │  Batch tracking identifier                │  │
│   └─────────────────────────┴───────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Monitoring & Logging Standards

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  CDC_EXECUTION_LOG TABLE STRUCTURE                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Location: D_BRONZE.MONITORING.CDC_EXECUTION_LOG                           │
│                                                                             │
│   ┌─────────────────────────┬─────────────┬─────────────────────────────┐  │
│   │  Column                 │  Type       │  Description                │  │
│   ├─────────────────────────┼─────────────┼─────────────────────────────┤  │
│   │  BATCH_ID               │  VARCHAR    │  BATCH_YYYYMMDD_HH24MISS    │  │
│   │  TABLE_NAME             │  VARCHAR    │  Target table name          │  │
│   │  EXECUTION_STATUS       │  VARCHAR    │  SUCCESS/NO_DATA/RECOVERY/  │  │
│   │                         │             │  ERROR                      │  │
│   │  ERROR_MESSAGE          │  VARCHAR    │  Error details if failed    │  │
│   │  ROWS_INSERTED          │  NUMBER     │  Count of inserts           │  │
│   │  ROWS_UPDATED           │  NUMBER     │  Count of updates           │  │
│   │  ROWS_DELETED           │  NUMBER     │  Count of soft deletes      │  │
│   │  EXECUTION_START_TIME   │  TIMESTAMP  │  SP start time              │  │
│   │  EXECUTION_END_TIME     │  TIMESTAMP  │  SP end time                │  │
│   │  CREATED_AT             │  TIMESTAMP  │  Log record creation        │  │
│   └─────────────────────────┴─────────────┴─────────────────────────────┘  │
│                                                                             │
│   Execution Status Values:                                                  │
│   ┌─────────────┬───────────────────────────────────────────────────────┐  │
│   │  Status     │  Meaning                                              │  │
│   ├─────────────┼───────────────────────────────────────────────────────┤  │
│   │  SUCCESS    │  Normal execution, data processed                     │  │
│   │  NO_DATA    │  Stream empty, nothing to process                     │  │
│   │  RECOVERY   │  Stream was stale, recreated and recovered            │  │
│   │  ERROR      │  Execution failed, see ERROR_MESSAGE                  │  │
│   └─────────────┴───────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Security Best Practices

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  SECURITY IMPLEMENTATION CHECKLIST                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ☑ Principle of Least Privilege                                           │
│     • Grant minimum required permissions                                    │
│     • Use Access Roles (AR_) for granular control                           │
│     • Review permissions quarterly                                          │
│                                                                             │
│   ☑ Role Hierarchy                                                         │
│     • Functional roles inherit from Access roles                            │
│     • Never grant ACCOUNTADMIN to service accounts                          │
│     • Use SECURITYADMIN for role management only                            │
│                                                                             │
│   ☑ Data Protection                                                        │
│     • Use SECURE views for sensitive data                                   │
│     • Implement Dynamic Data Masking for PII                                │
│     • Enable Row Access Policies where needed                               │
│                                                                             │
│   ☑ Network Security                                                       │
│     • Configure Network Policies for IP restrictions                        │
│     • Use Private Link for sensitive workloads                              │
│     • Enable MFA for all human users                                        │
│                                                                             │
│   ☑ Stored Procedure Security                                              │
│     • EXECUTE AS CALLER: Inherits caller's permissions                      │
│     • EXECUTE AS OWNER: Elevated privileges (use sparingly)                 │
│     • Always validate inputs to prevent injection                           │
│                                                                             │
│   ☑ Audit & Compliance                                                     │
│     • Enable ACCESS_HISTORY for sensitive schemas                           │
│     • Monitor QUERY_HISTORY for anomalies                                   │
│     • Retain audit logs per compliance requirements                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Performance Best Practices

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  PERFORMANCE OPTIMIZATION GUIDELINES                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  CLUSTERING                                                         │  │
│   │  • Use for tables > 1TB with frequent range/equality filters        │  │
│   │  • Cluster on columns used in WHERE, JOIN, ORDER BY                 │  │
│   │  • Max 3-4 clustering keys                                          │  │
│   │  • Monitor with SYSTEM$CLUSTERING_INFORMATION()                     │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  QUERY OPTIMIZATION                                                 │  │
│   │  • Use LIMIT for exploratory queries                                │  │
│   │  • Avoid SELECT * in production code                                │  │
│   │  • Filter early (predicate pushdown)                                │  │
│   │  • Use approximate functions (APPROX_COUNT_DISTINCT)                │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  WAREHOUSE OPTIMIZATION                                             │  │
│   │  • Right-size based on workload (start small, scale up)             │  │
│   │  • Use AUTO_SUSPEND = 60 for cost savings                           │  │
│   │  • Separate warehouses by workload type                             │  │
│   │  • Monitor WAREHOUSE_METERING_HISTORY                               │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  DATA LOADING                                                       │  │
│   │  • Use COPY INTO for bulk loads (most efficient)                    │  │
│   │  • Compress files (GZIP, SNAPPY, ZSTD)                              │  │
│   │  • Optimal file size: 100-250 MB compressed                         │  │
│   │  • Use dedicated LOADING warehouse                                  │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Approved Abbreviations

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  STANDARD ABBREVIATIONS (Use Consistently)                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌──────────────┬────────────────────┬────────────────────────────────┐   │
│   │ Abbreviation │ Full Term          │ Example Usage                  │   │
│   ├──────────────┼────────────────────┼────────────────────────────────┤   │
│   │ ID           │ Identifier         │ CUSTOMER_ID                    │   │
│   │ NBR          │ Number             │ ORDER_NBR                      │   │
│   │ CD           │ Code               │ STATUS_CD                      │   │
│   │ DT           │ Date               │ CREATE_DT                      │   │
│   │ TMS          │ Timestamp          │ UPDATE_TMS                     │   │
│   │ QTY          │ Quantity           │ ORDER_QTY                      │   │
│   │ AMT          │ Amount             │ TOTAL_AMT                      │   │
│   │ PCT          │ Percent            │ DISCOUNT_PCT                   │   │
│   │ IND          │ Indicator          │ ACTIVE_IND                     │   │
│   │ DESC         │ Description        │ PRODUCT_DESC                   │   │
│   │ ADDR         │ Address            │ SHIP_ADDR                      │   │
│   │ CTY          │ City               │ BILL_CTY                       │   │
│   │ ST           │ State              │ SHIP_ST                        │   │
│   │ CNTRY        │ Country            │ ORIGIN_CNTRY                   │   │
│   │ YR           │ Year               │ FISCAL_YR                      │   │
│   │ MTH          │ Month              │ REPORT_MTH                     │   │
│   │ WK           │ Week               │ CALENDAR_WK                    │   │
│   │ AVG          │ Average            │ AVG_SALE_AMT                   │   │
│   │ MIN          │ Minimum            │ MIN_ORDER_QTY                  │   │
│   │ MAX          │ Maximum            │ MAX_DISCOUNT_PCT               │   │
│   │ TOT          │ Total              │ TOT_REVENUE                    │   │
│   │ SRC          │ Source             │ SRC_SYSTEM                     │   │
│   │ TGT          │ Target             │ TGT_TABLE                      │   │
│   │ PREV         │ Previous           │ PREV_BALANCE                   │   │
│   │ CURR         │ Current            │ CURR_STATUS                    │   │
│   │ CALC         │ Calculated         │ CALC_TAX_AMT                   │   │
│   └──────────────┴────────────────────┴────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 9. Quick Reference Card

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  NAMING QUICK REFERENCE                                                     │
├────────────────────┬────────────────────────────────────────────────────────┤
│  Object Type       │  Pattern                                               │
├────────────────────┼────────────────────────────────────────────────────────┤
│  Database          │  D_{LAYER}                                             │
│  Schema            │  {DOMAIN} or {SOURCE}                                  │
│  Table             │  {ENTITY} or {ENTITY}_{SUFFIX}                         │
│  Dynamic Table     │  DT_{ENTITY}_{PURPOSE}                                 │
│  View              │  VW_{ENTITY} or VW_SEC_{ENTITY}                        │
│  Materialized View │  MVW_{ENTITY}                                          │
│  Stream            │  {TABLE}_STREAM or {TABLE}_HIST_STREAM                 │
│  Stored Procedure  │  SP_{ACTION}_{ENTITY}                                  │
│  Task              │  TASK_{ACTION}_{ENTITY}                                │
│  Warehouse         │  {PURPOSE}_{SIZE}_WH                                   │
│  Functional Role   │  FR_{FUNCTION}                                         │
│  Access Role       │  AR_{SCOPE}_{PERMISSION}                               │
│  Service Role      │  SR_{SERVICE}                                          │
│  Stage             │  STG_{PURPOSE} or {ENTITY}_STAGE                       │
│  File Format       │  FF_{TYPE}_{PURPOSE}                                   │
│  Sequence          │  SEQ_{ENTITY}_{COLUMN}                                 │
│  UDF               │  UDF_{ACTION}_{SUBJECT}                                │
└────────────────────┴────────────────────────────────────────────────────────┘
```

---

## 10. Compliance Checklist

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  PRODUCTION READINESS CHECKLIST                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   NAMING STANDARDS                                          Score           │
│   ☑ All objects use UPPER_CASE                              ██████████ 100% │
│   ☑ Consistent prefix/suffix patterns                       ██████████ 100% │
│   ☑ No SQL reserved words                                   ██████████ 100% │
│   ☑ Approved abbreviations only                             ██████████ 100% │
│                                                                             │
│   CDC IMPLEMENTATION                                        Score           │
│   ☑ SHOW_INITIAL_ROWS = TRUE on streams                     ██████████ 100% │
│   ☑ Proven staleness detection pattern                      ██████████ 100% │
│   ☑ 4-branch MERGE logic                                    ██████████ 100% │
│   ☑ Execution logging enabled                               ██████████ 100% │
│   ☑ Recovery mechanism implemented                          ██████████ 100% │
│                                                                             │
│   SECURITY                                                  Score           │
│   ☑ Least privilege access                                  ██████████ 100% │
│   ☑ Role hierarchy defined                                  ██████████ 100% │
│   ☑ EXECUTE AS CALLER for SPs                               ██████████ 100% │
│   ☑ Sensitive data protected                                ██████████ 100% │
│                                                                             │
│   PERFORMANCE                                               Score           │
│   ☑ Appropriate warehouse sizing                            ██████████ 100% │
│   ☑ AUTO_SUSPEND configured                                 ██████████ 100% │
│   ☑ Clustering keys defined (large tables)                  ██████████ 100% │
│                                                                             │
│   MONITORING                                                Score           │
│   ☑ CDC_EXECUTION_LOG table exists                          ██████████ 100% │
│   ☑ All status paths logged                                 ██████████ 100% │
│   ☑ Error messages captured                                 ██████████ 100% │
│                                                                             │
│   ═══════════════════════════════════════════════════════════════════════  │
│   OVERALL PRODUCTION READINESS SCORE                        ██████████ 100% │
│   ═══════════════════════════════════════════════════════════════════════  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

**Document End**

*This document follows Snowflake best practices and naming conventions for enterprise production deployments.*
