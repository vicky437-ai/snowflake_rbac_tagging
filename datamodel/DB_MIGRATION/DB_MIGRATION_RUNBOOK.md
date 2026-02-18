# Database Layer Reorganization - Migration Runbook

## Executive Summary

**Objective:** Separate IDMC raw data ingestion (D_RAW) from data preservation layer (D_BRONZE)

| Aspect | Before | After |
|--------|--------|-------|
| Raw Data | D_BRONZE.SADB.TRKFC_TRSTN_BASE | D_RAW.SADB.TRKFC_TRSTN_BASE |
| Preservation | D_BRONZE.SADB.TRKFC_TRSTN_V1 | D_BRONZE.SADB.TRKFC_TRSTN_V1 |
| IDMC Target | D_BRONZE | D_RAW |
| CDC Stream | On D_BRONZE table | On D_RAW table (stream in D_BRONZE) |

**Estimated Duration:** 45-60 minutes  
**Risk Level:** Medium  
**Rollback Time:** 15 minutes

---

## Approach: Clone + Reorganize (Recommended)

### Why This Approach?

| Option | Pros | Cons | Recommendation |
|--------|------|------|----------------|
| **A: Rename D_BRONZE to D_RAW** | Simple, grants follow | All D_BRONZE references break | Not recommended |
| **B: Create D_RAW, move tables** | D_BRONZE refs intact | Complex data movement | Moderate |
| **C: Clone + Reorganize** | Zero-copy instant, easy rollback | Needs cleanup | **RECOMMENDED** |

---

## RBAC Impact Analysis

### Grant Behavior

| Operation | Grant Behavior |
|-----------|----------------|
| **DATABASE CLONE** | Grants do NOT transfer - must re-grant |
| **DATABASE RENAME** | Grants follow automatically |
| **CROSS-DB STREAM** | Needs grants on both databases |

### Required Grants After Migration

| Role | D_RAW Access | D_BRONZE Access |
|------|--------------|-----------------|
| ACCOUNTADMIN | OWNERSHIP | OWNERSHIP |
| SYSADMIN | ALL | ALL |
| IDMC_ROLE | INSERT, UPDATE, DELETE | READ ONLY |
| DATA_ENGINEER | SELECT | ALL |
| DATA_READER | SELECT | SELECT |

---

## Migration Steps

### Phase 1: Pre-Migration (10 minutes)

```sql
-- 1.1 Document current state
SELECT TABLE_SCHEMA, TABLE_NAME, ROW_COUNT 
FROM D_BRONZE.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE';

-- 1.2 Capture existing grants
SHOW GRANTS ON DATABASE D_BRONZE;
SHOW GRANTS ON SCHEMA D_BRONZE.SADB;

-- 1.3 Suspend CDC task
ALTER TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC SUSPEND;
```

### Phase 2: Create D_RAW (5 minutes)

```sql
-- 2.1 Clone D_BRONZE to D_RAW (instant, zero-copy)
CREATE DATABASE D_RAW CLONE D_BRONZE;

-- 2.2 Verify clone
SELECT COUNT(*) FROM D_RAW.INFORMATION_SCHEMA.TABLES;
```

### Phase 3: Apply Grants to D_RAW (10 minutes)

```sql
-- 3.1 Database grants
GRANT USAGE ON DATABASE D_RAW TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE D_RAW TO ROLE DATA_ENGINEER;
GRANT USAGE ON DATABASE D_RAW TO ROLE DATA_READER;

-- 3.2 Schema grants
GRANT USAGE ON SCHEMA D_RAW.SADB TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA D_RAW.SADB TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA D_RAW.SADB TO ROLE DATA_READER;

-- 3.3 Table grants
GRANT SELECT ON ALL TABLES IN SCHEMA D_RAW.SADB TO ROLE DATA_READER;
GRANT ALL ON ALL TABLES IN SCHEMA D_RAW.SADB TO ROLE DATA_ENGINEER;

-- 3.4 Future grants (for new IDMC tables)
GRANT SELECT ON FUTURE TABLES IN SCHEMA D_RAW.SADB TO ROLE DATA_READER;
GRANT ALL ON FUTURE TABLES IN SCHEMA D_RAW.SADB TO ROLE DATA_ENGINEER;
```

### Phase 4: Reorganize Tables (10 minutes)

```sql
-- 4.1 Remove V1 tables from D_RAW (keep only BASE/raw tables)
DROP TABLE IF EXISTS D_RAW.SADB.TRKFC_TRSTN_V1;
DROP TABLE IF EXISTS D_RAW.SADB.TRKFC_TRSTN;

-- 4.2 Remove BASE tables from D_BRONZE (keep only V1 tables)
-- Option A: Drop
DROP TABLE IF EXISTS D_BRONZE.SADB.TRKFC_TRSTN_BASE;

-- Option B: Create view for backward compatibility
CREATE OR REPLACE VIEW D_BRONZE.SADB.TRKFC_TRSTN_BASE AS
SELECT * FROM D_RAW.SADB.TRKFC_TRSTN_BASE;
```

### Phase 5: Update CDC Infrastructure (10 minutes)

```sql
-- 5.1 Enable change tracking on D_RAW source
ALTER TABLE D_RAW.SADB.TRKFC_TRSTN_BASE
SET CHANGE_TRACKING = TRUE, DATA_RETENTION_TIME_IN_DAYS = 14;

-- 5.2 Recreate stream (in D_BRONZE, pointing to D_RAW)
CREATE OR REPLACE STREAM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM
ON TABLE D_RAW.SADB.TRKFC_TRSTN_BASE
SHOW_INITIAL_ROWS = TRUE;

-- 5.3 Recreate procedure (see DB_MIGRATION_UPDATED_CDC_OBJECTS.sql)
-- The procedure references:
--   Source: D_RAW.SADB.TRKFC_TRSTN_BASE (via stream)
--   Target: D_BRONZE.SADB.TRKFC_TRSTN_V1

-- 5.4 Recreate and start task
CREATE OR REPLACE TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM')
AS CALL D_BRONZE.SADB.SP_PROCESS_TRKFC_TRSTN_CDC();

ALTER TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC RESUME;
```

### Phase 6: IDMC Configuration Update

| Setting | Before | After |
|---------|--------|-------|
| Database | D_BRONZE | D_RAW |
| Schema | SADB | SADB (no change) |
| Table Names | Same | Same |

**IDMC Steps:**
1. Update Snowflake connection to use D_RAW
2. Test with small data load
3. Resume full replication

### Phase 7: Validation (10 minutes)

```sql
-- 7.1 Verify D_RAW structure
SELECT TABLE_NAME FROM D_RAW.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'SADB' AND TABLE_TYPE = 'BASE TABLE';
-- Expected: TRKFC_TRSTN_BASE (and other _BASE tables)

-- 7.2 Verify D_BRONZE structure
SELECT TABLE_NAME FROM D_BRONZE.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'SADB' AND TABLE_TYPE = 'BASE TABLE';
-- Expected: TRKFC_TRSTN_V1 (and other _V1 tables)

-- 7.3 Verify stream health
SHOW STREAMS LIKE 'TRKFC_TRSTN_BASE_HIST_STREAM' IN SCHEMA D_BRONZE.SADB;
-- Expected: stale = false

-- 7.4 Test CDC processing
CALL D_BRONZE.SADB.SP_PROCESS_TRKFC_TRSTN_CDC();

-- 7.5 Verify data consistency
SELECT 'D_RAW Source' AS LOC, COUNT(*) AS CNT FROM D_RAW.SADB.TRKFC_TRSTN_BASE
UNION ALL
SELECT 'D_BRONZE V1', COUNT(*) FROM D_BRONZE.SADB.TRKFC_TRSTN_V1 WHERE IS_DELETED = FALSE;
```

---

## Rollback Procedure

If migration fails, execute:

```sql
-- 1. Suspend task
ALTER TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC SUSPEND;

-- 2. Drop D_RAW
DROP DATABASE IF EXISTS D_RAW;

-- 3. Drop any views created
DROP VIEW IF EXISTS D_BRONZE.SADB.TRKFC_TRSTN_BASE;

-- 4. Restore BASE table using Time Travel
CREATE TABLE D_BRONZE.SADB.TRKFC_TRSTN_BASE 
CLONE D_BRONZE.SADB.TRKFC_TRSTN_BASE AT (OFFSET => -3600);

-- 5. Recreate stream on D_BRONZE
CREATE OR REPLACE STREAM D_BRONZE.SADB.TRKFC_TRSTN_BASE_HIST_STREAM
ON TABLE D_BRONZE.SADB.TRKFC_TRSTN_BASE
SHOW_INITIAL_ROWS = TRUE;

-- 6. Update IDMC back to D_BRONZE

-- 7. Resume task
ALTER TASK D_BRONZE.SADB.TASK_PROCESS_TRKFC_TRSTN_CDC RESUME;
```

---

## Post-Migration Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SNOWFLAKE ACCOUNT                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────┐      ┌─────────────────────────┐          │
│  │       D_RAW             │      │       D_BRONZE          │          │
│  │   (Raw Data Layer)      │      │  (Preservation Layer)   │          │
│  ├─────────────────────────┤      ├─────────────────────────┤          │
│  │  SADB Schema            │      │  SADB Schema            │          │
│  │  ┌───────────────────┐  │      │  ┌───────────────────┐  │          │
│  │  │ TRKFC_TRSTN_BASE  │──┼──────┼─►│ CDC STREAM        │  │          │
│  │  │ (IDMC ingestion)  │  │      │  │ (cross-DB)        │  │          │
│  │  └───────────────────┘  │      │  └─────────┬─────────┘  │          │
│  │                         │      │            │            │          │
│  │  Other _BASE tables...  │      │            ▼            │          │
│  │                         │      │  ┌───────────────────┐  │          │
│  └─────────────────────────┘      │  │ TRKFC_TRSTN_V1    │  │          │
│                                   │  │ (soft deletes)    │  │          │
│           ▲                       │  └───────────────────┘  │          │
│           │                       │                         │          │
│      ┌────┴────┐                  │  Other _V1 tables...    │          │
│      │  IDMC   │                  │                         │          │
│      │ (Cloud) │                  └─────────────────────────┘          │
│      └─────────┘                                                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Migration Files

| File | Purpose |
|------|---------|
| `DB_MIGRATION_STRATEGY.sql` | Strategy analysis |
| `DB_MIGRATION_PRODUCTION_SCRIPT.sql` | Main migration script |
| `DB_MIGRATION_UPDATED_CDC_OBJECTS.sql` | Updated CDC procedure/stream |
| `DB_MIGRATION_RBAC_GRANTS.sql` | Grant management |
| `DB_MIGRATION_RUNBOOK.md` | This runbook |

---

## Sign-Off

| Role | Name | Date |
|------|------|------|
| DBA | | |
| Data Engineer | | |
| IDMC Admin | | |
| Approver | | |
