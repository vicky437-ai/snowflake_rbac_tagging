# Database Migration: RENAME vs CLONE Approach

## Executive Summary

This document compares two approaches for reorganizing Snowflake databases to separate raw data ingestion (D_RAW) from data preservation (D_BRONZE).

---

## Detailed Comparison

| Aspect | DB RENAME | DB CLONE |
|--------|-----------|----------|
| **Operation** | `ALTER DATABASE D_BRONZE RENAME TO D_RAW` | `CREATE DATABASE D_RAW CLONE D_BRONZE` |
| **Speed** | Instant (metadata change) | Instant (zero-copy) |
| **Data Movement** | None | None |
| **Storage Cost** | None | None (until data diverges) |

---

## Pros & Cons

### DB RENAME

| PROS | CONS |
|------|------|
| ✅ Simpler - single command | ❌ All D_BRONZE references break |
| ✅ Grants follow automatically | ❌ Must create new D_BRONZE after |
| ✅ No re-granting needed | ❌ IDMC connections break temporarily |
| ✅ Streams/Tasks stay valid | |
| ✅ Best for early-stage environments | |

### DB CLONE

| PROS | CONS |
|------|------|
| ✅ Original DB remains intact | ❌ Grants do NOT transfer |
| ✅ Easy rollback (just DROP clone) | ❌ Need to re-grant all permissions |
| ✅ Can run both in parallel | ❌ Need cleanup (drop duplicates) |
| ✅ Good for production with existing data | ❌ More complex for simple scenarios |

---

## Decision Matrix

| Scenario | Recommended Approach |
|----------|---------------------|
| Early development, minimal objects | **RENAME** ⭐ |
| No V1/preservation tables yet | **RENAME** ⭐ |
| Production with existing data | **CLONE** |
| Need zero-downtime migration | **CLONE** |
| Complex RBAC with many roles | **CLONE** |
| Simple RBAC (few roles) | **RENAME** ⭐ |

---

## Recommendation for Early Development

Since you're in **early development** with:

- ✅ Handful of IDMC objects only
- ✅ No V1 tables created yet
- ✅ Minimal dependencies

**→ DB RENAME is the RIGHT choice** - simpler and grants follow automatically.

---

## RENAME Approach - Execution Steps

### Step-by-Step Commands

| Step | Command | Result |
|:----:|---------|--------|
| 1 | `ALTER DATABASE D_BRONZE RENAME TO D_RAW` | ✅ Instant rename |
| 2 | `SHOW GRANTS ON DATABASE D_RAW` | ✅ Grants followed automatically |
| 3 | `CREATE DATABASE D_BRONZE` | ✅ New preservation layer |
| 4 | `CREATE SCHEMA D_BRONZE.SADB` | ✅ Schema created |
| 5 | Apply grants to new D_BRONZE | ✅ Manual step required |
| 6 | Update IDMC connection to D_RAW | ✅ Configuration change |

### Key Observations

| Behavior | Verified |
|----------|:--------:|
| Grants follow rename | ✅ |
| Objects stay intact | ✅ |
| Instant operation | ✅ |
| New DB needs grants | ✅ |

---

## Summary Comparison

| Criteria | RENAME | CLONE |
|----------|:------:|:-----:|
| **Complexity** | ⭐ Simple | ⭐⭐⭐ Complex |
| **Grants** | ✅ Auto-follow | ❌ Need re-grant |
| **Rollback** | Rename back | DROP clone |
| **Best for** | Early development | Production with data |
| **IDMC Impact** | Update connection | Update connection |
| **Downtime** | ~1 minute | ~5 minutes |

---

## Architecture After Migration

```
BEFORE:
┌─────────────────────────────────────┐
│           D_BRONZE                  │
│  (Raw + Preservation - Mixed)       │
├─────────────────────────────────────┤
│  └── SADB                           │
│      ├── TRKFC_TRSTN_BASE (IDMC)    │
│      └── (future V1 tables)         │
└─────────────────────────────────────┘

AFTER:
┌─────────────────────────────────────┐    ┌─────────────────────────────────────┐
│           D_RAW                     │    │           D_BRONZE                  │
│  (Raw Data - IDMC Ingestion)        │    │  (Data Preservation Layer)          │
├─────────────────────────────────────┤    ├─────────────────────────────────────┤
│  └── SADB                           │    │  └── SADB                           │
│      └── TRKFC_TRSTN_BASE ──────────┼───►│      ├── CDC Stream                 │
│          (source table)             │    │      └── TRKFC_TRSTN_V1             │
│                                     │    │          (soft deletes)             │
└─────────────────────────────────────┘    └─────────────────────────────────────┘
        ▲                                          
        │                                          
   ┌────┴────┐                                     
   │  IDMC   │                                     
   │ (Cloud) │                                     
   └─────────┘                                     
```

---

## RBAC Impact

### RENAME Approach

| Database | Grant Behavior |
|----------|----------------|
| D_RAW (renamed from D_BRONZE) | ✅ All existing grants transfer automatically |
| D_BRONZE (newly created) | ❌ Needs fresh grants applied |

### CLONE Approach

| Database | Grant Behavior |
|----------|----------------|
| D_BRONZE (original) | ✅ Keeps all existing grants |
| D_RAW (cloned) | ❌ No grants transfer - needs full re-grant |

---

## Quick Reference

### RENAME Commands

```sql
-- Step 1: Rename existing database
ALTER DATABASE D_BRONZE RENAME TO D_RAW;

-- Step 2: Create new preservation database
CREATE DATABASE D_BRONZE;
CREATE SCHEMA D_BRONZE.SADB;

-- Step 3: Apply grants to new database
GRANT USAGE ON DATABASE D_BRONZE TO ROLE <role_name>;
GRANT USAGE ON SCHEMA D_BRONZE.SADB TO ROLE <role_name>;

-- Step 4: Update IDMC to point to D_RAW
```

### CLONE Commands

```sql
-- Step 1: Clone database
CREATE DATABASE D_RAW CLONE D_BRONZE;

-- Step 2: Apply all grants to clone
GRANT USAGE ON DATABASE D_RAW TO ROLE <role_name>;
-- ... (repeat for all roles)

-- Step 3: Reorganize tables
DROP TABLE D_RAW.SADB.TRKFC_TRSTN_V1;  -- Remove V1 from raw
DROP TABLE D_BRONZE.SADB.TRKFC_TRSTN_BASE;  -- Remove BASE from bronze

-- Step 4: Update IDMC to point to D_RAW
```

---

## Rollback Procedures

### RENAME Rollback

```sql
-- If migration fails:
DROP DATABASE IF EXISTS D_BRONZE;  -- Drop new (empty) database
ALTER DATABASE D_RAW RENAME TO D_BRONZE;  -- Rename back
-- Update IDMC back to D_BRONZE
```

### CLONE Rollback

```sql
-- If migration fails:
DROP DATABASE IF EXISTS D_RAW;  -- Drop the clone
-- D_BRONZE remains unchanged
-- No IDMC changes needed
```

---

## Files Reference

| File | Purpose |
|------|---------|
| `DB_MIGRATION_RENAME_APPROACH.sql` | RENAME migration script |
| `DB_MIGRATION_PRODUCTION_SCRIPT.sql` | CLONE migration script |
| `DB_MIGRATION_RBAC_GRANTS.sql` | Grant management templates |
| `DB_MIGRATION_RUNBOOK.md` | Complete deployment runbook |

---

## Conclusion

| Your Scenario | Recommendation |
|---------------|----------------|
| Early development | **Use RENAME** |
| Minimal objects | **Use RENAME** |
| No V1 tables yet | **Use RENAME** |

**The RENAME approach is simpler, faster, and grants follow automatically - perfect for your early-stage environment.**
