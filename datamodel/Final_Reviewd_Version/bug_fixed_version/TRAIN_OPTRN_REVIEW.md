# Code Review: TRAIN_OPTRN CDC Data Preservation Script
**Review Date:** March 10, 2026  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.TRAIN_OPTRN_BASE` (18 columns) |
| Target Table | `D_BRONZE.SADB.TRAIN_OPTRN` (18 source + 6 CDC = 24 columns) |
| Stream | `D_RAW.SADB.TRAIN_OPTRN_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_TRAIN_OPTRN()` |
| Task | `D_RAW.SADB.TASK_PROCESS_TRAIN_OPTRN` (5 min, no WHEN clause) |
| Primary Key | `OPTRN_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Original OPTRN.sql

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | Source table | `OPTRN_BASE` | `TRAIN_OPTRN_BASE` | DONE |
| 2 | Target table | `OPTRN` | `TRAIN_OPTRN` | DONE |
| 3 | Stream | `OPTRN_BASE_HIST_STREAM` | `TRAIN_OPTRN_BASE_HIST_STREAM` | DONE |
| 4 | Procedure | `SP_PROCESS_OPTRN(ENV)` | `SP_PROCESS_TRAIN_OPTRN()` | DONE |
| 5 | Task | `TASK_PROCESS_OPTRN` | `TASK_PROCESS_TRAIN_OPTRN` | DONE |
| 6 | Temp table | `_CDC_STAGING_OPTRN` | `_CDC_STAGING_TRAIN_OPTRN` | DONE |
| 7 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 8 | New column | N/A | `SNW_OPERATION_OWNER VARCHAR(256)` at end | DONE |
| 9 | Filter | `<> 'TSDPRG'` | `NOT IN ('TSDPRG', 'EMEPRG')` | DONE |
| 10 | Recovery | Base table LEFT JOIN | Stream read (consistent) | DONE |
| 11 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional) | DONE |
| 12 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 13 | SP style | ENV param + EXECUTE IMMEDIATE | Static SQL, no params (consistent pattern) | DONE |

---

## 3. Column Mapping (Source → Target) — 18/18 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | OPTRN_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | TRAIN_TYPE_CD | VARCHAR(16) | YES |
| 3 | TRAIN_KIND_CD | VARCHAR(16) | YES |
| 4 | MTP_OPTRN_PRFL_NM | VARCHAR(48) | YES |
| 5 | SCHDLD_TRAIN_TYPE_CD | VARCHAR(4) | YES |
| 6 | OPTRN_NM | VARCHAR(32) | YES |
| 7 | TRAIN_PRTY_NBR | NUMBER(1,0) | YES |
| 8 | TRAIN_RATING_CD | VARCHAR(4) | YES |
| 9 | VRNC_IND | VARCHAR(4) | YES |
| 10 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 11 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 12 | CREATE_USER_ID | VARCHAR(32) | YES |
| 13 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 14 | TRAIN_PLAN_ID | NUMBER(18,0) | YES |
| 15 | TENANT_SCAC_CD | VARCHAR(16) | YES |
| 16 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 17 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 18 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 16/16 | YES | 5/5 | PASS |
| Recovery INSERT | 24 cols = 24 vals | YES | 6/6 | PASS |
| Main UPDATE | 16/16 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 16/16 | YES | 5/5 | PASS |
| Main NEW INSERT | 24 cols = 24 vals | YES | 6/6 | PASS |

---

## 5. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 39,893 NULL rows included, 3,106 TSDPRG excluded, 0 EMEPRG (future-proofed).

---

## 6. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_TRAIN_OPTRN()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 7. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 18/18 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Object Naming | 10 | 10 | All objects correctly renamed |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **69** | **70** | **99%** |

---

## 8. Suggestions

1. **Low:** EMEPRG has 0 rows currently — filter is proactive, good practice.

---

## 9. Verdict

**APPROVED FOR PRODUCTION** — 100% column mapping, all MERGE branches validated, dual-value filter correctly applied, all objects properly renamed, SP compiled successfully.
