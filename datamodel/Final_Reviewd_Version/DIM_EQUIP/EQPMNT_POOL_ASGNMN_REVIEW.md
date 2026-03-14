# Code Review: EQPMNT_POOL_ASGNMN CDC Data Preservation Script
**Review Date:** March 13, 2026  
**Reviewer:** Cortex Code  
**Script:** Scripts/DIM_EQUIP/EQPMNT_POOL_ASGNMN.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.EQPMNT_POOL_ASGNMN_BASE` (17 columns) |
| Target Table | `D_BRONZE.SADB.EQPMNT_POOL_ASGNMN` (17 source + 6 CDC = 23 columns) |
| Stream | `D_RAW.SADB.EQPMNT_POOL_ASGNMN_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_EQPMNT_POOL_ASGNMN()` |
| Task | `D_RAW.SADB.TASK_PROCESS_EQPMNT_POOL_ASGNMN` (5 min, no WHEN clause) |
| Primary Key | `EQPMNT_POOL_ASGNMN_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from TRAIN_OPTRN_EVENT.sql Reference

| # | Change | Old Value | New Value | Status |
|---|--------|-----------|-----------|--------|
| 1 | Source table | `D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE` | `D_RAW.SADB.EQPMNT_POOL_ASGNMN_BASE` | DONE |
| 2 | Target table | `D_BRONZE.SADB.TRAIN_OPTRN_EVENT` | `D_BRONZE.SADB.EQPMNT_POOL_ASGNMN` | DONE |
| 3 | Stream | `TRAIN_OPTRN_EVENT_BASE_HIST_STREAM` | `EQPMNT_POOL_ASGNMN_BASE_HIST_STREAM` | DONE |
| 4 | Procedure | `SP_PROCESS_TRAIN_OPTRN_EVENT()` | `SP_PROCESS_EQPMNT_POOL_ASGNMN()` | DONE |
| 5 | Task | `TASK_PROCESS_TRAIN_OPTRN_EVENT` | `TASK_PROCESS_EQPMNT_POOL_ASGNMN` | DONE |
| 6 | Temp table | `_CDC_STAGING_TRAIN_OPTRN_EVENT` | `_CDC_STAGING_EQPMNT_POOL_ASGNMN` | DONE |
| 7 | Primary key | `OPTRN_EVENT_ID` | `EQPMNT_POOL_ASGNMN_ID` | DONE |
| 8 | Columns | 29 source columns | 17 source columns (14 business + 3 SNW) | DONE |

---

## 3. Source Table Verification

Source `D_RAW.SADB.EQPMNT_POOL_ASGNMN_BASE` confirmed via `DESCRIBE TABLE` — **17 columns**.

---

## 4. Column Mapping (Source → Target) — 17/17 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | EQPMNT_POOL_ASGNMN_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 3 | CREATE_USER_ID | VARCHAR(32) | YES |
| 4 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 5 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 6 | EQPMNT_ID | NUMBER(18,0) | YES |
| 7 | EQPMNT_POOL_ID | NUMBER(18,0) | YES |
| 8 | EQPMNT_POOL_ASGNMN_STATUS_CD | VARCHAR(60) | YES |
| 9 | CREATE_PARTY_RLTNSH_USER_ID | NUMBER(18,0) | YES |
| 10 | SOURCE_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 11 | UPDATE_PARTY_RLTNSH_USER_ID | NUMBER(18,0) | YES |
| 12 | SOURCE_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 13 | SOURCE_CREATE_USER_ID | VARCHAR(32) | YES |
| 14 | SOURCE_UPDATE_USER_ID | VARCHAR(32) | YES |
| 15 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 16 | SNW_OPERATION_OWNER | VARCHAR(256) | YES |
| 17 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID — all correctly set per MERGE branch.

---

## 5. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 16/16 | YES | 5/5 | PASS |
| Recovery INSERT | 23 cols = 23 vals | YES | 6/6 | PASS |
| Main UPDATE | 16/16 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 16/16 | YES | 5/5 | PASS |
| Main NEW INSERT | 23 cols = 23 vals | YES | 6/6 | PASS |

---

## 6. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact (current):**
- 93,739 NULL rows — INCLUDED (correct)
- 188 EDMGR rows — INCLUDED (correct, not in filter list)
- 0 TSDPRG rows currently — filter ready for future data
- 0 EMEPRG rows currently — filter ready for future data

---

## 7. Object Name Verification (No Old References)

Verified: **zero** occurrences of old/reference names (`TRAIN_OPTRN_EVENT`, `OPTRN_EVENT`, `SP_PROCESS_TRAIN_OPTRN_EVENT`, `_CDC_STAGING_TRAIN_OPTRN_EVENT`) in the script. All references use `EQPMNT_POOL_ASGNMN*` naming.

---

## 8. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_EQPMNT_POOL_ASGNMN()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | **COMPILED SUCCESSFULLY** |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 9. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 17/17 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Object Naming | 10 | 10 | All 6 object names correctly named |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent pattern with reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **69** | **70** | **99%** |

---

## 10. Suggestions

1. **Low:** Recovery MERGE uses `SELECT src.*` — explicitly listing columns (as in normal path) would be more defensive. Consistent with reference pattern, non-blocking.
2. **Info:** TSDPRG and EMEPRG have 0 rows currently — filter is proactive/preventive, which is good practice.
3. **Info:** Consider adding `SYSTEM$STREAM_HAS_DATA` as a WHEN condition on the task to skip unnecessary SP calls. Not in reference pattern, listed as optional.

---

## 11. Verdict

**APPROVED FOR PRODUCTION** — 100% column mapping (17/17 source + 6/6 CDC), all MERGE branches validated, primary key `EQPMNT_POOL_ASGNMN_ID` correctly applied across all join clauses, dual-value filter (TSDPRG + EMEPRG) correctly applied with NULL-safe NVL handling, all object names match requirements, SP compiled successfully.
