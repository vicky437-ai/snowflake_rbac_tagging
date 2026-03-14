# Code Review: EQPMNT_PARTY CDC Data Preservation Script
**Review Date:** March 13, 2026  
**Reviewer:** Cortex Code  
**Script:** Scripts/DIM_EQUIP/EQPMNT_PARTY.sql  
**Reference:** Scripts/DIM_EQUIP/EQPMNT_POOL_ASGNMN.sql, Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.EQPMNT_PARTY_BASE` (11 columns) |
| Target Table | `D_BRONZE.SADB.EQPMNT_PARTY` (11 source + 6 CDC = 17 columns) |
| Stream | `D_RAW.SADB.EQPMNT_PARTY_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_EQPMNT_PARTY()` |
| Task | `D_RAW.SADB.TASK_PROCESS_EQPMNT_PARTY` (5 min, no WHEN clause) |
| Primary Key | `EQPMNT_PARTY_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Reference Scripts

| # | Change | Old Value (EQPMNT_POOL_ASGNMN) | New Value | Status |
|---|--------|--------------------------------|-----------|--------|
| 1 | Source table | `D_RAW.SADB.EQPMNT_POOL_ASGNMN_BASE` | `D_RAW.SADB.EQPMNT_PARTY_BASE` | DONE |
| 2 | Target table | `D_BRONZE.SADB.EQPMNT_POOL_ASGNMN` | `D_BRONZE.SADB.EQPMNT_PARTY` | DONE |
| 3 | Stream | `EQPMNT_POOL_ASGNMN_BASE_HIST_STREAM` | `EQPMNT_PARTY_BASE_HIST_STREAM` | DONE |
| 4 | Procedure | `SP_PROCESS_EQPMNT_POOL_ASGNMN()` | `SP_PROCESS_EQPMNT_PARTY()` | DONE |
| 5 | Task | `TASK_PROCESS_EQPMNT_POOL_ASGNMN` | `TASK_PROCESS_EQPMNT_PARTY` | DONE |
| 6 | Temp table | `_CDC_STAGING_EQPMNT_POOL_ASGNMN` | `_CDC_STAGING_EQPMNT_PARTY` | DONE |
| 7 | Primary key | `EQPMNT_POOL_ASGNMN_ID` | `EQPMNT_PARTY_ID` | DONE |
| 8 | Columns | 17 source columns | 11 source columns (8 business + 3 SNW) | DONE |

---

## 3. Source Table Verification

Source `D_RAW.SADB.EQPMNT_PARTY_BASE` confirmed via `DESCRIBE TABLE` — **11 columns**.

---

## 4. Column Mapping (Source -> Target) — 11/11 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | EQPMNT_PARTY_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 3 | CREATE_USER_ID | VARCHAR(32) | YES |
| 4 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 5 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 6 | EQPMNT_ID | NUMBER(18,0) | YES |
| 7 | ENTITY_IDNTFR_CD | VARCHAR(12) | YES |
| 8 | MARK_CD | VARCHAR(16) | YES |
| 9 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 10 | SNW_OPERATION_OWNER | VARCHAR(256) | YES |
| 11 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID — all correctly set per MERGE branch.

---

## 5. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 10/10 | YES | 5/5 | PASS |
| Recovery INSERT | 17 cols = 17 vals | YES | 6/6 | PASS |
| Main UPDATE | 10/10 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 10/10 | YES | 5/5 | PASS |
| Main NEW INSERT | 17 cols = 17 vals | YES | 6/6 | PASS |

---

## 6. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact (current):**
- 2,605,110 NULL rows — INCLUDED (correct)
- 3,195 EDMGR rows — INCLUDED (correct, not in filter list)
- 0 TSDPRG rows currently — filter ready for future data
- 0 EMEPRG rows currently — filter ready for future data

---

## 7. Object Name Verification (No Old References)

Verified: **zero** occurrences of old/reference names (`EQPMNT_POOL_ASGNMN`, `TRAIN_OPTRN_EVENT`, `OPTRN_EVENT`) in the script. All references use `EQPMNT_PARTY*` naming.

---

## 8. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_EQPMNT_PARTY()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | **COMPILED SUCCESSFULLY** |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 9. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 11/11 source + 6/6 CDC = 100% |
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
3. **Info:** This table has 2.6M rows — largest in the DIM_EQUIP batch. Initial load via SHOW_INITIAL_ROWS will process all rows on first run. Monitor task execution time.
4. **Info:** `CREATE_USER_ID` and `UPDATE_USER_ID` are VARCHAR(32) (unlike VARCHAR(1020) in EQPMV tables), matching source exactly.

---

## 11. Verdict

**APPROVED FOR PRODUCTION** — 100% column mapping (11/11 source + 6/6 CDC), all MERGE branches validated, primary key `EQPMNT_PARTY_ID` correctly applied across all join clauses, dual-value filter (TSDPRG + EMEPRG) correctly applied with NULL-safe NVL handling, all object names follow naming convention, SP compiled successfully.
