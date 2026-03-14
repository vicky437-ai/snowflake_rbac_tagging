# Code Review: CSTMR_RGSTRD_MARK_JN_GRP CDC Data Preservation Script
**Review Date:** March 13, 2026  
**Reviewer:** Cortex Code  
**Script:** Scripts/DIM_EQUIP/CSTMR_RGSTRD_MARK_JN_GRP.sql  
**Reference:** Scripts/DIM_EQUIP/EQPMNT_POOL_ASGNMN.sql, Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.CSTMR_RGSTRD_MARK_JN_GRP_BASE` (10 columns) |
| Target Table | `D_BRONZE.SADB.CSTMR_RGSTRD_MARK_JN_GRP` (10 source + 6 CDC = 16 columns) |
| Stream | `D_RAW.SADB.CSTMR_RGSTRD_MARK_JN_GRP_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_CSTMR_RGSTRD_MARK_JN_GRP()` |
| Task | `D_RAW.SADB.TASK_PROCESS_CSTMR_RGSTRD_MARK_JN_GRP` (5 min, no WHEN clause) |
| Primary Key | `DATA_SOURCE_CD, GROUP_ROLE_CD, MARK_CD` (**composite, 3 VARCHAR columns**) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Reference Scripts

| # | Change | Old Value (EQPMNT_POOL_ASGNMN) | New Value | Status |
|---|--------|--------------------------------|-----------|--------|
| 1 | Source table | `D_RAW.SADB.EQPMNT_POOL_ASGNMN_BASE` | `D_RAW.SADB.CSTMR_RGSTRD_MARK_JN_GRP_BASE` | DONE |
| 2 | Target table | `D_BRONZE.SADB.EQPMNT_POOL_ASGNMN` | `D_BRONZE.SADB.CSTMR_RGSTRD_MARK_JN_GRP` | DONE |
| 3 | Stream | `EQPMNT_POOL_ASGNMN_BASE_HIST_STREAM` | `CSTMR_RGSTRD_MARK_JN_GRP_BASE_HIST_STREAM` | DONE |
| 4 | Procedure | `SP_PROCESS_EQPMNT_POOL_ASGNMN()` | `SP_PROCESS_CSTMR_RGSTRD_MARK_JN_GRP()` | DONE |
| 5 | Task | `TASK_PROCESS_EQPMNT_POOL_ASGNMN` | `TASK_PROCESS_CSTMR_RGSTRD_MARK_JN_GRP` | DONE |
| 6 | Temp table | `_CDC_STAGING_EQPMNT_POOL_ASGNMN` | `_CDC_STAGING_CSTMR_RGSTRD_MARK_JN_GRP` | DONE |
| 7 | Primary key | `EQPMNT_POOL_ASGNMN_ID` (single NUMBER) | `DATA_SOURCE_CD, GROUP_ROLE_CD, MARK_CD` (composite VARCHAR) | DONE |
| 8 | Columns | 17 source columns | 10 source columns (7 business + 3 SNW) | DONE |

---

## 3. Source Table Verification

Source `D_RAW.SADB.CSTMR_RGSTRD_MARK_JN_GRP_BASE` confirmed via `DESCRIBE TABLE` — **10 columns**.

---

## 4. Column Mapping (Source -> Target) — 10/10 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | DATA_SOURCE_CD | VARCHAR(40) NOT NULL | YES (PK1) |
| 2 | GROUP_ROLE_CD | VARCHAR(80) NOT NULL | YES (PK2) |
| 3 | MARK_CD | VARCHAR(16) NOT NULL | YES (PK3) |
| 4 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 5 | CREATE_USER_ID | VARCHAR(32) | YES |
| 6 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 7 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 8 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 9 | SNW_OPERATION_OWNER | VARCHAR(256) | YES |
| 10 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID — all correctly set per MERGE branch.

---

## 5. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | MERGE ON (3-col) | Result |
|--------|-------------|---------------------|----------|-------------------|--------|
| Recovery UPDATE | 7/7 | YES | 5/5 | 3/3 | PASS |
| Recovery INSERT | 16 cols = 16 vals | YES | 6/6 | N/A | PASS |
| Main UPDATE | 7/7 | YES | 5/5 | 3/3 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | 3/3 | PASS |
| Main RE-INSERT | 7/7 | YES | 5/5 | 3/3 | PASS |
| Main NEW INSERT | 16 cols = 16 vals | YES | 6/6 | N/A | PASS |

---

## 6. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact (current):**
- 60 NULL rows — INCLUDED (correct)
- 0 TSDPRG rows currently — filter ready for future data
- 0 EMEPRG rows currently — filter ready for future data

---

## 7. Object Name Verification (No Old References)

Verified: **zero** occurrences of old/reference names (`EQPMNT_POOL_ASGNMN`, `TRAIN_OPTRN_EVENT`, `OPTRN_EVENT`) in the script. All references use `CSTMR_RGSTRD_MARK_JN_GRP*` naming.

---

## 8. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_CSTMR_RGSTRD_MARK_JN_GRP()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | **COMPILED SUCCESSFULLY** |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 9. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 10/10 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct, 3-col composite ON clause |
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
3. **Info:** This is the **first composite primary key** in the DIM_EQUIP batch. The MERGE ON clause correctly joins on all 3 columns (`DATA_SOURCE_CD AND GROUP_ROLE_CD AND MARK_CD`) across all 4 MERGE locations (Recovery + Main x 2).

---

## 11. Verdict

**APPROVED FOR PRODUCTION** — 100% column mapping (10/10 source + 6/6 CDC), all MERGE branches validated, composite primary key (`DATA_SOURCE_CD, GROUP_ROLE_CD, MARK_CD`) correctly applied across all join clauses with 3-column AND conditions, dual-value filter (TSDPRG + EMEPRG) correctly applied with NULL-safe NVL handling, all object names follow naming convention, SP compiled successfully.
