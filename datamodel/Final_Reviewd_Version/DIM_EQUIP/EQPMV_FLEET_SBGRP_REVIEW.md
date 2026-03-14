# Code Review: EQPMV_FLEET_SBGRP CDC Data Preservation Script
**Review Date:** March 13, 2026  
**Reviewer:** Cortex Code  
**Script:** Scripts/DIM_EQUIP/EQPMV_FLEET_SBGRP.sql  
**Reference:** Scripts/DIM_EQUIP/EQPMNT_POOL_ASGNMN.sql, Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.EQPMV_FLEET_SBGRP_BASE` (13 columns) |
| Target Table | `D_BRONZE.SADB.EQPMV_FLEET_SBGRP` (13 source + 6 CDC = 19 columns) |
| Stream | `D_RAW.SADB.EQPMV_FLEET_SBGRP_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_EQPMV_FLEET_SBGRP()` |
| Task | `D_RAW.SADB.TASK_PROCESS_EQPMV_FLEET_SBGRP` (5 min, no WHEN clause) |
| Primary Key | `FLEET_SBGRP_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Reference Scripts

| # | Change | Old Value (EQPMNT_POOL_ASGNMN) | New Value | Status |
|---|--------|--------------------------------|-----------|--------|
| 1 | Source table | `D_RAW.SADB.EQPMNT_POOL_ASGNMN_BASE` | `D_RAW.SADB.EQPMV_FLEET_SBGRP_BASE` | DONE |
| 2 | Target table | `D_BRONZE.SADB.EQPMNT_POOL_ASGNMN` | `D_BRONZE.SADB.EQPMV_FLEET_SBGRP` | DONE |
| 3 | Stream | `EQPMNT_POOL_ASGNMN_BASE_HIST_STREAM` | `EQPMV_FLEET_SBGRP_BASE_HIST_STREAM` | DONE |
| 4 | Procedure | `SP_PROCESS_EQPMNT_POOL_ASGNMN()` | `SP_PROCESS_EQPMV_FLEET_SBGRP()` | DONE |
| 5 | Task | `TASK_PROCESS_EQPMNT_POOL_ASGNMN` | `TASK_PROCESS_EQPMV_FLEET_SBGRP` | DONE |
| 6 | Temp table | `_CDC_STAGING_EQPMNT_POOL_ASGNMN` | `_CDC_STAGING_EQPMV_FLEET_SBGRP` | DONE |
| 7 | Primary key | `EQPMNT_POOL_ASGNMN_ID` | `FLEET_SBGRP_ID` | DONE |
| 8 | Columns | 17 source columns | 13 source columns (10 business + 3 SNW) | DONE |

---

## 3. Source Table Verification

Source `D_RAW.SADB.EQPMV_FLEET_SBGRP_BASE` confirmed via `DESCRIBE TABLE` — **13 columns**.

---

## 4. Column Mapping (Source -> Target) — 13/13 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | FLEET_SBGRP_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | PARTY_NM | VARCHAR(400) | YES |
| 3 | FLEET_SBGRP_CTGRY_ID | NUMBER(18,0) | YES |
| 4 | FLEET_ID | NUMBER(18,0) | YES |
| 5 | CREATE_USER_ID | VARCHAR(1020) | YES |
| 6 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 7 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 8 | UPDATE_USER_ID | VARCHAR(1020) | YES |
| 9 | SOURCE_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 10 | SOURCE_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 11 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 12 | SNW_OPERATION_OWNER | VARCHAR(256) | YES |
| 13 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID — all correctly set per MERGE branch.

---

## 5. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 12/12 | YES | 5/5 | PASS |
| Recovery INSERT | 19 cols = 19 vals | YES | 6/6 | PASS |
| Main UPDATE | 12/12 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 12/12 | YES | 5/5 | PASS |
| Main NEW INSERT | 19 cols = 19 vals | YES | 6/6 | PASS |

---

## 6. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact (current):**
- 4 NULL rows — INCLUDED (correct)
- 0 TSDPRG rows currently — filter ready for future data
- 0 EMEPRG rows currently — filter ready for future data

---

## 7. Object Name Verification (No Old References)

Verified: **zero** occurrences of old/reference names (`EQPMNT_POOL_ASGNMN`, `TRAIN_OPTRN_EVENT`, `OPTRN_EVENT`, `EQPMV_FLEET_POOL`) in the script. All references including stream use `EQPMV_FLEET_SBGRP*` naming.

---

## 8. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_EQPMV_FLEET_SBGRP()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | **COMPILED SUCCESSFULLY** |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 9. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 13/13 source + 6/6 CDC = 100% |
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
3. **Info:** `CREATE_USER_ID` and `UPDATE_USER_ID` are VARCHAR(1020), matching source exactly.

---

## 11. Verdict

**APPROVED FOR PRODUCTION** — 100% column mapping (13/13 source + 6/6 CDC), all MERGE branches validated, primary key `FLEET_SBGRP_ID` correctly applied across all join clauses, dual-value filter (TSDPRG + EMEPRG) correctly applied with NULL-safe NVL handling, all object names follow naming convention, SP compiled successfully.
