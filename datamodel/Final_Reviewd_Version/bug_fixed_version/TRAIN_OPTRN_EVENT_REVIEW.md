# Code Review: TRAIN_OPTRN_EVENT CDC Data Preservation Script
**Review Date:** March 10, 2026  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE` (29 columns) |
| Target Table | `D_BRONZE.SADB.TRAIN_OPTRN_EVENT` (29 source + 6 CDC = 35 columns) |
| Stream | `D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_TRAIN_OPTRN_EVENT()` |
| Task | `D_RAW.SADB.TASK_PROCESS_TRAIN_OPTRN_EVENT` (5 min, no WHEN clause) |
| Primary Key | `OPTRN_EVENT_ID` (single) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from OPTRN_EVENT.sql Reference

| # | Change | Old Value | New Value | Status |
|---|--------|-----------|-----------|--------|
| 1 | Source table | `D_RAW.SADB.OPTRN_EVENT_BASE` | `D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE` | DONE |
| 2 | Target table | `D_BRONZE.SADB.OPTRN_EVENT` | `D_BRONZE.SADB.TRAIN_OPTRN_EVENT` | DONE |
| 3 | Stream | `OPTRN_EVENT_BASE_HIST_STREAM` | `TRAIN_OPTRN_EVENT_BASE_HIST_STREAM` | DONE |
| 4 | Procedure | `SP_PROCESS_OPTRN_EVENT()` | `SP_PROCESS_TRAIN_OPTRN_EVENT()` | DONE |
| 5 | Task | `TASK_PROCESS_OPTRN_EVENT` | `TASK_PROCESS_TRAIN_OPTRN_EVENT` | DONE |
| 6 | Temp table | `_CDC_STAGING_OPTRN_EVENT` | `_CDC_STAGING_TRAIN_OPTRN_EVENT` | DONE |
| 7 | Filter | `<> 'TSDPRG'` | `NOT IN ('TSDPRG', 'EMEPRG')` | DONE |
| 8 | Task WHEN | Had `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional) | DONE |

---

## 3. Source Table Verification

Source `D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE` confirmed via `DESCRIBE TABLE` — **29 columns, identical structure** to `OPTRN_EVENT_BASE`.

---

## 4. Column Mapping (Source → Target) — 29/29 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | OPTRN_EVENT_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | OPTRN_LEG_ID | NUMBER(18,0) | YES |
| 3 | EVENT_TMS | TIMESTAMP_NTZ(0) | YES |
| 4 | TRAIN_PLAN_LEG_ID | NUMBER(18,0) | YES |
| 5 | TRAIN_PLAN_EVENT_ID | NUMBER(18,0) | YES |
| 6 | TRAIN_EVENT_TYPE_CD | VARCHAR(16) | YES |
| 7 | MTP_ROUTE_POINT_SQNC_NBR | NUMBER(3,0) | YES |
| 8 | TRAVEL_DRCTN_CD | VARCHAR(20) | YES |
| 9 | SCAC_CD | VARCHAR(16) | YES |
| 10 | FSAC_CD | VARCHAR(20) | YES |
| 11 | TRSTN_VRSN_NBR | NUMBER(5,0) | YES |
| 12 | RGN_NM_TRK_NBR | NUMBER(18,0) | YES |
| 13 | REGION_NBR | NUMBER(18,0) | YES |
| 14 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 15 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 16 | CREATE_USER_ID | VARCHAR(32) | YES |
| 17 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 18 | TIME_ZONE_CD | VARCHAR(8) | YES |
| 19 | TIME_ZONE_YEAR_NBR | NUMBER(4,0) | YES |
| 20 | EVENT_SOURCE_CD | VARCHAR(32) | YES |
| 21 | MILE_NBR | NUMBER(8,3) | YES |
| 22 | AEIRD_NBR | VARCHAR(28) | YES |
| 23 | AEIRD_DRCTN_CD | VARCHAR(4) | YES |
| 24 | MTP_OMTS_PNDNG_IND | VARCHAR(4) | YES |
| 25 | CTC_SIGNAL_ID | VARCHAR(24) | YES |
| 26 | OPSNG_CTC_SIGNAL_ID | VARCHAR(24) | YES |
| 27 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 28 | SNW_OPERATION_OWNER | VARCHAR(256) | YES |
| 29 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID — all correctly set per MERGE branch.

---

## 5. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 27/27 | YES | 5/5 | PASS |
| Recovery INSERT | 35 cols = 35 vals | YES | 6/6 | PASS |
| Main UPDATE | 27/27 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 27/27 | YES | 5/5 | PASS |
| Main NEW INSERT | 35 cols = 35 vals | YES | 6/6 | PASS |

---

## 6. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source (line 129) | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT (line 205) | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact (current):**
- 1,284,337 NULL rows — INCLUDED (correct)
- 105,990 TSDPRG rows — EXCLUDED
- 49 TSDMGR rows — INCLUDED (correct, not in filter list)
- 0 EMEPRG rows currently — filter ready for future data

---

## 7. Object Name Verification (No Old References)

Verified: **zero** occurrences of old names (`OPTRN_EVENT_BASE_HIST_STREAM`, `SP_PROCESS_OPTRN_EVENT`, `TASK_PROCESS_OPTRN_EVENT`, `_CDC_STAGING_OPTRN_EVENT`, `D_BRONZE.SADB.OPTRN_EVENT`) in the new script. All references use `TRAIN_OPTRN_EVENT*` naming.

---

## 8. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_TRAIN_OPTRN_EVENT()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 9. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 29/29 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Object Naming | 10 | 10 | All 6 object names correctly renamed |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent pattern with reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **69** | **70** | **99%** |

---

## 10. Suggestions

1. **Low:** Consider adding ENV parameterization (like OPTRN.sql) for environment portability.
2. **Info:** EMEPRG has 0 rows currently — filter is proactive/preventive, which is good practice.

---

## 11. Verdict

**APPROVED FOR PRODUCTION** — 100% column mapping, all MERGE branches validated, dual-value filter (TSDPRG + EMEPRG) correctly applied with NULL-safe NVL handling, all object names properly renamed, SP compiled successfully.
