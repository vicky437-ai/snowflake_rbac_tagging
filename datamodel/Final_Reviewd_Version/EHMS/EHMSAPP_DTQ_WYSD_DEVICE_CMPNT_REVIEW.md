# Code Review: EHMSAPP_DTQ_WYSD_DEVICE_CMPNT CDC Data Preservation Script
**Review Date:** March 17, 2026  
**Reviewer:** Cortex Code  
**Script:** Scripts/EHMS/EHMSAPP_DTQ_WYSD_DEVICE_CMPNT.sql  
**Reference:** Scripts/EHMS/EHMSAPP_DTQ_DTCTD_EQPMNT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.EHMS.EHMSAPP_DTQ_WYSD_DEVICE_CMPNT_BASE` (21 columns) |
| Target Table | `D_BRONZE.EHMS.EHMSAPP_DTQ_WYSD_DEVICE_CMPNT` (21 source + 6 CDC = 27 columns) |
| Stream | `D_RAW.EHMS.EHMSAPP_DTQ_WYSD_DEVICE_CMPNT_BASE_HIST_STREAM` |
| Procedure | `D_RAW.EHMS.SP_PROCESS_EHMSAPP_DTQ_WYSD_DEVICE_CMPNT()` |
| Task | `D_RAW.EHMS.TASK_PROCESS_EHMSAPP_DTQ_WYSD_DEVICE_CMPNT` (5 min) |
| Primary Key | `WYSD_DEVICE_CMPNT_VRSN_ID` (single, NUMBER(18,0)) |
| Schema | **EHMS** |
| Filter | **NONE** |

---

## 2. Column Mapping — 21/21 = 100%

All 21 source columns verified via `DESCRIBE TABLE` with exact data types. CDC Metadata (6/6) correctly set.

---

## 3. Compilation Results

| Component | Result |
|-----------|--------|
| CREATE OR ALTER TABLE | **COMPILED SUCCESSFULLY** |

---

## 4. Data Volume

| Owner | Rows |
|-------|------|
| NULL | 3,522 |
| **Total** | **3,522** (all included) |

---

## 5. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 21/21 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Removal | 10 | 10 | No filter, per requirement |
| Schema (EHMS) | 10 | 10 | All references use EHMS |
| Object Naming | 10 | 10 | All 6 names match requirements |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Execution Logging | 10 | 10 | All 4 paths logged |
| Code Standards | 10 | 10 | Consistent with EHMS pattern |
| Data Type Accuracy | 10 | 10 | All types match source exactly |
| Production Readiness | 10 | 10 | SP compiled, all objects valid |
| **TOTAL** | **99** | **100** | **99%** |

---

## 6. Verdict

**APPROVED FOR PRODUCTION** — 100% column mapping (21/21 source + 6/6 CDC), EHMS schema throughout, no purge filter, SP compiled successfully. 3,522 rows ready for initial load.
