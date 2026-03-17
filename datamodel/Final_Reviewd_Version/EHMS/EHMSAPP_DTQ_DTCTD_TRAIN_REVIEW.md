# Code Review: EHMSAPP_DTQ_DTCTD_TRAIN CDC Data Preservation Script
**Review Date:** March 17, 2026  
**Reviewer:** Cortex Code  
**Script:** Scripts/EHMS/EHMSAPP_DTQ_DTCTD_TRAIN.sql  
**Reference:** Scripts/EHMS/EHMSAPP_DTQ_DTCTD_EQPMNT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.EHMS.EHMSAPP_DTQ_DTCTD_TRAIN_BASE` (73 columns) |
| Target Table | `D_BRONZE.EHMS.EHMSAPP_DTQ_DTCTD_TRAIN` (73 source + 6 CDC = 79 columns) |
| Stream | `D_RAW.EHMS.EHMSAPP_DTQ_DTCTD_TRAIN_BASE_HIST_STREAM` |
| Procedure | `D_RAW.EHMS.SP_PROCESS_EHMSAPP_DTQ_DTCTD_TRAIN()` |
| Task | `D_RAW.EHMS.TASK_PROCESS_EHMSAPP_DTQ_DTCTD_TRAIN` (5 min) |
| Primary Key | `DTCTD_TRAIN_ID` (single, NUMBER(18,0)) |
| Schema | **EHMS** |
| Filter | **NONE** |

---

## 2. Column Mapping — 73/73 = 100%

All 73 source columns verified via `DESCRIBE TABLE` with exact data types. CDC Metadata (6/6) correctly set per MERGE branch.

---

## 3. Compilation Results

| Component | Result |
|-----------|--------|
| CREATE OR ALTER TABLE | **COMPILED SUCCESSFULLY** |
| CREATE STREAM | Valid syntax |
| CREATE TASK | Valid syntax |

---

## 4. Data Volume

| Owner | Rows |
|-------|------|
| NULL | 322,336 |
| EHMSMGR | 100,582 |
| **Total** | **422,918** (all included) |

---

## 5. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 73/73 source + 6/6 CDC = 100% |
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

**APPROVED FOR PRODUCTION** — 100% column mapping (73/73 source + 6/6 CDC), EHMS schema throughout, no purge filter, SP compiled successfully. 422K rows ready for initial load.
