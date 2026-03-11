# Code Review: LCMTV_EMIS CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/LCMTV_EMIS.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.LCMTV_EMIS_BASE` (85 columns) |
| Target Table | `D_BRONZE.SADB.LCMTV_EMIS` (85 source + 6 CDC = 91 columns) |
| Stream | `D_RAW.SADB.LCMTV_EMIS_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_LCMTV_EMIS()` |
| Task | `D_RAW.SADB.TASK_PROCESS_LCMTV_EMIS` (5 min, no WHEN clause) |
| Primary Key | **COMPOSITE** (MARK_CD, EQPUN_NBR) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (84 source) | `SNW_OPERATION_OWNER VARCHAR(256)` added (85 source) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 84 source + 6 CDC = 90 | 85 source + 6 CDC = 91 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 85/85 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | MARK_CD | VARCHAR(16) NOT NULL | YES (PK1) |
| 2 | EQPUN_NBR | VARCHAR(40) NOT NULL | YES (PK2) |
| 3-82 | AIR_BRAKE_HOOKP_CD through LCMTV_STRTR_TYPE_CD | Various | YES (all 80) |
| 83 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 84 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 85 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. Composite PK Validation

| Check | Value | Result |
|-------|-------|--------|
| PK columns | MARK_CD, EQPUN_NBR | PASS |
| DDL PRIMARY KEY | `PRIMARY KEY (MARK_CD, EQPUN_NBR)` | PASS |
| Recovery MERGE ON | Both PK columns | PASS |
| Main MERGE ON | Both PK columns | PASS |

---

## 5. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 82/82 | YES | 5/5 | PASS |
| Recovery INSERT | 91 cols = 91 vals | YES | 6/6 | PASS |
| Main UPDATE | 82/82 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 82/82 | YES | 5/5 | PASS |
| Main NEW INSERT | 91 cols = 91 vals | YES | 6/6 | PASS |

---

## 6. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 41,562 NULL rows included, 19 EDMGR rows included (not filtered). 0 TSDPRG/EMEPRG -- filter is preventive.

---

## 7. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_LCMTV_EMIS()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 8. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 85/85 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Composite PK | 10 | 10 | Both MERGE ON clauses use MARK_CD + EQPUN_NBR |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **69** | **70** | **99%** |

---

## 9. Suggestions

1. **Info:** No TSDPRG/EMEPRG rows in source. 19 EDMGR rows exist but are NOT in the filter list (correctly included).

---

## 10. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (85/85 source + 6/6 CDC = 91 total), composite PK correctly implemented in all MERGE ON clauses, all branches validated, dual-value filter correctly applied, stream-based recovery implemented, SP compiled successfully.
