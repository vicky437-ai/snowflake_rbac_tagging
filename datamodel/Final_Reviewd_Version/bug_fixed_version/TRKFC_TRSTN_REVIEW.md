# Code Review: TRKFC_TRSTN CDC Data Preservation Script
**Review Date:** March 11, 2026 (Rev 2 - Bug Fix updates)  
**Reviewer:** Cortex Code  
**Script:** Scripts/Bug_Fix_2026_03_05/TRKFC_TRSTN.sql  
**Reference:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.TRKFC_TRSTN_BASE` (41 columns) |
| Target Table | `D_BRONZE.SADB.TRKFC_TRSTN` (41 source + 6 CDC = 47 columns) |
| Stream | `D_RAW.SADB.TRKFC_TRSTN_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_TRKFC_TRSTN()` |
| Task | `D_RAW.SADB.TASK_PROCESS_TRKFC_TRSTN` (5 min, no WHEN clause) |
| Primary Key | **COMPOSITE** (SCAC_CD, FSAC_CD) |
| Filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` |

---

## 2. Changes from Previous Version (Rev 1 dated 2026-02-24)

| # | Change | Old | New | Status |
|---|--------|-----|-----|--------|
| 1 | DDL | `CREATE TABLE IF NOT EXISTS` | `CREATE OR ALTER TABLE` | DONE |
| 2 | New column | N/A (40 source cols) | `SNW_OPERATION_OWNER VARCHAR(256)` added (41 source cols) | DONE |
| 3 | Filter | None | `NOT IN ('TSDPRG', 'EMEPRG')` at all entry points | DONE |
| 4 | Recovery MERGE | Base table LEFT JOIN | Stream read with `SHOW_INITIAL_ROWS` | DONE |
| 5 | Task WHEN | `SYSTEM$STREAM_HAS_DATA` | Removed (unconditional for stale recovery) | DONE |
| 6 | DEFAULTs | Inline in DDL | Removed (unsupported in CREATE OR ALTER) | DONE |
| 7 | Column count | 40 source + 6 CDC = 46 | 41 source + 6 CDC = 47 | DONE |

---

## 3. Column Mapping (Source -> Target) -- 41/41 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | SCAC_CD | VARCHAR(16) NOT NULL | YES (PK1) |
| 2 | FSAC_CD | VARCHAR(20) NOT NULL | YES (PK2) |
| 3 | VRSN_NBR | NUMBER(5,0) | YES |
| 4 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 5 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 6 | CREATE_USER_ID | VARCHAR(32) | YES |
| 7 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 8 | EFCTV_DT | TIMESTAMP_NTZ(0) | YES |
| 9 | DYLGHT_SVNGS_TIME_IND | VARCHAR(4) | YES |
| 10 | IRF_CREATE_DT | TIMESTAMP_NTZ(0) | YES |
| 11 | IRF_UPDATE_DT | TIMESTAMP_NTZ(0) | YES |
| 12 | CNTRY_CD | VARCHAR(8) | YES |
| 13 | AAR_LAST_MNTND_DT | TIMESTAMP_NTZ(0) | YES |
| 14 | ALTD_QTY | NUMBER(8,3) | YES |
| 15 | BEA_CD | VARCHAR(12) | YES |
| 16 | CNTY_ID | VARCHAR(24) | YES |
| 17 | DELETE_REASON_CD | VARCHAR(4) | YES |
| 18 | EXPIRY_DT | TIMESTAMP_NTZ(0) | YES |
| 19 | GPLTCL_NM | VARCHAR(120) | YES |
| 20 | GPLTCL_SPLC_CD | VARCHAR(24) | YES |
| 21 | GPLTCL_SPLC_SUFFIX_CD | VARCHAR(12) | YES |
| 22 | LNGTD_NBR | NUMBER(9,6) | YES |
| 23 | LTD_NBR | NUMBER(9,6) | YES |
| 24 | MSA_CD | VARCHAR(16) | YES |
| 25 | POSTAL_ZIP_CD | VARCHAR(36) | YES |
| 26 | RATE_POSTAL_ZIP_CD | VARCHAR(36) | YES |
| 27 | RATE_ZIP_EFCTV_DT | TIMESTAMP_NTZ(0) | YES |
| 28 | RELOAD_ABRVTN_TXT | VARCHAR(20) | YES |
| 29 | SPLC_CD | VARCHAR(24) | YES |
| 30 | SPLC_SUFFIX_CD | VARCHAR(12) | YES |
| 31 | STN_STATUS_CD | VARCHAR(4) | YES |
| 32 | STPRV_CD | VARCHAR(8) | YES |
| 33 | TIME_ZONE_CD | VARCHAR(8) | YES |
| 34 | TRNSCN_NBR | VARCHAR(36) | YES |
| 35 | TRSTN_NM | VARCHAR(120) | YES |
| 36 | TRSTN_SEARCH_NM | VARCHAR(120) | YES |
| 37 | PARTY_FCLTY_VRSN_ID | NUMBER(18,0) | YES |
| 38 | PARTY_FCLTY_ID | NUMBER(18,0) | YES |
| 39 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 40 | SNW_OPERATION_OWNER | VARCHAR(256) | YES (NEW) |
| 41 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID

---

## 4. Composite Primary Key Validation

| Check | Value | Result |
|-------|-------|--------|
| PK columns | SCAC_CD, FSAC_CD | PASS |
| DDL PRIMARY KEY | `PRIMARY KEY (SCAC_CD, FSAC_CD)` | PASS |
| Recovery MERGE ON | `tgt.SCAC_CD = src.SCAC_CD AND tgt.FSAC_CD = src.FSAC_CD` | PASS |
| Main MERGE ON | `tgt.SCAC_CD = src.SCAC_CD AND tgt.FSAC_CD = src.FSAC_CD` | PASS |

---

## 5. MERGE Branch Validation

| Branch | Non-PK Cols | SNW_OPERATION_OWNER | CDC Cols | Result |
|--------|-------------|---------------------|----------|--------|
| Recovery UPDATE | 38/38 | YES | 5/5 | PASS |
| Recovery INSERT | 47 cols = 47 vals | YES | 6/6 | PASS |
| Main UPDATE | 38/38 | YES | 5/5 | PASS |
| Main DELETE | Soft-delete only | N/A | 5/5 | PASS |
| Main RE-INSERT | 38/38 | YES | 5/5 | PASS |
| Main NEW INSERT | 47 cols = 47 vals | YES | 6/6 | PASS |

---

## 6. Filter Validation

| Location | Filter | NULL-Safe? |
|----------|--------|------------|
| Recovery MERGE source | `NVL(src.SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Staging SELECT | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |

**Data impact:** 377,167 NULL rows included. 0 TSDPRG/EMEPRG currently -- filter is proactive/preventive.

---

## 7. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_TRKFC_TRSTN()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | Valid syntax |
| CREATE STREAM | Valid syntax |
| CREATE TASK (no WHEN) | Valid syntax |

---

## 8. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 41/41 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Composite PK | 10 | 10 | Both MERGE ON clauses use SCAC_CD + FSAC_CD |
| Filter Coverage | 10 | 10 | NOT IN at all entry points, NVL-safe |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Code Standards | 10 | 10 | Consistent with TRAIN_OPTRN_EVENT reference |
| Production Readiness | 10 | 10 | No manual steps needed |
| **TOTAL** | **69** | **70** | **99%** |

---

## 9. Suggestions

1. **Info:** No TSDPRG/EMEPRG rows currently in source (377,167 rows all NULL owner) -- filter is preventive, good practice.

---

## 10. Verdict

**APPROVED FOR PRODUCTION** -- 100% column mapping (41/41 source + 6/6 CDC = 47 total), composite PK correctly implemented in all MERGE ON clauses, all branches validated, dual-value filter correctly applied, stream-based recovery implemented, SP compiled successfully.
