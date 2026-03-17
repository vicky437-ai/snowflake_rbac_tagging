# Consolidated Code Review: EHMS CDC Scripts — Final Validation
**Review Date:** March 17, 2026  
**Reviewer:** Cortex Code (Independent Review)  
**Folder:** Scripts/EHMS/  
**Total Scripts:** 7 SQL files  
**Reference Pattern:** Scripts/EHMS/EHMSAPP_DTQ_DTCTD_EQPMNT.sql (EHMS v1 pattern)  
**Verdict:** **ALL 7 APPROVED FOR PRODUCTION**

---

## 1. Inventory — All 7 Scripts

| # | Script | Source Table | PK | PK Type | Src Cols | Tgt Cols | Compiled |
|---|--------|-------------|-----|---------|----------|----------|----------|
| 1 | EHMSAPP_DTQ_DTCTD_EQPMNT.sql | EHMSAPP_DTQ_DTCTD_EQPMNT_BASE | DTCTD_EQPMNT_ID | NUMBER(18,0) | 49 | 55 | PASS |
| 2 | EHMSAPP_DTQ_DTCTD_EQPMNT_CMPNT.sql | EHMSAPP_DTQ_DTCTD_EQPMNT_CMPNT_BASE | DTCTD_EQPMNT_CMPNT_ID | NUMBER(18,0) | 128 | 134 | PASS |
| 3 | EHMSAPP_DTQ_DTCTD_TRAIN.sql | EHMSAPP_DTQ_DTCTD_TRAIN_BASE | DTCTD_TRAIN_ID | NUMBER(18,0) | 73 | 79 | PASS |
| 4 | EHMSAPP_DTQ_EQPMNT.sql | EHMSAPP_DTQ_EQPMNT_BASE | EQPMNT_ID | NUMBER(18,0) | 16 | 22 | PASS |
| 5 | EHMSAPP_DTQ_PSNG_SMRY.sql | EHMSAPP_DTQ_PSNG_SMRY_BASE | PSNG_SMRY_ID | NUMBER(18,0) | 27 | 33 | PASS |
| 6 | EHMSAPP_DTQ_WYSD_DEVICE_CMPNT.sql | EHMSAPP_DTQ_WYSD_DEVICE_CMPNT_BASE | WYSD_DEVICE_CMPNT_VRSN_ID | NUMBER(18,0) | 21 | 27 | PASS |
| 7 | EHMSAPP_DTQ_WYSD_DTCTN_DEVICE.sql | EHMSAPP_DTQ_WYSD_DTCTN_DEVICE_BASE | WYSD_DTCTN_DEVICE_VRSN_ID | NUMBER(18,0) | 35 | 41 | PASS |

**Total: 349 source columns mapped. 7/7 compiled successfully.**

---

## 2. Column Count Verification — 7/7 = 100%

All verified via `D_RAW.INFORMATION_SCHEMA.COLUMNS`:

| # | Source Table | DB Cols | Header Cols | Match |
|---|-------------|---------|-------------|-------|
| 1 | EHMSAPP_DTQ_DTCTD_EQPMNT_BASE | 49 | 49 | YES |
| 2 | EHMSAPP_DTQ_DTCTD_EQPMNT_CMPNT_BASE | 128 | 128 | YES |
| 3 | EHMSAPP_DTQ_DTCTD_TRAIN_BASE | 73 | 73 | YES |
| 4 | EHMSAPP_DTQ_EQPMNT_BASE | 16 | 16 | YES |
| 5 | EHMSAPP_DTQ_PSNG_SMRY_BASE | 27 | 27 | YES |
| 6 | EHMSAPP_DTQ_WYSD_DEVICE_CMPNT_BASE | 21 | 21 | YES |
| 7 | EHMSAPP_DTQ_WYSD_DTCTN_DEVICE_BASE | 35 | 35 | YES |

---

## 3. EHMS v1 Reference Pattern Compliance — 7/7

| Component | Expected | All 7 Match? |
|-----------|----------|---------------|
| Header: VERSION/DATE/CHANGES block | Yes | YES |
| Header: "Staleness detection via SELECT COUNT(*) WHERE 1=0 pattern" | Yes | YES |
| Header: "Added execution logging to D_BRONZE.MONITORING.CDC_EXECUTION_LOG" | Yes | YES |
| Step 1: CREATE OR ALTER TABLE with exact source data types | Yes | YES |
| Step 2: ALTER TABLE SET CHANGE_TRACKING (45/15) | Yes | YES |
| Step 3: CREATE STREAM SHOW_INITIAL_ROWS = TRUE | Yes | YES |
| Step 4: SP in D_RAW.EHMS, EXECUTE AS CALLER | Yes | YES |
| SP: 11 variables (v1 pattern with start/end time) | Yes | YES |
| SP: Staleness detection via SELECT COUNT(*) WHERE 1=0 | Yes | YES |
| SP: Recovery MERGE with CDC_OPERATION = 'RELOADED' | Yes | YES |
| SP: Staging temp table with explicit column list | Yes | YES |
| SP: **NO purge filter** (EHMS requirement) | Yes | YES |
| SP: Pre-merge metrics (I/U/D breakdown) | Yes | YES |
| SP: 4-branch MERGE (UPDATE/DELETE/RE-INSERT/NEW INSERT) | Yes | YES |
| SP: CDC_EXECUTION_LOG in all 4 paths (SUCCESS/NO_DATA/RECOVERY/ERROR) | Yes | YES |
| SP: Exception handler with cleanup + logging | Yes | YES |
| Step 5: CREATE TASK in D_RAW.EHMS (INFA_INGEST_WH, 5 MIN) | Yes | YES |
| Step 5: ALTER TASK RESUME | Yes | YES |
| Verification queries (commented) | Yes | YES |

---

## 4. Naming Convention Compliance — 7/7

| Convention | Pattern | All 7 Match? |
|------------|---------|---------------|
| Source: `D_RAW.EHMS.<name>_BASE` | Yes | YES |
| Target: `D_BRONZE.EHMS.<name>` | Yes | YES |
| Stream: `D_RAW.EHMS.<name>_BASE_HIST_STREAM` | Yes | YES |
| Procedure: `D_RAW.EHMS.SP_PROCESS_<name>()` | Yes | YES |
| Task: `D_RAW.EHMS.TASK_PROCESS_<name>` | Yes | YES |
| Staging: `_CDC_STAGING_<name>` | Yes | YES |

---

## 5. Filter Verification — No Purge Filter

Confirmed: **zero** occurrences of `TSDPRG`, `EMEPRG`, or `NOT IN` filter in any of the 7 scripts. All stream data is processed without filtering, per EHMS requirement.

---

## 6. Data Volume & Owner Distribution (Live Data)

| # | Table | Total Rows | NULL | EHMSMGR | All Included |
|---|-------|-----------|------|---------|-------------|
| 1 | EHMSAPP_DTQ_DTCTD_EQPMNT | 39,334,208 | 29,859,908 | 9,474,300 | YES |
| 2 | EHMSAPP_DTQ_DTCTD_EQPMNT_CMPNT | 351,360,617 | 266,243,730 | 85,116,887 | YES |
| 3 | EHMSAPP_DTQ_DTCTD_TRAIN | 423,271 | 322,672 | 100,599 | YES |
| 4 | EHMSAPP_DTQ_EQPMNT | 1,512,127 | 1,512,127 | 0 | YES |
| 5 | EHMSAPP_DTQ_PSNG_SMRY | 11,017,757 | 10,917,133 | 100,624 | YES |
| 6 | EHMSAPP_DTQ_WYSD_DEVICE_CMPNT | 3,522 | 3,522 | 0 | YES |
| 7 | EHMSAPP_DTQ_WYSD_DTCTN_DEVICE | 4,314 | 4,314 | 0 | YES |
| | **TOTALS** | **403,655,816** | **308,863,406** | **94,792,410** | **ALL** |

---

## 7. Compilation Status

| # | Script | CREATE TABLE | SP Pattern | Status |
|---|--------|-------------|-----------|--------|
| 1 | EHMSAPP_DTQ_DTCTD_EQPMNT | PASS | Verified | PASS |
| 2 | EHMSAPP_DTQ_DTCTD_EQPMNT_CMPNT | PASS | Verified | PASS |
| 3 | EHMSAPP_DTQ_DTCTD_TRAIN | PASS | Verified | PASS |
| 4 | EHMSAPP_DTQ_EQPMNT | PASS | Verified | PASS |
| 5 | EHMSAPP_DTQ_PSNG_SMRY | PASS | Verified | PASS |
| 6 | EHMSAPP_DTQ_WYSD_DEVICE_CMPNT | PASS | Verified | PASS |
| 7 | EHMSAPP_DTQ_WYSD_DTCTN_DEVICE | PASS | Verified | PASS |

---

## 8. Key Differences from SADB Pattern

| Aspect | SADB (v2_production_ready) | EHMS |
|--------|---------------------------|------|
| Schema | `D_RAW.SADB` / `D_BRONZE.SADB` | `D_RAW.EHMS` / `D_BRONZE.EHMS` |
| Purge Filter | `NVL(SNW_OPERATION_OWNER,'') NOT IN ('TSDPRG','EMEPRG')` | **NONE** |
| Owner Types | NULL, TSDPRG, EMEPRG, EDMGR, etc. | NULL, EHMSMGR |
| Execution Logging | CDC_EXECUTION_LOG | Same |
| All other patterns | Identical | Identical |

---

## 9. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 349/349 source columns = 100% |
| Data Type Accuracy | 10 | 10 | All types match DESCRIBE TABLE |
| Pattern Compliance | 10 | 10 | All 7 follow EHMS v1 reference |
| Naming Conventions | 10 | 10 | All 42 object names compliant |
| MERGE Logic | 10 | 10 | All 4+2 branches correct in all 7 |
| Filter (No Purge) | 10 | 10 | Zero filter occurrences confirmed |
| Execution Logging | 10 | 10 | CDC_EXECUTION_LOG in all 4 paths × 7 |
| Staleness Detection | 10 | 10 | Proven SELECT COUNT(*) WHERE 1=0 |
| Error Handling | 10 | 10 | Exception handler with cleanup + logging |
| Stream/Task Standards | 10 | 10 | Identical config across all 7 |
| **TOTAL** | **100** | **100** | **100%** |

---

## 10. Verdict

### **ALL 7 SCRIPTS APPROVED FOR PRODUCTION**

| Metric | Value |
|--------|-------|
| Scripts reviewed | 7 |
| Source columns mapped | 349 / 349 (100%) |
| CREATE TABLEs compiled | 7 / 7 (100%) |
| Object names compliant | 42 / 42 (100%) |
| Filter removed (per requirement) | 7 / 7 (100%) |
| Total source data volume | 403.7M rows |
| Largest table | DTCTD_EQPMNT_CMPNT (351M rows, 128 cols) |
| Schema | D_RAW.EHMS → D_BRONZE.EHMS |
| Purge filter | NONE (all rows included) |
