# Code Review: EHMSAPP_DTQ_DTCTD_EQPMNT CDC Data Preservation Script
**Review Date:** March 17, 2026  
**Reviewer:** Cortex Code  
**Script:** Scripts/EHMS/EHMSAPP_DTQ_DTCTD_EQPMNT.sql  
**Reference:** Scripts/v2_production_ready/TRAIN_OPTRN_EVENT_v2.sql

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.EHMS.EHMSAPP_DTQ_DTCTD_EQPMNT_BASE` (49 columns) |
| Target Table | `D_BRONZE.EHMS.EHMSAPP_DTQ_DTCTD_EQPMNT` (49 source + 6 CDC = 55 columns) |
| Stream | `D_RAW.EHMS.EHMSAPP_DTQ_DTCTD_EQPMNT_BASE_HIST_STREAM` |
| Procedure | `D_RAW.EHMS.SP_PROCESS_EHMSAPP_DTQ_DTCTD_EQPMNT()` |
| Task | `D_RAW.EHMS.TASK_PROCESS_EHMSAPP_DTQ_DTCTD_EQPMNT` (5 min) |
| Primary Key | `DTCTD_EQPMNT_ID` (single, NUMBER(18,0)) |
| Schema | **EHMS** (not SADB) |
| Filter | **NONE** (no purge filter — per requirement) |

---

## 2. Schema Adaptation from SADB Pattern

| Aspect | SADB Pattern | EHMS Adaptation | Status |
|--------|-------------|-----------------|--------|
| Source schema | `D_RAW.SADB` | `D_RAW.EHMS` | DONE |
| Target schema | `D_BRONZE.SADB` | `D_BRONZE.EHMS` | DONE |
| SP schema | `D_RAW.SADB` | `D_RAW.EHMS` | DONE |
| Task schema | `D_RAW.SADB` | `D_RAW.EHMS` | DONE |
| Purge filter | `NVL(SNW_OPERATION_OWNER,'') NOT IN ('TSDPRG','EMEPRG')` | **Removed** | DONE |
| Execution logging | `D_BRONZE.MONITORING.CDC_EXECUTION_LOG` | Same (cross-schema) | DONE |

---

## 3. Filter Removal Verification

Confirmed: **zero** occurrences of `TSDPRG`, `EMEPRG`, or `NOT IN` filter in the script. All 3 data entry points (Recovery MERGE, Staging SELECT, Main MERGE) read stream data without filtering.

**Data impact:** All 39,034,926 rows will be included (30,419,951 NULL + 8,614,975 EHMSMGR).

---

## 4. Column Mapping (Source -> Target) — 49/49 = 100%

| # | Column | Data Type | Mapped? |
|---|--------|-----------|---------|
| 1 | DTCTD_EQPMNT_ID | NUMBER(18,0) NOT NULL | YES (PK) |
| 2 | CREATE_USER_ID | VARCHAR(32) | YES |
| 3 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 4 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | YES |
| 5 | UPDATE_USER_ID | VARCHAR(32) | YES |
| 6 | DTCTD_TRAIN_ID | NUMBER(18,0) | YES |
| 7 | EQPMNT_SQNC_NBR | NUMBER(4,0) | YES |
| 8 | RPRTD_MARK_CD | VARCHAR(16) | YES |
| 9 | RPRTD_EQPUN_NBR | VARCHAR(40) | YES |
| 10 | RPRTD_AEI_RAW_DATA_TXT | VARCHAR(1020) | YES |
| 11 | RPRTD_AXLE_QTY | NUMBER(4,0) | YES |
| 12 | RPRTD_EQPMNT_CTGRY_TXT | VARCHAR(160) | YES |
| 13 | RPRTD_EQPMNT_TYPE_CD | VARCHAR(8) | YES |
| 14 | RPRTD_EQPMNT_ORNTN_CD | VARCHAR(4) | YES |
| 15 | RPRTD_SPEED_QTY | NUMBER(5,2) | YES |
| 16 | RPRTD_WEIGHT_QTY | NUMBER(8,2) | YES |
| 17 | CNFDNC_NBR | NUMBER(6,3) | YES |
| 18 | GROSS_WEIGHT_QTY | NUMBER(7,0) | YES |
| 19 | TRUCK_QTY | NUMBER(3,0) | YES |
| 20 | TARE_WEIGHT_QTY | NUMBER(7,0) | YES |
| 21 | VRFD_EQPMNT_ID | NUMBER(18,0) | YES |
| 22 | VRFD_EQPMNT_ORNTN_CD | VARCHAR(4) | YES |
| 23 | VRFD_ORIGIN_SCAC_CD | VARCHAR(16) | YES |
| 24 | VRFD_ORIGIN_FSAC_CD | VARCHAR(20) | YES |
| 25 | VRFD_ORIGIN_TRSTN_NM | VARCHAR(120) | YES |
| 26 | VRFD_DSTNTN_SCAC_CD | VARCHAR(16) | YES |
| 27 | VRFD_DSTNTN_FSAC_CD | VARCHAR(20) | YES |
| 28 | VRFD_DSTNTN_TRSTN_NM | VARCHAR(120) | YES |
| 29 | VRFD_LOAD_EMPTY_CD | VARCHAR(4) | YES |
| 30 | VRFD_NET_WEIGHT_QTY | NUMBER(10,0) | YES |
| 31 | WEIGHT_UOM_BASIS_CD | VARCHAR(8) | YES |
| 32 | DTCTD_EQPMNT_CTGRY_CD | VARCHAR(16) | YES |
| 33 | RPRTD_TRUCK_QTY | NUMBER(2,0) | YES |
| 34 | CPR_EQPMNT_POOL_ID | VARCHAR(28) | YES |
| 35 | OWNER_MARK_CD | VARCHAR(16) | YES |
| 36 | MNTNC_RSPNSB_PARTY_CD | VARCHAR(16) | YES |
| 37 | STCC_CD | VARCHAR(28) | YES |
| 38 | CAR_OVERLOAD | NUMBER(10,3) | YES |
| 39 | RATIO_ETE | NUMBER(6,3) | YES |
| 40 | RATIO_STS | NUMBER(6,3) | YES |
| 41 | ALERT_STATUS | VARCHAR(4) | YES |
| 42 | SUMNOMINAL_A | NUMBER(8,3) | YES |
| 43 | SUMNOMINAL_B | NUMBER(8,3) | YES |
| 44 | SUMNOMINAL_L | NUMBER(8,3) | YES |
| 45 | SUMNOMINAL_R | NUMBER(8,3) | YES |
| 46 | SUMNOMINAL | NUMBER(8,3) | YES |
| 47 | SNW_OPERATION_TYPE | VARCHAR(1) | YES |
| 48 | SNW_OPERATION_OWNER | VARCHAR(256) | YES |
| 49 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID.

---

## 5. MERGE Branch Validation

| Branch | Non-PK Cols | CDC Cols | No Filter | Result |
|--------|-------------|----------|-----------|--------|
| Recovery UPDATE | 48/48 | 5/5 | YES | PASS |
| Recovery INSERT | 55 cols = 55 vals | 6/6 | YES | PASS |
| Main UPDATE | 48/48 | 5/5 | YES | PASS |
| Main DELETE | Soft-delete only | 5/5 | YES | PASS |
| Main RE-INSERT | 48/48 | 5/5 | YES | PASS |
| Main NEW INSERT | 55 cols = 55 vals | 6/6 | YES | PASS |

---

## 6. Compilation Results

| Component | Result |
|-----------|--------|
| SP `SP_PROCESS_EHMSAPP_DTQ_DTCTD_EQPMNT()` | **COMPILED SUCCESSFULLY** |
| CREATE OR ALTER TABLE | **COMPILED SUCCESSFULLY** |
| CREATE STREAM | Valid syntax |
| CREATE TASK | Valid syntax |

---

## 7. Data Volume

| Owner | Rows | Included? |
|-------|------|-----------|
| NULL | 30,419,951 | YES |
| EHMSMGR | 8,614,975 | YES |
| **Total** | **39,034,926** | **ALL** |

---

## 8. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 49/49 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Removal | 10 | 10 | No filter applied, per requirement |
| Schema Adaptation | 10 | 10 | All references use EHMS, not SADB |
| Object Naming | 10 | 10 | All 6 names match requirements |
| Error Handling | 9 | 10 | Stream stale + exception handler |
| Execution Logging | 10 | 10 | All 4 paths logged |
| Code Standards | 10 | 10 | Consistent with v2 reference pattern |
| Data Type Accuracy | 10 | 10 | All types match DESCRIBE TABLE exactly |
| Production Readiness | 10 | 10 | SP compiled, all objects valid |
| **TOTAL** | **99** | **100** | **99%** |

---

## 9. Verdict

**APPROVED FOR PRODUCTION** — 100% column mapping (49/49 source + 6/6 CDC), all MERGE branches validated, primary key `DTCTD_EQPMNT_ID` correctly applied, schema correctly changed to EHMS throughout, purge filter correctly removed per requirement, execution logging in all 4 paths, SP compiled successfully. 39M rows ready for initial load.
