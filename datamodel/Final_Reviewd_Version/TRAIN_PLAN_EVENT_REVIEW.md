# TRAIN_PLAN_EVENT Script Review Report

**Review Date:** 2026-02-24  
**Script:** Scripts/Final/TRAIN_PLAN_EVENT.sql  
**Source Table:** D_RAW.SADB.TRAIN_PLAN_EVENT_BASE  
**Target Table:** D_BRONZE.SADB.TRAIN_PLAN_EVENT

---

## Executive Summary

| Category | Score | Status |
|----------|-------|--------|
| **Column Mapping** | 100% | ✅ PERFECT |
| **Data Type Mapping** | 100% | ✅ PERFECT |
| **Primary Key** | 100% | ✅ CORRECT |
| **SP Logic** | 100% | ✅ COMPLETE |
| **Syntax** | 100% | ✅ VALID |
| **Coding Standards** | 100% | ✅ EXCELLENT |
| **Overall Score** | **100/100** | ✅ **PRODUCTION READY** |

---

## 1. Column Mapping Validation (100% ✅)

### Source Table Columns (36 columns):
| # | Source Column | Source Type | Script Line | Match |
|---|---------------|-------------|-------------|-------|
| 1 | TRAIN_PLAN_EVENT_ID | NUMBER(18,0) NOT NULL | 19 | ✅ |
| 2 | TRAIN_PLAN_LEG_ID | NUMBER(18,0) | 20 | ✅ |
| 3 | TRAIN_EVENT_TYPE_CD | VARCHAR(16) | 21 | ✅ |
| 4 | EVENT_TMS | TIMESTAMP_NTZ(0) | 22 | ✅ |
| 5 | MTP_ROUTE_POINT_SQNC_NBR | NUMBER(3,0) | 23 | ✅ |
| 6 | TRAVEL_DRCTN_CD | VARCHAR(20) | 24 | ✅ |
| 7 | EVENT_CRTNTY_CD | VARCHAR(24) | 25 | ✅ |
| 8 | EVENT_STATUS_CD | VARCHAR(24) | 26 | ✅ |
| 9 | ANCHOR_TMS | TIMESTAMP_NTZ(0) | 27 | ✅ |
| 10 | SCAC_CD | VARCHAR(16) | 28 | ✅ |
| 11 | FSAC_CD | VARCHAR(20) | 29 | ✅ |
| 12 | TRSTN_VRSN_NBR | NUMBER(5,0) | 30 | ✅ |
| 13 | RGN_NM_TRK_NBR | NUMBER(18,0) | 31 | ✅ |
| 14 | REGION_NBR | NUMBER(18,0) | 32 | ✅ |
| 15 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | 33 | ✅ |
| 16 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | 34 | ✅ |
| 17 | CREATE_USER_ID | VARCHAR(32) | 35 | ✅ |
| 18 | UPDATE_USER_ID | VARCHAR(32) | 36 | ✅ |
| 19 | TIME_ZONE_CD | VARCHAR(8) | 37 | ✅ |
| 20 | TIME_ZONE_YEAR_NBR | NUMBER(4,0) | 38 | ✅ |
| 21 | EVENT_SOURCE_CD | VARCHAR(32) | 39 | ✅ |
| 22 | MILE_NBR | NUMBER(8,3) | 40 | ✅ |
| 23 | SCHDLD_EVENT_TMS | TIMESTAMP_NTZ(0) | 41 | ✅ |
| 24 | THRTCL_EVENT_TMS | TIMESTAMP_NTZ(0) | 42 | ✅ |
| 25 | RQRD_OMTS_RPTNG_POINT_IND | VARCHAR(4) | 43 | ✅ |
| 26 | YARD_RPRTNG_IND | VARCHAR(4) | 44 | ✅ |
| 27 | CNST_CHNG_POINT_IND | VARCHAR(4) | 45 | ✅ |
| 28 | LCMTV_CHNG_IND | VARCHAR(4) | 46 | ✅ |
| 29 | TRAIN_LINE_UP_RPRTNG_IND | VARCHAR(4) | 47 | ✅ |
| 30 | CREW_CHANGE_CD | VARCHAR(4) | 48 | ✅ |
| 31 | ROUTE_POINT_ACTVTY_IND | VARCHAR(4) | 49 | ✅ |
| 32 | PRFL_YARD_REPORT_CD | VARCHAR(4) | 50 | ✅ |
| 33 | STN_CNTXT_CD | VARCHAR(4) | 51 | ✅ |
| 34 | SBDVSN_CNTXT_CD | VARCHAR(4) | 52 | ✅ |
| 35 | SNW_OPERATION_TYPE | VARCHAR(1) | 53 | ✅ |
| 36 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | 54 | ✅ |

**Result: 36/36 columns mapped correctly (100%)**

### CDC Metadata Columns (6 additional):
| Column | Line | Purpose | Status |
|--------|------|---------|--------|
| CDC_OPERATION | 57 | Track INSERT/UPDATE/DELETE | ✅ |
| CDC_TIMESTAMP | 58 | When CDC captured | ✅ |
| IS_DELETED | 59 | Soft delete flag | ✅ |
| RECORD_CREATED_AT | 60 | Row creation time | ✅ |
| RECORD_UPDATED_AT | 61 | Row update time | ✅ |
| SOURCE_LOAD_BATCH_ID | 62 | Batch tracking | ✅ |

**Total: 42 columns (36 source + 6 CDC) ✅**

---

## 2. Primary Key Validation (100% ✅)

| Attribute | Source | Script | Match |
|-----------|--------|--------|-------|
| PK Column | TRAIN_PLAN_EVENT_ID | TRAIN_PLAN_EVENT_ID | ✅ |
| PK Type | Single | Single | ✅ |
| Target PK | Line 64 | `PRIMARY KEY (TRAIN_PLAN_EVENT_ID)` | ✅ |
| MERGE ON | Lines 141, 260 | `ON tgt.TRAIN_PLAN_EVENT_ID = src.TRAIN_PLAN_EVENT_ID` | ✅ |

---

## 3. Stored Procedure Logic Validation (100% ✅)

### Pattern Compliance:
| Pattern | Required | Line(s) | Status |
|---------|----------|---------|--------|
| EXECUTE AS CALLER | ✅ | 89 | ✅ |
| Stream staleness detection | ✅ | 105-116 | ✅ |
| Stream recovery (SHOW_INITIAL_ROWS=TRUE) | ✅ | 124-127 | ✅ |
| Staging table pattern | ✅ | 216-230 | ✅ |
| 4 MERGE scenarios | ✅ | 262-385 | ✅ |
| All columns in UPDATE | ✅ | 264-304, 317-357 | ✅ |
| Error handling | ✅ | 392-395 | ✅ |
| Temp table cleanup | ✅ | 388, 394 | ✅ |

### MERGE Scenario Coverage:
| Scenario | Condition | Lines | Columns Updated | Status |
|----------|-----------|-------|-----------------|--------|
| UPDATE | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=TRUE | 263-304 | All 36 source + 5 CDC | ✅ |
| DELETE | CDC_ACTION='DELETE' AND CDC_IS_UPDATE=FALSE | 307-313 | 4 CDC (soft delete) | ✅ |
| RE-INSERT | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=FALSE (MATCHED) | 316-357 | All 36 source + 5 CDC | ✅ |
| INSERT | CDC_ACTION='INSERT' (NOT MATCHED) | 360-385 | All 42 columns | ✅ |

### Column Mapping Verification (All Locations):

| Location | Description | Lines | Columns | Status |
|----------|-------------|-------|---------|--------|
| 1 | Target table DDL | 18-65 | 42 | ✅ |
| 2 | Staging table CTAS | 216-230 | 36+3 CDC | ✅ |
| 3 | Recovery MERGE UPDATE | 142-182 | 36 | ✅ |
| 4 | Recovery MERGE INSERT | 183-207 | 42 | ✅ |
| 5 | Main MERGE UPDATE | 264-304 | 36 | ✅ |
| 6 | Main MERGE DELETE | 308-313 | 4 CDC | ✅ |
| 7 | Main MERGE RE-INSERT | 317-357 | 36 | ✅ |
| 8 | Main MERGE INSERT | 361-385 | 42 | ✅ |

---

## 4. Syntax Validation (100% ✅)

| Check | Line(s) | Status |
|-------|---------|--------|
| CREATE TABLE IF NOT EXISTS | 18-65 | ✅ Valid |
| ALTER TABLE SET CHANGE_TRACKING | 70-73 | ✅ Valid |
| CREATE OR REPLACE STREAM | 78-81 | ✅ Valid |
| CREATE OR REPLACE PROCEDURE | 86-397 | ✅ Valid |
| MERGE statement | 242-385 | ✅ Valid |
| CREATE OR REPLACE TASK | 402-410 | ✅ Valid |
| Variable declarations | 92-98 | ✅ Valid |
| Exception handling | 392-396 | ✅ Valid |
| ALTER TASK RESUME | 412 | ✅ Valid |

---

## 5. Coding Standards (100% ✅)

| Standard | Score | Evidence |
|----------|-------|----------|
| Consistent naming | 100% | All objects follow `TRAIN_PLAN_EVENT*` pattern |
| Fully qualified names | 100% | `D_RAW.SADB.*`, `D_BRONZE.SADB.*` throughout |
| Clear section headers | 100% | STEP 1-5 clearly labeled |
| Meaningful comments | 100% | Each section has descriptive comments |
| Error messages | 100% | Informative RETURN values with timestamps |
| Batch ID format | 100% | `BATCH_YYYYMMDD_HH24MISS` |
| Indentation | 100% | Consistent formatting |
| Column alignment | 100% | Columns aligned in INSERT/UPDATE statements |

---

## 6. Task Configuration (100% ✅)

| Setting | Value | Best Practice | Status |
|---------|-------|---------------|--------|
| WAREHOUSE | INFA_INGEST_WH | ✅ Dedicated WH | ✅ |
| SCHEDULE | '5 MINUTE' | ✅ Reasonable CDC interval | ✅ |
| ALLOW_OVERLAPPING_EXECUTION | FALSE | ✅ Prevents conflicts | ✅ |
| WHEN condition | SYSTEM$STREAM_HAS_DATA() | ✅ Efficient | ✅ |
| ALTER TASK RESUME | Included (Line 412) | ✅ Task activated | ✅ |

---

## Final Verification Checklist

| Requirement | Status |
|-------------|--------|
| All 36 source columns present in target table | ✅ |
| All 36 source columns in staging table | ✅ |
| All 36 source columns in UPDATE scenarios | ✅ |
| All 42 columns in INSERT scenarios | ✅ |
| Primary key (TRAIN_PLAN_EVENT_ID) correctly defined | ✅ |
| MERGE ON clause uses PK | ✅ |
| Data types match source exactly | ✅ |
| Stream staleness handled | ✅ |
| Error handling with cleanup | ✅ |
| Task properly configured and resumed | ✅ |

---

## Verdict

### ✅ **APPROVED FOR PRODUCTION DEPLOYMENT**

**Score: 100/100**

| Category | Points |
|----------|--------|
| Column Mapping | 30/30 |
| Data Types | 15/15 |
| Primary Key | 10/10 |
| SP Logic | 20/20 |
| Syntax | 10/10 |
| Coding Standards | 10/10 |
| Task Configuration | 5/5 |
| **Total** | **100/100** |

---

## Summary

The `TRAIN_PLAN_EVENT.sql` script is **100% aligned** with the source table definition:

- ✅ All 36 source columns correctly mapped with exact data types
- ✅ Primary key (TRAIN_PLAN_EVENT_ID) correctly implemented
- ✅ All SP logic patterns implemented correctly
- ✅ All 4 MERGE scenarios handle full column updates
- ✅ Stream staleness detection and recovery
- ✅ Proper error handling with temp table cleanup
- ✅ Task properly configured with SYSTEM$STREAM_HAS_DATA()

**No modifications required. Ready for production implementation.**

---

**Reviewed By:** Snowflake CDC Expert  
**Date:** 2026-02-24  
**Status:** ✅ PRODUCTION READY
