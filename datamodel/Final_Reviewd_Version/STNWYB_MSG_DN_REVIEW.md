# STNWYB_MSG_DN Script Review Report

**Review Date:** 2026-02-24  
**Script:** Scripts/Final/STNWYB_MSG_DN.sql  
**Source Table:** D_RAW.SADB.STNWYB_MSG_DN_BASE  
**Target Table:** D_BRONZE.SADB.STNWYB_MSG_DN

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

### Source Table Columns (130 columns):
| # | Source Column | Source Type | Script Line | Match |
|---|---------------|-------------|-------------|-------|
| 1 | STNWYB_MSG_VRSN_ID | NUMBER(18,0) NOT NULL | 19 | ✅ |
| 2 | RECORD_CREATE_TMS | TIMESTAMP_NTZ(0) | 20 | ✅ |
| 3 | CREATE_USER_ID | VARCHAR(32) | 21 | ✅ |
| 4 | RECORD_UPDATE_TMS | TIMESTAMP_NTZ(0) | 22 | ✅ |
| 5 | UPDATE_USER_ID | VARCHAR(32) | 23 | ✅ |
| 6 | CRNT_VRSN_CD | VARCHAR(4) | 24 | ✅ |
| 7 | SOURCE_CREATE_TMS | TIMESTAMP_NTZ(0) | 25 | ✅ |
| 8 | SRVASG_EQPMT_ASGNMN_ID | NUMBER(18,0) | 26 | ✅ |
| 9 | SRVASG_SHPSRV_ITEM_SHPMT_ID | NUMBER(18,0) | 27 | ✅ |
| 10 | SRVASG_SHPSRV_ITEM_SQNC_NBR | NUMBER(6,0) | 28 | ✅ |
| 11 | SRVASG_SQNC_NBR | NUMBER(6,0) | 29 | ✅ |
| 12 | SHPSRV_EQPASG_CD | VARCHAR(40) | 30 | ✅ |
| 13 | CYCLE_SERIAL_NBR | NUMBER(10,0) | 31 | ✅ |
| 14 | WYBL_NBR | NUMBER(17,0) | 32 | ✅ |
| 15 | WYBL_CREATE_LOCAL_TMS | TIMESTAMP_NTZ(0) | 33 | ✅ |
| 16 | WYBL_CREATE_TIME_CD | VARCHAR(8) | 34 | ✅ |
| 17 | SHPMT_ID | NUMBER(18,0) | 35 | ✅ |
| 18 | CREATE_DATA_SOURCE_CD | VARCHAR(40) | 36 | ✅ |
| 19 | CREATE_SOURCE_TMS | TIMESTAMP_NTZ(0) | 37 | ✅ |
| 20 | CREATE_SOURCE_USER_ID | VARCHAR(32) | 38 | ✅ |
| 21 | UPDATE_DATA_SOURCE_CD | VARCHAR(40) | 39 | ✅ |
| 22 | UPDATE_SOURCE_TMS | TIMESTAMP_NTZ(0) | 40 | ✅ |
| 23 | UPDATE_SOURCE_USER_ID | VARCHAR(32) | 41 | ✅ |
| 24 | RLS_LOCAL_TMS | TIMESTAMP_NTZ(0) | 42 | ✅ |
| 25 | RLS_TIME_CD | VARCHAR(8) | 43 | ✅ |
| 26 | SHPMT_STATUS_ID | NUMBER(18,0) | 44 | ✅ |
| 27 | WYBL_CANCEL_CD | VARCHAR(4) | 45 | ✅ |
| 28 | MARK_CD | VARCHAR(16) | 46 | ✅ |
| 29 | EQPUN_NBR | VARCHAR(40) | 47 | ✅ |
| 30 | SHPMT_TYPE_CD | VARCHAR(4) | 48 | ✅ |
| 31 | LOAD_EMPTY_CD | VARCHAR(4) | 49 | ✅ |
| 32 | WYBL_CNTNT_WEIGHT_QTY | NUMBER(10,0) | 50 | ✅ |
| 33 | WYBL_CNTNT_WEIGHT_UNIT_CD | VARCHAR(4) | 51 | ✅ |
| 34 | WEIGHT_QLFR_CD | VARCHAR(8) | 52 | ✅ |
| 35 | IN_BOND_CD | VARCHAR(4) | 53 | ✅ |
| 36 | CSA_CD | VARCHAR(4) | 54 | ✅ |
| 37 | CHECK_DIGIT_NBR | VARCHAR(8) | 55 | ✅ |
| 38 | WYBL_TARE_WEIGHT_QTY | NUMBER(6,0) | 56 | ✅ |
| 39 | WYBL_TARE_WEIGHT_UNIT_CD | VARCHAR(4) | 57 | ✅ |
| 40 | LEAD_EQPMT_ASGNMN_ID | NUMBER(18,0) | 58 | ✅ |
| 41 | LEAD_SHPSRV_ITEM_SHPMT_ID | NUMBER(18,0) | 59 | ✅ |
| 42 | LEAD_SHPSRV_ITEM_SQNC_NBR | NUMBER(6,0) | 60 | ✅ |
| 43 | LEAD_SQNC_NBR | NUMBER(6,0) | 61 | ✅ |
| 44 | LEAD_CYCLE_SERIAL_NBR | NUMBER(10,0) | 62 | ✅ |
| 45 | WYBL_MERGE_CD | VARCHAR(4) | 63 | ✅ |
| 46 | PRVS_STCC_CD | VARCHAR(28) | 64 | ✅ |
| 47 | EQPMT_POOL_ID | NUMBER(18,0) | 65 | ✅ |
| 48 | CPR_EQPMT_POOL_ID | VARCHAR(28) | 66 | ✅ |
| 49 | ORIGIN_ROUTE_POINT_ID | NUMBER(18,0) | 67 | ✅ |
| 50 | ORIGIN_SCAC_CD | VARCHAR(16) | 68 | ✅ |
| 51 | ORIGIN_FSAC_CD | VARCHAR(20) | 69 | ✅ |
| 52 | ORIGIN_TRSTN_NM | VARCHAR(120) | 70 | ✅ |
| 53 | ORIGIN_STPRV_CD | VARCHAR(8) | 71 | ✅ |
| 54 | DSTNTN_ROUTE_POINT_ID | NUMBER(18,0) | 72 | ✅ |
| 55 | DSTNTN_SCAC_CD | VARCHAR(16) | 73 | ✅ |
| 56 | DSTNTN_FSAC_CD | VARCHAR(20) | 74 | ✅ |
| 57 | DSTNTN_TRSTN_NM | VARCHAR(120) | 75 | ✅ |
| 58 | DSTNTN_STPRV_CD | VARCHAR(8) | 76 | ✅ |
| 59 | CSTMS_CLRNC_ROUTE_POINT_ID | NUMBER(18,0) | 77 | ✅ |
| 60 | CSTMS_CLRNC_SCAC_CD | VARCHAR(16) | 78 | ✅ |
| 61 | CSTMS_CLRNC_FSAC_CD | VARCHAR(20) | 79 | ✅ |
| 62 | SHPR_SHPMT_PARTY_ID | NUMBER(18,0) | 80 | ✅ |
| 63 | SHPR_CPRS_CSTMR_ID | VARCHAR(32) | 81 | ✅ |
| 64 | CNSGN_SHPMT_PARTY_ID | NUMBER(18,0) | 82 | ✅ |
| 65 | CNSGN_CPRS_CSTMR_ID | VARCHAR(32) | 83 | ✅ |
| 66 | CARE_OF_SHPMT_PARTY_ID | NUMBER(18,0) | 84 | ✅ |
| 67 | CARE_OF_CPRS_CSTMR_ID | VARCHAR(32) | 85 | ✅ |
| 68 | THIRD_PARTY_SHPMT_PARTY_ID | NUMBER(18,0) | 86 | ✅ |
| 69 | THIRD_PARTY_CPRS_CSTMR_ID | VARCHAR(32) | 87 | ✅ |
| 70 | SHPSRV_ITEM_SHPMT_ID | NUMBER(18,0) | 88 | ✅ |
| 71 | SHPSRV_ITEM_SQNC_NBR | NUMBER(6,0) | 89 | ✅ |
| 72 | INTRMD_SRVC_CD | VARCHAR(8) | 90 | ✅ |
| 73 | INTRMD_PLAN_CD | VARCHAR(12) | 91 | ✅ |
| 74 | METHOD_OF_PYMNT_CD | VARCHAR(8) | 92 | ✅ |
| 75 | BOL_RFRNC_ID | NUMBER(18,0) | 93 | ✅ |
| 76 | BOL_RFRNC_TXT | VARCHAR(200) | 94 | ✅ |
| 77 | BOL_RFRNC_LOCAL_TMS | TIMESTAMP_NTZ(0) | 95 | ✅ |
| 78 | BOL_RFRNC_LOCAL_TIME_CD | VARCHAR(8) | 96 | ✅ |
| 79 | COAL_TRAIN_RFRNC_ID | NUMBER(18,0) | 97 | ✅ |
| 80 | COAL_TRAIN_RFRNC_TXT | VARCHAR(200) | 98 | ✅ |
| 81 | STGNG_ATHRZT_RFRNC_ID | NUMBER(18,0) | 99 | ✅ |
| 82 | STGNG_ATHRZT_RFRNC_TXT | VARCHAR(200) | 100 | ✅ |
| 83 | DRCTV_RFRNC_ID | NUMBER(18,0) | 101 | ✅ |
| 84 | DRCTV_RFRNC_TXT | VARCHAR(200) | 102 | ✅ |
| 85 | DRCTV_RFRNC_LOCAL_TMS | TIMESTAMP_NTZ(0) | 103 | ✅ |
| 86 | DRCTV_RFRNC_LOCAL_TIME_CD | VARCHAR(8) | 104 | ✅ |
| 87 | DEP_ORDER_RFRNC_ID | NUMBER(18,0) | 105 | ✅ |
| 88 | DEP_ORDER_RFRNC_TXT | VARCHAR(200) | 106 | ✅ |
| 89 | DEP_ORDER_RFRNC_LOCAL_TMS | TIMESTAMP_NTZ(0) | 107 | ✅ |
| 90 | DEP_ORDER_RFRNC_LOCAL_TIME_CD | VARCHAR(8) | 108 | ✅ |
| 91 | OPRTNL_BOL_TMSTMP_RFRNC_ID | NUMBER(18,0) | 109 | ✅ |
| 92 | OPRTNL_BOL_TMSTMP_RFRNC_TXT | VARCHAR(200) | 110 | ✅ |
| 93 | CR_ASGND_RFRNC_ID | NUMBER(18,0) | 111 | ✅ |
| 94 | CR_ASGND_RFRNC_TXT | VARCHAR(200) | 112 | ✅ |
| 95 | TRNSWR_LOAD_NUMBER_RFRNC_ID | NUMBER(18,0) | 113 | ✅ |
| 96 | TRNSWR_LOAD_NUMBER_RFRNC_TXT | VARCHAR(200) | 114 | ✅ |
| 97 | MVMNT_SRVC_SHPMT_ID | NUMBER(18,0) | 115 | ✅ |
| 98 | MVMNT_SRVC_SQNC_NBR | NUMBER(6,0) | 116 | ✅ |
| 99 | MVMNT_ATHRTY_CD | VARCHAR(8) | 117 | ✅ |
| 100 | RTNG_SQNC_CD | VARCHAR(8) | 118 | ✅ |
| 101 | SWITCH_CD | VARCHAR(8) | 119 | ✅ |
| 102 | CSTMS_CLRNC_CD | VARCHAR(4) | 120 | ✅ |
| 103 | DLVR_TO_SCAC_CD | VARCHAR(16) | 121 | ✅ |
| 104 | ORIGIN_SWITCH_SCAC_CD | VARCHAR(16) | 122 | ✅ |
| 105 | SHPMT_ROUTE_ID | NUMBER(18,0) | 123 | ✅ |
| 106 | RVN_CD | VARCHAR(4) | 124 | ✅ |
| 107 | ADTNL_LADING_ITEM_CD | VARCHAR(4) | 125 | ✅ |
| 108 | EQPMT_LOAD_LADING_ITEM_ID | NUMBER(18,0) | 126 | ✅ |
| 109 | DNG_WEIGHT_QTY | NUMBER(6,0) | 127 | ✅ |
| 110 | DNG_WEIGHT_UNIT_CD | VARCHAR(4) | 128 | ✅ |
| 111 | DNG_WEIGHT_QLFR_CD | VARCHAR(8) | 129 | ✅ |
| 112 | GRAIN_TYPE_CD | VARCHAR(8) | 130 | ✅ |
| 113 | GRAIN_GRADE_CD | VARCHAR(16) | 131 | ✅ |
| 114 | PRTCTV_SRVC_SHPMT_ID | NUMBER(18,0) | 132 | ✅ |
| 115 | PRTCTV_SRVC_SQNC_NBR | NUMBER(6,0) | 133 | ✅ |
| 116 | PRTCTV_SRVC_CD | VARCHAR(16) | 134 | ✅ |
| 117 | PRTCTV_SRVC_RULE_CD | VARCHAR(36) | 135 | ✅ |
| 118 | PRTCTV_SRVC_TMPRTR_QTY | NUMBER(8,4) | 136 | ✅ |
| 119 | PRTCTV_SRVC_TMPRTR_UOM_CD | VARCHAR(8) | 137 | ✅ |
| 120 | SHPMT_ITEM_SHPMT_ID | NUMBER(18,0) | 138 | ✅ |
| 121 | SHPMT_ITEM_SQNC_NBR | NUMBER(6,0) | 139 | ✅ |
| 122 | STCC_CD | VARCHAR(28) | 140 | ✅ |
| 123 | ADTNL_HNDLNG_DSCRPT_CD | VARCHAR(4) | 141 | ✅ |
| 124 | PBLSHD_WYBL_VRSN_NBR | NUMBER(4,0) | 142 | ✅ |
| 125 | FRA_APRVL_AGRMNT_NBR_RFRNC_ID | NUMBER(18,0) | 143 | ✅ |
| 126 | FRA_APRVL_AGRMNT_NBR_RFRNC_TXT | VARCHAR(200) | 144 | ✅ |
| 127 | DESPACHO_HOLD_IND | VARCHAR(4) | 145 | ✅ |
| 128 | CBP_HOLD_IND | VARCHAR(4) | 146 | ✅ |
| 129 | SNW_OPERATION_TYPE | VARCHAR(1) | 147 | ✅ |
| 130 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | 148 | ✅ |

**Result: 130/130 columns mapped correctly (100%)**

### CDC Metadata Columns (6 additional):
| Column | Line | Purpose | Status |
|--------|------|---------|--------|
| CDC_OPERATION | 150 | Track INSERT/UPDATE/DELETE | ✅ |
| CDC_TIMESTAMP | 151 | When CDC captured | ✅ |
| IS_DELETED | 152 | Soft delete flag | ✅ |
| RECORD_CREATED_AT | 153 | Row creation time | ✅ |
| RECORD_UPDATED_AT | 154 | Row update time | ✅ |
| SOURCE_LOAD_BATCH_ID | 155 | Batch tracking | ✅ |

**Total: 136 columns (130 source + 6 CDC) ✅**

---

## 2. Primary Key Validation (100% ✅)

| Attribute | Source | Script | Match |
|-----------|--------|--------|-------|
| PK Column | STNWYB_MSG_VRSN_ID | STNWYB_MSG_VRSN_ID | ✅ |
| PK Type | Single | Single | ✅ |
| Target PK | Line 157 | `PRIMARY KEY (STNWYB_MSG_VRSN_ID)` | ✅ |
| MERGE ON | Lines 234, 513 | `ON tgt.STNWYB_MSG_VRSN_ID = src.STNWYB_MSG_VRSN_ID` | ✅ |

---

## 3. Stored Procedure Logic Validation (100% ✅)

### Pattern Compliance:
| Pattern | Required | Line(s) | Status |
|---------|----------|---------|--------|
| EXECUTE AS CALLER | ✅ | 182 | ✅ |
| Stream staleness detection | ✅ | 198-209 | ✅ |
| Stream recovery (SHOW_INITIAL_ROWS=TRUE) | ✅ | 217-220 | ✅ |
| Staging table pattern | ✅ | 435-466 | ✅ |
| 4 MERGE scenarios | ✅ | 516-858 | ✅ |
| All columns in UPDATE | ✅ | 518-651, 665-798 | ✅ |
| Error handling | ✅ | 865-868 | ✅ |
| Temp table cleanup | ✅ | 861, 867 | ✅ |

### MERGE Scenario Coverage:
| Scenario | Condition | Lines | Columns Updated | Status |
|----------|-----------|-------|-----------------|--------|
| UPDATE | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=TRUE | 516-651 | All 129 non-PK source + 5 CDC | ✅ |
| DELETE | CDC_ACTION='DELETE' AND CDC_IS_UPDATE=FALSE | 654-660 | 4 CDC (soft delete) | ✅ |
| RE-INSERT | CDC_ACTION='INSERT' AND CDC_IS_UPDATE=FALSE (MATCHED) | 663-798 | All 129 non-PK source + 5 CDC | ✅ |
| INSERT | CDC_ACTION='INSERT' (NOT MATCHED) | 801-858 | All 136 columns | ✅ |

### Staging Table Column Verification:
| Location | Lines | All 130 Source Columns | Status |
|----------|-------|----------------------|--------|
| Staging SELECT | 437-465 | ✅ Complete | ✅ |
| MERGE Source SELECT | 481-510 | ✅ Complete | ✅ |
| Recovery MERGE INSERT | 371-425 | ✅ Complete | ✅ |
| Main MERGE INSERT | 803-857 | ✅ Complete | ✅ |

---

## 4. Syntax Validation (100% ✅)

| Check | Line(s) | Status |
|-------|---------|--------|
| CREATE TABLE IF NOT EXISTS | 18-158 | ✅ Valid |
| ALTER TABLE SET CHANGE_TRACKING | 163-166 | ✅ Valid |
| CREATE OR REPLACE STREAM | 171-174 | ✅ Valid |
| CREATE OR REPLACE PROCEDURE | 179-870 | ✅ Valid |
| MERGE statement (Recovery) | 222-426 | ✅ Valid |
| MERGE statement (Main) | 478-858 | ✅ Valid |
| CREATE OR REPLACE TASK | 875-883 | ✅ Valid |
| Variable declarations | 185-191 | ✅ Valid |
| Exception handling | 865-868 | ✅ Valid |
| ALTER TASK RESUME | 885 | ✅ Valid |

---

## 5. Coding Standards (100% ✅)

| Standard | Score | Evidence |
|----------|-------|----------|
| Consistent naming | 100% | All objects follow `STNWYB_MSG_DN*` pattern |
| Fully qualified names | 100% | `D_RAW.SADB.*`, `D_BRONZE.SADB.*` throughout |
| Clear section headers | 100% | STEP 1-5 clearly labeled |
| Meaningful comments | 100% | Each section has descriptive comments |
| Error messages | 100% | Informative RETURN values with timestamps |
| Batch ID format | 100% | `BATCH_YYYYMMDD_HH24MISS` |
| Indentation | 100% | Consistent formatting |
| Column alignment | 100% | Columns aligned in INSERT/UPDATE statements |
| Header documentation | 100% | Complete header with column count (Line 11) |

---

## 6. Task Configuration (100% ✅)

| Setting | Value | Best Practice | Status |
|---------|-------|---------------|--------|
| WAREHOUSE | INFA_INGEST_WH | ✅ Dedicated WH | ✅ |
| SCHEDULE | '5 MINUTE' | ✅ Reasonable CDC interval | ✅ |
| ALLOW_OVERLAPPING_EXECUTION | FALSE | ✅ Prevents conflicts | ✅ |
| WHEN condition | SYSTEM$STREAM_HAS_DATA() | ✅ Efficient | ✅ |
| ALTER TASK RESUME | Included (Line 885) | ✅ Task activated | ✅ |

---

## 7. Change Tracking Configuration (100% ✅)

| Setting | Value | Line | Status |
|---------|-------|------|--------|
| CHANGE_TRACKING | TRUE | 164 | ✅ |
| DATA_RETENTION_TIME_IN_DAYS | 45 | 165 | ✅ |
| MAX_DATA_EXTENSION_TIME_IN_DAYS | 15 | 166 | ✅ |

---

## 8. Header Documentation Accuracy ✅

| Item | Header Value | Actual | Status |
|------|--------------|--------|--------|
| Total Columns | "130 source + 6 CDC = 136" | 130 + 6 = 136 | ✅ |
| Primary Key | "STNWYB_MSG_VRSN_ID (Single)" | Single PK | ✅ |

---

## Final Verification Checklist

| Requirement | Status |
|-------------|--------|
| All 130 source columns present in target table | ✅ |
| All 130 source columns in staging table | ✅ |
| All 130 source columns in UPDATE scenarios | ✅ |
| All 136 columns in INSERT scenarios | ✅ |
| Primary key (STNWYB_MSG_VRSN_ID) correctly defined | ✅ |
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

The `STNWYB_MSG_DN.sql` script is **100% aligned** with the source table definition:

- ✅ All 130 source columns correctly mapped with exact data types
- ✅ 6 CDC metadata columns properly added (total 136)
- ✅ Primary key (STNWYB_MSG_VRSN_ID) correctly implemented
- ✅ All 4 MERGE scenarios handle full column updates
- ✅ Stream staleness detection and recovery mechanism
- ✅ Proper error handling with temp table cleanup
- ✅ Task properly configured with SYSTEM$STREAM_HAS_DATA()
- ✅ Change tracking with 45-day retention configured
- ✅ This is the **largest table** in the batch with 130 source columns

**No modifications required. Ready for production implementation.**

---

**Reviewed By:** Snowflake CDC Expert  
**Date:** 2026-02-24  
**Status:** ✅ PRODUCTION READY
