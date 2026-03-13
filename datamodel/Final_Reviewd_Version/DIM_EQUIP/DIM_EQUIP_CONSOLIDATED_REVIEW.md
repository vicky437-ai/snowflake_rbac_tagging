# Consolidated Code Review: DIM_EQUIP CDC Data Preservation Scripts
**Review Date:** March 13, 2026  
**Reviewer:** Cortex Code  
**Folder:** Scripts/DIM_EQUIP/  
**Total Scripts:** 13 SQL files  
**Reference Pattern:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql

---

## 1. Inventory — All 13 Scripts

| # | Script | Source Table | Target Table | PK | PK Type | Src Cols | Tgt Cols | Compiled |
|---|--------|-------------|--------------|-----|---------|----------|----------|----------|
| 1 | EQPMNT_POOL_ASGNMN.sql | EQPMNT_POOL_ASGNMN_BASE | D_BRONZE.SADB.EQPMNT_POOL_ASGNMN | EQPMNT_POOL_ASGNMN_ID | NUMBER(18,0) | 17 | 23 | PASS |
| 2 | EQPMV_FLEET_POOL.sql | EQPMV_FLEET_POOL_BASE | D_BRONZE.SADB.EQPMV_FLEET_POOL | FLEET_POOL_ID | NUMBER(18,0) | 14 | 20 | PASS |
| 3 | EQPMV_FLEET_SBGRP.sql | EQPMV_FLEET_SBGRP_BASE | D_BRONZE.SADB.EQPMV_FLEET_SBGRP | FLEET_SBGRP_ID | NUMBER(18,0) | 13 | 19 | PASS |
| 4 | EQPMV_FLEET_SBGRP_CTGRY.sql | EQPMV_FLEET_SBGRP_CTGRY_BASE | D_BRONZE.SADB.EQPMV_FLEET_SBGRP_CTGRY | FLEET_SBGRP_CTGRY_ID | NUMBER(18,0) | 12 | 18 | PASS |
| 5 | EQPMV_FLEET.sql | EQPMV_FLEET_BASE | D_BRONZE.SADB.EQPMV_FLEET | FLEET_ID | NUMBER(18,0) | 12 | 18 | PASS |
| 6 | EQPMV_FLEET_EQPMT_CTGRY.sql | EQPMV_FLEET_EQPMT_CTGRY_BASE | D_BRONZE.SADB.EQPMV_FLEET_EQPMT_CTGRY | FLEET_EQPMT_CTGRY_ID | NUMBER(18,0) | 13 | 19 | PASS |
| 7 | EQPMV_TARGET_EQPMT_CTGRY.sql | EQPMV_TARGET_EQPMT_CTGRY_BASE | D_BRONZE.SADB.EQPMV_TARGET_EQPMT_CTGRY | TARGET_EQPMT_CTGRY_ID | NUMBER(18,0) | 13 | 19 | PASS |
| 8 | EQPMV_TARGET_PRDCT.sql | EQPMV_TARGET_PRDCT_BASE | D_BRONZE.SADB.EQPMV_TARGET_PRDCT | TARGET_PRDCT_ID | NUMBER(18,0) | 12 | 18 | PASS |
| 9 | EQPMNT_PARTY.sql | EQPMNT_PARTY_BASE | D_BRONZE.SADB.EQPMNT_PARTY | EQPMNT_PARTY_ID | NUMBER(18,0) | 11 | 17 | PASS |
| 10 | EQPMNT_POOL.sql | EQPMNT_POOL_BASE | D_BRONZE.SADB.EQPMNT_POOL | EQPMNT_POOL_ID | NUMBER(18,0) | 32 | 38 | PASS |
| 11 | EQPMNT_MARK_OWNER_CLASS_CD.sql | EQPMNT_MARK_OWNER_CLASS_CD_BASE | D_BRONZE.SADB.EQPMNT_MARK_OWNER_CLASS_CD | MARK_OWNER_CLASS_CD | VARCHAR(8) | 10 | 16 | PASS |
| 12 | EQPMNT_NON_RGSTRD.sql | EQPMNT_NON_RGSTRD_BASE | D_BRONZE.SADB.EQPMNT_NON_RGSTRD | EQPMNT_ID | NUMBER(18,0) | 11 | 17 | PASS |
| 13 | CSTMR_RGSTRD_MARK_JN_GRP.sql | CSTMR_RGSTRD_MARK_JN_GRP_BASE | D_BRONZE.SADB.CSTMR_RGSTRD_MARK_JN_GRP | DATA_SOURCE_CD, GROUP_ROLE_CD, MARK_CD | VARCHAR (Composite 3) | 10 | 16 | PASS |

**Total: 13/13 scripts compiled successfully. 180 source columns mapped across all tables.**

---

## 2. Naming Convention Compliance — 13/13 = 100%

| Convention | Pattern | All 13 Match? |
|------------|---------|---------------|
| Source Table | `D_RAW.SADB.<name>_BASE` | YES |
| Target Table | `D_BRONZE.SADB.<name>` (removes `_BASE` suffix) | YES |
| Stream | `D_RAW.SADB.<name>_BASE_HIST_STREAM` | YES |
| Procedure | `D_RAW.SADB.SP_PROCESS_<name>()` (removes `_BASE`) | YES |
| Task | `D_RAW.SADB.TASK_PROCESS_<name>` (removes `_BASE`) | YES |
| Staging Table | `_CDC_STAGING_<name>` (removes `_BASE`) | YES |
| Schema | Source in `D_RAW.SADB`, Target in `D_BRONZE.SADB` | YES |

---

## 3. Column Mapping Accuracy — 180/180 = 100%

| # | Script | Source Cols | CREATE TABLE Cols | Staging SELECT Cols | MERGE UPDATE Cols | MERGE INSERT Cols | Mapping |
|---|--------|------------|-------------------|--------------------|--------------------|-------------------|---------|
| 1 | EQPMNT_POOL_ASGNMN | 17 | 17+6=23 | 17 | 16 non-PK | 23 | 100% |
| 2 | EQPMV_FLEET_POOL | 14 | 14+6=20 | 14 | 13 non-PK | 20 | 100% |
| 3 | EQPMV_FLEET_SBGRP | 13 | 13+6=19 | 13 | 12 non-PK | 19 | 100% |
| 4 | EQPMV_FLEET_SBGRP_CTGRY | 12 | 12+6=18 | 12 | 11 non-PK | 18 | 100% |
| 5 | EQPMV_FLEET | 12 | 12+6=18 | 12 | 11 non-PK | 18 | 100% |
| 6 | EQPMV_FLEET_EQPMT_CTGRY | 13 | 13+6=19 | 13 | 12 non-PK | 19 | 100% |
| 7 | EQPMV_TARGET_EQPMT_CTGRY | 13 | 13+6=19 | 13 | 12 non-PK | 19 | 100% |
| 8 | EQPMV_TARGET_PRDCT | 12 | 12+6=18 | 12 | 11 non-PK | 18 | 100% |
| 9 | EQPMNT_PARTY | 11 | 11+6=17 | 11 | 10 non-PK | 17 | 100% |
| 10 | EQPMNT_POOL | 32 | 32+6=38 | 32 | 31 non-PK | 38 | 100% |
| 11 | EQPMNT_MARK_OWNER_CLASS_CD | 10 | 10+6=16 | 10 | 9 non-PK | 16 | 100% |
| 12 | EQPMNT_NON_RGSTRD | 11 | 11+6=17 | 11 | 10 non-PK | 17 | 100% |
| 13 | CSTMR_RGSTRD_MARK_JN_GRP | 10 | 10+6=16 | 10 | 7 non-PK | 16 | 100% |

---

## 4. Primary Key Validation — 13/13 = 100%

| # | Script | PK Column(s) | Type | NOT NULL | CREATE TABLE PK | MERGE ON Clause | Recovery MERGE ON | Result |
|---|--------|-------------|------|----------|-----------------|-----------------|-------------------|--------|
| 1 | EQPMNT_POOL_ASGNMN | EQPMNT_POOL_ASGNMN_ID | NUMBER(18,0) | YES | YES | YES | YES | PASS |
| 2 | EQPMV_FLEET_POOL | FLEET_POOL_ID | NUMBER(18,0) | YES | YES | YES | YES | PASS |
| 3 | EQPMV_FLEET_SBGRP | FLEET_SBGRP_ID | NUMBER(18,0) | YES | YES | YES | YES | PASS |
| 4 | EQPMV_FLEET_SBGRP_CTGRY | FLEET_SBGRP_CTGRY_ID | NUMBER(18,0) | YES | YES | YES | YES | PASS |
| 5 | EQPMV_FLEET | FLEET_ID | NUMBER(18,0) | YES | YES | YES | YES | PASS |
| 6 | EQPMV_FLEET_EQPMT_CTGRY | FLEET_EQPMT_CTGRY_ID | NUMBER(18,0) | YES | YES | YES | YES | PASS |
| 7 | EQPMV_TARGET_EQPMT_CTGRY | TARGET_EQPMT_CTGRY_ID | NUMBER(18,0) | YES | YES | YES | YES | PASS |
| 8 | EQPMV_TARGET_PRDCT | TARGET_PRDCT_ID | NUMBER(18,0) | YES | YES | YES | YES | PASS |
| 9 | EQPMNT_PARTY | EQPMNT_PARTY_ID | NUMBER(18,0) | YES | YES | YES | YES | PASS |
| 10 | EQPMNT_POOL | EQPMNT_POOL_ID | NUMBER(18,0) | YES | YES | YES | YES | PASS |
| 11 | EQPMNT_MARK_OWNER_CLASS_CD | MARK_OWNER_CLASS_CD | **VARCHAR(8)** | YES | YES | YES | YES | PASS |
| 12 | EQPMNT_NON_RGSTRD | EQPMNT_ID | NUMBER(18,0) | YES | YES | YES | YES | PASS |
| 13 | CSTMR_RGSTRD_MARK_JN_GRP | DATA_SOURCE_CD, GROUP_ROLE_CD, MARK_CD | **VARCHAR (Composite 3)** | YES x3 | YES | 3-col AND | 3-col AND | PASS |

**Notable:** 11 tables use single NUMBER(18,0) PK. 1 table uses VARCHAR(8) PK. 1 table uses 3-column composite VARCHAR PK. All correctly implemented.

---

## 5. Stored Procedure Pattern Compliance — 13/13 = 100%

| Component | Expected Pattern | All 13 Match? |
|-----------|-----------------|---------------|
| RETURNS VARCHAR | Yes | YES |
| LANGUAGE SQL | Yes | YES |
| EXECUTE AS CALLER | Yes (cross-database) | YES |
| Batch ID format | `BATCH_YYYYMMDD_HH24MISS` | YES |
| Stream stale detection | `SELECT COUNT(*) WHERE 1=0` with EXCEPTION | YES |
| Recovery: recreate stream | `CREATE OR REPLACE STREAM ... SHOW_INITIAL_ROWS = TRUE` | YES |
| Recovery: MERGE | Full MERGE with RELOADED CDC_OPERATION | YES |
| Staging: temp table | `CREATE OR REPLACE TEMPORARY TABLE _CDC_STAGING_*` | YES |
| Staging: explicit columns | All source + 3 METADATA$ columns listed | YES |
| Staging: purge filter | `NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | YES |
| Empty check + early exit | `IF (v_staging_count = 0) THEN DROP + RETURN` | YES |
| Main MERGE: 4 branches | UPDATE, DELETE, RE-INSERT, NEW INSERT | YES |
| CDC_OPERATION values | UPDATE, DELETE, INSERT, RELOADED | YES |
| Exception handler | `WHEN OTHER THEN DROP + RETURN ERROR` | YES |
| Temp table cleanup | `DROP TABLE IF EXISTS` in success + exception | YES |

---

## 6. Stream & Task Standards — 13/13 = 100%

| Component | Expected | All 13 Match? |
|-----------|----------|---------------|
| Stream: SHOW_INITIAL_ROWS | TRUE | YES |
| Stream: ON TABLE points to correct source | `D_RAW.SADB.<name>_BASE` | YES |
| Stream: COMMENT present | Yes | YES |
| Change Tracking: CHANGE_TRACKING | TRUE | YES |
| Change Tracking: DATA_RETENTION_TIME_IN_DAYS | 45 | YES |
| Change Tracking: MAX_DATA_EXTENSION_TIME_IN_DAYS | 15 | YES |
| Task: WAREHOUSE | INFA_INGEST_WH | YES |
| Task: SCHEDULE | 5 MINUTE | YES |
| Task: ALLOW_OVERLAPPING_EXECUTION | FALSE | YES |
| Task: COMMENT present | Yes | YES |
| Task: CALL correct SP | Yes | YES |
| Task: ALTER TASK RESUME | Yes | YES |
| Verification queries (commented) | Yes | YES |

---

## 7. CDC Metadata Columns — Consistent Across All 13

| CDC Column | Type | INSERT | UPDATE | DELETE | RELOADED |
|------------|------|--------|--------|--------|----------|
| CDC_OPERATION | VARCHAR(10) | 'INSERT' | 'UPDATE' | 'DELETE' | 'RELOADED' |
| CDC_TIMESTAMP | TIMESTAMP_NTZ | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() |
| IS_DELETED | BOOLEAN | FALSE | FALSE | TRUE | FALSE |
| RECORD_CREATED_AT | TIMESTAMP_NTZ | CURRENT_TIMESTAMP() | (not updated) | (not updated) | (not updated) |
| RECORD_UPDATED_AT | TIMESTAMP_NTZ | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() | CURRENT_TIMESTAMP() |
| SOURCE_LOAD_BATCH_ID | VARCHAR(100) | src.BATCH_ID | src.BATCH_ID | src.BATCH_ID | src.BATCH_ID |

---

## 8. Data Type Accuracy — Verified Against DESCRIBE TABLE

| Data Type Pattern | Tables Using It | Verified |
|-------------------|-----------------|----------|
| NUMBER(18,0) for PK | 11 tables | YES |
| VARCHAR(8) for PK | EQPMNT_MARK_OWNER_CLASS_CD | YES |
| VARCHAR(40,80,16) composite PK | CSTMR_RGSTRD_MARK_JN_GRP | YES |
| VARCHAR(32) for user IDs | EQPMNT_POOL_ASGNMN, EQPMNT_PARTY, EQPMNT_POOL, EQPMNT_MARK_OWNER_CLASS_CD, EQPMNT_NON_RGSTRD, CSTMR_RGSTRD_MARK_JN_GRP | YES |
| VARCHAR(1020) for user IDs | EQPMV_FLEET_POOL, EQPMV_FLEET_SBGRP, EQPMV_FLEET_SBGRP_CTGRY, EQPMV_FLEET, EQPMV_FLEET_EQPMT_CTGRY, EQPMV_TARGET_EQPMT_CTGRY, EQPMV_TARGET_PRDCT | YES |
| TIMESTAMP_NTZ(0) for business timestamps | All 13 | YES |
| TIMESTAMP_NTZ(9) for SNW_LAST_REPLICATED | All 13 | YES |
| VARCHAR(256) for SNW_OPERATION_OWNER | All 13 | YES |
| VARCHAR(1) for SNW_OPERATION_TYPE | All 13 | YES |

---

## 9. Data Volume & Filter Impact

| # | Table | Total Rows | NULL Owner | EDMGR | TSDPRG | EMEPRG | Excluded |
|---|-------|-----------|------------|-------|--------|--------|----------|
| 1 | EQPMNT_POOL_ASGNMN_BASE | 93,927 | 93,739 | 188 | 0 | 0 | 0 |
| 2 | EQPMV_FLEET_POOL_BASE | 571 | 571 | 0 | 0 | 0 | 0 |
| 3 | EQPMV_FLEET_SBGRP_BASE | 4 | 4 | 0 | 0 | 0 | 0 |
| 4 | EQPMV_FLEET_SBGRP_CTGRY_BASE | 2 | 2 | 0 | 0 | 0 | 0 |
| 5 | EQPMV_FLEET_BASE | 63 | 63 | 0 | 0 | 0 | 0 |
| 6 | EQPMV_FLEET_EQPMT_CTGRY_BASE | 61 | 61 | 0 | 0 | 0 | 0 |
| 7 | EQPMV_TARGET_EQPMT_CTGRY_BASE | 20 | 20 | 0 | 0 | 0 | 0 |
| 8 | EQPMV_TARGET_PRDCT_BASE | 4 | 4 | 0 | 0 | 0 | 0 |
| 9 | EQPMNT_PARTY_BASE | 2,608,305 | 2,605,110 | 3,195 | 0 | 0 | 0 |
| 10 | EQPMNT_POOL_BASE | 4,256 | 4,256 | 0 | 0 | 0 | 0 |
| 11 | EQPMNT_MARK_OWNER_CLASS_CD_BASE | 16 | 16 | 0 | 0 | 0 | 0 |
| 12 | EQPMNT_NON_RGSTRD_BASE | 7,846,350 | 7,846,348 | 2 | 0 | 0 | 0 |
| 13 | CSTMR_RGSTRD_MARK_JN_GRP_BASE | 60 | 60 | 0 | 0 | 0 | 0 |
| | **TOTALS** | **10,553,639** | **10,550,254** | **3,385** | **0** | **0** | **0** |

---

## 10. Cross-Script Consistency Check

| Check | Result | Details |
|-------|--------|---------|
| No old reference names leaked | PASS | Zero occurrences of template names in any script |
| All scripts use same 5-step structure | PASS | Table → Change Tracking → Stream → SP → Task |
| All SPs use identical variable declarations | PASS | 6 variables: v_batch_id, v_stream_stale, v_staging_count, v_rows_merged, v_result, v_error_msg |
| All MERGE statements have exactly 4 branches | PASS | UPDATE + DELETE + RE-INSERT + NEW INSERT |
| Recovery MERGE uses `SELECT src.*` | 13/13 | Consistent pattern (see suggestion below) |
| Main MERGE uses explicit column lists | 13/13 | Best practice |
| Filter applied at all entry points | PASS | Recovery + Staging SELECT |
| NVL null-safe filter | PASS | `NVL(SNW_OPERATION_OWNER, '')` in all 26 locations |
| SNW_OPERATION_OWNER in target table | PASS | Present in all 13 CREATE TABLE statements |
| Compilation validated | 13/13 | All CREATE TABLE + SP compiled successfully |

---

## 11. Consolidated Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping (all 13) | 10 | 10 | 180/180 source columns = 100% |
| Primary Key Accuracy | 10 | 10 | All PKs correct incl. VARCHAR + composite |
| MERGE Logic (all 13) | 10 | 10 | All 4+2 branches correct in all scripts |
| Filter Coverage | 10 | 10 | NVL-safe NOT IN at all 26 entry points |
| Naming Conventions | 10 | 10 | All 78 object names follow convention |
| Error Handling | 9 | 10 | Stream stale + exception in all 13 |
| Code Standards | 10 | 10 | 100% pattern consistency |
| Data Type Accuracy | 10 | 10 | All types match DESCRIBE TABLE |
| Stream/Task Standards | 10 | 10 | Identical config across all 13 |
| Compilation Status | 10 | 10 | 26/26 validations passed |
| **TOTAL** | **109** | **110** | **99%** |

---

## 12. Suggestions (Non-Blocking)

1. **Low (All 13):** Recovery MERGE uses `SELECT src.*` — explicitly listing columns would be more defensive. Consistent with reference pattern, non-blocking.
2. **Info:** Consider adding `SYSTEM$STREAM_HAS_DATA` as WHEN condition on tasks to skip unnecessary SP calls when no data exists. Not in reference pattern, optional.
3. **Info:** EQPMNT_NON_RGSTRD (7.8M rows) and EQPMNT_PARTY (2.6M rows) are high-volume tables. Monitor initial load execution time on INFA_INGEST_WH.
4. **Info:** 0 TSDPRG/EMEPRG rows across all 13 tables currently. Filter is proactive/preventive — good practice for future data protection.

---

## 13. Deployment Execution Order (Recommended)

Run in this order to avoid dependency issues:

```
1. EQPMV_TARGET_PRDCT.sql            (leaf — no FK dependencies)
2. EQPMV_FLEET_SBGRP_CTGRY.sql       (leaf)
3. EQPMNT_MARK_OWNER_CLASS_CD.sql    (leaf)
4. EQPMV_TARGET_EQPMT_CTGRY.sql      (refs TARGET_PRDCT)
5. EQPMV_FLEET.sql                    (refs nothing)
6. EQPMV_FLEET_SBGRP.sql             (refs FLEET, SBGRP_CTGRY)
7. EQPMV_FLEET_POOL.sql              (refs FLEET, SBGRP)
8. EQPMV_FLEET_EQPMT_CTGRY.sql      (refs FLEET, TARGET_EQPMT_CTGRY)
9. EQPMNT_POOL.sql                    (standalone)
10. EQPMNT_POOL_ASGNMN.sql           (refs POOL)
11. EQPMNT_NON_RGSTRD.sql            (standalone — 7.8M rows, run last)
12. EQPMNT_PARTY.sql                  (standalone — 2.6M rows)
13. CSTMR_RGSTRD_MARK_JN_GRP.sql     (standalone)
```

---

## 14. Final Verdict

**ALL 13 SCRIPTS APPROVED FOR PRODUCTION**

- 180/180 source columns mapped with exact data types (100%)
- 13/13 primary keys correctly implemented (including VARCHAR and composite)
- 13/13 stored procedures compiled successfully
- 13/13 CREATE TABLE statements compiled successfully
- 78/78 Snowflake object names follow naming convention
- 26/26 filter entry points use NVL-safe NOT IN pattern
- 100% pattern consistency with reference script (TRAIN_OPTRN_EVENT.sql)
- Total data volume: 10.5M rows across 13 tables, zero rows excluded by current filter
