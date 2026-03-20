# Code Review: EVENTS_LOCOMOTIVE_MVMNT_EVENT CDC Data Preservation Script (v2.1)
**Review Date:** March 20, 2026 (Updated from March 10, 2026)  
**Reviewer:** Cortex Code  
**Script:** Scripts/AWS/EVENTS_LOCOMOTIVE_MVMNT_EVENT.sql  
**Reference:** Scripts/v2_production_ready/TRAIN_OPTRN_EVENT_v2.sql  
**Version:** v2.1 — Inline COMMENT + TASK_SP_PROCESS_ naming + Execution logging

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.AWS.EVENTS_LOCOMOTIVE_MVMNT_EVENT_BASE` (25 columns) |
| Target Table | `D_BRONZE.AWS.EVENTS_LOCOMOTIVE_MVMNT_EVENT` (25 source + 6 CDC = 31 columns) |
| Stream | `D_RAW.AWS.EVENTS_LOCOMOTIVE_MVMNT_EVENT_BASE_HIST_STREAM` |
| Procedure | `D_RAW.AWS.SP_PROCESS_EVENTS_LOCOMOTIVE_MVMNT_EVENT()` |
| Task | `D_RAW.AWS.TASK_SP_PROCESS_EVENTS_LOCOMOTIVE_MVMNT_EVENT` (5 min) |
| Primary Key | Composite: `"event_id"`, `"event_timestamp_utc"` (2 columns) |
| Schema | **AWS** (D_RAW.AWS → D_BRONZE.AWS) |
| Source System | **AWS PostgreSQL** (not Oracle SADB) |
| Filter | **NONE** (no purge filter for AWS schema) |
| Table COMMENT | **Inline** (lineage metadata) |
| Column Style | **Lowercase with double-quotes** (case-sensitive) |

---

## 2. Changes Applied in v2.1

| # | Change | Old Value | New Value | Status |
|---|--------|-----------|-----------|--------|
| 1 | Inline COMMENT | Not present | Full lineage metadata (AWS PostgreSQL source) | DONE |
| 2 | Task naming | `TASK_PROCESS_EVENTS_LOCOMOTIVE_MVMNT_EVENT` | `TASK_SP_PROCESS_EVENTS_LOCOMOTIVE_MVMNT_EVENT` | DONE |
| 3 | Execution logging | Not present | All 4 paths log to CDC_EXECUTION_LOG | DONE |
| 4 | SP variables | 6 vars | 12 vars (added I/U/D counts, start/end time, status) | DONE |
| 5 | Pre-merge metrics | Not present | COUNT by CDC_ACTION/IS_UPDATE before MERGE | DONE |
| 6 | Exception handler | Return error string only | Log to CDC_EXECUTION_LOG + return | DONE |
| 7 | Version header | Not present | v2.1, 2026-03-20 | DONE |

---

## 3. Column Mapping — 25/25 = 100%

| # | Column | Data Type | PK? | Quoted? | Mapped? |
|---|--------|-----------|:---:|:---:|---------|
| 1 | "event_id" | NUMBER(38,0) NOT NULL | PK1 | YES | YES |
| 2 | "event" | VARCHAR(80) | | YES | YES |
| 3 | "event_timestamp_utc" | TIMESTAMP_NTZ(6) NOT NULL | PK2 | YES | YES |
| 4 | "source_system" | VARCHAR(80) | | YES | YES |
| 5 | "direction" | VARCHAR(8) | | YES | YES |
| 6 | "mark_cd" | VARCHAR(16) | | YES | YES |
| 7 | "eqpun_nbr" | VARCHAR(40) | | YES | YES |
| 8 | "fsac" | VARCHAR(20) | | YES | YES |
| 9 | "latitude" | NUMBER(11,8) | | YES | YES |
| 10 | "longitude" | NUMBER(11,8) | | YES | YES |
| 11 | "mile_km_number" | NUMBER(10,2) | | YES | YES |
| 12 | "subdivision_name" | VARCHAR(200) | | YES | YES |
| 13 | "station_name" | VARCHAR(200) | | YES | YES |
| 14 | "train_plan_leg_id" | NUMBER(18,0) | | YES | YES |
| 15 | "train_name" | VARCHAR(60) | | YES | YES |
| 16 | "scac" | VARCHAR(16) | | YES | YES |
| 17 | "division" | VARCHAR(200) | | YES | YES |
| 18 | "region" | VARCHAR(200) | | YES | YES |
| 19 | "district" | VARCHAR(200) | | YES | YES |
| 20 | "state_province" | VARCHAR(12) | | YES | YES |
| 21 | "country" | VARCHAR(8) | | YES | YES |
| 22 | "subdivision_id" | NUMBER(18,0) | | YES | YES |
| 23 | "record_update_timestamp_utc" | TIMESTAMP_NTZ(6) | | YES | YES |
| 24 | SNW_OPERATION_TYPE | VARCHAR(1) | | NO | YES |
| 25 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | | NO | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID.

**Note:** 23 business columns (lowercase, quoted) + 2 SNW metadata (uppercase, unquoted) = 25 source. No SNW_OPERATION_OWNER in AWS schema.

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | CDC Cols | Composite PK Join | Result |
|--------|-------------|----------|:---:|--------|
| Recovery UPDATE | 23/23 | 5/5 (RELOADED) | 2-col AND | PASS |
| Recovery INSERT | 31 cols = 31 vals | 6/6 | N/A | PASS |
| Main UPDATE | 23/23 | 5/5 | 2-col AND | PASS |
| Main DELETE | Soft-delete only | 5/5 | 2-col AND | PASS |
| Main RE-INSERT | 23/23 | 5/5 | 2-col AND | PASS |
| Main NEW INSERT | 31 cols = 31 vals | 6/6 | N/A | PASS |

**Composite PK join:** `tgt."event_id" = src."event_id" AND tgt."event_timestamp_utc" = src."event_timestamp_utc"` — verified in Recovery and Main MERGE.

---

## 5. SP Logic Validation

### 5.1 Execution Logging (4 paths)

| Path | Status | Logged to CDC_EXECUTION_LOG? |
|------|--------|:---:|
| Recovery | `RECOVERY` | PASS |
| No data | `NO_DATA` | PASS |
| Success | `SUCCESS` | PASS |
| Error | `ERROR` | PASS |

### 5.2 SP Variables (12/12)

All 12 variables present: v_batch_id, v_stream_stale, v_staging_count, v_rows_merged, v_rows_inserted, v_rows_updated, v_rows_deleted, v_result, v_error_msg, v_start_time, v_end_time, v_execution_status. **PASS**

### 5.3 Pre-Merge Metrics

COUNT by CDC_ACTION/IS_UPDATE into v_rows_inserted, v_rows_updated, v_rows_deleted from staging table. **PASS**

### 5.4 Case-Sensitive Column Handling

| Check | Status |
|-------|:---:|
| 23 lowercase columns quoted in CREATE TABLE | PASS |
| 23 lowercase columns quoted in staging SELECT | PASS |
| 23 lowercase columns quoted in all MERGE SET assignments | PASS |
| 23 lowercase columns quoted in all INSERT/VALUES | PASS |
| SNW/CDC columns UPPERCASE without quotes | PASS |

---

## 6. AWS-Specific Differences from SADB Pattern

| Aspect | SADB Pattern | This AWS Script |
|--------|-------------|-----------------|
| Schema | D_RAW.SADB / D_BRONZE.SADB | D_RAW.AWS / D_BRONZE.AWS |
| Source | Oracle SADB via IDMC | AWS PostgreSQL via CDC |
| Purge Filter | `NVL(SNW_OPERATION_OWNER,'') NOT IN (...)` | **NONE** (no filter needed) |
| Column Casing | UPPERCASE, no quotes | **lowercase, double-quoted** |
| SNW Columns | 3 (TYPE, OWNER, REPLICATED) | **2** (TYPE, REPLICATED — no OWNER) |
| All other patterns | Same | Same |

---

## 7. Coding Standards Compliance

| Standard | Result |
|----------|:---:|
| 5-Step structure | PASS |
| Fully qualified object names (D_RAW.AWS / D_BRONZE.AWS) | PASS |
| `CREATE OR ALTER TABLE` (idempotent) | PASS |
| Inline COMMENT with lineage metadata (v2.1) | PASS |
| `TASK_SP_PROCESS_` naming (v2.1) | PASS |
| Execution logging in 4 paths (v2.1) | PASS |
| Pre-merge I/U/D metrics (v2.1) | PASS |
| 12 SP variables (v2.1) | PASS |
| VERSION header (v2.1) | PASS |
| `INFA_INGEST_WH` / `5 MINUTE` / no overlap | PASS |
| Retention 45 days / extension 15 days | PASS |
| Verification queries at bottom | PASS |

---

## 8. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 25/25 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches correct, composite PK verified |
| No Filter (AWS) | 10 | 10 | Correctly omitted for AWS schema |
| Schema (AWS) | 10 | 10 | All references use D_RAW.AWS / D_BRONZE.AWS |
| Task Naming (v2.1) | 10 | 10 | TASK_SP_PROCESS_ pattern applied |
| Table COMMENT (v2.1) | 10 | 10 | Inline COMMENT with AWS PostgreSQL source |
| Error Handling (v2.1) | 10 | 10 | All 4 paths log to CDC_EXECUTION_LOG |
| Execution Logging (v2.1) | 10 | 10 | RECOVERY, NO_DATA, SUCCESS, ERROR |
| Pre-Merge Metrics (v2.1) | 10 | 10 | I/U/D counts before MERGE |
| Data Type Accuracy | 10 | 10 | All types match DESCRIBE TABLE exactly |
| Case-Sensitive Handling | 10 | 10 | All 23 lowercase cols properly quoted |
| **TOTAL** | **110** | **110** | **100%** |

---

## 9. Verdict

**APPROVED FOR PRODUCTION (v2.1)** — 100% column mapping (25/25 source + 6/6 CDC), composite PK join validated ("event_id" + "event_timestamp_utc"), all MERGE branches correct, execution logging in all 4 paths, inline COMMENT with AWS PostgreSQL lineage, TASK_SP_PROCESS_ naming applied, case-sensitive column handling verified across all SQL statements. Score: 110/110 (100%).
