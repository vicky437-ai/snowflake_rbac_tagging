# Code Review: EVENTS_TRAIN_MVMNT_EVENT CDC Data Preservation Script (v2.1)
**Review Date:** March 20, 2026 (Updated from March 12, 2026)  
**Reviewer:** Cortex Code  
**Script:** Scripts/AWS/EVENTS_TRAIN_MVMNT_EVENT.sql  
**Reference:** Scripts/v2_production_ready/TRAIN_OPTRN_EVENT_v2.sql  
**Version:** v2.1 — Inline COMMENT + TASK_SP_PROCESS_ naming + Execution logging

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE` (27 columns) |
| Target Table | `D_BRONZE.AWS.EVENTS_TRAIN_MVMNT_EVENT` (27 source + 6 CDC = 33 columns) |
| Stream | `D_RAW.AWS.EVENTS_TRAIN_MVMNT_EVENT_BASE_HIST_STREAM` |
| Procedure | `D_RAW.AWS.SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT()` |
| Task | `D_RAW.AWS.TASK_SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT` (5 min) |
| Primary Key | Composite: `"event_id"`, `"event_timestamp_utc"` (2 columns) |
| Schema | **AWS** (D_RAW.AWS → D_BRONZE.AWS) |
| Source System | **AWS PostgreSQL** |
| Filter | **NONE** |
| Table COMMENT | **Inline** (lineage metadata) |
| Column Style | **Lowercase with double-quotes** (case-sensitive) |

---

## 2. Changes Applied in v2.1

| # | Change | Old Value | New Value | Status |
|---|--------|-----------|-----------|--------|
| 1 | Inline COMMENT | Not present | Full lineage metadata (AWS PostgreSQL) | DONE |
| 2 | Task naming | `TASK_PROCESS_EVENTS_TRAIN_MVMNT_EVENT` | `TASK_SP_PROCESS_EVENTS_TRAIN_MVMNT_EVENT` | DONE |
| 3 | Execution logging | Not present | All 4 paths log to CDC_EXECUTION_LOG | DONE |
| 4 | SP variables | 6 → 12 (added I/U/D, start/end time, status) | | DONE |
| 5 | Pre-merge metrics | Not present | COUNT by CDC_ACTION/IS_UPDATE | DONE |
| 6 | Exception handler | Return only | Log to CDC_EXECUTION_LOG + return | DONE |
| 7 | Version header | Not present | v2.1, 2026-03-20 | DONE |

---

## 3. Column Mapping — 27/27 = 100%

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
| 24 | "train_id" | NUMBER(38,0) | | YES | YES |
| 25 | "train_symbol" | VARCHAR(80) | | YES | YES |
| 26 | SNW_OPERATION_TYPE | VARCHAR(1) | | NO | YES |
| 27 | SNW_LAST_REPLICATED | TIMESTAMP_NTZ(9) | | NO | YES |

CDC Metadata (6/6): CDC_OPERATION, CDC_TIMESTAMP, IS_DELETED, RECORD_CREATED_AT, RECORD_UPDATED_AT, SOURCE_LOAD_BATCH_ID.

**Note:** 25 business (lowercase, quoted) + 2 SNW (uppercase, unquoted) = 27 source. +2 extra cols vs LOCOMOTIVE: "train_id", "train_symbol".

---

## 4. MERGE Branch Validation

| Branch | Non-PK Cols | CDC Cols | Composite PK Join | Result |
|--------|-------------|----------|:---:|--------|
| Recovery UPDATE | 25/25 | 5/5 (RELOADED) | 2-col AND | PASS |
| Recovery INSERT | 33 cols = 33 vals | 6/6 | N/A | PASS |
| Main UPDATE | 25/25 | 5/5 | 2-col AND | PASS |
| Main DELETE | Soft-delete only | 5/5 | 2-col AND | PASS |
| Main RE-INSERT | 25/25 | 5/5 | 2-col AND | PASS |
| Main NEW INSERT | 33 cols = 33 vals | 6/6 | N/A | PASS |

---

## 5. SP Logic Validation

| Path | Status | Logged? | Pre-Merge Metrics? |
|------|--------|:---:|:---:|
| Recovery | `RECOVERY` | PASS | N/A |
| No data | `NO_DATA` | PASS | N/A |
| Success | `SUCCESS` | PASS | PASS (I/U/D counts) |
| Error | `ERROR` | PASS | N/A |

**12/12 SP variables present. Case-sensitive quoting verified across all SQL statements.**

---

## 6. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 27/27 source + 6/6 CDC = 100% |
| MERGE Logic | 10 | 10 | All 4+2 branches, composite PK |
| No Filter (AWS) | 10 | 10 | Correctly omitted |
| Schema (AWS) | 10 | 10 | D_RAW.AWS / D_BRONZE.AWS |
| Task Naming (v2.1) | 10 | 10 | TASK_SP_PROCESS_ applied |
| Table COMMENT (v2.1) | 10 | 10 | Inline, AWS PostgreSQL source |
| Error Handling (v2.1) | 10 | 10 | All 4 paths log to CDC_EXECUTION_LOG |
| Pre-Merge Metrics (v2.1) | 10 | 10 | I/U/D counts |
| Data Type Accuracy | 10 | 10 | All types match source |
| Case-Sensitive Handling | 10 | 10 | 25 lowercase cols quoted |
| **TOTAL** | **100** | **100** | **100%** |

---

## 7. Verdict

**APPROVED FOR PRODUCTION (v2.1)** — 100% column mapping (27/27 source + 6/6 CDC), composite PK verified, execution logging in all 4 paths, inline COMMENT with AWS PostgreSQL lineage, TASK_SP_PROCESS_ naming, case-sensitive columns properly quoted. Score: 100/100 (100%).

---

## 8. AWS Scripts Consolidated Summary (2/2)

| # | Script | Src Cols | Tgt Cols | PK | v2.1 | Score |
|---|--------|---------|---------|-----|:---:|:---:|
| 1 | EVENTS_LOCOMOTIVE_MVMNT_EVENT | 25 | 31 | Composite (2) | DONE | 110/110 |
| 2 | EVENTS_TRAIN_MVMNT_EVENT | 27 | 33 | Composite (2) | DONE | 100/100 |

**Both AWS scripts approved for production (v2.1).**
