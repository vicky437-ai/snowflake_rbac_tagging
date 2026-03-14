# Code Review: TRAIN_OPTRN_EVENT v1 — Production Deployment Review
**Review Date:** March 14, 2026  
**Reviewer:** Cortex Code (Independent Review)  
**Script:** Scripts/1_production_deployment/TRAIN_OPTRN_EVENT_v1.sql  
**Previous Version:** Scripts/Bug_Fix_2026_03_05/TRAIN_OPTRN_EVENT.sql  
**Verdict:** **NOT APPROVED — 1 BLOCKER**

---

## 1. Overview

| Item | Detail |
|------|--------|
| Source Table | `D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE` (29 columns) |
| Target Table | `D_BRONZE.SADB.TRAIN_OPTRN_EVENT` (29 source + 6 CDC = 35 columns) |
| Stream | `D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE_HIST_STREAM` |
| Procedure | `D_RAW.SADB.SP_PROCESS_TRAIN_OPTRN_EVENT()` |
| Task | `D_RAW.SADB.TASK_PROCESS_TRAIN_OPTRN_EVENT` |
| Primary Key | `OPTRN_EVENT_ID` (single, NUMBER(18,0)) |
| Logging Table | `D_BRONZE.MONITORING.CDC_EXECUTION_LOG` (verified: **exists**) |

---

## 2. Changes from Previous Version (v0 → v1)

| # | Change | v0 | v1 | Impact |
|---|--------|-----|-----|--------|
| 1 | Proactive staleness check | Not present | `SYSTEM$STREAM_GET_STALE_AFTER()` | **BLOCKER — invalid function** |
| 2 | Fallback stale check | Primary method | Now secondary fallback | Good layered approach |
| 3 | Execution logging | Not present | INSERT to `CDC_EXECUTION_LOG` | Good observability |
| 4 | Pre-merge metrics | Not present | COUNT by CDC_ACTION type | Good granularity |
| 5 | New variables | 6 variables | 12 variables (+6 new) | Clean |
| 6 | Return message | Simple count | Includes I/U/D breakdown | Good |
| 7 | Logging coverage | N/A | SUCCESS, NO_DATA, RECOVERY, ERROR | Complete |
| 8 | Version header | Not present | VERSION/DATE/CHANGES block | Good |

---

## 3. CRITICAL ISSUES

### ISSUE 1: `SYSTEM$STREAM_GET_STALE_AFTER` Does Not Exist — BLOCKER

**Location:** Proactive staleness check block in SP

```sql
SELECT SYSTEM$STREAM_GET_STALE_AFTER('D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE_HIST_STREAM') 
INTO v_stale_after;
```

**Problem:** `SYSTEM$STREAM_GET_STALE_AFTER()` is **not a valid Snowflake function**. Compilation test confirms:

```
SQL compilation error: Unknown function SYSTEM$STREAM_GET_STALE_AFTER
```

**Severity:** **BLOCKER** — The CREATE OR REPLACE PROCEDURE statement will fail entirely. The SP will not be created/updated.

**Cascading Impact:**
- If this is a first deployment: no SP exists, task will fail
- If replacing existing SP: the CREATE fails, old SP remains (no damage but deployment fails silently)

**Why the exception handler does NOT save this:** The `EXCEPTION WHEN OTHER` block on the proactive check would catch a runtime error, but `SYSTEM$STREAM_GET_STALE_AFTER` is an **unknown function at compile time**. Snowflake SQL Scripting validates function references during SP compilation, not at runtime. **The entire SP body fails to compile.**

**Verified:** `SYSTEM$STREAM_HAS_DATA()` is the valid Snowflake system function for stream checks. There is no `SYSTEM$STREAM_GET_STALE_AFTER` function in Snowflake.

**Fix Options:**

**Option A (Recommended — minimal change):** Remove the proactive staleness block entirely. The fallback `SELECT COUNT(*) WHERE 1=0` approach is already proven and sufficient:
```sql
-- Remove lines 113-127 entirely
-- The fallback check (lines 132-145) handles staleness correctly
```

**Option B:** Replace with `SYSTEM$STREAM_HAS_DATA()` for a pre-check, but note this checks for data presence, not staleness:
```sql
SELECT SYSTEM$STREAM_HAS_DATA('D_RAW.SADB.TRAIN_OPTRN_EVENT_BASE_HIST_STREAM') INTO v_has_data;
```

**Option C:** Query the `STALE_AFTER` column from `SHOW STREAMS` via `RESULT_SCAN`, but this adds complexity with no significant benefit over Option A.

---

## 4. MEDIUM ISSUES

### ISSUE 2: Exception-Path Logging Could Fail Silently

**Location:** Exception handler logging INSERT

If the original error was caused by a permissions issue or `D_BRONZE` unavailability, the logging INSERT to `D_BRONZE.MONITORING.CDC_EXECUTION_LOG` will also fail, potentially masking the original error message.

**Fix:** Wrap in nested exception block:
```sql
EXCEPTION
    WHEN OTHER THEN
        v_error_msg := SQLERRM;
        DROP TABLE IF EXISTS _CDC_STAGING_TRAIN_OPTRN_EVENT;
        BEGIN
            INSERT INTO D_BRONZE.MONITORING.CDC_EXECUTION_LOG (...) VALUES (...);
        EXCEPTION
            WHEN OTHER THEN NULL;
        END;
        RETURN 'ERROR: ' || v_error_msg || ' at ' || CURRENT_TIMESTAMP()::VARCHAR;
```

### ISSUE 3: Pre-Merge Metrics Variable Naming Is Misleading

**Location:** Pre-merge metrics section

```sql
SELECT COUNT(*) INTO v_rows_inserted FROM _CDC_STAGING_TRAIN_OPTRN_EVENT 
WHERE CDC_ACTION = 'INSERT' AND CDC_IS_UPDATE = FALSE;
```

This counts rows with `ACTION=INSERT, IS_UPDATE=FALSE`, but some of these will match existing rows in target and become UPDATEs (the RE-INSERT branch). The variable name `v_rows_inserted` implies they will all be new INSERTs, which is inaccurate.

**Impact:** Logging accuracy only — no data impact.

**Fix:** Rename to `v_rows_new_or_reinserted` or add a clarifying comment.

---

## 5. LOW ISSUES

### ISSUE 4: Recovery Path Logs v_rows_inserted = 0

In the recovery path, `v_rows_inserted` is never populated (still defaults to 0), but the logging INSERT writes `ROWS_INSERTED = :v_rows_inserted`. The `ROWS_PROCESSED = :v_rows_merged` value is correct, but the I/U/D breakdown is all zeros.

**Fix:** Set `v_rows_inserted = v_rows_merged` in recovery path before logging.

### ISSUE 5: Recovery MERGE Uses SELECT src.*

Same as v0 — explicitly listing columns would be more defensive. Non-blocking, consistent with pattern.

### ISSUE 6: Monitoring Table DDL Is Commented Out

The CREATE TABLE for CDC_EXECUTION_LOG is commented out in the script. Should either be uncommented with `IF NOT EXISTS` or moved to a prerequisite script.

---

## 6. What's Good (Improvements Worth Keeping)

| Enhancement | Assessment |
|-------------|------------|
| Execution logging to CDC_EXECUTION_LOG | **Excellent** — enables monitoring dashboards |
| Logging in all 4 paths (SUCCESS, NO_DATA, RECOVERY, ERROR) | **Complete coverage** |
| Pre-merge metrics (I/U/D breakdown) | **Good operational visibility** |
| Enhanced return message with I/U/D counts | **Easier debugging** |
| Version header with change log | **Good practice** |
| v_start_time / v_end_time for duration tracking | **Good for SLA monitoring** |

---

## 7. Column Mapping — 29/29 = 100% (No Regression)

All 29 source columns + 6 CDC metadata columns verified:
- CREATE TABLE (35 columns) — PASS
- Staging SELECT (29 + 3 METADATA$) — PASS
- Recovery MERGE (27 non-PK + 5 CDC in UPDATE, 35 in INSERT) — PASS
- Main MERGE 4 branches — PASS

**No column mapping changes from previous version. Zero regressions.**

---

## 8. Scoring

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Column Mapping | 10 | 10 | 29/29 = 100%, no regression |
| MERGE Logic | 10 | 10 | All 4+2 branches correct |
| Filter Coverage | 10 | 10 | NVL-safe NOT IN at all entry points |
| Object Naming | 10 | 10 | All names correct |
| Error Handling | 8 | 10 | Exception-path logging vulnerability (-2) |
| Code Standards | 10 | 10 | Clean, well-structured, version header |
| Staleness Detection | 2 | 10 | **BLOCKER: Invalid function, SP won't compile** (-8) |
| Execution Logging | 8 | 10 | Good coverage, minor metric accuracy (-2) |
| Production Readiness | 2 | 10 | **Cannot deploy due to compile error** (-8) |
| Documentation | 10 | 10 | Version header, inline comments |
| **TOTAL** | **80** | **100** | **80%** |

---

## 9. Verdict

### **NOT APPROVED FOR PRODUCTION — 1 BLOCKER**

| # | Priority | Issue | Fix Required | Lines |
|---|----------|-------|-------------|-------|
| 1 | **BLOCKER** | `SYSTEM$STREAM_GET_STALE_AFTER` is not a valid Snowflake function — SP will not compile | Remove proactive staleness block OR replace with valid function | ~113-127 |
| 2 | MEDIUM | Exception-path logging INSERT can fail if D_BRONZE is unavailable | Wrap in nested BEGIN/EXCEPTION | Exception handler |
| 3 | MEDIUM | Pre-merge metrics variable naming misleading | Rename or add comment | Pre-merge section |
| 4 | LOW | Recovery path logs I/U/D as 0 | Set v_rows_inserted = v_rows_merged | Recovery logging |

### After Fixing Blocker:

If Issue 1 is resolved (recommended: remove the proactive staleness block), the script quality jumps to **~95/100** and would be **APPROVED FOR PRODUCTION**. The execution logging enhancement is a significant and well-implemented improvement over v0.
