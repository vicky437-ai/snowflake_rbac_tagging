# DTQ_DTCTD_EQPMNT v1 — Final Template Test Report
## Enhanced Exception Handling + Deduplication

**Script:** `/cpkc/DTQ_DTCTD_EQPMNT_v1.sql` (Final Template)
**Date:** March 26, 2026 | **Account:** qsb28595 | **Warehouse:** NIFI_WH
**Test Method:** Clean-room — D_RAW + D_BRONZE dropped and rebuilt from scratch

---

## Result: 11/11 PASS — ALL TESTS PASSED

| TC | Test Case | Result | SP Output |
|:--|:---|:---:|:---|
| 1 | Initial Load (10 rows) | **PASS** | `SUCCESS: 10 (I:10 U:0 D:0 DUP:0)` |
| 2 | Normal CDC UPDATE (3 rows) | **PASS** | `SUCCESS: 3 (I:0 U:3 D:0 DUP:0)` |
| 3 | Normal CDC DELETE / Soft Delete (2 rows) | **PASS** | `SUCCESS: 2 (I:0 U:0 D:2 DUP:0)` |
| 4 | NO_DATA (empty stream) | **PASS** | `NO_DATA` |
| 5 | Mixed Operations (2I+2U+1D) | **PASS** | `SUCCESS: 5 (I:2 U:2 D:1 DUP:0)` |
| 6 | Duplicate PK — different timestamps (latest wins) | **PASS** | 2 dups → 1 row (NEW_MK/99.9 wins) |
| 7 | Duplicate PK — same timestamp, 3 dups | **PASS** | 3 dups → 1 row (deterministic) |
| 8 | RECOVERY MODE with duplicates in source | **PASS** | 13 source → 11 merged (2 OLD dups removed) |
| 9 | NULL PK in source — filtered | **PASS** | NULL filtered, NO_DATA returned |
| 10 | Normal mode works after recovery | **PASS** | `SUCCESS: 1 (I:0 U:1 D:0 DUP:0)` |
| 11 | Execution Log Validation (10 entries, 0 errors) | **PASS** | All statuses: SUCCESS/NO_DATA/RECOVERY |

---

## Template Capabilities (9 Layers)

### Enhanced Exception Handling (Phase 2)

| # | Layer | Implementation |
|:--|:---|:---|
| 1 | **Specific stale detection** | `WHEN STATEMENT_ERROR` + `ILIKE '%stale%/%dropped%/%invalid%'` + codes `2000,2003,2043,91901,2151`. Non-stale errors RE-RAISED. |
| 2 | **Base table validation** | `SELECT COUNT(*) FROM _BASE LIMIT 1` before recovery. If missing → `RECOVERY_FAILED` + clean exit. |
| 3 | **Nested exception handlers** | Separate `STATEMENT_ERROR` + `WHEN OTHER`. Logging wrapped in inner `BEGIN...EXCEPTION WHEN OTHER THEN NULL`. |
| 4 | **Monitoring auto-creation** | `CREATE TABLE IF NOT EXISTS CDC_EXECUTION_LOG` at SP start. |
| 5 | **SQLCODE/SQLSTATE capture** | Error messages include `(Code: ..., State: ...)` for diagnostics. |

### Data Quality Guards

| # | Layer | Implementation |
|:--|:---|:---|
| 6 | **Deduplication (normal)** | `ROW_NUMBER() OVER (PARTITION BY PK, ACTION, ISUPDATE ORDER BY SNW_LAST_REPLICATED DESC, ROW_ID DESC)` |
| 7 | **Deduplication (recovery)** | `ROW_NUMBER() OVER (PARTITION BY PK ORDER BY SNW_LAST_REPLICATED DESC)` |
| 8 | **NULL PK filter** | `WHERE DTCTD_EQPMNT_ID IS NOT NULL` in both modes |

### Pipeline Safety

| # | Layer | Implementation |
|:--|:---|:---|
| 9 | **Staging-first pattern** | Stream → Temp Table → MERGE. Stream offset safe on MERGE failure. |

---

## Execution Status Coverage

| Status | When | Logged |
|:---|:---|:---:|
| `SUCCESS` | Normal CDC processing completed | Yes |
| `NO_DATA` | Stream empty, early exit | Yes |
| `RECOVERY` | Stale stream detected, re-synced | Yes |
| `RECOVERY_FAILED` | Base table missing during recovery | Yes |
| `SQL_ERROR` | SQL compilation/execution error | Yes |
| `UNKNOWN_ERROR` | Any other unhandled error | Yes |

---

## Execution Log

| # | Batch | Status | Rows | I | U | D | Test |
|:--|:---|:---|:---:|:---:|:---:|:---:|:---|
| 1 | ...125552 | SUCCESS | 10 | 10 | 0 | 0 | TC1 |
| 2 | ...125703 | SUCCESS | 3 | 0 | 3 | 0 | TC2 |
| 3 | ...125750 | SUCCESS | 2 | 0 | 0 | 2 | TC3 |
| 4 | ...125816 | NO_DATA | 0 | 0 | 0 | 0 | TC4 |
| 5 | ...125958 | SUCCESS | 5 | 2 | 2 | 1 | TC5 |
| 6 | ...130044 | SUCCESS | 1 | 1 | 0 | 0 | TC6 (dedup) |
| 7 | ...130158 | SUCCESS | 1 | 1 | 0 | 0 | TC7 (3→1 dedup) |
| 8 | ...130407 | RECOVERY | 11 | 11 | 0 | 0 | TC8 (recovery+dedup) |
| 9 | ...130512 | NO_DATA | 0 | 0 | 0 | 0 | TC9 (NULL filtered) |
| 10 | ...130553 | SUCCESS | 1 | 0 | 1 | 0 | TC10 (post-recovery) |

---

## Bronze Final State

| Metric | Value |
|:---|:---|
| Total rows | 14 |
| Unique PKs | 14 |
| NULL PKs | **0** |
| Duplicates | **0** |
| Active (IS_DELETED=FALSE) | 11 |
| Soft-deleted (IS_DELETED=TRUE) | 3 |

---

## Potential Edge Cases Reviewed

| Edge Case | Status | Mitigation |
|:---|:---:|:---|
| IDMC DROP+RECREATE source | Covered | Recovery mode with `WHEN STATEMENT_ERROR` + specific codes |
| Source has duplicate PKs | Covered | `ROW_NUMBER()` dedup in both normal + recovery |
| Source has NULL PKs | Covered | `WHERE PK IS NOT NULL` in both modes |
| Stream stale after retention expires | Covered | Auto-detect + recreate with `SHOW_INITIAL_ROWS` |
| Base table missing during recovery | Covered | Validate before stream recreation → `RECOVERY_FAILED` |
| Monitoring table doesn't exist | Covered | `CREATE TABLE IF NOT EXISTS` at SP start |
| Logging INSERT fails during error | Covered | Nested `BEGIN...EXCEPTION WHEN OTHER THEN NULL` |
| Non-stale error (permissions, network) | Covered | `RAISE` re-raises — doesn't false-positive into recovery |
| Late-arriving records (older timestamp) | Covered | `ORDER BY SNW_LAST_REPLICATED DESC` ensures latest wins |
| MERGE fails mid-execution | Covered | Staging-first pattern preserves stream data |
| Concurrent task execution | Covered | Task `ALLOW_OVERLAPPING_EXECUTION = FALSE` |
| Batch ID collision (same second) | Low risk | Second-level granularity; Task runs 5 min apart |

---

## Template Usage for Other Tables

To create a new table's script from this template, change:
1. **Table names:** `DTQ_DTCTD_EQPMNT` → `{TABLE_NAME}` (5 places)
2. **Schema:** `D_RAW.EHMS` → `D_RAW.SADB` (if SADB table)
3. **PK column:** `DTCTD_EQPMNT_ID` → `{PK}` in MERGE ON + PARTITION BY + NULL filter
4. **Column lists:** Update all 49 columns in CTAS + MERGE SET + INSERT VALUES
5. **Purge filter:** Add `WHERE NVL(SNW_OPERATION_OWNER,'') NOT IN ('TSDPRG','EMEPRG')` for SADB tables
6. **For composite PKs:** Change `PARTITION BY` and `ON` clause to include all PK columns + NULL filter for each

---

## Sign-Off

| Field | Value |
|:---|:---|
| **Result** | **11/11 PASS** |
| **Exception layers** | 9 (5 enhanced handling + 3 data quality + 1 pipeline safety) |
| **Dedup** | Both normal + recovery modes |
| **NULL PK filter** | Both modes |
| **Zero regressions** | All original tests pass |
| **Zero errors** | 10 clean log entries |
| **Template ready** | Yes — documented substitution points for other tables |

---

*Tested: March 26, 2026 | Clean-room deployment on Snowflake account qsb28595*