# Production Script Test Report — DTQ_DTCTD_EQPMNT.sql

**Script:** `/cpkc/DTQ_DTCTD_EQPMNT.sql` (Production-deployed version)
**Date:** March 25, 2026 | **Account:** qsb28595 | **Warehouse:** NIFI_WH
**Test Method:** Clean-room — D_RAW + D_BRONZE dropped and rebuilt from scratch

---

## Result: 7/7 PASS — ALL TESTS PASSED (zero errors, zero retries)

| TC | Test Case | Expected | Actual | Result |
|:--|:---|:---|:---|:---:|
| 1 | Initial Load (10 rows) | 10 INSERTs | `SUCCESS: 10 changes (I:10 U:0 D:0)` | **PASS** |
| 2 | Normal CDC UPDATE (3 rows) | 3 UPDATEs | `SUCCESS: 3 changes (I:0 U:3 D:0)` | **PASS** |
| 3 | Normal CDC DELETE / Soft Delete (2 rows) | 2 DELETEs, IS_DELETED=TRUE, data preserved | `SUCCESS: 2 changes (I:0 U:0 D:2)` | **PASS** |
| 4 | NO_DATA (empty stream) | Early exit, 0 rows | `NO_DATA: No changes` | **PASS** |
| 5 | Mixed Operations (2I+2U+1D) | 5 total changes | `SUCCESS: 5 changes (I:2 U:2 D:1)` | **PASS** |
| 6 | RECOVERY MODE (DROP+RECREATE source) | Detect stale, recreate stream, re-sync | `RECOVERY_COMPLETE: 9 rows merged` | **PASS** |
| 7 | Execution Log Validation | 6 clean entries | 6 rows, 0 errors | **PASS** |

---

## Execution Log (Complete)

| # | Batch ID | Status | Rows | I | U | D | Error |
|:--|:---|:---|:---:|:---:|:---:|:---:|:---|
| 1 | BATCH_20260325_214007 | SUCCESS | 10 | 10 | 0 | 0 | — |
| 2 | BATCH_20260325_214119 | SUCCESS | 3 | 0 | 3 | 0 | — |
| 3 | BATCH_20260325_214215 | SUCCESS | 2 | 0 | 0 | 2 | — |
| 4 | BATCH_20260325_214248 | NO_DATA | 0 | 0 | 0 | 0 | — |
| 5 | BATCH_20260325_214337 | SUCCESS | 5 | 2 | 2 | 1 | — |
| 6 | BATCH_20260325_214540 | RECOVERY | 9 | 9 | 0 | 0 | Base table dropped... |

---

## Data Verification

### After TC1 (Initial Load)
- Bronze: 10 rows, all CDC_OPERATION='INSERT', all IS_DELETED=FALSE

### After TC2 (UPDATE)
- IDs 1001/1003/1005: ALERT_STATUS='A', RPRTD_SPEED_QTY=60.00, CDC_OPERATION='UPDATE'

### After TC3 (DELETE)
- IDs 1009/1010: IS_DELETED=TRUE, CDC_OPERATION='DELETE', RPRTD_MARK_CD='NS' (data preserved)

### After TC5 (Mixed)
- 2001/2002: new inserts
- 1002/1004: ALERT_STATUS='W' (updated)
- 1008: IS_DELETED=TRUE (soft-deleted)

### After TC6 (Recovery)
- 9 existing rows: CDC_OPERATION='RELOADED', IS_DELETED=FALSE
- Stream recreated and healthy

---

## Key Finding: WHEN OTHER vs Specific Error Codes

The production script uses `WHEN OTHER` for stale detection (catches ALL exceptions), while the v1 script uses `WHEN STATEMENT_ERROR` with specific error code matching. For recovery mode:

| Approach | Production (`WHEN OTHER`) | v1 (`WHEN STATEMENT_ERROR + codes`) |
|:---|:---|:---|
| Catches `dropped` (91901) | Yes | **No** (without fix) |
| Catches `stale` | Yes | Yes |
| Catches `invalid` | Yes | Yes |
| Catches unrelated errors | Yes (potential false positive) | No (more precise) |
| Recovery mode triggers correctly | **Yes** | **Requires fix #3** |

**Conclusion:** The production script's `WHEN OTHER` approach is simpler and more robust for the DROP+RECREATE scenario. It catches all stream errors as stale triggers. The trade-off (potential false positive on non-stale errors) is acceptable because the recovery path (recreate stream + re-sync) is idempotent and safe.

---

## Sign-Off

| Field | Value |
|:---|:---|
| **Script** | `/cpkc/DTQ_DTCTD_EQPMNT.sql` (production version) |
| **Result** | **7/7 PASS — ALL TESTS PASSED** |
| **Errors** | 0 |
| **Retries** | 0 |
| **Recovery Mode** | Triggers correctly on first attempt |
| **Bugs Found** | **0** (production script works correctly as-is) |

---

*Tested: March 25, 2026 | Clean-room deployment on Snowflake account qsb28595*