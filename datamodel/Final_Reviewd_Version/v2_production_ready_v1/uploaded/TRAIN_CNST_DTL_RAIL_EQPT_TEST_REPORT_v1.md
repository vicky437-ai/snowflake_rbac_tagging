# TRAIN_CNST_DTL_RAIL_EQPT — Full Test Report (INSERT-Priority Dedup)

**Script:** `/cpkc/TRAIN_CNST_DTL_RAIL_EQPT_updated.sql`
**Date:** March 28, 2026 | **Account:** qsb28595 | **Warehouse:** NIFI_WH
**Composite PK:** `TRAIN_CNST_SMRY_ID` + `TRAIN_CNST_SMRY_VRSN_NBR` + `SQNC_NBR`
**Test Method:** Clean-room — D_RAW + D_BRONZE dropped and rebuilt from scratch

---

## Result: 14/14 PASS — ALL TESTS PASSED

| TC | Test Case | Result | SP Output |
|:--|:---|:---:|:---|
| 1 | Initial Load (10 rows) | **PASS** | `SUCCESS: 10 (I:10 U:0 D:0)` |
| 2 | Normal CDC UPDATE (3 rows) | **PASS** | `SUCCESS: 3 (I:0 U:3 D:0)` |
| 3 | Normal CDC DELETE / Soft Delete (2 rows) | **PASS** | `SUCCESS: 2 (I:0 U:0 D:2)` |
| 4 | NO_DATA (empty stream) | **PASS** | `NO_DATA` |
| 5 | Mixed Operations (2I+2U+1D) | **PASS** | `SUCCESS: 5 (I:2 U:2 D:1)` |
| 6 | Duplicate composite PK — different timestamps | **PASS** | 2→1, NEW_MK/99.9 wins |
| 7 | Partial PK match — NOT deduped | **PASS** | 3 rows kept (different PKs) |
| 8 | RECOVERY MODE with duplicates | **PASS** | 15 source → 13 merged |
| 9 | NULL in any PK column — filtered | **PASS** | 3 NULLs filtered, NO_DATA |
| 10 | Purge filter (TSDPRG/EMEPRG) | **PASS** | 2 purge filtered, 1 valid |
| **11** | **TRUNCATE+RELOAD (INSERT wins over DELETE)** | **PASS** | **14 rows (I:5 D:9), 0 dups** |
| **12** | **Real DELETE preserved after INSERT-priority** | **PASS** | Soft delete works correctly |
| 13 | Normal mode after TRUNCATE+RELOAD | **PASS** | `SUCCESS: 1 (I:0 U:1 D:0)` |
| 14 | Execution Log Validation (13 entries) | **PASS** | 13 clean rows, 0 errors |

---

## Key Fix: INSERT-Priority Dedup

### Problem (Production Bug)
IDMC TRUNCATE+RELOAD produces DELETE+INSERT pairs for every PK in the stream. The old dedup (`ORDER BY SNW_LAST_REPLICATED DESC`) was non-deterministic — DELETE could win over INSERT for the same PK, causing false soft-deletes.

### Fix Applied
```sql
ROW_NUMBER() OVER (
    PARTITION BY TRAIN_CNST_SMRY_ID, TRAIN_CNST_SMRY_VRSN_NBR, SQNC_NBR
    ORDER BY 
        CASE WHEN METADATA$ACTION = 'INSERT' THEN 0 ELSE 1 END ASC,  -- INSERT wins
        SNW_LAST_REPLICATED DESC NULLS LAST,
        METADATA$ROW_ID DESC
) AS DEDUP_RN
```

### How It Handles All 4 Scenarios

| Scenario | Stream Records per PK | Dedup Winner | Result |
|:---|:---|:---|:---|
| **Normal CDC** | 1 record (INSERT or DELETE) | Only record | Correct |
| **CDC UPDATE** | 1 INSERT+IS_UPDATE=TRUE | Only record | Correct UPDATE |
| **TRUNCATE+RELOAD** | DELETE + INSERT (same PK) | **INSERT wins** (CASE=0) | Refresh, no false delete |
| **Real DELETE** | DELETE only (no INSERT) | DELETE (only record) | Correct soft delete |

---

## TC11 Detail: TRUNCATE+RELOAD

**Setup:**
- Bronze had 17 rows (from TC1-TC10)
- TRUNCATE source `_BASE` table (stream stays active)
- Reload 5 rows into `_BASE` (subset of original 10)

**Stream contained:**
- 5 PKs with BOTH DELETE + INSERT (reloaded rows) → INSERT wins → UPDATE existing bronze rows
- 9 PKs with DELETE only (not reloaded) → soft-deleted correctly
- 3 PKs already soft-deleted → DELETE is no-op

**Result:** 17 rows, 17 unique PKs, **0 duplicates**, 4 active + 13 soft-deleted

**Verification:**
```
Active rows after TRUNCATE+RELOAD:
(100,1,1) CPKC_V2 200.00 ← refreshed with V2 data
(100,1,2) CPKC_V2 180.00 ← refreshed
(100,2,1) CPKC_V2 190.00 ← refreshed
(200,1,2) CSX_V2    0.00 ← refreshed
(300,1,1) CPKC_V2 195.00 ← refreshed (later deleted in TC12)
```

## TC12 Detail: Real DELETE After INSERT-Priority

**Verified that INSERT-priority does NOT suppress real deletes:**
- DELETE (300,1,1) from source → stream has 1 DELETE record (no INSERT pair)
- Dedup: DELETE is only record for this PK → survives with DEDUP_RN=1
- MERGE: Clause 2 fires → IS_DELETED=TRUE
- **Real delete works correctly**

---

## Execution Log

| # | Batch | Status | Rows | I | U | D | Test |
|:--|:---|:---|:---:|:---:|:---:|:---:|:---|
| 1 | ...203835 | SUCCESS | 10 | 10 | 0 | 0 | TC1 |
| 2 | ...203900 | SUCCESS | 3 | 0 | 3 | 0 | TC2 |
| 3 | ...203926 | SUCCESS | 2 | 0 | 0 | 2 | TC3 |
| 4 | ...203952 | NO_DATA | 0 | 0 | 0 | 0 | TC4 |
| 5 | ...204041 | SUCCESS | 5 | 2 | 2 | 1 | TC5 |
| 6 | ...204126 | SUCCESS | 1 | 1 | 0 | 0 | TC6 (dedup) |
| 7 | ...204213 | SUCCESS | 3 | 3 | 0 | 0 | TC7 (partial PK) |
| 8 | ...204344 | RECOVERY | 13 | 13 | 0 | 0 | TC8 (recovery+dedup) |
| 9 | ...204419 | NO_DATA | 0 | 0 | 0 | 0 | TC9 (NULL filtered) |
| 10 | ...204444 | SUCCESS | 1 | 1 | 0 | 0 | TC10 (purge) |
| **11** | **...204628** | **SUCCESS** | **14** | **5** | **0** | **9** | **TC11 (TRUNCATE+RELOAD)** |
| 12 | ...205056 | SUCCESS | 1 | 0 | 0 | 1 | TC12 (real delete) |
| 13 | ...205144 | SUCCESS | 1 | 0 | 1 | 0 | TC13 (post-reload) |

---

## Bronze Final State

| Metric | Value |
|:---|:---|
| Total rows | 17 |
| Unique composite PKs | **17** |
| Duplicates | **0** |
| Active (IS_DELETED=FALSE) | 4 |
| Soft-deleted (IS_DELETED=TRUE) | 13 |

---

## Sign-Off

| Field | Value |
|:---|:---|
| **Result** | **14/14 PASS** |
| **Key fix** | INSERT-priority in `ORDER BY CASE WHEN ACTION='INSERT' THEN 0 ELSE 1 END` |
| **TRUNCATE+RELOAD** | INSERT wins over DELETE per PK — zero duplicates, zero false deletes |
| **Real DELETE** | Still works — only affected when INSERT+DELETE coexist for same PK |
| **Recovery dedup** | PK-only (no ACTION in PARTITION BY) |
| **Composite PK** | PARTITION BY all 3 columns validated |
| **NULL PK filter** | All 3 positions validated |
| **Zero regressions** | All original 11 test cases still pass |

---

*Tested: March 28, 2026 | Clean-room deployment on Snowflake account qsb28595*