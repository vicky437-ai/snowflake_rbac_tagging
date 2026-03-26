# TRAIN_CNST_DTL_RAIL_EQPT — Composite PK Dedup Test Report

**Script:** `/cpkc/TRAIN_CNST_DTL_RAIL_EQPT.sql` (with ROW_NUMBER dedup)
**Date:** March 26, 2026 | **Account:** qsb28595 | **Warehouse:** NIFI_WH
**Composite PK:** `TRAIN_CNST_SMRY_ID` + `TRAIN_CNST_SMRY_VRSN_NBR` + `SQNC_NBR`
**Test Method:** Clean-room — D_RAW + D_BRONZE dropped and rebuilt from scratch

---

## Result: 11/11 PASS — ALL TESTS PASSED

| TC | Test Case | Result | SP Output |
|:--|:---|:---:|:---|
| 1 | Initial Load (10 rows) | **PASS** | `SUCCESS: 10 (I:10 U:0 D:0)` |
| 2 | Normal CDC UPDATE (3 rows) | **PASS** | `SUCCESS: 3 (I:0 U:3 D:0)` |
| 3 | Normal CDC DELETE / Soft Delete (2 rows) | **PASS** | `SUCCESS: 2 (I:0 U:0 D:2)` |
| 4 | NO_DATA (empty stream) | **PASS** | `NO_DATA` |
| 5 | Mixed Operations (2I+2U+1D) | **PASS** | `SUCCESS: 5 (I:2 U:2 D:1)` |
| **6** | **DUPLICATE: Same composite PK, diff timestamps** | **PASS** | 2 dups → 1 row (latest wins) |
| **7** | **PARTIAL PK match (should NOT dedup)** | **PASS** | 3 rows kept (different composite PKs) |
| **8** | **RECOVERY MODE with duplicates** | **PASS** | 15 source → 13 merged (2 dups removed) |
| **9** | **NULL in any PK column** | **PASS** | 3 NULL rows filtered, NO_DATA returned |
| **10** | **Purge filter (TSDPRG/EMEPRG)** | **PASS** | 2 purge rows filtered, 1 valid inserted |
| 11 | Execution Log Validation (10 entries) | **PASS** | 10 clean rows, 0 errors |

---

## Dedup Implementation for Composite PK

### Recovery Mode
```sql
ROW_NUMBER() OVER (
    PARTITION BY src.TRAIN_CNST_SMRY_ID,
                 src.TRAIN_CNST_SMRY_VRSN_NBR,
                 src.SQNC_NBR
    ORDER BY src.SNW_LAST_REPLICATED DESC NULLS LAST
) AS DEDUP_RN
...
WHERE src.TRAIN_CNST_SMRY_ID IS NOT NULL
  AND src.TRAIN_CNST_SMRY_VRSN_NBR IS NOT NULL
  AND src.SQNC_NBR IS NOT NULL
```

### Normal Mode
```sql
ROW_NUMBER() OVER (
    PARTITION BY TRAIN_CNST_SMRY_ID,
                 TRAIN_CNST_SMRY_VRSN_NBR,
                 SQNC_NBR,
                 METADATA$ACTION,
                 METADATA$ISUPDATE
    ORDER BY SNW_LAST_REPLICATED DESC NULLS LAST,
             METADATA$ROW_ID DESC
) AS DEDUP_RN
...
WHERE TRAIN_CNST_SMRY_ID IS NOT NULL
  AND TRAIN_CNST_SMRY_VRSN_NBR IS NOT NULL
  AND SQNC_NBR IS NOT NULL
```

**Key difference:** Normal mode includes `METADATA$ACTION + METADATA$ISUPDATE` in PARTITION BY to avoid deduping an INSERT against a DELETE for the same PK.

---

## Critical Test Details

### TC6: Duplicate Composite PK — Latest Wins

**Source had 2 rows for (500,1,1):**
| MARK_CD | NET_WEIGHT | SNW_LAST_REPLICATED |
|:---|:---:|:---|
| OLD_MK | 10.0 | 2026-03-20 01:00 |
| **NEW_MK** | **99.9** | **2026-03-25 15:00** |

**Bronze result:** `NEW_MK, 99.9` — latest timestamp won.

### TC7: Partial PK Match — NOT Deduped

**3 rows sharing SMRY_ID=600 but with different version/sequence:**
| SMRY_ID | VRSN | SQNC | MARK_CD | Deduped? |
|:---:|:---:|:---:|:---|:---:|
| 600 | 1 | **1** | PART_A | NO |
| 600 | 1 | **2** | PART_B | NO |
| 600 | **2** | 1 | PART_C | NO |

All 3 inserted. Composite PK partitioning correctly treats each combination as unique.

### TC8: Recovery + Duplicates

**Source:** 15 rows (13 unique composite PKs + 2 OLD duplicates for (100,1,1) and (100,2,2))
**Result:** 13 merged. Duplicates removed, latest data preserved:
- (100,1,1): `CPKC, 99.9` won (not `DUP_OLD, 1.0`)
- (100,2,2): `UP, 110.0` won (not `DUP_OLD, 2.0`)

### TC9: NULL in Any PK Column

3 rows inserted with NULL in each PK position:
- (NULL, 1, 1) — filtered
- (700, NULL, 1) — filtered
- (700, 1, NULL) — filtered

All filtered by `WHERE ... IS NOT NULL`. SP returned NO_DATA.

---

## Execution Log

| # | Batch | Status | Rows | I | U | D | Test |
|:--|:---|:---|:---:|:---:|:---:|:---:|:---|
| 1 | ...065902 | SUCCESS | 10 | 10 | 0 | 0 | TC1 |
| 2 | ...065942 | SUCCESS | 3 | 0 | 3 | 0 | TC2 |
| 3 | ...070018 | SUCCESS | 2 | 0 | 0 | 2 | TC3 |
| 4 | ...070046 | NO_DATA | 0 | 0 | 0 | 0 | TC4 |
| 5 | ...070201 | SUCCESS | 5 | 2 | 2 | 1 | TC5 |
| 6 | ...070309 | SUCCESS | 1 | 1 | 0 | 0 | TC6 (dedup: 2→1) |
| 7 | ...070427 | SUCCESS | 3 | 3 | 0 | 0 | TC7 (partial PK: no dedup) |
| 8 | ...070622 | RECOVERY | 13 | 13 | 0 | 0 | TC8 (dedup in recovery: 15→13) |
| 9 | ...070730 | NO_DATA | 0 | 0 | 0 | 0 | TC9 (NULL PKs filtered) |
| 10 | ...070825 | SUCCESS | 1 | 1 | 0 | 0 | TC10 (purge filter) |

---

## Bronze Final State

| Metric | Value |
|:---|:---|
| Total rows | 17 |
| Unique composite PKs | 17 |
| Duplicates | **0** |
| Active (IS_DELETED=FALSE) | 14 |
| Soft-deleted (IS_DELETED=TRUE) | 3 |

---

## Sign-Off

| Field | Value |
|:---|:---|
| **Result** | **11/11 PASS** |
| **Composite PK dedup** | Validated with 3-column PARTITION BY |
| **Partial key match** | Correctly NOT deduped (different composite PKs) |
| **Recovery mode dedup** | Working (15 source → 13 merged) |
| **NULL PK handling** | All 3 NULL positions filtered |
| **Purge filter** | TSDPRG/EMEPRG excluded in both modes |
| **Zero regressions** | All original CDC tests still pass |

---

*Tested: March 26, 2026 | Clean-room deployment on Snowflake account qsb28595*