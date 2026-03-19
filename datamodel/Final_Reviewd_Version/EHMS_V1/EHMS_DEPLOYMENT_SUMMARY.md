# EHMS CDC Deployment Planning — v2.1 Consolidated Summary
**Date:** March 19, 2026 (Updated from March 17, 2026)  
**Version:** v2.1 — Object rename + inline COMMENT + Task naming update  
**Schema:** D_RAW.EHMS → D_BRONZE.EHMS  
**Total Scripts:** 7  
**Total Source Columns:** 349  
**Total Rows:** ~401M  
**Filter:** NONE (no purge filter for EHMS)

---

## 1. Script Inventory (v2.1 Object Names)

| # | Script | New Source Table | New Target Table | PK | Src Cols | Tgt Cols | Rows | Status |
|---|--------|-----------------|-----------------|-----|----------|----------|------|--------|
| 1 | EHMSAPP_DTQ_DTCTD_EQPMNT.sql | `DTQ_DTCTD_EQPMNT_BASE` | `DTQ_DTCTD_EQPMNT` | DTCTD_EQPMNT_ID | 49 | 55 | 39.0M | APPROVED |
| 2 | EHMSAPP_DTQ_DTCTD_EQPMNT_CMPNT.sql | `DTQ_DTCTD_EQPMNT_CMPNT_BASE` | `DTQ_DTCTD_EQPMNT_CMPNT` | DTCTD_EQPMNT_CMPNT_ID | 128 | 134 | 351.3M | APPROVED |
| 3 | EHMSAPP_DTQ_DTCTD_TRAIN.sql | `DTQ_DTCTD_TRAIN_BASE` | `DTQ_DTCTD_TRAIN` | DTCTD_TRAIN_ID | 73 | 79 | 422K | APPROVED |
| 4 | EHMSAPP_DTQ_EQPMNT.sql | `DTQ_EQPMNT_BASE` | `DTQ_EQPMNT` | EQPMNT_ID | 16 | 22 | 1.5M | APPROVED |
| 5 | EHMSAPP_DTQ_PSNG_SMRY.sql | `DTQ_PSNG_SMRY_BASE` | `DTQ_PSNG_SMRY` | PSNG_SMRY_ID | 27 | 33 | 11.0M | APPROVED |
| 6 | EHMSAPP_DTQ_WYSD_DEVICE_CMPNT.sql | `DTQ_WYSD_DEVICE_CMPNT_BASE` | `DTQ_WYSD_DEVICE_CMPNT` | WYSD_DEVICE_CMPNT_VRSN_ID | 21 | 27 | 3.5K | APPROVED |
| 7 | EHMSAPP_DTQ_WYSD_DTCTN_DEVICE.sql | `DTQ_WYSD_DTCTN_DEVICE_BASE` | `DTQ_WYSD_DTCTN_DEVICE` | WYSD_DTCTN_DEVICE_VRSN_ID | 35 | 41 | 4.3K | APPROVED |

---

## 2. v2.1 Object Name Mapping (Old → New)

| Object Type | Old Pattern | New Pattern |
|------------|------------|------------|
| Source Table | `D_RAW.EHMS.EHMSAPP_DTQ_*_BASE` | `D_RAW.EHMS.DTQ_*_BASE` |
| Target Table | `D_BRONZE.EHMS.EHMSAPP_DTQ_*` | `D_BRONZE.EHMS.DTQ_*` |
| Stream | `EHMSAPP_DTQ_*_BASE_HIST_STREAM` | `DTQ_*_BASE_HIST_STREAM` |
| Procedure | `SP_PROCESS_EHMSAPP_DTQ_*()` | `SP_PROCESS_DTQ_*()` |
| Task | `TASK_PROCESS_EHMSAPP_DTQ_*` | `TASK_SP_PROCESS_DTQ_*` |
| Staging | `_CDC_STAGING_EHMSAPP_DTQ_*` | `_CDC_STAGING_DTQ_*` |
| Log Entry | `'EHMSAPP_DTQ_*'` | `'DTQ_*'` |

---

## 3. Recommended Deployment Order (Smallest → Largest)

| Phase | Script | New Target | Rows | Est. Load Time | Warehouse |
|-------|--------|-----------|------|----------------|-----------|
| 1 | DTQ_WYSD_DEVICE_CMPNT | `D_BRONZE.EHMS.DTQ_WYSD_DEVICE_CMPNT` | 3,522 | < 1 min | INFA_INGEST_WH |
| 2 | DTQ_WYSD_DTCTN_DEVICE | `D_BRONZE.EHMS.DTQ_WYSD_DTCTN_DEVICE` | 4,314 | < 1 min | INFA_INGEST_WH |
| 3 | DTQ_DTCTD_TRAIN | `D_BRONZE.EHMS.DTQ_DTCTD_TRAIN` | 422,918 | ~1 min | INFA_INGEST_WH |
| 4 | DTQ_EQPMNT | `D_BRONZE.EHMS.DTQ_EQPMNT` | 1,512,124 | ~2 min | INFA_INGEST_WH |
| 5 | DTQ_PSNG_SMRY | `D_BRONZE.EHMS.DTQ_PSNG_SMRY` | 11,017,371 | ~5 min | INFA_INGEST_WH |
| 6 | DTQ_DTCTD_EQPMNT | `D_BRONZE.EHMS.DTQ_DTCTD_EQPMNT` | 39,034,926 | ~15 min | INFA_INGEST_WH |
| 7 | DTQ_DTCTD_EQPMNT_CMPNT | `D_BRONZE.EHMS.DTQ_DTCTD_EQPMNT_CMPNT` | 351,307,409 | ~30-60 min | **Consider LARGE WH** |

---

## 4. Volume Comparison

```
DTQ_DTCTD_EQPMNT_CMPNT  ████████████████████████████████████  351.3M (87.5%)
DTQ_DTCTD_EQPMNT        █████                                 39.0M  (9.7%)
DTQ_PSNG_SMRY            █                                     11.0M  (2.7%)
DTQ_EQPMNT               ▏                                      1.5M  (0.4%)
DTQ_DTCTD_TRAIN          ▏                                      0.4M  (0.1%)
DTQ_WYSD_DTCTN_DEVICE    ▏                                      4.3K  (0.0%)
DTQ_WYSD_DEVICE_CMPNT    ▏                                      3.5K  (0.0%)
                                                      Total: ~401M rows
```

---

## 5. Pre-Deployment Checklist

- [ ] Verify `D_BRONZE.EHMS` schema exists
- [ ] Verify `D_BRONZE.MONITORING.CDC_EXECUTION_LOG` table exists
- [ ] Verify source tables exist with new names (e.g. `D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE`)
- [ ] Deploy scripts in Phase 1-7 order above
- [ ] After each script: verify task created and RESUMED
- [ ] After Phase 7: run validation count query (below)
- [ ] Monitor CDC_EXECUTION_LOG for first 24 hours

---

## 6. Post-Deployment Validation Query (v2.1 Names)

```sql
SELECT 'DTQ_DTCTD_EQPMNT' AS TBL,
       (SELECT COUNT(*) FROM D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE) AS SOURCE,
       (SELECT COUNT(*) FROM D_BRONZE.EHMS.DTQ_DTCTD_EQPMNT) AS TARGET
UNION ALL SELECT 'DTQ_DTCTD_EQPMNT_CMPNT',
       (SELECT COUNT(*) FROM D_RAW.EHMS.DTQ_DTCTD_EQPMNT_CMPNT_BASE),
       (SELECT COUNT(*) FROM D_BRONZE.EHMS.DTQ_DTCTD_EQPMNT_CMPNT)
UNION ALL SELECT 'DTQ_DTCTD_TRAIN',
       (SELECT COUNT(*) FROM D_RAW.EHMS.DTQ_DTCTD_TRAIN_BASE),
       (SELECT COUNT(*) FROM D_BRONZE.EHMS.DTQ_DTCTD_TRAIN)
UNION ALL SELECT 'DTQ_EQPMNT',
       (SELECT COUNT(*) FROM D_RAW.EHMS.DTQ_EQPMNT_BASE),
       (SELECT COUNT(*) FROM D_BRONZE.EHMS.DTQ_EQPMNT)
UNION ALL SELECT 'DTQ_PSNG_SMRY',
       (SELECT COUNT(*) FROM D_RAW.EHMS.DTQ_PSNG_SMRY_BASE),
       (SELECT COUNT(*) FROM D_BRONZE.EHMS.DTQ_PSNG_SMRY)
UNION ALL SELECT 'DTQ_WYSD_DEVICE_CMPNT',
       (SELECT COUNT(*) FROM D_RAW.EHMS.DTQ_WYSD_DEVICE_CMPNT_BASE),
       (SELECT COUNT(*) FROM D_BRONZE.EHMS.DTQ_WYSD_DEVICE_CMPNT)
UNION ALL SELECT 'DTQ_WYSD_DTCTN_DEVICE',
       (SELECT COUNT(*) FROM D_RAW.EHMS.DTQ_WYSD_DTCTN_DEVICE_BASE),
       (SELECT COUNT(*) FROM D_BRONZE.EHMS.DTQ_WYSD_DTCTN_DEVICE)
ORDER BY 1;
```

---

## 7. Monitoring Query (Post-Deploy, v2.1 Names)

```sql
SELECT TABLE_NAME, EXECUTION_STATUS, COUNT(*) AS EXECUTIONS,
       SUM(ROWS_PROCESSED) AS TOTAL_ROWS,
       MIN(START_TIME) AS FIRST_RUN,
       MAX(END_TIME) AS LAST_RUN
FROM D_BRONZE.MONITORING.CDC_EXECUTION_LOG
WHERE TABLE_NAME LIKE 'DTQ_%'
  AND TABLE_NAME NOT LIKE 'DTQ_DTCTD_%' OR TABLE_NAME LIKE 'DTQ_DTCTD_%'
  AND CREATED_AT >= DATEADD('DAY', -1, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1, 2;
```

---

## 8. Task Verification Query (v2.1 Names)

```sql
SHOW TASKS LIKE 'TASK_SP_PROCESS_DTQ_%' IN SCHEMA D_RAW.EHMS;
```

Expected: 7 tasks, all with state = `started`.

---

## 9. Key Differences from SADB Pattern

| Aspect | SADB Scripts | EHMS Scripts (v2.1) |
|--------|-------------|---------------------|
| Schema | D_RAW.SADB / D_BRONZE.SADB | D_RAW.EHMS / D_BRONZE.EHMS |
| Prefix | None | `EHMSAPP_` removed in v2.1 |
| Purge Filter | `NVL(SNW_OPERATION_OWNER,'') NOT IN ('TSDPRG','EMEPRG')` | **NONE** |
| Task Naming | `TASK_SP_PROCESS_*` | `TASK_SP_PROCESS_*` (aligned in v2.1) |
| Table COMMENT | Inline lineage metadata | Same (added in v2.1) |
| All other patterns | Identical | Identical |
