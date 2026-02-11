# SCD Type 2 Implementation Guide
## Slowly Changing Dimension Type 2 for TRAIN_PLAN_LEG

---

## What is SCD Type 2?

**Slowly Changing Dimension Type 2 (SCD2)** is a data warehousing technique that **preserves complete history** of all changes to dimension records. When an attribute changes, instead of overwriting the old value, a **new row is created** with the updated values while the previous row is marked as historical.

---

## When to Use SCD Type 2

### ✅ **Ideal Scenarios for SCD Type 2**

| Scenario | Example | Why SCD2? |
|----------|---------|-----------|
| **Regulatory Compliance** | Financial reporting, audit trails | Must prove what data looked like at any point in time |
| **Historical Analysis** | Train schedule changes over time | Analyze how train plans evolved |
| **Point-in-Time Reporting** | "What was the status on June 15?" | Query exact state at any historical moment |
| **Trend Analysis** | Track how attributes change over time | Understand patterns and changes |
| **Legal/Contractual** | SLA tracking, compliance | Need evidence of historical states |
| **Slowly Changing Attributes** | Train direction, route completion status | Values change infrequently but history matters |

### ❌ **When NOT to Use SCD Type 2**

| Scenario | Better Alternative |
|----------|-------------------|
| Real-time operational data | SCD Type 1 (overwrite) |
| High-frequency changes (every second) | Event/fact tables |
| Only current state needed | SCD Type 1 |
| Large dimension with frequent changes | SCD Type 4 (mini-dimension) |
| Storage is severely limited | SCD Type 1 or Type 3 |

---

## Key Benefits of SCD Type 2

### 1. **Complete Audit Trail**
- Every change is preserved with timestamps
- Know exactly who changed what and when
- Supports compliance requirements (SOX, GDPR, etc.)

### 2. **Point-in-Time Analysis**
```sql
-- Query: What was the train plan status on a specific date?
SELECT * FROM TRAIN_PLAN_LEG_CURR_DT_2
WHERE TRAIN_PLAN_LEG_ID = 12345
  AND '2024-06-15 10:00:00' BETWEEN DW_EFFECTIVE_TS AND DW_EXPIRY_TS;
```

### 3. **Version Comparison**
```sql
-- Query: Show all versions of a specific record
SELECT * FROM TRAIN_PLAN_LEG_CURR_DT_2
WHERE TRAIN_PLAN_LEG_ID = 12345
ORDER BY DW_VERSION_NBR;
```

### 4. **Trend Analysis**
```sql
-- Query: How many times has this record changed?
SELECT TRAIN_PLAN_LEG_ID, MAX(DW_VERSION_NBR) as TOTAL_VERSIONS
FROM TRAIN_PLAN_LEG_CURR_DT_2
GROUP BY TRAIN_PLAN_LEG_ID
HAVING MAX(DW_VERSION_NBR) > 1;
```

### 5. **Accurate Historical Reporting**
- Reports always show data as it existed at that time
- Eliminates "data drift" issues in historical reports

---

## SCD Type 2 Column Definitions

| Column | Purpose | Example Value |
|--------|---------|---------------|
| `TRAIN_PLAN_LEG_SK` | Surrogate key (unique per version) | `a1b2c3d4...` |
| `TRAIN_PLAN_LEG_ID` | Natural/Business key | `12345` |
| `DW_EFFECTIVE_TS` | When this version became active | `2024-01-15 08:30:00` |
| `DW_EXPIRY_TS` | When this version was superseded | `2024-06-20 14:22:59` |
| `DW_IS_CURRENT` | Is this the latest version? | `TRUE` / `FALSE` |
| `DW_IS_DELETED` | Was this record soft-deleted? | `TRUE` / `FALSE` |
| `DW_VERSION_NBR` | Version sequence number | `1`, `2`, `3`... |
| `DW_RECORD_HASH` | Hash of tracked attributes | `e5f6g7h8...` |
| `DW_LOAD_TS` | When record was loaded to DW | `2024-06-20 14:23:00` |
| `DW_RECORD_SOURCE` | Source of the record | `BASE` / `CDC` |

---

## Common Query Patterns

### Get Current State Only
```sql
SELECT * FROM D_SILVER.SADB.TRAIN_PLAN_LEG_CURRENT_V;
-- OR
SELECT * FROM TRAIN_PLAN_LEG_CURR_DT_2 WHERE DW_IS_CURRENT = TRUE;
```

### Get State at Specific Point in Time
```sql
SELECT * FROM TRAIN_PLAN_LEG_CURR_DT_2
WHERE @query_timestamp BETWEEN DW_EFFECTIVE_TS AND DW_EXPIRY_TS;
```

### Get Full History for a Record
```sql
SELECT * FROM TRAIN_PLAN_LEG_CURR_DT_2
WHERE TRAIN_PLAN_LEG_ID = 12345
ORDER BY DW_VERSION_NBR;
```

### Find Records Changed in Date Range
```sql
SELECT * FROM TRAIN_PLAN_LEG_CURR_DT_2
WHERE DW_EFFECTIVE_TS BETWEEN '2024-01-01' AND '2024-06-30'
  AND DW_VERSION_NBR > 1;  -- Exclude initial loads
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           BRONZE LAYER                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  TRAIN_PLAN_LEG_BASE          TRAIN_PLAN_LEG_BASE_LOG                   │
│  (Full Snapshot)              (CDC Changes: I/U/D)                      │
└──────────────┬────────────────────────────┬─────────────────────────────┘
               │                            │
               ▼                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           SILVER LAYER (SCD2)                            │
├─────────────────────────────────────────────────────────────────────────┤
│  TRAIN_PLAN_LEG_BASE_DT_2     TRAIN_PLAN_LEG_CDC_DT_2                   │
│  (Initial + SCD2 columns)     (Changes + SCD2 columns)                  │
└──────────────┬────────────────────────────┬─────────────────────────────┘
               │                            │
               └────────────┬───────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    TRAIN_PLAN_LEG_CURR_DT_2                              │
│              (Full History with Versioning & Date Ranges)                │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ Version 1: 2024-01-01 → 2024-03-14  (Historical)                │    │
│  │ Version 2: 2024-03-15 → 2024-06-19  (Historical)                │    │
│  │ Version 3: 2024-06-20 → 9999-12-31  (Current) ✓                 │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Storage Considerations

| Factor | Impact |
|--------|--------|
| **Record Growth** | Each change creates a new row |
| **Typical Overhead** | 2-5x original table size (depends on change frequency) |
| **Mitigation** | Use clustering, partitioning, data retention policies |
| **Snowflake Advantage** | Automatic micro-partitioning, compression, and pruning |

---

## Best Practices

1. **Define Tracked Attributes Clearly** - Only include columns that matter for history
2. **Use Hash for Change Detection** - Avoid creating versions when nothing changed
3. **Implement Soft Deletes** - Mark as deleted instead of removing rows
4. **Create Helper Views** - Simplify common query patterns
5. **Monitor Table Growth** - Set alerts for unexpected growth
6. **Document Business Rules** - Define what constitutes a "change"

---

## Summary

**SCD Type 2 is ideal when:**
- ✅ Historical accuracy is required for reporting
- ✅ Regulatory/compliance requirements exist
- ✅ Point-in-time analysis is needed
- ✅ Attributes change infrequently (slowly changing)
- ✅ Audit trails are mandatory

**For TRAIN_PLAN_LEG specifically:**
- Track how train plans evolve over time
- Support historical reporting on train scheduling
- Enable audit trails for operational changes
- Allow comparison of plan versions

---

*Generated for Snowflake Dynamic Tables SCD Type 2 Implementation*
