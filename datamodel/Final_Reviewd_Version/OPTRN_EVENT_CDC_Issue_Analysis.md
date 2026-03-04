# OPTRN_EVENT CDC Pipeline - Data Gap Analysis

**Date:** March 4, 2026  
**Issue:** 76,261 records missing from target table  
**Status:** Root cause identified, recovery SQL ready

---

## Pipeline Overview

| Component | Object Name |
|-----------|-------------|
| Source Table | `D_RAW.SADB.OPTRN_EVENT_BASE` |
| Target Table | `D_BRONZE.SADB.OPTRN_EVENT` |
| Stream | `D_RAW.SADB.OPTRN_EVENT_BASE_HIST_STREAM` |
| Stored Procedure | `D_RAW.SADB.SP_PROCESS_OPTRN_EVENT()` |
| Task | `D_RAW.SADB.TASK_PROCESS_OPTRN_EVENT` (runs every 5 min) |

---

## 1. Root Cause: IDMC Bulk Reload Bypassed Stream

### How Snowflake Streams Work (Normal CDC Flow)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           NORMAL CDC FLOW                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   SOURCE TABLE                    STREAM                      TARGET TABLE   │
│   ┌──────────┐                 ┌──────────┐                  ┌──────────┐   │
│   │  Record  │ ──INSERT──────► │  Record  │ ──SP MERGE────►  │  Record  │   │
│   │  Record  │ ──UPDATE──────► │  Record  │                  │  Record  │   │
│   │  Record  │ ──DELETE──────► │  Record  │                  │  Record  │   │
│   └──────────┘                 └──────────┘                  └──────────┘   │
│                                                                              │
│   Stream OFFSET ────────────────────►                                        │
│   (tracks last consumed position)                                            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key Concept:** A Snowflake stream has an **OFFSET** that points to the last consumed transaction. It only shows changes that happened **AFTER** that offset.

---

### What Happened on March 2nd (The Problem)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      IDMC TRUNCATE-RELOAD EVENT                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   TIMELINE: March 2, 2026 @ 11:06 AM                                        │
│                                                                              │
│   Step 1: IDMC runs TRUNCATE TABLE D_RAW.SADB.OPTRN_EVENT_BASE              │
│           ┌──────────────────────────────────────────────────────┐          │
│           │  ⚠️  TRUNCATE invalidates stream's internal offset   │          │
│           │  ⚠️  Stream becomes STALE                            │          │
│           └──────────────────────────────────────────────────────┘          │
│                                                                              │
│   Step 2: IDMC bulk loads 1.3M+ records (including 76k new ones)            │
│           ┌──────────────────────────────────────────────────────┐          │
│           │  All records loaded with SNW_OPERATION_TYPE = NULL   │          │
│           │  All records have SAME SNW_LAST_REPLICATED timestamp │          │
│           │  (within milliseconds - proving bulk load)           │          │
│           └──────────────────────────────────────────────────────┘          │
│                                                                              │
│   Step 3: SP detects stale stream, recreates it                             │
│           ┌──────────────────────────────────────────────────────┐          │
│           │  New stream offset starts AFTER the bulk load        │          │
│           │  76,261 records are BEHIND the new offset            │          │
│           │  ❌ These records are NEVER captured                 │          │
│           └──────────────────────────────────────────────────────┘          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Evidence Summary

| Evidence | What We Found | What It Means |
|----------|---------------|---------------|
| **SNW_OPERATION_TYPE** | ALL 76,261 missing records have `NULL` | IDMC bulk loads don't set operation type (I/U/D) |
| **SNW_LAST_REPLICATED** | All records: `2026-03-02 11:06:35.313-314` | 76k records loaded in <1 second = bulk operation |
| **RECORD_CREATE_TMS** | Ranges from Feb 28 - Mar 4 | Records existed in source DB before, reloaded in bulk |
| **Stream Status** | Empty (0 records), not stale | Stream was recreated and is now healthy |
| **Missing Pattern** | 76,136 records on Mar 2, then 125 on Mar 4 | Mar 2 = bulk load event, Mar 4 = new ongoing issue |

---

### Visual Timeline

```
Feb 25                    Mar 2 @ 11:06 AM                    Mar 4 (Today)
   │                            │                                  │
   ▼                            ▼                                  ▼
┌──────┐                  ┌───────────┐                      ┌──────────┐
│Stream│                  │IDMC BULK  │                      │76k records│
│Created│                 │RELOAD     │                      │MISSING   │
└──────┘                  └───────────┘                      └──────────┘
   │                            │                                  │
   │  ◄── Stream capturing ──►  │  ◄── Stream STALE ──►           │
   │      CDC normally          │      then recreated              │
   │                            │                                  │
                                │                                  
                          76,261 records                           
                          fell into this gap                       
```

---

## 2. SP Issue Analysis & Fix

### Current SP Logic (Simplified)

```sql
-- CURRENT SP PSEUDOCODE
BEGIN
    -- Step 1: Check if stream is stale
    IF stream_is_stale THEN
        
        -- Step 2: Recreate stream
        CREATE OR REPLACE STREAM ... SHOW_INITIAL_ROWS = TRUE;
        
        -- Step 3: Do differential load (source vs target)
        MERGE INTO target
        USING (SELECT * FROM source WHERE NOT IN target)
        ...
        
        -- Step 4: Consume stream to reset offset
        SELECT COUNT(*) FROM stream;  -- Advances offset
        
    ELSE
        -- Normal CDC processing from stream
        MERGE INTO target USING stream ...
    END IF;
END;
```

---

### The Bug: Race Condition Window

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         THE TIMING PROBLEM                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   WHAT SHOULD HAPPEN:                                                        │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐              │
│   │Recreate │ ──► │Diff Load│ ──► │ COMMIT  │ ──► │Stream   │              │
│   │ Stream  │     │(MERGE)  │     │         │     │Consumed │              │
│   └─────────┘     └─────────┘     └─────────┘     └─────────┘              │
│                                                                              │
│   WHAT ACTUALLY HAPPENED:                                                    │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐                               │
│   │Recreate │ ──► │Diff Load│ ──► │ ERROR/  │ ──► Records lost!            │
│   │ Stream  │     │ STARTS  │     │TIMEOUT  │                               │
│   └─────────┘     └─────────┘     └─────────┘                               │
│                         │                                                    │
│                         ▼                                                    │
│                   Stream offset already                                      │
│                   advanced past records                                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Why SHOW_INITIAL_ROWS=TRUE Didn't Help

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SHOW_INITIAL_ROWS BEHAVIOR                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   SHOW_INITIAL_ROWS = TRUE means:                                           │
│   "When stream is first created, show ALL existing rows as INSERTs"         │
│                                                                              │
│   PROBLEM:                                                                   │
│   ┌────────────────────────────────────────────────────────────────┐        │
│   │  The SP does a DIFFERENTIAL MERGE (source vs target)           │        │
│   │  INSTEAD of processing the stream's initial rows               │        │
│   │                                                                 │        │
│   │  Then it CONSUMES the stream with a COUNT(*) query             │        │
│   │  This advances the offset WITHOUT processing the rows!         │        │
│   └────────────────────────────────────────────────────────────────┘        │
│                                                                              │
│   RESULT: 76k initial rows were in stream, but never processed              │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Recovery Plan

### Immediate Recovery (One-Time)

Run the MERGE statement in `Scripts/OPTRN_EVENT.sql` to backfill 76,261 missing records:

```sql
MERGE INTO D_BRONZE.SADB.OPTRN_EVENT AS tgt
USING (
    SELECT src.*
    FROM D_RAW.SADB.OPTRN_EVENT_BASE src
    LEFT JOIN D_BRONZE.SADB.OPTRN_EVENT tgt 
        ON src.OPTRN_EVENT_ID = tgt.OPTRN_EVENT_ID
    WHERE tgt.OPTRN_EVENT_ID IS NULL
) AS src
ON tgt.OPTRN_EVENT_ID = src.OPTRN_EVENT_ID
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
```

### Prevention (Ongoing)

Deploy daily gap detection task:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      GAP DETECTION TASK                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   DAILY @ 6 AM                                                              │
│   ┌─────────────────────────────────────────────────────────────────┐       │
│   │                                                                  │       │
│   │   1. COUNT records in SOURCE not in TARGET                      │       │
│   │                                                                  │       │
│   │   2. IF gaps found:                                             │       │
│   │      - Auto-MERGE missing records                               │       │
│   │      - Tag with CDC_OPERATION = 'GAP_RECOVERY'                  │       │
│   │      - Log recovery in SOURCE_LOAD_BATCH_ID                     │       │
│   │                                                                  │       │
│   │   3. Return status: 'NO_GAPS' or 'GAP_RECOVERED: N records'     │       │
│   │                                                                  │       │
│   └─────────────────────────────────────────────────────────────────┘       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Recommendations for IDMC Team

| Current Practice | Recommended Practice |
|------------------|---------------------|
| TRUNCATE + RELOAD entire table | Use **incremental/delta loads** |
| Bulk INSERT without operation type | Set `SNW_OPERATION_TYPE = 'I'` for inserts |
| No coordination with downstream | **Pause CDC task** before bulk operations |

---

## 5. Summary

### One-Liner Root Cause
> "IDMC did a truncate-reload on March 2nd which made our stream stale. When the SP recreated the stream, 76k records fell into the gap between the old and new stream offsets."

### One-Liner Fix
> "We'll MERGE the missing records directly, then deploy a daily gap-detection task to auto-recover any future gaps caused by bulk loads."

---

## 6. Files

| File | Purpose |
|------|---------|
| `Scripts/OPTRN_EVENT.sql` | Recovery SQL + Gap Detection SP & Task |
| `Scripts/OPTRN_EVENT_CDC_Issue_Analysis.md` | This documentation |
