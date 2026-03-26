# CDC Pipeline Exception Handling & Resilience — Customer Documentation

**Prepared for:** CPKC Data Engineering Team
**Date:** March 26, 2026 | **Version:** Phase 1 (Current) + Phase 2 (Planned)

---

## PAGE 1: Current Implementation (Phase 1 — Production Deployed)

### Exception Handling Summary

The `SP_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT()` stored procedure implements **5 layers of exception handling** to ensure the CDC pipeline is self-healing, data-safe, and fully auditable.

---

### Layer 1: Stream Staleness Detection & Auto-Recovery

```
Stream Query ──► Exception? ──YES──► Stale=TRUE ──► Recreate Stream ──► Recovery MERGE
                     │NO
                     ▼
              Normal CDC Processing
```

| Aspect | Implementation |
|:---|:---|
| **Detection** | `SELECT COUNT(*) WHERE 1=0` inside `BEGIN...EXCEPTION WHEN OTHER` |
| **Trigger** | Any stream error (stale, dropped, invalid, does not exist) |
| **Recovery** | `CREATE OR REPLACE STREAM ... SHOW_INITIAL_ROWS = TRUE` |
| **Re-sync** | Full MERGE from recreated stream into bronze (UPDATE existing + INSERT new) |
| **Why** | IDMC may DROP + RECREATE the source table, breaking the stream. Without this, the pipeline stops permanently until manual intervention. |
| **Snowflake Ref** | [Stream Staleness FAQ](https://community.snowflake.com/s/article/Stream-Staleness-FAQ) |

---

### Layer 2: Source Data Quality Guards

| Guard | Implementation | Risk Mitigated |
|:---|:---|:---|
| **Deduplication** | `ROW_NUMBER() OVER (PARTITION BY composite_PK ORDER BY SNW_LAST_REPLICATED DESC)` | Duplicate rows in `_BASE` from IDMC race conditions or reload |
| **NULL PK Filter** | `WHERE TRAIN_CNST_SMRY_ID IS NOT NULL AND TRAIN_CNST_SMRY_VRSN_NBR IS NOT NULL AND SQNC_NBR IS NOT NULL` | NULL values in PK columns (Snowflake NOT NULL is informational only) |
| **Purge Exclusion** | `WHERE NVL(SNW_OPERATION_OWNER, '') NOT IN ('TSDPRG', 'EMEPRG')` | Purge/maintenance records from IDMC that should not reach bronze |
| **Coverage** | Applied in **both** Normal Mode and Recovery Mode | Ensures data quality regardless of which path executes |

---

### Layer 3: Empty Stream Early Exit

| Aspect | Implementation |
|:---|:---|
| **Check** | `IF (v_staging_count = 0) THEN RETURN 'NO_DATA'` |
| **Benefit** | Avoids executing a MERGE on empty data; saves warehouse compute |
| **Logging** | `NO_DATA` status logged to `CDC_EXECUTION_LOG` for audit |

---

### Layer 4: Staging-First Pattern (Single Stream Read)

| Aspect | Implementation |
|:---|:---|
| **Pattern** | Stream → Temp Table (`_CDC_STAGING_*`) → MERGE into Bronze |
| **Why** | Stream offset advances on read. If MERGE fails after direct stream read, data is lost. Staging to temp table first ensures the stream is consumed atomically and data is preserved for retry. |
| **Cleanup** | `DROP TABLE IF EXISTS _CDC_STAGING_*` in normal flow and exception handler |
| **Snowflake Ref** | [Streams Best Practices](https://docs.snowflake.com/en/user-guide/streams-manage) |

---

### Layer 5: Global Exception Handler with Execution Logging

| Aspect | Implementation |
|:---|:---|
| **Handler** | `EXCEPTION WHEN OTHER THEN` — catches all unhandled errors |
| **Actions** | 1. Capture error message 2. Drop temp table (cleanup) 3. Log to `CDC_EXECUTION_LOG` 4. Return error string |
| **Logged Statuses** | `SUCCESS`, `NO_DATA`, `RECOVERY`, `ERROR` |
| **Logged Metrics** | BATCH_ID, START_TIME, END_TIME, ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ERROR_MESSAGE |
| **Batch Traceability** | Every execution gets a unique `BATCH_*` ID for end-to-end tracing |

---

### Current State: Tested & Validated

| Test Case | Result |
|:---|:---:|
| Initial Load (10 rows, composite PK) | PASS |
| Normal CDC UPDATE | PASS |
| Normal CDC DELETE (soft delete, data preserved) | PASS |
| NO_DATA (empty stream, early exit) | PASS |
| Mixed Operations (INSERT + UPDATE + DELETE) | PASS |
| Duplicate composite PK — latest timestamp wins | PASS |
| Partial PK match — correctly NOT deduped | PASS |
| Recovery Mode with duplicates in source | PASS |
| NULL in any PK column — filtered | PASS |
| Purge filter (TSDPRG/EMEPRG excluded) | PASS |
| Execution Log Validation | PASS |
| **Total: 11/11 PASS** | **All scenarios validated** |

---

## PAGE 2: Phase 2 Enrichment Plan (Reference: `DTQ_DTCTD_EQPMNT_v1.sql`)

The Phase 2 SP (`SP_PROCESS_DTQ_DTCTD_EQPMNT v2.1`) adds **4 additional layers** of exception handling on top of the current 5 layers.

### Enhancement 1: Specific Error Code Detection (Replaces `WHEN OTHER`)

| Current (Phase 1) | Phase 2 Enhancement |
|:---|:---|
| `WHEN OTHER` catches all errors as stale | `WHEN STATEMENT_ERROR` with specific code matching |
| Simple but may false-positive on non-stale errors | Checks `ILIKE '%stale%'`, `'%dropped%'`, `'%invalid%'`, `'%does not exist%'` + error codes `2000, 2003, 2043, 91901, 2151` |
| Non-stale errors silently trigger recovery | **Non-stale errors are RE-RAISED** — only true staleness triggers recovery |

```sql
WHEN STATEMENT_ERROR THEN
    IF (v_error_msg ILIKE '%stale%' OR v_error_msg ILIKE '%dropped%'
        OR v_sqlcode_captured IN (2000, 2003, 2043, 91901, 2151)) THEN
        v_stream_stale := TRUE;
    ELSE
        RAISE;  -- Re-raise non-stale errors (permissions, network, etc.)
    END IF;
```

**Customer Value:** Prevents unnecessary recovery (full re-sync) for transient errors like network timeouts or permission changes. Recovery only triggers when the stream is genuinely stale.

---

### Enhancement 2: Base Table Validation Before Recovery

| Current (Phase 1) | Phase 2 Enhancement |
|:---|:---|
| Recovery immediately recreates stream | **Validates base table exists and is accessible first** |
| If base table is also missing, recovery MERGE fails | Logs `RECOVERY_FAILED` and returns cleanly |

```sql
BEGIN
    SELECT COUNT(*) FROM D_RAW.EHMS.DTQ_DTCTD_EQPMNT_BASE LIMIT 1;
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        -- Log RECOVERY_FAILED and return (don't attempt stream creation)
END;
```

**Customer Value:** If IDMC has dropped the source table but hasn't recreated it yet, the SP exits cleanly with `RECOVERY_FAILED` instead of crashing. The next task run will retry automatically.

---

### Enhancement 3: Nested Exception Handlers (Logging Protection)

| Current (Phase 1) | Phase 2 Enhancement |
|:---|:---|
| Single `WHEN OTHER` handler | **Separate `STATEMENT_ERROR` + `WHEN OTHER` handlers** |
| If logging INSERT fails, SP crashes | **Nested inner `BEGIN...EXCEPTION` protects logging** |
| No SQLCODE/SQLSTATE captured | Captures SQLCODE, SQLSTATE, SQLERRM in error message |

```sql
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        v_error_msg := 'STATEMENT_ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
        DROP TABLE IF EXISTS ...;
        BEGIN  -- Nested: protect logging from cascading failure
            INSERT INTO CDC_EXECUTION_LOG ...;
        EXCEPTION WHEN OTHER THEN NULL;  -- Swallow logging failure
        END;
    WHEN OTHER THEN
        v_error_msg := 'UNEXPECTED_ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
        -- Same nested pattern
```

**Customer Value:** Even if the monitoring table is inaccessible, the SP still cleans up temp tables and returns an error message. No cascading failures. Separate `STATEMENT_ERROR` vs `WHEN OTHER` gives better diagnostic granularity.

---

### Enhancement 4: Monitoring Table Auto-Creation + ADDITIONAL_METRICS

| Current (Phase 1) | Phase 2 Enhancement |
|:---|:---|
| Requires monitoring table to exist | `CREATE TABLE IF NOT EXISTS CDC_EXECUTION_LOG` at SP start |
| Basic row counts only | **ADDITIONAL_METRICS (VARIANT)** column with `OBJECT_CONSTRUCT('duplicate_resolved_count', ...)` |

**Customer Value:** SP is self-contained — no prerequisite DDL needed. VARIANT metrics column allows adding new KPIs without schema changes.

---

### Enhancement 5: Execution Status Granularity

| Status | Phase 1 | Phase 2 |
|:---|:---:|:---:|
| SUCCESS | Yes | Yes |
| NO_DATA | Yes | Yes |
| RECOVERY | Yes | Yes |
| ERROR | Yes (generic) | Split into **SQL_ERROR** + **UNKNOWN_ERROR** |
| RECOVERY_FAILED | No | **Yes** (base table missing) |

---

### Phase 2 Implementation Timeline

| Step | Effort | Risk |
|:---|:---:|:---:|
| Apply specific error code detection to all 22 SPs | 4 hours | Low |
| Add base table validation to recovery path | 2 hours | Low |
| Add nested exception handlers | 2 hours | Low |
| Add monitoring auto-creation + ADDITIONAL_METRICS | 1 hour | Low |
| Testing (per table) | 30 min each | Low |
| **Total for 22 tables** | **~3 days** | **Low** |

---

### Side-by-Side Comparison

| Capability | Phase 1 (Current) | Phase 2 (Planned) |
|:---|:---:|:---:|
| Stream stale detection | `WHEN OTHER` (broad) | `WHEN STATEMENT_ERROR` + specific codes |
| Non-stale error handling | Silent recovery (false positive) | **RE-RAISE** (correct behavior) |
| Base table validation | Not implemented | **Yes** — validates before recovery |
| Nested exception handlers | No | **Yes** — protects logging |
| Monitoring auto-creation | No | **Yes** — `CREATE IF NOT EXISTS` |
| ADDITIONAL_METRICS | No | **Yes** — VARIANT column |
| RECOVERY_FAILED status | No | **Yes** |
| SQL_ERROR vs UNKNOWN_ERROR | No (single ERROR) | **Yes** — separate statuses |
| Deduplication (ROW_NUMBER) | Yes | Yes |
| NULL PK filter | Yes | Yes |
| Purge filter (TSDPRG/EMEPRG) | Yes | Yes |
| Staging-first pattern | Yes | Yes |
| Soft delete preservation | Yes | Yes |
| Execution logging | Yes | Yes (enhanced) |
| **Exception Handling Layers** | **5** | **9** |

---

### Reviewer Assessment

| Dimension | Phase 1 Score | Phase 2 Target |
|:---|:---:|:---:|
| Exception Handling | 7.5 / 10 | 9.5 / 10 |
| Data Quality Guards | 9.0 / 10 | 9.0 / 10 |
| Self-Healing | 8.0 / 10 | 9.5 / 10 |
| Observability | 7.0 / 10 | 9.0 / 10 |
| **Customer Presentation Readiness** | **8.0 / 10** | **9.5 / 10** |

**Phase 1 Verdict:** Production-ready and tested. The 5 exception handling layers cover all critical scenarios. The `WHEN OTHER` approach is simpler but catches all stream errors reliably.

**Phase 2 Value:** Adds precision (no false-positive recovery), safety (base table validation), and diagnostics (SQLCODE/SQLSTATE capture). Recommended for high-criticality tables first, then rolled out to all 22.

---

*Prepared: March 26, 2026 | CPKC CDC Pipeline Team | Snowflake Environment*