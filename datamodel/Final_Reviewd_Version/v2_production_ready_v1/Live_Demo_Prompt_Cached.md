# Live Demo Prompt — Cache-Based End-to-End Pipeline

**Tested**: 2026-04-07 | **Result**: 12/12 models PASS, 55/55 tests PASS
**Input**: `DIM_INPUT/s_m_INCR_DM_DIM_EQUIPMENT.XML`
**Source Schema**: `DIM_EQUIPMENT_SOURCES` (11 tables, 65K+ rows of real data)
**Cache**: Enabled (first run populates cache ~6 min; subsequent runs read from cache in seconds)

---

## How Cache Works

- Cache key = SHA-256(XML content + converter version + LLM model + prompt hash)
- Cache files are stored at `.infa2dbt/cache/` and **never expire** (no TTL)
- They are only removed by: `infa2dbt cache clear`, `infa2dbt cache remove <key>`, or manual deletion
- First `convert` run (cache MISS) calls LLM and takes ~6 minutes, then stores result in cache
- Subsequent `convert` runs (cache HIT) return instantly from disk — no LLM call
- If you change the XML, LLM model, or system prompt, a new cache key is generated (old entries remain)
- **No overwrite, no versioning — content-addressed (append-only)**. Each unique input combination creates a new entry with a different hash. Old entries are never overwritten; they just become stale (unused) because their key no longer matches. There is no limit on stored entries.
- To clean up stale entries: `PYTHONPATH="" python3 -m informatica_to_dbt.cli cache remove <short_key>`
- Use `PYTHONPATH="" python3 -m informatica_to_dbt.cli cache list` to see cached entries
- Use `PYTHONPATH="" python3 -m informatica_to_dbt.cli cache stats` for summary

---

## Prompt (copy everything below this line)

---

I want to run a clean end-to-end pipeline for the DIM_INPUT directory using the infa2dbt framework. This uses REAL source data in `DIM_EQUIPMENT_SOURCES` schema (NOT MOCK_SOURCES). Execute all 11 steps sequentially and stop if any step fails with unrecoverable errors.

**IMPORTANT: This is a CACHE-ENABLED run.** The convert step should read from cache if available. Do NOT use the `--no-cache` flag. If cache is empty (first run), it will call the LLM and populate cache automatically.

**IMPORTANT NOTES (learned from prior runs):**
- All `python3 -m informatica_to_dbt.cli` commands MUST be prefixed with `PYTHONPATH=""` (Python 3.12/3.13 site-packages conflict with lxml)
- SKIP local dbt compile/validate — there is a known version conflict between local dbt-fusion v2.0 (`accepted_values` requires `arguments:` format) and Snowflake native dbt 1.9.4 (uses old `values:` format). The YAML files are correct for the deployment target (Snowflake native dbt 1.9.4). Deploy directly.
- When suspending Snowflake TASKs: suspend ROOT task first, then child tasks (opposite of resume order)
- When resuming Snowflake TASKs: resume CHILD tasks first, then ROOT task
- Use `--schema-source snowflake` for the discover command (NOT `--source-schema`)
- The `--source-schema` flag is for the convert command only

### Step 0: CLEANUP
```bash
# 0a. Suspend and drop any existing Snowflake TASKs (if they exist)
# Suspend root FIRST, then child
ALTER TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_DAILY_RUN SUSPEND;
ALTER TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_DAILY_TEST SUSPEND;
DROP TASK IF EXISTS TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_DAILY_TEST;
DROP TASK IF EXISTS TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_DAILY_RUN;

# 0b. Delete old output directory
rm -rf dim_output_demo

# 0c. Ensure DIM_EQUIPMENT table exists in source schema (for SCD Type 2 self-referencing)
# If it doesn't exist, create it from the target table:
# CREATE TABLE TPC_DI_RAW_DATA.DIM_EQUIPMENT_SOURCES.DIM_EQUIPMENT AS
#   SELECT * FROM TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT;
# If the target doesn't exist either, create an empty shell matching the expected schema.
```

### Step 1: DISCOVER — XML inventory + Snowflake schema validation
```bash
PYTHONPATH="" python3 -m informatica_to_dbt.cli discover \
  --input DIM_INPUT \
  --schema-source snowflake \
  --database TPC_DI_RAW_DATA \
  --schema DIM_EQUIPMENT_SOURCES
```
**Expected**: 1 XML file, 1 mapping, 3 sources, 1 target, 34 transformations. Verify all 11 tables exist in DIM_EQUIPMENT_SOURCES.

### Step 2: CONVERT — Parse XML and generate dbt project (CACHE ENABLED)
```bash
PYTHONPATH="" python3 -m informatica_to_dbt.cli convert \
  --input DIM_INPUT \
  --output dim_output_demo \
  --source-schema DIM_EQUIPMENT_SOURCES
```
**Note**: No `--no-cache` flag. If cache exists, output appears in seconds (cache HIT). If not, LLM generates fresh output (~6 min) and stores it in cache for next time.

**Expected**: ~18 files generated. Quality score 100/100. If cache HIT, you'll see "Cache HIT for mapping" in the logs.

### Step 3: INSPECT — Verify and fix generated models
Verify and fix these known LLM generation issues:

1. **Check `_sources.yml`**: Must use `schema: DIM_EQUIPMENT_SOURCES` (uppercase) and source name `dim_equipment_sources` (lowercase). Must list all 11 tables. If it says `MOCK_SOURCES` or `mock_sources`, fix it.

2. **Check staging models for EDW columns**: `stg_eqpmnt_aar_base.sql` and `stg_eqpmnt_non_rgstrd.sql` must NOT reference `EDW_CREATE_USER`, `EDW_CREATE_TMS`, `EDW_UPDATE_USER`, or `EDW_UPDATE_TMS` — these columns only exist on the target DIM_EQUIPMENT table, not on the source tables EQPMNT_AAR_BASE or EQPMNT_NON_RGSTRD. Remove them if present.

3. **Check WHERE clauses in intermediate models**: Any WHERE clause filtering on `EDW_UPDATE_TMS` should use `RECORD_UPDATE_TMS` instead (the source tables use `RECORD_UPDATE_TMS`, not `EDW_UPDATE_TMS`).

4. **Check for ambiguous column references**: After JOINs, columns like `MARK_CD`, `EQPUN_NBR`, `RECORD_CREATE_TMS` must be qualified with table aliases (e.g., `n.MARK_CD` not just `MARK_CD`).

5. **Check `STATUS_CD` accepted_values**: Real data has: DIM_EQUIPMENT uses `['AC', 'IN']`, EQPMNT_AAR_BASE uses `['A', 'I', 'S', 'R']`. Update any `STATUS_CD` accepted_values tests to include `['AC', 'IN', 'A', 'I', 'S', 'R']`.

6. **Check schema YAML tests**: Ensure no test references a column that doesn't exist in the corresponding model (e.g., `I_U_FLG` test on a model that doesn't produce that column).

7. **Check `_marts__schema.yml` for truncation**: The LLM sometimes truncates this file mid-line (e.g., `- name:` with no value, or missing column definitions). Open the file and verify it has complete entries for all columns in `dim_equipment.sql` and `dim_equipment_soft_delete.sql`. If truncated, complete it with the missing column definitions (EQPMNT_GROUP_CD, STATUS_CD, REGISTERED_IND, CP_OWNED_IND, CAR_OWNERSHIP, LEASED_IND, FLEET_NM, EQPMNT_POOL_ID, DLTD_TMS, etc.). A `name:` with a `None` value will cause a deploy parsing error: `None is not of type 'string'`.

8. **Check Jinja `var()` quoting in soft delete model**: In `int_dim_equipment_soft_delete.sql`, the LLM sometimes generates `{{ var('repository_user_name', 'DBT_USER') }} AS EDW_UPDATE_USER` — this renders as a bare identifier, not a string. Fix to: `'{{ var("repository_user_name", "DBT_USER") }}' AS EDW_UPDATE_USER` (wrap in single quotes, switch inner quotes to double).

9. **Verify `dim_equipment.sql`**: Should be materialized as `incremental` with `merge_update_columns`, containing 3 UNION ALL branches (insert/update/type2). Must not be truncated.

### Step 4: COPY profiles.yml
```bash
cp profiles.yml dim_output_demo/profiles.yml
```

### Step 5: VALIDATE — SKIP
**SKIP this step.** Local dbt-fusion v2.0 rejects the old `accepted_values` format (`values:` at top level) that Snowflake native dbt 1.9.4 requires. The generated YAML is correct for the deployment target. Proceed directly to deploy.

### Step 6: DEPLOY — Deploy to Snowflake
```bash
snow dbt deploy DIM_EQUIPMENT_CLEAN \
  --source dim_output_demo \
  -c myconnection \
  --database TPC_DI_RAW_DATA \
  --schema INFORMATICA_TO_DBT \
  --force
```
**Expected**: `DIM_EQUIPMENT_CLEAN successfully created.` (~18-21 files copied)

### Step 7: RUN — Execute dbt models
```bash
snow dbt execute -c myconnection \
  --database TPC_DI_RAW_DATA \
  --schema INFORMATICA_TO_DBT \
  DIM_EQUIPMENT_CLEAN run
```
**Expected**: `PASS=12 WARN=0 ERROR=0 SKIP=0` (10 views + 2 incremental models). Model count is typically 12.

### Step 8: TEST — Run dbt tests
```bash
snow dbt execute -c myconnection \
  --database TPC_DI_RAW_DATA \
  --schema INFORMATICA_TO_DBT \
  DIM_EQUIPMENT_CLEAN test
```
**Expected**: `PASS=55 WARN=0 ERROR=0 SKIP=0` (test count may vary 50-56 depending on LLM generation and inspect fixes)

### Step 9: FIX — Fix any failures
If any models or tests fail:
1. Query actual data to understand the real values/schema:
   ```sql
   -- Check what columns exist on a source table
   SELECT COLUMN_NAME FROM TPC_DI_RAW_DATA.INFORMATION_SCHEMA.COLUMNS
   WHERE TABLE_SCHEMA = 'DIM_EQUIPMENT_SOURCES' AND TABLE_NAME = '<TABLE>';
   
   -- Check actual values for a column
   SELECT <COL>, COUNT(*) FROM TPC_DI_RAW_DATA.DIM_EQUIPMENT_SOURCES.<TABLE>
   GROUP BY <COL> ORDER BY COUNT(*) DESC;
   ```
2. Fix the generated SQL/YAML files locally in `dim_output_demo/`
3. Redeploy with `snow dbt deploy ... --force`
4. Re-run the failed step

**Common issues to watch for:**
- Staging models referencing EDW audit columns that don't exist on source tables -> remove them, use `RECORD_UPDATE_TMS` instead of `EDW_UPDATE_TMS` in WHERE clauses
- Truncated model files -> reconstruct complete file from intermediate model column lists
- `accepted_values` test mismatches -> query actual data and update YAML
- Ambiguous column names after JOINs -> qualify with table alias
- Tests referencing columns not produced by the model -> remove the test
- Truncated `_marts__schema.yml` with `name:` having no value -> complete the file with remaining column definitions
- Unquoted Jinja `var()` rendering as bare identifier -> wrap in single quotes

### Step 10: SCHEDULE — Create daily Snowflake TASKs
```sql
-- Root task: dbt run at 6 AM UTC daily
CREATE OR REPLACE TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_DAILY_RUN
  WAREHOUSE = SMALL_WH
  SCHEDULE = 'USING CRON 0 6 * * * UTC'
  COMMENT = 'Daily dbt run for DIM_EQUIPMENT_CLEAN at 6 AM UTC'
AS
  EXECUTE DBT PROJECT TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_CLEAN ARGS = 'run';

-- Child task: dbt test after run completes
CREATE OR REPLACE TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_DAILY_TEST
  WAREHOUSE = SMALL_WH
  COMMENT = 'Daily dbt test after run completes'
  AFTER TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_DAILY_RUN
AS
  EXECUTE DBT PROJECT TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_CLEAN ARGS = 'test';

-- Resume: CHILD first, then ROOT
ALTER TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_DAILY_TEST RESUME;
ALTER TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_DAILY_RUN RESUME;
```

### Step 11: REPORT + GIT
1. Print a final pipeline summary table (step, action, result)
2. `git add -A && git commit` with a descriptive message
3. `git push` to all remotes

---

## Cache Management Commands

```bash
# List all cached entries
PYTHONPATH="" python3 -m informatica_to_dbt.cli cache list

# Show cache statistics
PYTHONPATH="" python3 -m informatica_to_dbt.cli cache stats

# Clear ALL cache entries (forces fresh LLM generation on next convert)
PYTHONPATH="" python3 -m informatica_to_dbt.cli cache clear
```

---

## Environment Info

| Setting | Value |
|---------|-------|
| Connection | `myconnection` |
| Database | `TPC_DI_RAW_DATA` |
| Source Schema | `DIM_EQUIPMENT_SOURCES` (11 tables, 65K+ rows) |
| Target Schema | `INFORMATICA_TO_DBT` |
| Warehouse | `SMALL_WH` |
| Snowflake native dbt | v1.9.4 |
| Local dbt | dbt-fusion v2.0 (DO NOT use for compile/validate) |
| Python | Use `PYTHONPATH="" python3` (v3.12) |
| Snowflake CLI | v3.16.0 |
| Deploy project name | `DIM_EQUIPMENT_CLEAN` |
| Cache location | `.infa2dbt/cache/` (never expires) |
