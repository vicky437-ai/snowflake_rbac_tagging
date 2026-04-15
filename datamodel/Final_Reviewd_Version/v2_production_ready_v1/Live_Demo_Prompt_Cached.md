# Live Demo Prompt — Cache-Based End-to-End Pipeline

**Tested**: 2026-04-15 | **Result**: 17/17 models PASS, 62/62 tests PASS
**Input**: `DIM_INPUT/s_m_INCR_DM_DIM_EQUIPMENT.XML`
**Source Schema**: `DIM_EQUIPMENT_SOURCES` (11 tables, 65K+ rows of real data)
**Cache**: Enabled (first run populates cache ~6 min; subsequent runs read from cache in seconds)
**Logging**: Enabled — full debug log captured to `dim_output_demo/logs/convert.log`
**Current cache key**: `142847f6a3b3` (25 files, score 99) — will HIT on next run

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

**Working directory**: `cd /Users/vicky/informatica-to-dbt` (all relative paths below are relative to this directory)

I want to run a clean end-to-end pipeline for the DIM_INPUT directory using the infa2dbt framework. This uses REAL source data in `DIM_EQUIPMENT_SOURCES` schema (NOT MOCK_SOURCES). Execute all 14 steps (0-13) sequentially and stop if any step fails with unrecoverable errors.

**IMPORTANT: This is a CACHE-ENABLED run.** The convert step should read from cache if available. Do NOT use the `--no-cache` flag. If cache is empty (first run), it will call the LLM and populate cache automatically.

**IMPORTANT NOTES (learned from prior runs):**
- All `python3 -m informatica_to_dbt.cli` commands MUST be prefixed with `PYTHONPATH=""` (Python 3.12/3.13 site-packages conflict with lxml)
- SKIP local dbt compile/validate — there is a known version conflict between local dbt-fusion v2.0 (`accepted_values` requires `arguments:` format) and Snowflake native dbt 1.9.4 (uses old `values:` format). The YAML files are correct for the deployment target (Snowflake native dbt 1.9.4). Deploy directly.
- When suspending Snowflake TASKs: suspend ROOT task first, then CHILD task (root must be suspended before any child can be modified)
- When dropping Snowflake TASKs: drop CHILD task first, then ROOT task (same dependency reason)
- When creating Snowflake TASKs: create ROOT task first, then CHILD task (AFTER clause references root)
- When resuming Snowflake TASKs: resume CHILD task first, then ROOT task
- Use `--schema-source snowflake` for the discover command (NOT `--source-schema`)
- The `--source-schema` flag is for the convert command only
- `snow dbt deploy` syntax: `snow dbt deploy NAME --source DIR --profiles-dir DIR --force` (do NOT pass `-c`, `--database`, or `--schema` — these come from `config.toml`)
- `snow dbt execute` syntax: `snow dbt execute NAME run|test` (same — no `-c`, `--database`, or `--schema` needed)
- `EXECUTE DBT PROJECT` uses the short project name (e.g., `DIM_EQUIPMENT_CLEAN`), NOT the fully-qualified name

### Step 0: CLEANUP
```bash
# 0a. Suspend and drop any existing Snowflake TASKs (if they exist)
# NOTE: The SUSPEND commands may fail on first run if tasks don't exist — that's OK, continue to the DROP commands.
# Suspend ROOT first, then CHILD (root must be suspended before any child can be modified)
ALTER TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_CLEAN_RUN_TASK SUSPEND;
ALTER TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_CLEAN_TEST_TASK SUSPEND;
# Drop CHILD first, then ROOT (same reason — can't drop root while child references it)
DROP TASK IF EXISTS TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_CLEAN_TEST_TASK;
DROP TASK IF EXISTS TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_CLEAN_RUN_TASK;

# 0b. Drop existing dbt project (if it exists)
DROP DBT PROJECT IF EXISTS TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_CLEAN;

# 0c. Delete old output directory
rm -rf dim_output_demo

# 0d. Ensure DIM_EQUIPMENT table exists in source schema (for SCD Type 2 self-referencing)
# If it doesn't exist, create it from the target table:
# CREATE TABLE TPC_DI_RAW_DATA.DIM_EQUIPMENT_SOURCES.DIM_EQUIPMENT AS
#   SELECT * FROM TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT;
# If the target doesn't exist either, create an empty shell matching the expected schema.

# 0e. CRITICAL: Refresh source data timestamps for incremental filters
# The pipeline uses EDW_UPDATE_TMS to filter recent rows. If source data is stale,
# the incremental models will process 0 rows. Update 500 rows in each source table:
```
```sql
-- Run these IMMEDIATELY before the demo (especially important for the 1-hour window):
UPDATE TPC_DI_RAW_DATA.DIM_EQUIPMENT_SOURCES.EQPMNT_AAR_BASE
SET EDW_UPDATE_TMS = CURRENT_TIMESTAMP()
WHERE EQPMNT_ID IN (
    SELECT EQPMNT_ID FROM TPC_DI_RAW_DATA.DIM_EQUIPMENT_SOURCES.EQPMNT_AAR_BASE LIMIT 500
);

UPDATE TPC_DI_RAW_DATA.DIM_EQUIPMENT_SOURCES.EQPMNT_NON_RGSTRD
SET EDW_UPDATE_TMS = CURRENT_TIMESTAMP()
WHERE EQPMNT_ID IN (
    SELECT EQPMNT_ID FROM TPC_DI_RAW_DATA.DIM_EQUIPMENT_SOURCES.EQPMNT_NON_RGSTRD LIMIT 500
);

-- VERIFY both filters will find rows:
SELECT 'AAR_BASE (24h)' AS filter, COUNT(*) AS rows
FROM TPC_DI_RAW_DATA.DIM_EQUIPMENT_SOURCES.EQPMNT_AAR_BASE
WHERE EDW_UPDATE_TMS > DATEADD('DAY', -1, CURRENT_TIMESTAMP())
UNION ALL
SELECT 'NON_RGSTRD (1h)', COUNT(*)
FROM TPC_DI_RAW_DATA.DIM_EQUIPMENT_SOURCES.EQPMNT_NON_RGSTRD
WHERE EDW_UPDATE_TMS > DATEADD('HOUR', -1, CURRENT_TIMESTAMP());
-- Both should return 500. If NON_RGSTRD returns 0, re-run the UPDATE above.
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
  --source-schema DIM_EQUIPMENT_SOURCES \
  --log-level DEBUG \
  --log-file dim_output_demo/logs/convert.log
```
**Note**: No `--no-cache` flag. If cache exists, output appears in seconds (cache HIT). If not, LLM generates fresh output (~6 min) and stores it in cache for next time. Full debug log (including LLM prompts, cache lookups, timing) is written to `dim_output_demo/logs/convert.log`.

**Expected**: 25 files generated. Quality score 99/100. If cache HIT (expected), you'll see "Cache HIT for mapping" in the logs and output appears in seconds. Current cache key: `142847f6a3b3`.

### Step 3: INSPECT — Verify and fix generated models
Verify and fix these known LLM generation issues:

1. **Check `_sources.yml`**: Must use `schema: DIM_EQUIPMENT_SOURCES` (uppercase) and source name `dim_equipment_sources` (lowercase). Must list all 11 tables. If it says `MOCK_SOURCES` or `mock_sources`, fix it.

2. **Verify staging models include EDW columns**: `stg_eqpmnt_aar_base.sql` and `stg_eqpmnt_non_rgstrd.sql` should reference `EDW_UPDATE_TMS` (and optionally `EDW_CREATE_USER`, `EDW_CREATE_TMS`, `EDW_UPDATE_USER`). These columns DO exist on both source tables `EQPMNT_AAR_BASE` and `EQPMNT_NON_RGSTRD`. If the LLM omits them, add them — the incremental filter models depend on `EDW_UPDATE_TMS`.

3. **Check WHERE clauses in intermediate filter models**: The incremental filters use `EDW_UPDATE_TMS` — this is CORRECT. Both source tables have this column. Note the two filter models use different windows: `int_aar_base_filtered.sql` uses `DATEADD('DAY', -1, ...)` (24 hours) and `int_non_registered_filtered.sql` uses `DATEADD('HOUR', -1, ...)` (1 hour). Both are valid; the 1-hour window is stricter.

4. **Check for ambiguous column references**: After JOINs, columns like `MARK_CD`, `EQPUN_NBR`, `RECORD_CREATE_TMS` must be qualified with table aliases (e.g., `n.MARK_CD` not just `MARK_CD`).

5. **Check `STATUS_CD` accepted_values in `_marts__schema.yml`**: Real data has: DIM_EQUIPMENT uses `['AC', 'IN']`, EQPMNT_AAR_BASE uses `['A', 'I', 'S', 'R']`. Update the `STATUS_CD` accepted_values test in `marts/_marts__schema.yml` to include `['AC', 'IN', 'A', 'I', 'S', 'R']`. The LLM typically generates `['A', 'I', 'D', 'R', 'S']` which is wrong.

6. **Check schema YAML tests**: Ensure no test references a column that doesn't exist in the corresponding model (e.g., `I_U_FLG` test on a model that doesn't produce that column).

7. **Check `_marts__schema.yml` for truncation**: The LLM sometimes truncates this file mid-line (e.g., `- name:` with no value, or missing column definitions). Open the file and verify it has complete entries for all columns in `dim_equipment.sql` and `dim_equipment_soft_delete.sql`. If truncated, complete it with the missing column definitions (EQPMNT_GROUP_CD, STATUS_CD, REGISTERED_IND, CP_OWNED_IND, CAR_OWNERSHIP, LEASED_IND, FLEET_NM, EQPMNT_POOL_ID, DLTD_TMS, etc.). A `name:` with a `None` value will cause a deploy parsing error: `None is not of type 'string'`.

8. **Check Jinja `var()` quoting in soft delete model**: In `int_dim_equipment_soft_delete.sql`, the LLM sometimes generates `{{ var('repository_user_name', 'DBT_USER') }} AS EDW_UPDATE_USER` — this renders as a bare identifier, not a string. Fix to: `'{{ var("repository_user_name", "DBT_USER") }}' AS EDW_UPDATE_USER` (wrap in single quotes, switch inner quotes to double).

9. **Verify `dim_equipment.sql`**: Should be materialized as `incremental` with `merge_update_columns`, containing 3 UNION ALL branches (insert/update/type2). Must not be truncated.

10. **Check for duplicate column `I_U_FLG` in `int_equipment_final_gather.sql`**: The `final_transformations` CTE uses `d.*` from upstream, which includes an `I_U_FLG` placeholder column. The same CTE then recomputes `I_U_FLG` via a CASE expression, causing Snowflake error `002025 (42S21): duplicate column name 'I_U_FLG'`. **Fix**: Change `d.*` to `d.* EXCLUDE (I_U_FLG)` — this uses Snowflake's EXCLUDE syntax to drop the upstream placeholder while keeping the recomputed CASE expression. This is a known LLM generation issue.

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
  --profiles-dir dim_output_demo \
  --force
```
**Expected**: `DIM_EQUIPMENT_CLEAN successfully created.` (~25 files copied)

### Step 7: RUN — Execute dbt models
```bash
snow dbt execute DIM_EQUIPMENT_CLEAN run
```
**Expected**: `PASS=17 WARN=0 ERROR=0 SKIP=0` (16 views + 1 incremental model).

### Step 8: TEST — Run dbt tests
```bash
snow dbt execute DIM_EQUIPMENT_CLEAN test
```
**Expected**: `PASS=62 WARN=0 ERROR=0 SKIP=0` (test count may vary 58-65 depending on LLM generation and inspect fixes)

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
- Duplicate column `I_U_FLG` in `int_equipment_final_gather.sql` -> use `d.* EXCLUDE (I_U_FLG)` to drop upstream placeholder
- Incremental filters returning 0 rows -> run data freshness UPDATE from Step 0e before dbt run
- `accepted_values` test mismatches -> query actual data and update YAML
- Ambiguous column names after JOINs -> qualify with table alias
- Tests referencing columns not produced by the model -> remove the test
- Truncated `_marts__schema.yml` with `name:` having no value -> complete the file with remaining column definitions
- Unquoted Jinja `var()` rendering as bare identifier -> wrap in single quotes
- Truncated model files -> reconstruct complete file from intermediate model column lists

### Step 10: RECONCILE — Validate source-to-target data accuracy
```bash
PYTHONPATH="" python3 -m informatica_to_dbt.cli reconcile \
  -sd TPC_DI_RAW_DATA \
  -ss DIM_EQUIPMENT_SOURCES \
  -td TPC_DI_RAW_DATA \
  -ts INFORMATICA_TO_DBT \
  -l L1,L2,L3 \
  -o ./recon_reports \
  --format both
```
**Expected**: L1 (schema match), L2 (row counts match), L3 (aggregate checks pass). Generates HTML + JSON report in a timestamped subdirectory under `./recon_reports/` (e.g., `recon_reports/20260415_025139/`).

**Note**: Use `-l all` for full 6-layer validation (adds L4 hash, L5 row diff, L6 business rules — slower). L1-L3 is sufficient for a demo.

### Step 11: REPORT — Generate EWI assessment report
```bash
PYTHONPATH="" python3 -m informatica_to_dbt.cli report \
  -p dim_output_demo \
  -o ./recon_reports \
  -f both
```
**Expected**: HTML + JSON report with conversion quality scores, transformation coverage, errors/warnings/info summary. Output writes directly to `./recon_reports/` (e.g., `recon_reports/ewi_assessment_report.html` and `.json`). Note: unlike `reconcile`, the `report` command does NOT create a timestamped subdirectory.

### Step 12: SCHEDULE — Create daily Snowflake TASKs
```sql
-- Root task: dbt run at 6 AM UTC daily
CREATE OR REPLACE TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_CLEAN_RUN_TASK
  WAREHOUSE = SMALL_WH
  SCHEDULE = 'USING CRON 0 6 * * * UTC'
  COMMENT = 'Daily dbt run for DIM_EQUIPMENT_CLEAN pipeline'
AS
  EXECUTE DBT PROJECT DIM_EQUIPMENT_CLEAN ARGS = 'run';

-- Child task: dbt test after run completes
-- NOTE: COMMENT must come BEFORE the AFTER clause (Snowflake syntax requirement)
CREATE OR REPLACE TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_CLEAN_TEST_TASK
  WAREHOUSE = SMALL_WH
  COMMENT = 'Daily dbt test for DIM_EQUIPMENT_CLEAN pipeline (runs after run task)'
  AFTER TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_CLEAN_RUN_TASK
AS
  EXECUTE DBT PROJECT DIM_EQUIPMENT_CLEAN ARGS = 'test';

-- Resume: CHILD first, then ROOT
ALTER TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_CLEAN_TEST_TASK RESUME;
ALTER TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.DIM_EQUIPMENT_CLEAN_RUN_TASK RESUME;
```

### Step 13: GIT-PUSH (SKIP FOR DEMO)
```bash
# Framework CLI command (shown for capability demonstration — DO NOT execute during demo):
# PYTHONPATH="" python3 -m informatica_to_dbt.cli git-push \
#   --project dim_output_demo \
#   --remote origin \
#   --branch feature/dim-equipment \
#   --message "Add DIM_EQUIPMENT dbt project from Informatica conversion"
```
**SKIP**: Do NOT git push during a demo run. This step shows that the framework supports automated git commit+push as the final pipeline step. In production, this would version-control the generated dbt project.

### Final: PIPELINE SUMMARY
Print a summary table showing all 14 steps with their status (step number, name, CLI command used, result). This demonstrates the complete end-to-end framework pipeline:

```
discover → convert → inspect → validate → deploy → run → test → fix → reconcile → report → schedule → git-push
```

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
| Log file | `dim_output_demo/logs/convert.log` (DEBUG level) |
