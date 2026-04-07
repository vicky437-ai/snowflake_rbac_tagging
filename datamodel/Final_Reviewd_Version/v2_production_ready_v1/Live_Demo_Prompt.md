# Live Demo Prompt — Clean End-to-End Pipeline

**Tested**: 2026-04-06 | **Result**: 13/13 models PASS, 56/56 tests PASS  
**Input**: `DIM_INPUT/s_m_INCR_DM_DIM_EQUIPMENT.XML`  
**Source Schema**: `DIM_EQUIPMENT_SOURCES` (11 tables, 65K+ rows of real data)

---

## Prompt (copy everything below this line)

---

I want to run a clean end-to-end pipeline for the DIM_INPUT directory using the infa2dbt framework. This uses REAL source data in `DIM_EQUIPMENT_SOURCES` schema (not MOCK_SOURCES). Execute all 11 steps sequentially and stop if any step fails with unrecoverable errors.

**IMPORTANT NOTES (learned from prior runs):**
- All `python3 -m informatica_to_dbt.cli` commands MUST be prefixed with `PYTHONPATH=""` (Python 3.12/3.13 site-packages conflict with lxml)
- SKIP local dbt compile/validate — there is a known version conflict between local dbt-fusion v2.0 (`accepted_values` requires `arguments:` format) and Snowflake native dbt 1.9.4 (uses old `values:` format). The YAML files are correct for the deployment target (Snowflake native dbt 1.9.4). Deploy directly.
- When suspending Snowflake TASKs: suspend ROOT task first, then child tasks (opposite of resume order)
- When resuming Snowflake TASKs: resume CHILD tasks first, then ROOT task

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
  --source-schema DIM_EQUIPMENT_SOURCES
```
**Expected**: 1 XML file, 1 mapping, 3 sources, 1 target, 34 transformations. Verify all 11 tables exist in DIM_EQUIPMENT_SOURCES.

### Step 2: CONVERT — Parse XML and generate dbt project
```bash
PYTHONPATH="" python3 -m informatica_to_dbt.cli convert \
  --input DIM_INPUT \
  --output dim_output_demo \
  --source-schema DIM_EQUIPMENT_SOURCES \
  --no-cache
```
**Expected**: 18 files generated (13 SQL models + 3 YAML schemas + 1 macro + 1 dbt_project.yml). Quality score ~99/100.

### Step 3: INSPECT — Verify generated models
Verify:
- All `source()` references use `dim_equipment_sources` (lowercase)
- `_sources.yml` lists `schema: DIM_EQUIPMENT_SOURCES` (uppercase) with all 11 tables
- 13 SQL models: 3 staging + 9 intermediate + 1 marts
- The marts model `dim_equipment.sql` is materialized as `incremental` with `merge_update_columns`
- No staging models reference `EDW_CREATE_USER`, `EDW_CREATE_TMS`, `EDW_UPDATE_USER`, or `EDW_UPDATE_TMS` from source tables (these only exist on the target DIM_EQUIPMENT table, not on EQPMNT_AAR_BASE or EQPMNT_NON_RGSTRD)
- The `dim_equipment.sql` file is complete (should be ~240 lines with 3 UNION ALL branches for insert/update/type2 records, not truncated)
- `STATUS_CD` accepted_values tests include 2-char codes `['AC', 'IN', 'DE']` (real data uses 2-char codes, not single-char `['A', 'I', 'D']`)

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
**Expected**: `DIM_EQUIPMENT_CLEAN successfully created.` (~237 files copied)

### Step 7: RUN — Execute dbt models
```bash
snow dbt execute -c myconnection \
  --database TPC_DI_RAW_DATA \
  --schema INFORMATICA_TO_DBT \
  DIM_EQUIPMENT_CLEAN run
```
**Expected**: `PASS=13 WARN=0 ERROR=0 SKIP=0 TOTAL=13` (12 views + 1 incremental model)

### Step 8: TEST — Run dbt tests
```bash
snow dbt execute -c myconnection \
  --database TPC_DI_RAW_DATA \
  --schema INFORMATICA_TO_DBT \
  DIM_EQUIPMENT_CLEAN test
```
**Expected**: `PASS=56 WARN=0 ERROR=0 SKIP=0 TOTAL=56`

### Step 9: FIX — Fix any failures
If any models or tests fail:
1. Query actual data to understand the real values/schema
2. Fix the generated SQL/YAML files locally
3. Redeploy with `snow dbt deploy ... --force`
4. Re-run the failed step

**Common issues to watch for:**
- Staging models referencing EDW audit columns that don't exist on source tables → remove them, use `RECORD_UPDATE_TMS` instead of `EDW_UPDATE_TMS` in WHERE clauses
- Truncated model files (Opus max_tokens 8192 limit) → reconstruct complete file from intermediate model column lists
- `accepted_values` test mismatches → query actual data with `SELECT col, COUNT(*) FROM table GROUP BY col` and update YAML

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
3. `git push` to both remotes (personal + company)

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
