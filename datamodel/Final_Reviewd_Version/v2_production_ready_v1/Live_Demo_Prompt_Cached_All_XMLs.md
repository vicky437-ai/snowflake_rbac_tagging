# Live Demo Prompt — Cache-Based End-to-End Pipeline (All 8 XMLs)

**Tested**: 2026-04-10 | **Result**: 33/33 models PASS, 171/171 tests PASS
**Input**: `all_xmls/` (8 XML files)
**dbt Project**: `ALL_XMLS_TEST`
**Source Schema**: `MOCK_SOURCES` (35 tables with synthetic data)
**Cache**: Enabled (first run populates cache ~18 min; subsequent runs read from cache in seconds)

---

## How Cache Works

- Cache key = SHA-256(XML content + converter version + LLM model + prompt hash)
- Cache files are stored at `.infa2dbt/cache/` and **never expire** (no TTL)
- They are only removed by: `infa2dbt cache clear`, `infa2dbt cache remove <key>`, or manual deletion
- First `convert` run (cache MISS) calls LLM and takes ~18 minutes for 8 XMLs, then stores results in cache
- Subsequent `convert` runs (cache HIT) return instantly from disk — no LLM call
- If you change the XML, LLM model, or system prompt, a new cache key is generated (old entries remain)
- **No overwrite, no versioning — content-addressed (append-only)**. Each unique input combination creates a new entry with a different hash. Old entries are never overwritten; they just become stale (unused) because their key no longer matches.
- Use `PYTHONPATH="" python3 -m informatica_to_dbt.cli cache list` to see cached entries
- Use `PYTHONPATH="" python3 -m informatica_to_dbt.cli cache stats` for summary

---

## 8 Mappings in This Demo

| # | XML File | Mapping | Complexity | Strategy | Quality |
|---|----------|---------|-----------|----------|---------|
| 1 | wf_AM_DI_CUSTOMER.XML | m_AM_DI_CUSTOMER | 22 | DIRECT | 100 |
| 2 | wf_AP_FF_CITIBANK_VCA.XML | m_AP_FF_CITIBANK_VCA | 12 | DIRECT | 100 |
| 3 | wf_BL_FF_ZJ_JOURNALS.XML | m_BL_FF_ZJ_JOURNALS | 55 | STAGED | 100 |
| 4 | wf_BL_FF_ZJ_JOURNALS_STG.XML | m_BL_FF_ZJ_JOURNALS_STG | 67 | LAYERED | 100 |
| 5 | wf_CM_Z1_DAILY_SALES_FEED.XML | m_CM_Z1_DAILY_SALES_FEED | 15 | DIRECT | 96 |
| 6 | wf_DI_ITEM_MTRL_MASTER.XML | m_DI_ITEM_MTRL_MASTER | 82 | COMPLEX | 97 |
| 7 | wf_FF_AT_Z1_GL_ACCOUNT.XML | m_FF_AT_Z1_GL_ACCOUNT | 12 | DIRECT | 100 |
| 8 | wf_FF_AT_ZJ_GL_ACCOUNT.XML | m_FF_AT_ZJ_GL_ACCOUNT | 12 | DIRECT | 100 |

---

## Prompt (copy everything below this line)

---

I want to run a clean end-to-end pipeline for ALL 8 XMLs in the `all_xmls/` directory using the infa2dbt framework. This uses SYNTHETIC source data in `MOCK_SOURCES` schema. Create a new dbt project called `ALL_XMLS_TEST` in DB `TPC_DI_RAW_DATA` schema `INFORMATICA_TO_DBT`. Execute all 12 steps (0-11) sequentially. Stop if any step fails with unrecoverable errors.

**IMPORTANT: This is a CACHE-ENABLED run.** The convert step should read from cache if available. Do NOT use the `--no-cache` flag. If cache is empty (first run), it will call the LLM and populate cache automatically.

**IMPORTANT NOTES (learned from prior runs):**
- All `python3 -m informatica_to_dbt.cli` commands MUST be prefixed with `PYTHONPATH=""` (Python 3.12/3.13 site-packages conflict with lxml)
- SKIP local dbt compile/validate — there is a known version conflict between local dbt-fusion v2.0 (`accepted_values` requires `arguments:` format) and Snowflake native dbt 1.9.4 (uses old `values:` format). The YAML files are correct for the deployment target (Snowflake native dbt 1.9.4). Deploy directly.
- When suspending Snowflake TASKs: suspend ROOT task first, then child tasks (opposite of resume order)
- When resuming Snowflake TASKs: resume CHILD tasks first, then ROOT task
- Use `--schema-source snowflake` for the discover command (NOT `--source-schema`)
- The `--source-schema MOCK_SOURCES` flag is for the convert command only
- `snow dbt deploy` syntax: `snow dbt deploy ALL_XMLS_TEST --source DIR --profiles-dir DIR --force` (NOT `--project`)
- `snow dbt execute` syntax: `snow dbt execute ALL_XMLS_TEST run` (no `--connection` flag passed to execute)
- When mock data has domain-value issues (e.g., T1/T2/T3 placeholders), fix the mock data (TRUNCATE and reload), do NOT relax or remove the dbt tests
- Source schema is always `MOCK_SOURCES` — all source references must point to MOCK_SOURCES
- dbt project name is always `ALL_XMLS_TEST`

### Step 0: CLEANUP
```bash
# 0a. Suspend and drop any existing Snowflake TASKs (if they exist)
# Suspend root FIRST, then child
ALTER TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.ALL_XMLS_TEST_DAILY_RUN SUSPEND;
ALTER TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.ALL_XMLS_TEST_DAILY_TEST SUSPEND;
DROP TASK IF EXISTS TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.ALL_XMLS_TEST_DAILY_TEST;
DROP TASK IF EXISTS TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.ALL_XMLS_TEST_DAILY_RUN;

# 0b. Delete old output directory
rm -rf all_xmls_output
```

### Step 1: DISCOVER — XML inventory + Snowflake schema validation
```bash
PYTHONPATH="" python3 -m informatica_to_dbt.cli discover \
  --input all_xmls \
  --schema-source snowflake \
  --database TPC_DI_RAW_DATA \
  --schema MOCK_SOURCES
```
**Expected**: 8 XML files, 8 mappings, 15 sources, 35 tables. Verify all tables exist in MOCK_SOURCES.

### Step 2: CONVERT — Parse XMLs and generate unified dbt project (CACHE ENABLED)
```bash
PYTHONPATH="" python3 -m informatica_to_dbt.cli convert \
  --input all_xmls \
  --output all_xmls_output \
  --source-schema MOCK_SOURCES
```
**Note**: No `--no-cache` flag. The CLI natively supports directory input and merges all mappings via ProjectMerger. If cache exists for all 8 XMLs, output appears in seconds (cache HIT). If not, LLM generates fresh output (~18 min total) and stores it in cache.

**Expected**: ~62 files generated across 8 mapping folders. Average quality ~99/100.

### Step 3: INSPECT — Verify and fix generated models
Verify and fix these known LLM generation issues:

1. **Source table name mismatch for m_FF_AT_Z1_GL_ACCOUNT**: The LLM sometimes generates source table name `zb_reporting_finance_global_apptio_gb_z1_appt` from the XML Shortcut name. The actual MOCK_SOURCES table is `finance_global_apptio_gb_z1_appt`. Check and fix in:
   - `models/m_FF_AT_Z1_GL_ACCOUNT/staging/_sources.yml` — table name must reference `finance_global_apptio_gb_z1_appt`, and schema must be `MOCK_SOURCES`
   - `models/m_FF_AT_Z1_GL_ACCOUNT/staging/stg_ff_at_z1_gl_account.sql` — `{{ source() }}` reference must match

2. **Duplicate model names between m_BL_FF_ZJ_JOURNALS and m_BL_FF_ZJ_JOURNALS_STG**: Both mappings generate staging models named `stg_f0011.sql`, `stg_f0901.sql`, and `stg_f0911.sql`. dbt requires globally unique model names. Fix by renaming the STG versions:
   - `stg_f0011.sql` → `stg_f0011_journals_stg.sql`
   - `stg_f0901.sql` → `stg_f0901_journals_stg.sql`
   - `stg_f0911.sql` → `stg_f0911_journals_stg.sql`
   - Update all `ref()` calls in `m_BL_FF_ZJ_JOURNALS_STG/intermediate/` models to use new names
   - Update `_stg__schema.yml` model names to match renamed files

3. **Special character `#` in column name**: In `m_BL_FF_ZJ_JOURNALS/staging/stg_f0911.sql` and `m_BL_FF_ZJ_JOURNALS_STG/staging/stg_f0911_journals_stg.sql`, the column `glreg#` must use uppercase double-quoted identifier: `"GLREG#"`

4. **Special character `/` in column names**: In `m_BL_FF_ZJ_JOURNALS/staging/stg_ff_bl_zj_journals_src.sql`, columns with `/` must be double-quoted:
   - `"FOREIGN/DOMESTIC" AS foreign_domestic`
   - `"G/L_POSTED_CODE" AS gl_posted_code`
   - `"REVERSE/VOID" AS reverse_void`

5. **TRY_TO_DECIMAL on already-numeric column**: In `m_DI_ITEM_MTRL_MASTER/intermediate/int_material_transformed.sql`, the LLM may generate `TRY_TO_DECIMAL(material_volume, 13, 3)` on a column that is already NUMBER(18,9). Snowflake cannot TRY_CAST between numeric precisions. Fix by using direct cast:
   - `m.material_volume::NUMBER(13, 3) AS material_volume_decimal`
   - `m.unit_specific_product_width::NUMBER(13, 3) AS product_width_decimal`

6. **Wrong column names in fct_ff_bl_zj_journals mart**: The mart model may reference flat-file column names but upstream `int_journal_posted` produces `clean_*` prefixed columns. Rewrite the mart to SELECT from `int_journal_posted` using the actual `clean_*` column names.

7. **`{{ this }}` self-reference in fct_zj_journals**: The model may LEFT JOIN against itself (`{{ this }}`), which fails on first run when the table doesn't exist. Remove the self-reference and make it a simple SELECT from upstream `int_journals_filtered`.

8. **Missing `out_flag` in int_journal_base output**: The model computes `out_flag` in a CTE but the final explicit column list may not include it. The test schema expects `out_flag` to exist in the output. Add `out_flag,` to the final CTE's SELECT list if missing.

9. **All `_sources.yml` schema references**: Every `_sources.yml` file across all 8 mappings must have `schema: MOCK_SOURCES`. Verify this after convert.

### Step 4: COPY profiles.yml
```bash
cp profiles.yml all_xmls_output/profiles.yml
```

### Step 5: VALIDATE — SKIP
**SKIP this step.** Local dbt-fusion v2.0 rejects the old `accepted_values` format (`values:` at top level) that Snowflake native dbt 1.9.4 requires. The generated YAML is correct for the deployment target. Proceed directly to deploy.

### Step 6: DEPLOY — Deploy to Snowflake
```bash
snow dbt deploy ALL_XMLS_TEST \
  --source all_xmls_output \
  --profiles-dir all_xmls_output \
  --force
```
**Expected**: `ALL_XMLS_TEST successfully created.` (~58 files copied)

### Step 7: RUN — Execute dbt models
```bash
snow dbt execute ALL_XMLS_TEST run
```
**Expected**: `PASS=33 WARN=0 ERROR=0 SKIP=0` (28 views + 4 tables + 1 incremental)

If errors occur, read the error messages, fix the model SQL locally in `all_xmls_output/`, redeploy with `--force`, and re-run. Common errors are listed in Step 3.

### Step 8: TEST — Run dbt tests
```bash
snow dbt execute ALL_XMLS_TEST test
```
**Expected**: `PASS=171 WARN=0 ERROR=0 SKIP=0`

### Step 9: FIX — Fix mock data if tests fail
If tests fail due to mock data having invalid domain values (T1/T2/T3 placeholders instead of real domain values), fix the MOCK_SOURCES data. **Do NOT relax or remove the tests.**

Known mock data fixes needed:

1. **F0011** (MOCK_SOURCES): TRUNCATE and reload with valid data. Key columns:
   - `icicu` must be numeric (e.g., 1001, 1002, 1003) — not 'T1/T2/T3'
   - `icist` must be 'A' or 'I' (batch status) — not 'T1/T2/T3'
   - `icicut` must match F0911.glicut values

2. **F0911** (MOCK_SOURCES): Update domain columns:
   - `glre` (reverse/void) must be 'R', 'N', or '' — not 'T1/T2/T3'
   - `glicu` must be numeric matching F0011.icicu
   - `glicut` must be 'GL' or 'AP' — not 'T1/T2/T3'
   - `glaid` must match F0901.gmaid for JOIN keys

3. **F0901** (MOCK_SOURCES): Update `gmaid` to match F0911.glaid values (e.g., 'ACC001', 'ACC002', 'ACC003')

4. **FINANCE_GLOBAL_APPTIO_GB_Z1_APPT** (MOCK_SOURCES): Update `debitcreditindicator` to valid values: 'S', 'H', or 'D' — not 'T1/T2/T3'

5. **T_CM_Z1_SALES** (MOCK_SOURCES): Update `trailerrecord_yn` to 'Y' or 'N' — not 'T1/T2/T3'

After fixing mock data, redeploy and re-run models and tests:
```bash
snow dbt deploy ALL_XMLS_TEST --source all_xmls_output --profiles-dir all_xmls_output --force
snow dbt execute ALL_XMLS_TEST run
snow dbt execute ALL_XMLS_TEST test
```

### Step 10: SCHEDULE — Create daily Snowflake TASKs
```sql
-- Root task: dbt run at 7 AM UTC daily
CREATE OR REPLACE TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.ALL_XMLS_TEST_DAILY_RUN
  WAREHOUSE = SMALL_WH
  SCHEDULE = 'USING CRON 0 7 * * * UTC'
  COMMENT = 'Daily dbt run for ALL_XMLS_TEST project (8 Informatica mappings)'
AS
  EXECUTE DBT PROJECT TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.ALL_XMLS_TEST
    ARGS = 'run';

-- Child task: dbt test after run completes
CREATE OR REPLACE TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.ALL_XMLS_TEST_DAILY_TEST
  WAREHOUSE = SMALL_WH
  AFTER TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.ALL_XMLS_TEST_DAILY_RUN
AS
  EXECUTE DBT PROJECT TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.ALL_XMLS_TEST
    ARGS = 'test';

-- Resume: CHILD first, then ROOT
ALTER TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.ALL_XMLS_TEST_DAILY_TEST RESUME;
ALTER TASK TPC_DI_RAW_DATA.INFORMATICA_TO_DBT.ALL_XMLS_TEST_DAILY_RUN RESUME;
```

### Step 11: REPORT (NO GIT PUSH)
1. Print a final pipeline summary table (step, action, result)
2. Include: conversion stats per mapping, model count, test count, task status
3. **Do NOT git push** — this is a demo run

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
| Source Schema | `MOCK_SOURCES` (35 tables, synthetic data) |
| Target Schema | `INFORMATICA_TO_DBT` |
| Warehouse | `SMALL_WH` |
| Snowflake native dbt | v1.9.4 |
| Local dbt | dbt-fusion v2.0 (DO NOT use for compile/validate) |
| Python | Use `PYTHONPATH="" python3` (v3.12) |
| Snowflake CLI | v3.16.0 |
| Deploy project name | `ALL_XMLS_TEST` |
| Cache location | `.infa2dbt/cache/` (never expires) |
| XMLs | 8 files in `all_xmls/` directory |
| Mappings | 8 (DIRECT x5, STAGED x1, LAYERED x1, COMPLEX x1) |
| Expected models | 33 (28 views + 4 tables + 1 incremental) |
| Expected tests | 171 |
