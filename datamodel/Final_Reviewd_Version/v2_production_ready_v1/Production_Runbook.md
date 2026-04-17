# Production Runbook — infa2dbt Framework

## Purpose

This runbook provides step-by-step instructions for setting up and running the `infa2dbt` migration framework on a new machine. Follow this guide to go from a fresh environment to a fully executed Informatica-to-dbt migration pipeline.

---

## Prerequisites

### Software Requirements

| Software | Minimum Version | Purpose |
|----------|----------------|---------|
| Python | 3.9+ | Runtime for infa2dbt CLI |
| pip | 21.0+ | Python package installer (ships with Python) |
| Git | 2.30+ | Version control, used by `git-push` command |
| Snowflake CLI (`snow`) | 3.0+ | Deploy and execute dbt projects natively on Snowflake |
| dbt-core + dbt-snowflake | 1.7+ | Local dbt compilation and validation (optional if deploying via `snow dbt deploy`) |

### Snowflake Requirements

| Requirement | Details |
|-------------|---------|
| Snowflake account | Active account with `ACCOUNTADMIN` or equivalent role |
| Warehouse | A running warehouse (e.g. `SMALL_WH`) |
| Database | Target database for dbt models (e.g. `MY_DATABASE`) |
| Source schema | Schema containing source tables that Informatica mappings reference |
| Target schema | Schema where dbt models will be materialized (e.g. `INFORMATICA_TO_DBT`) |
| Deploy schema | Schema for the Snowflake-native dbt project object (e.g. `MY_DBT_SCHEMA`) |
| Cortex LLM access | The `convert` command uses `SNOWFLAKE.CORTEX.COMPLETE()` for code generation |

### Input Files

| Item | Details |
|------|---------|
| Informatica XML exports | PowerCenter XML files exported from the Informatica repository (workflows, sessions, or mappings) |
| Source table data | The Snowflake source schema must contain tables matching the sources defined in the Informatica mappings |

---

## Step 1: Install Python and System Tools

### macOS

```bash
# Install Homebrew (if not installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Python 3.9+
brew install python@3.12

# Install Git (if not installed)
brew install git

# Verify
python3 --version   # Must be 3.9+
git --version        # Must be 2.30+
```

> **IMPORTANT — Python 3.12/3.13 users**: The `snowflake.snowpark` namespace package conflicts with `lxml` imports. You **must** prefix all `python3 -m informatica_to_dbt.cli` and `pytest` commands with `PYTHONPATH=""`. Example: `PYTHONPATH="" python3 -m informatica_to_dbt.cli convert ...`. This applies to both CLI commands and test execution.

### Windows

```powershell
# Download and install Python from https://www.python.org/downloads/
# Ensure "Add Python to PATH" is checked during installation

# Install Git from https://git-scm.com/download/win

# Verify (from PowerShell or Command Prompt)
python --version
git --version
```

> **Windows PYTHONPATH note:** On Windows, the equivalent of `PYTHONPATH=""` is `set PYTHONPATH=` (CMD) or `$env:PYTHONPATH=""` (PowerShell) before running `infa2dbt` commands, if you encounter `lxml` import errors.

### Linux (Ubuntu/Debian)

```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv git

# Verify
python3 --version
git --version
```

---

## Step 2: Install Snowflake CLI

```bash
# Option 1: pip (inside a virtual environment — recommended)
pip install snowflake-cli

# Option 2: Homebrew (macOS)
brew install snowflake-cli

# Option 3: pipx (isolated install — avoids dependency conflicts)
pipx install snowflake-cli

# Verify
snow --version   # Must be 3.0+
```

> **Note:** On macOS 13+ with Homebrew Python, `pip install` outside a virtual environment may fail with "externally-managed-environment" error. Use `brew install` or `pipx install` instead, or install within a virtual environment (Step 4).

---

## Step 3: Configure Snowflake Connection

Create or edit `~/.snowflake/config.toml`:

```toml
default_connection_name = "myconnection"

[connections]
[connections.myconnection]
user = "<YOUR_USERNAME>"
account = "<YOUR_ACCOUNT_IDENTIFIER>"
password = "<YOUR_PASSWORD>"
authenticator = "snowflake"
warehouse = "<YOUR_WAREHOUSE>"
database = "<YOUR_DATABASE>"
schema = "<YOUR_SCHEMA>"
role = "<YOUR_ROLE>"
connection_timeout = 60
```

> **Security note:** For production use, consider `authenticator = "externalbrowser"` or key-pair authentication instead of storing a password in plaintext.

Verify the connection:

```bash
snow connection test -c myconnection
```

---

## Step 4: Clone and Install the Framework

```bash
# Clone the repository
git clone <REPOSITORY_URL>
cd informatica-to-dbt

# Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate   # macOS/Linux
# .venv\Scripts\activate    # Windows

# Install the framework and all dependencies
pip install -e .
```

This installs the following Python dependencies (from `pyproject.toml`):

| Package | Version | Purpose |
|---------|---------|---------|
| lxml | >=4.9 | XML parsing of Informatica PowerCenter exports |
| networkx | >=3.0 | DAG construction for transformation dependency graphs |
| sqlparse | >=0.4 | SQL parsing and formatting |
| jinja2 | >=3.1 | dbt model template rendering |
| pyyaml | >=6.0 | YAML generation for dbt schema and source files |
| click | >=8.1 | CLI framework |

Verify installation:

```bash
infa2dbt version
```

Expected output:
```
infa2dbt v1.0.0
```

---

## Step 5: Install dbt

```bash
pip install dbt-core dbt-snowflake

# Verify
dbt --version
```

> **Note:** A local dbt installation is optional if you plan to deploy directly to Snowflake using `infa2dbt deploy --mode direct` and execute via `snow dbt execute`. The Snowflake-native dbt runtime handles compilation, model materialization, and testing without a local dbt CLI.

---

## Step 6: Prepare Input Files

Place your Informatica PowerCenter XML exports into an input directory:

```
project_root/
├── input/
│   ├── wf_mapping_one.XML
│   ├── wf_mapping_two.XML
│   └── s_m_mapping_three.XML
└── ...
```

> XML files can be workflow exports (`wf_*.XML`), session exports (`s_m_*.XML`), or mapping-only exports. The framework auto-detects the type.

---

## Step 7: Run the Migration Pipeline

Execute each step in order. All commands are run from the project root directory.

> **Re-deployment cleanup:** If you are re-deploying a project that already exists in Snowflake, you must first suspend and drop the existing TASKs and project object. Run these commands in exact order:
> ```sql
> -- 1. Suspend ROOT task first, then CHILD
> ALTER TASK <DATABASE>.<SCHEMA>.<PROJECT_NAME>_RUN_TASK SUSPEND;
> ALTER TASK <DATABASE>.<SCHEMA>.<PROJECT_NAME>_TEST_TASK SUSPEND;
> -- 2. Drop CHILD task first, then ROOT
> DROP TASK IF EXISTS <DATABASE>.<SCHEMA>.<PROJECT_NAME>_TEST_TASK;
> DROP TASK IF EXISTS <DATABASE>.<SCHEMA>.<PROJECT_NAME>_RUN_TASK;
> -- 3. Drop the dbt project object
> DROP DBT PROJECT IF EXISTS <DATABASE>.<SCHEMA>.<PROJECT_NAME>;
> ```

### 7.1 Convert

Parse Informatica XML files and generate a dbt project:

```bash
infa2dbt convert \
    -i ./input/ \
    -o ./output/dbt_project \
    -m new \
    --connection myconnection \
    --source-schema <YOUR_SOURCE_SCHEMA> \
    --log-level info \
    --log-file conversion.log
```

**What this does:**
- Parses all XML files in `./input/`
- Extracts mappings, transformations, source/target definitions
- Calls Snowflake Cortex LLM to generate dbt SQL models
- Produces a complete dbt project in `./output/dbt_project/`

**Expected output:** A dbt project directory containing `dbt_project.yml`, `models/`, `macros/`, `seeds/`, `snapshots/`, and `tests/`.

> **Important:** The `convert` command does **not** generate `profiles.yml` — you must create it manually before running `validate` (see Step 7.2 below).

### 7.2 Create profiles.yml

The `validate` command requires a `profiles.yml` file in the dbt project directory. Create it manually:

```bash
cat > ./output/dbt_project/profiles.yml << 'EOF'
dbt_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "<YOUR_ACCOUNT_IDENTIFIER>"
      user: "<YOUR_USERNAME>"
      password: "<YOUR_PASSWORD>"
      warehouse: "<YOUR_WAREHOUSE>"
      database: "<YOUR_DATABASE>"
      schema: "<YOUR_DEPLOY_SCHEMA>"
      role: "<YOUR_ROLE>"
      threads: 4
EOF
```

> **Note:** The profile name (first line) must match the `profile:` value in `dbt_project.yml`. Check with: `grep profile ./output/dbt_project/dbt_project.yml`

### 7.3 Inspect (Manual Review)

Before proceeding, manually review the generated dbt models:

```bash
# Review the generated SQL models
ls -la ./output/dbt_project/models/

# Spot-check a staging model
cat ./output/dbt_project/models/staging/stg_<YOUR_MAPPING>.sql

# Verify schema YAML
cat ./output/dbt_project/models/staging/_stg__schema.yml
```

**What to check:**
- SQL models contain valid SELECT statements with `ref()` / `source()` references
- No leftover Informatica functions (IIF, ISNULL, ADD_TO_DATE, etc.)
- Schema YAML has appropriate tests (not_null, unique, accepted_values)
- Materialization settings are correct (view for staging, table/incremental for marts)

> This step is not automated — it's a manual quality gate before validation and deployment.

> **packages.yml warning:** If the LLM output references `dbt_utils`, the framework may auto-generate a `packages.yml` file. **Snowflake native dbt (v1.9.x) does not support packages.** Delete it before deploying:
> ```bash
> rm -f ./output/dbt_project/packages.yml
> ```
> Replace any `dbt_utils` macro calls in generated SQL with native Snowflake SQL equivalents.

### 7.4 Discover (Optional)

Inventory the XML files and discover source schemas:

```bash
infa2dbt discover \
    -i ./input/ \
    --schema-source snowflake \
    --database <YOUR_DATABASE> \
    --schema <YOUR_SOURCE_SCHEMA>
```

**What this does:**
- Scans the input directory for all XML files
- Reports mapping count, source tables, and target tables
- Queries Snowflake `INFORMATION_SCHEMA` to discover column definitions

### 7.5 Report

Generate an EWI (Errors/Warnings/Informational) assessment report:

```bash
infa2dbt report \
    -p ./output/dbt_project \
    -f both
```

**What this does:**
- Reads conversion metrics saved during the `convert` step
- Generates an HTML report and JSON report in `./output/dbt_project/reports/`

### 7.6 Validate

Compile, run models, and execute tests:

```bash
infa2dbt validate \
    -p ./output/dbt_project \
    --run-tests
```

**What this does:**
- Runs `dbt compile` to check SQL syntax
- Runs `dbt run` to materialize all models in Snowflake
- Runs `dbt test` to execute all schema and data tests

**Success criteria:** All models pass, all tests pass, zero errors.

### 7.7 Deploy

Deploy the dbt project to Snowflake as a native dbt project object:

```bash
infa2dbt deploy \
    -p ./output/dbt_project \
    -d <YOUR_DATABASE> \
    -s <YOUR_DEPLOY_SCHEMA> \
    -n <YOUR_PROJECT_NAME> \
    -w <YOUR_WAREHOUSE> \
    --connection myconnection \
    --mode direct
```

**What this does:**
- Packages the dbt project
- Uses `snow dbt deploy` to push it as a Snowflake-native dbt project object

> **Re-deploy note:** `infa2dbt deploy --mode direct` does **not** pass `--force` to `snow dbt deploy`. If the project already exists, the deploy will fail. Either drop the existing project first (see the re-deployment cleanup section at the top of Step 7) or deploy directly with `snow dbt deploy`:
> ```bash
> snow dbt deploy <YOUR_PROJECT_NAME> --source ./output/dbt_project \
>     --profiles-dir ./output/dbt_project --force
> ```

> **TASK Ordering (if using scheduled deployment):** Snowflake TASKs have strict ordering requirements when managing parent-child chains:
> - **Suspend**: Suspend ROOT task first, then CHILD tasks
> - **Drop**: Drop CHILD tasks first, then ROOT task
> - **Create**: Create ROOT task first, then CHILD tasks
> - **Resume**: Resume CHILD tasks first, then ROOT task
>
> Violating this ordering will cause Snowflake errors (e.g., "Cannot resume a child task while root is suspended").

### 7.8 Execute on Snowflake

Run models and tests natively on Snowflake:

```bash
# Run all models
snow dbt execute <YOUR_PROJECT_NAME> run

# Run all tests
snow dbt execute <YOUR_PROJECT_NAME> test
```

### 7.9 Schedule (Optional)

Create Snowflake TASKs to run the dbt project on a schedule. This uses a parent-child TASK chain: the ROOT task runs models, and the CHILD task runs tests after the ROOT completes.

```sql
-- 1. Create ROOT task (runs models on schedule)
CREATE OR REPLACE TASK <DATABASE>.<SCHEMA>.<PROJECT_NAME>_RUN_TASK
  WAREHOUSE = <YOUR_WAREHOUSE>
  SCHEDULE = 'USING CRON 0 6 * * * UTC'
  COMMENT = 'Daily dbt run for <PROJECT_NAME>'
AS
  EXECUTE DBT PROJECT <PROJECT_NAME> ARGS = 'run';

-- 2. Create CHILD task (runs tests after models complete)
CREATE OR REPLACE TASK <DATABASE>.<SCHEMA>.<PROJECT_NAME>_TEST_TASK
  WAREHOUSE = <YOUR_WAREHOUSE>
  AFTER <DATABASE>.<SCHEMA>.<PROJECT_NAME>_RUN_TASK
AS
  EXECUTE DBT PROJECT <PROJECT_NAME> ARGS = 'test';

-- 3. Resume tasks (CHILD first, then ROOT)
ALTER TASK <DATABASE>.<SCHEMA>.<PROJECT_NAME>_TEST_TASK RESUME;
ALTER TASK <DATABASE>.<SCHEMA>.<PROJECT_NAME>_RUN_TASK RESUME;

-- 4. Verify tasks are running
SHOW TASKS IN SCHEMA <DATABASE>.<SCHEMA>;
```

> **Note:** The `COMMENT` clause cannot appear after the `AFTER` clause in Snowflake TASK DDL. That is why the CHILD task above does not include a `COMMENT`.

### 7.10 Reconcile Source vs Target

Validate that the dbt output matches the original source data using the 6-layer reconciliation engine:

```bash
infa2dbt reconcile \
    -sd <YOUR_DATABASE> -ss <YOUR_SOURCE_SCHEMA> \
    -td <YOUR_DATABASE> -ts <YOUR_DEPLOY_SCHEMA> \
    -c <YOUR_CONNECTION> -l all \
    -o ./recon_reports --format both
```

**What each layer checks**:

| Layer | Check | What It Catches |
|-------|-------|----------------|
| L1 | Schema comparison | Missing or mistyped columns, wrong data types |
| L2 | Row count | Dropped or duplicated rows |
| L3 | Aggregate (SUM/MIN/MAX) | Truncated values, rounding errors, NULL handling |
| L4 | Hash fingerprint | Any data difference (fast full-table check) |
| L5 | Row-level diff | Exactly which rows and columns differ |
| L6 | Business rules | Custom SQL assertions from YAML config |

**For a quick smoke test**, run only L1 and L2:
```bash
infa2dbt reconcile \
    -sd <YOUR_DATABASE> -ss <YOUR_SOURCE_SCHEMA> \
    -td <YOUR_DATABASE> -ts <YOUR_DEPLOY_SCHEMA> \
    -c <YOUR_CONNECTION> -l L1,L2
```

**For config-driven reconciliation** with explicit table mappings:
```bash
infa2dbt reconcile \
    -sd <YOUR_DATABASE> -ss <YOUR_SOURCE_SCHEMA> \
    -td <YOUR_DATABASE> -ts <YOUR_DEPLOY_SCHEMA> \
    -c <YOUR_CONNECTION> --config ./recon_config.yml
```

Reports are written to `./recon_reports/` as HTML (interactive dashboard) and JSON (CI/CD integration).

### 7.11 Push to Git

Commit and push the generated dbt project to a Git repository:

```bash
infa2dbt git-push \
    -p ./output/dbt_project \
    --remote-url <YOUR_GIT_REPO_URL> \
    -b main \
    -m "Informatica to dbt migration"
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Resolution |
|-------|-------|------------|
| `ModuleNotFoundError: informatica_to_dbt` | Framework not installed | Run `pip install -e .` from the project root |
| `ModuleNotFoundError: lxml` (Python 3.12+) | `snowflake.snowpark` namespace package conflicts with `lxml` import | Prefix all CLI commands with `PYTHONPATH=""` (e.g. `PYTHONPATH="" python -m informatica_to_dbt.cli convert ...`) |
| `snow: command not found` | Snowflake CLI not installed | Run `pip install snowflake-cli` |
| `dbt: command not found` | dbt not installed | Run `pip install dbt-core dbt-snowflake` |
| `Connection failed` | Bad credentials in `config.toml` | Verify account, user, password; run `snow connection test` |
| `TRY_CAST error (001065)` | Source column already the target type | The framework handles this automatically; if seen in custom models, use `TO_CHAR()` directly |
| `Cortex LLM timeout` | Large mapping or slow LLM response | Retry the `convert` command; cached results are reused automatically |
| `dbt test failures` | Source data quality issues | Review test definitions in `_stg__schema.yml`; adjust accepted values or thresholds |
| `Permission denied on deploy` | Insufficient Snowflake role | Ensure the configured role has `CREATE DATABASE`, `CREATE SCHEMA`, and `USAGE` grants |
| `TRY_CAST from TIMESTAMP_NTZ to DATE not supported` | `TRY_TO_DATE()` called on a TIMESTAMP_NTZ column | Use `column::DATE` cast instead; the post-processor handles this automatically |
| `accepted_values` test format error | dbt-fusion v2.0 uses `arguments:` wrapper; Snowflake native dbt 1.9.x does not | Skip local `dbt compile` if using dbt-fusion v2.0. Deploy directly via `snow dbt deploy` and execute via `snow dbt execute` — Snowflake native dbt 1.9.4 handles compilation correctly |
| `Cannot resume a child task while root is suspended` | Snowflake TASK ordering violated | Suspend ROOT first, then CHILD. Resume CHILD first, then ROOT. Drop CHILD first, then ROOT. Create ROOT first, then CHILD |
| `profiles.yml not found` during validate | `convert` does not generate `profiles.yml` | Create it manually (see Step 7.2) before running `validate` |
| `packages.yml` causes deploy failure | Snowflake native dbt 1.9.x does not support packages | Delete `packages.yml` from the project directory before deploying |
| `COMMENT` syntax error in TASK DDL | `COMMENT` clause placed after `AFTER` clause | Move `COMMENT` before `AFTER`, or remove it from CHILD tasks |
| Deploy fails with "project already exists" | `infa2dbt deploy` does not pass `--force` | Drop existing project first, or use `snow dbt deploy --force` directly |

### Verifying a Successful Run

After completing all steps, verify:

```bash
# Check model count and test results
infa2dbt validate -p ./output/dbt_project --run-tests

# Query a mart table in Snowflake
snow sql -c myconnection -q "SELECT COUNT(*) FROM <YOUR_DATABASE>.<YOUR_SCHEMA>.<MART_TABLE>"
```

### Cache Management

If you need to re-run conversion from scratch (e.g. after updating XML files):

```bash
# List cached entries and stats
infa2dbt cache list
infa2dbt cache stats

# Clear the entire LLM response cache (requires confirmation)
infa2dbt cache clear --yes

# Re-run convert
infa2dbt convert -i ./input/ -o ./output/dbt_project -m new \
    --connection myconnection --source-schema <YOUR_SOURCE_SCHEMA>
```

> **Note:** There is no `cache remove` subcommand for selective deletion. To remove a single entry, delete the corresponding `.json` file from the cache directory (default: `~/.infa2dbt/cache/`). Use `infa2dbt cache list` to find the entry key, then remove the matching file.

---

## Directory Structure After a Complete Run

```
project_root/
├── input/                          # Informatica XML exports (your input)
│   ├── wf_mapping_one.XML
│   └── ...
├── output/
│   └── dbt_project/                # Generated dbt project (your output)
│       ├── dbt_project.yml
│       ├── profiles.yml            # Manually created (see Step 7.2)
│       ├── macros/
│       ├── models/
│       │   └── <mapping_name>/
│       │       ├── staging/        # Source views + schema tests
│       │       ├── intermediate/   # Transformation logic
│       │       └── marts/          # Final business tables
│       ├── reports/                # EWI assessment reports
│       ├── seeds/
│       ├── snapshots/
│       └── tests/
├── recon_reports/                   # Reconciliation reports (HTML + JSON)
├── pyproject.toml
└── src/
    └── informatica_to_dbt/         # Framework source code
```

---

## Quick Reference — Full Pipeline (Copy-Paste)

Replace all `<PLACEHOLDER>` values with your environment-specific settings:

> **Python 3.12/3.13 users**: Prefix all `infa2dbt` and `python3 -m` commands below with `PYTHONPATH=""` if you encounter `lxml` import errors.

```bash
# Activate virtual environment
source .venv/bin/activate

# 1. Convert
infa2dbt convert -i ./input/ -o ./output/dbt_project -m new \
    --connection <CONNECTION> --source-schema <SOURCE_SCHEMA>

# 2. Create profiles.yml (convert does NOT generate this)
cat > ./output/dbt_project/profiles.yml << 'EOF'
dbt_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "<ACCOUNT>"
      user: "<USER>"
      password: "<PASSWORD>"
      warehouse: "<WAREHOUSE>"
      database: "<DATABASE>"
      schema: "<DEPLOY_SCHEMA>"
      role: "<ROLE>"
      threads: 4
EOF

# 3. Inspect (manual review — also delete packages.yml if present)
ls ./output/dbt_project/models/
rm -f ./output/dbt_project/packages.yml

# 4. Discover (optional)
infa2dbt discover -i ./input/ --schema-source snowflake \
    --database <DATABASE> --schema <SOURCE_SCHEMA>

# 5. Report
infa2dbt report -p ./output/dbt_project -f both

# 6. Validate
infa2dbt validate -p ./output/dbt_project --run-tests

# 7. Deploy
infa2dbt deploy -p ./output/dbt_project -d <DATABASE> -s <DEPLOY_SCHEMA> \
    -n <PROJECT_NAME> -w <WAREHOUSE> --connection <CONNECTION> --mode direct

# 8. Execute
snow dbt execute <PROJECT_NAME> run
snow dbt execute <PROJECT_NAME> test

# 9. Reconcile
infa2dbt reconcile -sd <DATABASE> -ss <SOURCE_SCHEMA> -td <DATABASE> -ts <DEPLOY_SCHEMA> \
    -c <CONNECTION> -l all -o ./recon_reports --format both

# 10. Git Push
infa2dbt git-push -p ./output/dbt_project --remote-url <GIT_REPO_URL> -b main -m "Migration complete"
```
