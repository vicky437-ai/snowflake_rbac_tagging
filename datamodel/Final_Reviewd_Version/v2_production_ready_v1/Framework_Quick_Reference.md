# infa2dbt Framework — Quick Reference

## What is infa2dbt?

**infa2dbt** converts Informatica PowerCenter ETL workflows into production-ready dbt projects for Snowflake. You give it XML exports from Informatica — it gives you a complete dbt project with staging models, intermediate transformations, mart tables, tests, and documentation. It uses Snowflake Cortex LLM (AI) to understand your Informatica logic and translate it to Snowflake SQL.

---

## End-to-End Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        infa2dbt PIPELINE                                │
│                                                                         │
│  YOUR INFORMATICA XMLs                                                  │
│        │                                                                │
│        ▼                                                                │
│  ┌──────────┐   Scan XMLs, count mappings,                             │
│  │ DISCOVER │   validate source tables exist                            │
│  └────┬─────┘   in Snowflake                                           │
│       │                                                                 │
│       ▼                                                                 │
│  ┌──────────┐   Parse XML → Analyze complexity                         │
│  │ CONVERT  │   → Chunk → Build prompt → Call LLM                      │
│  │          │   → Parse response → Post-process                        │
│  │ (engine) │   → Validate → Self-heal → Score                         │
│  │          │   → Merge all mappings → Write files                     │
│  └────┬─────┘                                                          │
│       │         ┌───────────────────────┐                              │
│       │         │ CACHE (optional)      │                              │
│       │◄────────│ Skip LLM if cached    │                              │
│       │         └───────────────────────┘                              │
│       ▼                                                                 │
│  ┌──────────┐   Check SQL syntax, YAML structure,                      │
│  │ VALIDATE │   ref() integrity, run dbt compile/test                  │
│  └────┬─────┘                                                          │
│       │                                                                 │
│       ▼                                                                 │
│  ┌──────────┐   Push to Snowflake via snow dbt deploy                  │
│  │ DEPLOY   │   (direct, git-based, or scheduled)                      │
│  └────┬─────┘                                                          │
│       │                                                                 │
│       ├──────────┐                                                      │
│       ▼          ▼                                                      │
│  ┌────────┐ ┌───────────┐  Compare source vs target:                   │
│  │ REPORT │ │ RECONCILE │  schema, row counts, aggregates,             │
│  │  (EWI) │ │           │  hashes, row diffs, business rules           │
│  └────────┘ └───────────┘                                              │
│       │                                                                 │
│       ▼                                                                 │
│  ┌──────────┐   Commit + push to Git                                   │
│  │ GIT-PUSH │                                                          │
│  └──────────┘                                                          │
│                                                                         │
│  RESULT: dbt project running on Snowflake with daily scheduled tasks   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## The 9 CLI Modules

| # | Command | What It Does | When To Use |
|---|---------|-------------|-------------|
| 1 | `discover` | Scans XML files, lists all mappings/sources/targets, validates tables exist in Snowflake | First — before converting, to see what you have |
| 2 | `convert` | Parses XML, calls LLM, generates complete dbt project (models, tests, YAML) | Main step — this is where the magic happens |
| 3 | `validate` | Runs `dbt compile` and optionally `dbt run` + `dbt test` against Snowflake | After convert — to check if generated code works |
| 4 | `deploy` | Pushes dbt project to Snowflake (direct, git-based, or with TASK schedule) | After validate — to make it live on Snowflake |
| 5 | `reconcile` | Compares source tables vs dbt output tables using 6-layer validation | After deploy — to prove data matches |
| 6 | `report` | Generates HTML/JSON assessment report (Errors, Warnings, Info) | After convert — for stakeholder review |
| 7 | `cache` | List, stats, or clear the LLM response cache | Anytime — to manage cached conversions |
| 8 | `git-push` | Commits and pushes the dbt project to a Git repository | After everything passes — to version control |
| 9 | `version` | Shows framework version | Anytime |

### Execution Order

```
discover → convert → validate → deploy → reconcile → report → git-push
    1          2          3         4          5          6         7
```

You don't have to run all of them. The minimum is: `convert` → `deploy`. Everything else is optional but recommended.

---

## Sample Commands (with all options)

### 1. discover
```bash
infa2dbt discover \
  --input ./xml_exports/ \
  --schema-source snowflake \
  --database MY_DB \
  --schema MY_SCHEMA \
  --output source_map.json
```
| Option | Required | What it does |
|--------|----------|-------------|
| `--input` | Yes | Path to XML file or folder of XMLs |
| `--schema-source` | No | Where to get table schemas: `xml`, `snowflake`, or `json` |
| `--database` | If snowflake | Snowflake database to look up tables |
| `--schema` | If snowflake | Snowflake schema to look up tables |
| `--json-path` | If json | Path to a previously saved source_map.json |
| `--output` | No | Save discovered schema to a JSON file |

### 2. convert
```bash
infa2dbt convert \
  --input ./xml_exports/ \
  --output ./my_dbt_project \
  --mode new \
  --project-name my_project \
  --model claude-4-sonnet \
  --connection myconnection \
  --source-schema MOCK_SOURCES \
  --log-level INFO \
  --no-cache
```
| Option | Required | What it does |
|--------|----------|-------------|
| `--input` | Yes | XML file or folder |
| `--output` | No | Output dbt project directory |
| `--mode` | No | `new` (fresh project) or `merge` (add to existing) |
| `--project-name` | No | dbt project name (default: `informatica_dbt`) |
| `--model` | No | LLM model for code generation (default: `claude-4-sonnet`) |
| `--connection` | No | Snowflake connection name |
| `--source-schema` | No | Override source schema in all _sources.yml |
| `--log-level` | No | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |
| `--no-cache` | No | Force fresh LLM generation (skip cache) |
| `--clear-cache` | No | Clear all cached entries before running |

### 3. validate
```bash
infa2dbt validate \
  --project ./my_dbt_project \
  --profiles-dir ./my_dbt_project \
  --compile-only \
  --run-tests \
  --select tag:m_load_orders \
  --full-refresh \
  --install-deps \
  --dbt-path /usr/local/bin/dbt
```
| Option | Required | What it does |
|--------|----------|-------------|
| `--project` | Yes | Path to dbt project |
| `--profiles-dir` | No | Where profiles.yml lives |
| `--compile-only` | No | Only compile, skip run |
| `--run-tests` | No | Also run `dbt test` |
| `--select` | No | dbt selection syntax (e.g., specific mapping tag) |
| `--full-refresh` | No | Force full refresh on incremental models |
| `--install-deps` | No | Run `dbt deps` first |
| `--dbt-path` | No | Custom path to dbt executable |

### 4. deploy
```bash
infa2dbt deploy \
  --project ./my_dbt_project \
  --database MY_DB \
  --schema MY_SCHEMA \
  --project-name MY_PROJECT \
  --warehouse SMALL_WH \
  --connection myconnection \
  --mode direct \
  --dry-run
```
| Option | Required | What it does |
|--------|----------|-------------|
| `--project` | Yes | Path to dbt project |
| `--database` | Yes | Snowflake database |
| `--schema` | Yes | Snowflake schema |
| `--project-name` | Yes | Name for the Snowflake dbt project object |
| `--warehouse` | Yes | Snowflake warehouse |
| `--connection` | No | Snowflake connection name |
| `--mode` | No | `direct` (snow dbt deploy), `git` (from repo), `schedule` (TASK) |
| `--git-url` | If git | Git HTTPS URL |
| `--git-repo-name` | If git | Snowflake GIT REPOSITORY name |
| `--git-branch` | If git | Branch to deploy from |
| `--cron` | If schedule | Cron schedule (default: daily 2 AM) |
| `--dry-run` | No | Show SQL without executing |

### 5. reconcile
```bash
infa2dbt reconcile \
  --source-database MY_DB \
  --source-schema LEGACY_DATA \
  --target-database MY_DB \
  --target-schema DBT_OUTPUT \
  --connection myconnection \
  --layers L1,L2,L3 \
  --config recon.yml \
  --output ./recon_reports \
  --format both
```
| Option | Required | What it does |
|--------|----------|-------------|
| `--source-database` | Yes | Database with original/legacy data |
| `--source-schema` | Yes | Schema with source tables |
| `--target-database` | Yes | Database with dbt output |
| `--target-schema` | Yes | Schema with dbt-created tables |
| `--connection` | No | Snowflake connection |
| `--layers` | No | Which checks to run: `L1`-`L6` or `all` (default: all) |
| `--config` | No | YAML with custom table mappings and PK overrides |
| `--output` | No | Output directory for reports |
| `--format` | No | `html`, `json`, or `both` |

### 6. report
```bash
infa2dbt report \
  --project-dir ./my_dbt_project \
  --metrics-file .infa2dbt/last_metrics.json \
  --output ./reports \
  --format both
```

### 7. cache
```bash
infa2dbt cache list      # Show all cached entries
infa2dbt cache stats     # Show hit/miss statistics
infa2dbt cache clear     # Delete all cached entries
```

### 8. git-push
```bash
infa2dbt git-push \
  --project ./my_dbt_project \
  --remote origin \
  --branch feature/new-mapping \
  --remote-url https://github.com/org/repo.git \
  --message "Add journals mapping"
```

---

## What Happens Inside `convert` (The Engine)

When you run `infa2dbt convert`, here's what happens for each mapping:

```
XML File
   │
   ▼
┌──────────────────────┐
│ Step 1: XML PARSER   │  Parse XML elements → Source, Target,
│                      │  Transformation, Connector objects
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Step 2: ENRICHMENT   │  Resolve cross-folder shortcuts,
│                      │  build complete mapping metadata
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Step 3: COMPLEXITY   │  Score 0-100 on 11 dimensions
│   ANALYZER           │  → Pick strategy: DIRECT/STAGED/
│                      │    LAYERED/COMPLEX
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Step 4: CACHE CHECK  │  SHA-256 key = XML + model + version
│                      │  HIT? → Return cached files instantly
│                      │  MISS? → Continue to LLM
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Step 5: CHUNKER      │  Split large mappings along
│                      │  transformation chain boundaries
│                      │  (respects LLM token limits)
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Step 6: PROMPT       │  System prompt (500+ lines of rules)
│   BUILDER            │  + Strategy instructions
│                      │  + Transformation hints
│                      │  + Serialized mapping chunk
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Step 7: LLM CALL     │  Send to Snowflake Cortex LLM
│                      │  (claude-4-sonnet by default)
│                      │  Parse response → extract file blocks
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Step 8: POST-PROCESS │  Clean whitespace, fix paths,
│   + VALIDATE         │  check SQL syntax, YAML structure,
│   + SELF-HEAL        │  ref() integrity, DAG cycles
│                      │  ↳ Errors? → Send back to LLM
│                      │    (up to 2 correction rounds)
│                      │  ↳ Still failing? → Escalate to
│                      │    stronger model (claude-4-opus)
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Step 9: QUALITY      │  Score 0-100 on 5 dimensions:
│   SCORER             │  file structure, dbt conventions,
│                      │  SQL syntax, function conversion,
│                      │  YAML quality
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Step 10: MERGER      │  Combine all mapping outputs into
│                      │  one unified dbt project
│                      │  (dedupe sources, resolve conflicts)
└──────────┬───────────┘
           ▼
      dbt project on disk
```

---

## The 14 Internal Packages

| Package | Files | What It Does | When It Runs |
|---------|-------|-------------|-------------|
| **xml_parser** | `parser.py`, `models.py`, `dependency_graph.py` | Parses Informatica XML into Python objects. Builds data-flow graph (networkx) | During `discover` and `convert` |
| **analyzer** | `complexity.py`, `multi_workflow.py`, `transformation_registry.py` | Scores complexity (0-100), picks strategy, resolves cross-folder shortcuts | During `convert` (Step 3) |
| **chunker** | `context_preserving.py` | Splits large mappings into LLM-sized chunks along chain boundaries | During `convert` (Step 5) |
| **generator** | `prompt_builder.py`, `llm_client.py`, `response_parser.py`, `post_processor.py`, `quality_scorer.py`, `dbt_project_generator.py` | Builds prompts, calls LLM, parses output, cleans up, scores quality | During `convert` (Steps 6-9) |
| **validator** | `sql_validator.py`, `yaml_validator.py`, `project_validator.py`, `patterns.py` | Checks SQL syntax, YAML structure, ref() integrity, DAG cycles | During `convert` (Step 8) and `validate` |
| **cache** | `conversion_cache.py` | SHA-256 content-addressed cache. Store/retrieve LLM results | During `convert` (Step 4) and `cache` command |
| **merger** | `project_merger.py`, `source_consolidator.py`, `conflict_resolver.py` | Combines multiple mapping outputs into one dbt project | During `convert` (Step 10) |
| **deployment** | `deployer.py` | Direct deploy, Git-based deploy, or TASK schedule | During `deploy` |
| **reconciliation** | `engine.py`, `models.py`, `table_map.py`, `recon_metrics.py`, `recon_report.py` | 6-layer data comparison between source and target | During `reconcile` |
| **reports** | `ewi_report.py` | Generates HTML/JSON EWI assessment report | During `report` |
| **git** | `git_manager.py` | Init, commit, branch, push Git operations | During `git-push` |
| **persistence** | `snowflake_io.py` | Read/write conversion results to Snowflake tables | During `convert` (optional) |
| **config** | `config.py` | All tunable settings (LLM model, token limits, retries, etc.) | Always — loaded at startup |
| **orchestrator** | `orchestrator.py` | Ties everything together: runs Steps 1-10 per mapping, manages checkpoints | During `convert` |

---

## Key Component FAQ

### What does the Prompt Builder do?

It constructs the message sent to the AI (LLM). It has three parts:
1. **System prompt** (~500 lines) — Rules for generating dbt code: naming conventions, function mappings (50+ Informatica→Snowflake translations), materialization rules, anti-patterns, few-shot examples
2. **Strategy instructions** — Tells the LLM how many layers to generate (DIRECT=1, STAGED=2, LAYERED=3, COMPLEX=full project)
3. **User prompt** — The serialized mapping chunk (XML metadata in structured text format)

It also includes **prompt injection defense** — strips any suspicious patterns from XML content before sending to the LLM.

### What does Validate check?

Three layers of validation, **without needing a Snowflake connection**:

| Validator | What It Checks |
|-----------|---------------|
| **SQL Validator** | Balanced parentheses, no trailing semicolons, no leftover Informatica functions (IIF, ISNULL, DECODE(TRUE,...), etc.), config() block present, no SELECT *, no hardcoded table names |
| **YAML Validator** | Valid YAML syntax, `version: 2` present, proper sources.yml structure, model definitions have columns and tests |
| **Project Validator** | Every `ref('x')` points to a model that exists, no circular dependencies (DAG cycles), source references match sources.yml |

The `validate` **CLI command** additionally runs `dbt compile` and optionally `dbt run` + `dbt test` against a live Snowflake connection.

### What does Reconcile compare?

It compares your **original source data** against the **dbt output tables** using a 6-layer validation pyramid:

| Layer | Name | What It Checks | Speed |
|-------|------|---------------|-------|
| L1 | Schema | Column names and data types match | Fast |
| L2 | Row Count | `COUNT(*)` is the same | Fast |
| L3 | Aggregate | `SUM`, `MIN`, `MAX`, `COUNT DISTINCT` per numeric column | Medium |
| L4 | Hash | `HASH_AGG()` whole-table comparison | Medium |
| L5 | Row Diff | PK-based `FULL OUTER JOIN` — finds added, removed, changed rows | Slow |
| L6 | Business | Your custom SQL assertions (from config YAML) | Varies |

You can run all layers or pick specific ones (e.g., `--layers L1,L2` for a quick check).

### What does the Quality Scorer measure?

Five dimensions, scored 0-100:

| Dimension | Weight | What It Measures |
|-----------|--------|-----------------|
| File Structure | 20% | Expected files present (sources.yml, schema.yml, correct layers) |
| dbt Conventions | 25% | `config()` block, `ref()`/`source()` usage, staging uses `source()` |
| SQL Syntax | 20% | Balanced parens/braces, no trailing semicolons |
| Function Conversion | 25% | No leftover Informatica functions (IIF, ISNULL, REPLACESTR, etc.) |
| YAML Quality | 10% | YAML parses, has `version: 2`, defines tests |

### What is the Self-Healing Loop?

When the LLM generates code with errors, instead of failing, the framework:
1. Collects all validation errors (SQL + YAML + project)
2. Sends the errors back to the LLM with the original code: "Here's what you generated, here are the errors — fix them"
3. Re-validates the corrected output
4. Repeats up to **2 times** (configurable)

If still failing after 2 attempts → **Model Escalation** kicks in.

### What is Model Escalation?

If the default LLM (`claude-4-sonnet`) can't produce error-free output after self-healing:
- The framework automatically retries with a more powerful model (`claude-4-opus`)
- This is a last resort — opus is slower and more expensive, but handles complex mappings better

### What is the Complexity Analyzer?

It scores each mapping on **11 dimensions** (0-100):

| Dimension | Weight | What It Measures |
|-----------|--------|-----------------|
| Depth | 12% | Longest path in the data-flow graph |
| Breadth | 8% | Number of source tables |
| Transformations | 12% | Total transformation count |
| Expressions | 12% | Fields with non-trivial expressions |
| Lookups | 12% | Number of Lookup transformations |
| Routing | 8% | Router / Union / Filter presence |
| Chains | 8% | Number of independent data-flow paths |
| SCD | 8% | SCD Type-2 pattern detection |
| Joiners | 6% | Multi-source join complexity |
| Type Weight | 8% | Registry-driven complexity per transformation type |
| Update Strategy | 6% | INSERT/UPDATE/DELETE merge patterns |

The score maps to a **strategy**:

| Score | Strategy | What Gets Generated |
|-------|----------|-------------------|
| 0-30 | DIRECT | 1 staging model (simple pass-through) |
| 31-55 | STAGED | Staging + Intermediate (2 layers) |
| 56-80 | LAYERED | Staging + Intermediate + Mart (3 layers) |
| 81-100 | COMPLEX | Full project: multiple staging, intermediate, marts, macros, snapshots |

---

## The 3 Cortex Code Skills

Skills are specialized knowledge files that Cortex Code loads automatically when it detects relevant work. You don't call them — they activate themselves.

| Skill | What It Knows | When It Activates |
|-------|-------------|------------------|
| **informatica-xml-patterns** | All 33+ Informatica transformation types and their dbt equivalents, 50+ function mappings, XML structure patterns | When parsing or troubleshooting Informatica XML conversions |
| **scd-type2-patterns** | SCD Type 2 implementation patterns — dbt snapshots, incremental merge, effective/expiry dates, history tracking | When converting Update Strategy / SCD transformations |
| **snowflake-dbt-conventions** | dbt naming conventions, materialization rules, schema.yml structure, Snowflake SQL gotchas, test coverage requirements | When reviewing or validating generated dbt code |

---

## Cache System

- **Key**: SHA-256 hash of (XML content + converter version + LLM model + mapping name + prompt hash)
- **Storage**: `.infa2dbt/cache/` directory (local disk)
- **Behavior**: Content-addressed, append-only. Old entries never overwritten — they just become unused when inputs change
- **First run**: Calls LLM (~2-6 min per mapping). Stores result
- **Subsequent runs**: Returns instantly from cache (0 seconds, no LLM call)
- **No expiry**: Cache entries never expire automatically

---

## Common Q&A

**Q: Can it handle multiple XMLs at once?**
A: Yes. Point `--input` at a folder. The CLI discovers all XML files, converts each mapping, and merges everything into one unified dbt project via the ProjectMerger.

**Q: What if the LLM generates wrong SQL?**
A: The framework has a 3-tier safety net: (1) Post-processor cleans common issues automatically, (2) Self-healing loop sends errors back to LLM for correction (up to 2 rounds), (3) Model escalation retries with a more powerful LLM.

**Q: Do I need a Snowflake connection to convert?**
A: The LLM call goes through Snowflake Cortex, so yes — you need a connection. But the validation (SQL/YAML/project checks) runs locally without Snowflake.

**Q: How long does a conversion take?**
A: First run: ~2-6 minutes per mapping (LLM generation). With cache: seconds (no LLM call). A batch of 8 XMLs takes ~18 minutes first time, then instant on re-runs.

**Q: What Informatica transformations are supported?**
A: 33+ types including: Source Qualifier, Expression, Lookup, Filter, Router, Joiner, Aggregator, Sorter, Rank, Normalizer, Union, Update Strategy, Sequence Generator, Mapplet, XML Source Qualifier, and more. Only truly unconvertible types (Java Transformation, HTTP Transformation, External Procedure) are blocked.

**Q: What if my mapping is too large for the LLM?**
A: The Chunker automatically splits it along transformation chain boundaries. Each chunk gets its own LLM call, and results are merged back together. Shared context (sources, targets) is included in every chunk.

**Q: Can I merge new mappings into an existing dbt project?**
A: Yes. Use `--mode merge` on the convert command. The merger preserves your existing models and adds new ones, deduplicating sources.

**Q: What's the difference between `validate` (internal) and `validate` (CLI)?**
A: Internal validation (during convert) checks SQL/YAML/project structure offline. The CLI `validate` command runs `dbt compile` and optionally `dbt run` + `dbt test` against live Snowflake.

**Q: How does deploy work?**
A: Three modes: (1) **Direct** — uses `snow dbt deploy` to push files to Snowflake, (2) **Git** — creates a Snowflake Git Repository integration and deploys from your repo, (3) **Schedule** — creates Snowflake TASKs to run on a cron schedule.

**Q: How do I prove the converted dbt output matches the original Informatica output?**
A: Run `reconcile`. It compares source vs target across 6 layers: schema match, row counts, aggregates, hashes, row-level diffs, and custom business rules.

**Q: What is the EWI report?**
A: EWI = Errors, Warnings, Informational. It's an assessment report (HTML or JSON) showing: which mappings succeeded, which had issues, error categories, transformation coverage, quality scores, and recommendations. Similar to a SnowConvert assessment.

**Q: Can I use a different LLM model?**
A: Yes. Use `--model` on the convert command. Default is `claude-4-sonnet`. Fallback (for model escalation) is `claude-4-opus`. Both run on Snowflake Cortex.

**Q: What happens if I run convert again on the same XML?**
A: If cache is enabled (default), it returns the cached result instantly — no LLM call, same output. Use `--no-cache` to force a fresh generation.

**Q: Is the generated code safe to edit manually?**
A: Yes. The generated dbt project is standard dbt — edit any file. If you re-run convert with `--mode merge`, your manual edits in existing files are preserved (new mappings are added alongside).

**Q: How do I schedule the dbt project to run daily on Snowflake?**
A: Either use `infa2dbt deploy --mode schedule --cron "0 7 * * *"`, or manually create Snowflake TASKs with `EXECUTE DBT PROJECT` commands (as shown in the demo prompts).

---

## Configuration Defaults

| Setting | Default | What It Controls |
|---------|---------|-----------------|
| LLM Model | `claude-4-sonnet` | Primary model for code generation |
| Fallback Model | `claude-4-opus` | Model escalation target |
| Max Context Tokens | 80,000 | LLM context window limit |
| Chunk Token Limit | 75,000 | Max tokens per chunk sent to LLM |
| LLM Temperature | 0.1 | Low = deterministic output |
| Self-Heal Attempts | 2 | Max correction rounds per mapping |
| Max Retries | 3 | API call retries |
| Rate Limit | 3 calls/min | LLM rate limiting |

All configurable via environment variables prefixed with `INFA_DBT_` (e.g., `INFA_DBT_LLM_MODEL=claude-4-opus`).
