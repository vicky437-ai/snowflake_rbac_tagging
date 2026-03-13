# Cortex Code (CoCo) Quick Reference

## Startup Commands

```bash
# Basic startup
cortex                              # Interactive REPL
cortex -p "explain this code"       # One-shot prompt
cortex --resume last                # Resume last session
cortex -r <session_id>              # Resume specific session
cortex --connection myconnection    # Use specific Snowflake connection

# Help
cortex --help                       # CLI help
cortex --version                    # Version info
```

---

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Enter` | Submit message |
| `Ctrl+J` | Insert newline (multiline input) |
| `Ctrl+C` | Cancel/Exit |
| `Shift+Tab` | Cycle modes (Confirm → Plan → Bypass) |
| `Ctrl+P` | Toggle Plan mode |
| `Ctrl+D` | Fullscreen todo view |
| `Ctrl+B` | Background bash process |
| `Ctrl+O` | Cycle display mode |
| `↑` / `↓` | Navigate history |

---

## Operational Modes

| Mode | Toggle | Description |
|------|--------|-------------|
| **Confirm** | Default | Asks permission for risky actions |
| **Plan** | `/plan` or `Ctrl+P` | Shows plan before execution |
| **Bypass** | `/bypass` | Auto-approves everything (use carefully!) |

**Tip:** Use `Shift+Tab` to cycle between modes.

---

## Slash Commands

```bash
# Session
/help                   # Show help
/status                 # Session status
/compact                # Compress context (when running low)
/fork                   # Branch conversation for experiments
/rewind                 # Undo to previous checkpoint
/clear                  # Clear screen

# Model
/model                  # Show current model
/model sonnet           # Switch to Claude Sonnet
/model opus             # Switch to Claude Opus
/model haiku            # Switch to Claude Haiku

# Snowflake
/sql SELECT 1           # Execute SQL directly
/connections            # List Snowflake connections

# Tools
/skill                  # List available skills
/agents                 # List available agents
/diff                   # Fullscreen git diff viewer
/plan                   # Enter plan mode
/bypass                 # Enter bypass mode (auto-approve)
```

---

## Smart References (Prefixes)

| Prefix | Syntax | Example | Effect |
|--------|--------|---------|--------|
| `@` | `@path/file` | `@src/app.py` | Include entire file |
| `@$` | `@file$start-end` | `@app.py$10-50` | Include lines 10-50 |
| `#` | `#DB.SCHEMA.TABLE` | `#D_RAW.SADB.TRAIN` | Inject table schema + sample rows |
| `$` | `$skill-name` | `$cost-intelligence` | Activate a skill |
| `!` | `!command` | `!git status` | Run bash, output goes to agent |

---

## Sample Prompts by Task

### Code Tasks
```
Review this file for bugs: @src/main.py

Refactor @utils/helper.py$50-100 to use async/await

Create unit tests for @services/auth.py

Explain what this function does: @lib/parser.py$20-45
```

### Snowflake Tasks
```
Show me the schema of #D_BRONZE.SADB.TRAIN_OPTRN_EVENT

Write a query to find duplicate records in #MY_DB.SCHEMA.USERS

$cost-intelligence where is my money going?

$analyzing-data how many orders were placed last month?

Create a stream and task for CDC on #RAW.PUBLIC.CUSTOMERS
```

### Git Tasks
```
Show me what changed in the last commit

Create a PR for my current branch

Commit my changes with a descriptive message

!git log --oneline -10
```

### DBT Tasks
```
$dbt-projects-on-snowflake deploy my project

Create a dbt model for customer lifetime value

Run dbt tests and fix any failures
```

### Streamlit Tasks
```
$developing-with-streamlit create a dashboard for sales data

Debug why my Streamlit app isn't loading

Add a date filter to @app.py
```

---

## Available Skills (Common)

| Skill | Trigger | Purpose |
|-------|---------|---------|
| `$cost-intelligence` | Cost questions | Spending analysis, budgets |
| `$analyzing-data` | Data questions | Query warehouse, answer business questions |
| `$developing-with-streamlit` | Streamlit | Build/debug Streamlit apps |
| `$cortex-agent` | Agents | Create/manage Cortex Agents |
| `$semantic-view` | Semantic views | Create/debug semantic views |
| `$lineage` | Lineage | Trace data dependencies |
| `$dynamic-tables` | Dynamic tables | Create/monitor DTs |
| `$data-governance` | Governance | Masking, access policies |
| `$machine-learning` | ML | Train/deploy models |

**View all:** `/skill` or type `$` and press Tab

---

## Agents (Subagents)

| Agent | Purpose |
|-------|---------|
| `Explore` | Fast codebase exploration |
| `Plan` | Architecture planning (read-only) |
| `general-purpose` | Complex multi-step tasks |
| `feedback` | Capture user feedback |

**View all:** `/agents`

---

## Configuration

**Location:** `~/.snowflake/cortex/`

```
~/.snowflake/cortex/
├── settings.json      # Main settings
├── skills/            # Custom skills
├── agents/            # Custom agents
└── mcp.json           # MCP server config
```

---

## Tips & Tricks

1. **Multiline input:** Use `Ctrl+J` for newlines, `Enter` to submit
2. **Long context:** Use `/compact` when context gets too long
3. **Experiments:** Use `/fork` before trying risky approaches
4. **Wrong path:** Use `/rewind` to go back to a checkpoint
5. **Background tasks:** Use `Ctrl+B` for long-running commands
6. **Table context:** `#TABLE` auto-injects schema + 3 sample rows
7. **Specific lines:** `@file$100-200` includes only those lines
8. **Quick SQL:** `/sql SELECT * FROM table LIMIT 5`
9. **Skills:** Type `$` then Tab to see available skills
10. **Files:** Type `@` then Tab for file autocomplete

---

## Example Session

```
# Start cortex
$ cortex --connection myconnection

# Ask about costs
> $cost-intelligence show me warehouse costs for last week

# Include a file for review
> Review this CDC script for issues: @scripts/cdc_pipeline.sql

# Reference a Snowflake table
> Create a view that aggregates #D_BRONZE.SADB.TRAIN_OPTRN by day

# Run bash inline
> !ls -la src/

# Switch to plan mode for risky operation
> /plan
> Delete all test data from staging tables

# Compact when context is long
> /compact

# Fork before experimenting
> /fork
> Try a completely different approach using dynamic tables
```

---

*Generated: March 2026*
