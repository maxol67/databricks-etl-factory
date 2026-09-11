# Databricks ETL Factory

A native-Databricks-first ETL framework - validating and demonstrating a target architecture
for medallion (Bronze/Silver/Gold) pipelines built on **Lakeflow**'s declarative model
(Pipelines and Connect), instead of the imperative, hand-rolled orchestration that
classic cloud and on-prem ETL tools still require. Hyperscaler-agnostic by design -
nothing here is Azure/AWS/GCP-specific; Databricks itself runs on all three.

## About this project

This is a personal, from-scratch open-source project - a research lab for how a modern
Databricks data platform and ETL should be architected, built entirely from publicly available
Databricks/Microsoft guidance and general industry practice, not from any specific client
engagement. It contains no proprietary or client intellectual property. Views expressed are my
own.

## Table of contents

- [About this project](#about-this-project)
- [Why this exists](#why-this-exists)
  - [Declarative, not imperative](#declarative-not-imperative)
  - [Agentic ETL](#agentic-etl)
  - [A framework, not a codebase](#a-framework-not-a-codebase)
- [Target architecture](#target-architecture)
- [Current status](#current-status)
- [Getting started](#getting-started)
  - [Deploying](#deploying)
  - [Local Python environment](#local-python-environment)
- [Working with AI Agents (Genie)](#working-with-ai-agents-genie)
- [Tutorial: building your own data platform](#tutorial-building-your-own-data-platform)
- [Code Formatting](#code-formatting)
- [Testing](#testing)
- [Versioning](#versioning)
- [Conventions](#conventions)
- [Backlog](#backlog)
- [License](#license)

## Why this exists

### Declarative, not imperative

Classic ETL tools are fundamentally imperative: you write, or a metadata-driven framework
generates, an explicit loop that triggers one pipeline run per source, because the tool itself
can't infer an execution plan from a declared end state - a cloud ETL tool's `ForEach`
loop and pipeline canvas are the same control-flow model 30-year-old on-prem ETL tools used,
just relocated to the cloud. Lakeflow Pipelines are different in kind, not just in tooling: you
declare tables and the flows between them, and the platform infers the dependency graph from
what each one actually reads and parallelizes independent work itself - nobody writes the loop.
That's the same move SQL made over procedural cursors, applied to pipeline orchestration, and
it's the part of Databricks' advantage most comparisons never get to because they stop at
connectors and pricing.

This project exists to demonstrate that shift concretely, medallion layer by medallion layer.

### Agentic ETL

Metadata-driven frameworks (a control table describing sources/objects, a generic engine
interpreting it at runtime) were the standard way to avoid hand-writing dozens of near-identical
pipelines - necessary because there was no cheaper alternative. That's no longer strictly true:
an AI agent can generate and maintain a pipeline's actual code directly, following a fixed set
of house conventions (see [`CONVENTIONS.md`](CONVENTIONS.md)), instead of a generic runtime
interpreting metadata at every execution. This project takes that path deliberately - plain,
inspectable pipeline code stays the source of truth; an agent does the generation/maintenance
work a heavy metadata engine used to do.

### A framework, not a codebase

This isn't a system meant to keep growing indefinitely - it's a **methodology for structuring
ETL on Databricks**: naming conventions, folder structure, schema-allocation rules, and
job/pipeline patterns (see [`CONVENTIONS.md`](CONVENTIONS.md)), proven out end-to-end by a
working reference implementation, not sold as the point of the repo. The six example sources
under `resources/`/`src/` are worked examples of *applying* the method, not a product you deploy
and extend one source at a time forever. Adopting this repo means applying its conventions to
your own sources, not accumulating more sources inside this one.

## Target architecture

Per-source routing, not one uniform ingestion path:

- **Sources with a Lakeflow Connect managed connector** (e.g. SQL Server) → the connector lands
  data **directly to Bronze** as governed Delta. No external orchestrator needed.
- **Sources with no native connector** (e.g. SAP - confirmed to have none as of 2026-08) → an
  external tool (e.g. a cloud-native or on-prem ETL/orchestration tool) lands raw files into
  **Staging** - a Unity Catalog
  Volume holding non-Delta, ungoverned raw files, explicitly *not* Bronze - and **Auto Loader**
  promotes Staging into **Bronze**.
- Both paths converge: **Bronze** (raw, no validation, per Databricks' own medallion guidance)
  → **Silver** (validated, deduplicated, reject/warning checks enforced) → **Gold**
  (business-facing aggregates) - all via **Lakeflow (Declarative) Pipelines**.
- **Bronze, Silver, and Gold each live in their own Unity Catalog schema** (not just
  separate tables in one schema), per Databricks' own Unity Catalog governance best
  practices - this is what makes it possible to grant `USE SCHEMA`/`SELECT` differently
  per layer (e.g. Bronze restricted to data engineers, Gold open broadly to business
  users). Staging is likewise its own schema, holding only the Volume, no tables.
- **Within Silver and Gold, schemas split further, but by different axes** - this is a real
  access-control boundary, not just naming: Unity Catalog grants are scoped to schemas, not
  to table-name prefixes, so only a genuine per-subject-area or per-domain schema lets one
  team's access be granted without exposing every other team's data in the same schema.
  - **Silver**: one schema per *subject area* (`silver_sales`, `silver_customers`, ... -
    what entity the data is about), plus a `silver_data_quality` schema for rows that fail Silver
    validation - kept reviewable instead of silently dropped, and out of the subject-area
    schemas since those are meant to be safely consumable once a row lands there.
  - **Gold**: one schema per *business domain* (`gold_sales`, `gold_finance`, ... - which
    business function/department consumes it), plus a shared `gold_shared` schema for
    conformed dimensions used across domains (deliberately domain-*agnostic*, not itself a
    domain).
- **Bronze stays a single schema** with source-system-prefixed tables (`sap_orders`,
  `crm_customers` - a 3-letter - 4-5 as exception - source abbreviation) instead, since
  Bronze access is normally uniform (engineers only) regardless of source, so splitting it
  further wouldn't buy any real governance benefit and would just cause schema sprawl with
  many source systems.

## Current status

Six hardcoded sources (SAP, a webshop, a CRM, Finance's credit-scoring system, MDM's product
master data, and Product Management's ("prodman") reference pricing - all synthetic JSON
data) implement the full Staging/Drop → Bronze → Silver → Gold path end-to-end, deployed and
validated on Databricks Free Edition, as twelve Lakeflow Pipelines in two tiers and twelve
Lakeflow Jobs (six automatic per-source Bronze triggers, three automatic Silver/Gold
cascades, three on-demand full-domain reloads). See
[`CONVENTIONS.md`](CONVENTIONS.md) for the
naming, folder structure, and job/pipeline patterns behind those counts, each pipeline's own
`transformations/` files for what it actually does, and each job's own
`resources/jobs/**/*.job.yml` for its exact stages/dependencies.

No config-driven generation layer yet - every source and its schema is hand-written (by hand,
or by an agent working from full context - see "Agentic ETL" above). An
agent working from a lightweight per-source config instead is a deferred idea, not a scheduled
step - see [`BACKLOG.md`](BACKLOG.md)'s Config-driven pipeline generation entry.

## Getting started

`databricks.yml` is gitignored - it's your personal copy of the tracked `databricks.yml.example`
template, filled in with your own workspace host. Create it once:

```bash
cp databricks.yml.example databricks.yml
# then replace <YOUR_DEV_WORKSPACE_HOST> and <YOUR_PROD_WORKSPACE_HOST> with your workspace
# host(s) - the same host for both is fine, or a genuinely separate workspace for prod
```

`dev` and `prod` can point at the same workspace or different ones - whichever you choose,
DABs ties a CLI profile to a target by matching `workspace.host`: `bundle validate`/`deploy`
reject any `--profile` whose host doesn't match the target's, naming the profile that does.
So if prod uses a different host than dev, that alone requires the matching `--profile` -
`deploy.sh` doesn't need (and shouldn't have) any target→profile mapping of its own.

Both targets deploy under `/Workspace/deployment/...` by default - a plain workspace folder,
not a Databricks-reserved path. That's only a suggestion: create it once in the workspace UI
(or via `databricks workspace mkdirs`), or edit `root_path` in your `databricks.yml` to
whichever path you have write access to. See the comments above `targets:` in
`databricks.yml.example` for the tradeoffs (e.g. `/Workspace/Shared` opens read/write to every
workspace user by default).

```bash
databricks bundle validate --profile <profile>
databricks bundle deploy -t dev --profile <profile>
# The six example sources have nothing to ingest until sample data actually lands in
# Staging/Drop - upload this repo's committed sample_data/ (see sample_data/generate_sample_data.py's
# docstring for what's in it, including a couple of deliberately invalid rows that exercise
# Silver's reject/warning checks):
./upload_sample_data.sh --catalog <your_catalog>_dev --profile <profile>
# Use the refresh_*_full jobs for a full domain refresh - they sequence Tier 1 before Tier 2
# for you. Day to day, the trigger_* jobs keep each domain current automatically (file
# arrival -> Bronze -> table update -> Silver/Gold) - see Current status.
databricks bundle run refresh_customers_full -t dev --profile <profile>
databricks bundle run refresh_sales_full -t dev --profile <profile>
```

### Deploying

`deploy.sh` wraps the validate+deploy steps above (profile defaults to `free`, dev's workspace;
prod requires whichever `--profile` matches your prod workspace's host, see above):

```bash
./deploy.sh                                 # dev target, free profile (defaults)
./deploy.sh --target prod --profile <prod-profile>   # prod target - whatever profile matches its host
```

It doesn't run anything - use `databricks bundle run refresh_customers_full` /
`databricks bundle run refresh_sales_full` (or the CLI/UI) for a full domain refresh, same as the
raw commands above. Deploying does unpause the `trigger_*` jobs' triggers, though, so they start
firing automatically on real file arrivals / table updates in whichever workspace you deploy to.

### Local Python environment

`.venv` is gitignored - create it with [uv](https://docs.astral.sh/uv/) (installed via
`brew install uv` on macOS), which reads `pyproject.toml`'s `dependencies`/`dependency-groups`
directly:

```bash
uv sync --group dev
```

This is also what VS Code's Databricks extension uses under the hood for its "Configure
Databricks Connect" flow - if that fails with `uv was not found`, install `uv` first, then
retry. Verify the local Databricks Connect setup:

```bash
DATABRICKS_CONFIG_PROFILE=<profile> .venv/bin/databricks-connect test
```

> **Note:** A `PermissionDenied: Public DBFS root is disabled` error on the `dbutils.fs` check
> is expected and harmless against this project's `free`-profile workspace - it's a
> workspace-level security setting, unrelated to whether Databricks Connect itself is working
> (live-verified: the session/cluster-connection checks before it pass cleanly).

## Working with AI Agents (Genie)

This project includes AI agent context files that Databricks Genie automatically loads when you work within this project:

- **`AGENTS.md`** - Primary instructions for AI agents (Genie Code, Claude, etc.)
- **`CLAUDE.md`** - Claude Code-specific import that references `AGENTS.md`

### How It Works

**Location-based context loading:** When you open or work on any file within `/Users/<your-email>/databricks-etl-factory/` or its subdirectories, Genie automatically:
1. Looks for `AGENTS.md` and `CLAUDE.md` in the current directory and all parent directories
2. Loads these files as "Project instructions" in the agent's context
3. Uses them to guide all code generation, explanations, and assistance within this project

**This is automatic** - you don't need to manually reference these files in your conversations.

### Scope

✅ **AGENTS.md applies to work on:**
- Any file in `/databricks-etl-factory/` (notebooks, Python files, YAML configs, etc.)
- Any file in subdirectories (`src/`, `resources/`, `docs/`, etc.)

❌ **AGENTS.md does NOT apply to:**
- Files in other projects (`/other-project/`)
- Files outside this project directory

**Think of it like `.gitignore`:** Just as `.gitignore` applies to all files in its directory tree, `AGENTS.md` guides Genie for all work within the `databricks-etl-factory` project directory.

### What's In AGENTS.md

- **Databricks AI Tools skills**: Instructions to load `databricks-core`, `databricks-pipelines`, and `databricks-dabs` skills first
- **Project philosophy**: Framework vs. codebase, agentic approach, conventions-first development
- **Current architecture**: Six hardcoded sources, twelve pipelines, twelve jobs
- **References**: Pointers to `CONVENTIONS.md`, `TUTORIAL.md`, `BACKLOG.md`, `README.md`
- **Development guidelines**: Generic implementation, no client-specific data, no config-driven layer yet

When working with Genie in this project, it will follow these instructions automatically.

## Tutorial: building your own data platform

Setting up your own platform from this repo, and a full worked example of adding one real
source (routing decision, schema/volume creation, and the Bronze/Silver/Gold pipeline+job files
themselves) live in [TUTORIAL.md](TUTORIAL.md) - the practical, step-by-step counterpart to
[CONVENTIONS.md](CONVENTIONS.md)'s rules.

## Code Formatting

All Python code is linted and formatted with [Ruff](https://docs.astral.sh/ruff/) (config:
`pyproject.toml`, `line-length = 120`), enforced via a pre-commit hook rather than left to habit:

```bash
pip install -e . --group dev
pre-commit install      # one-time per clone - enables the git pre-commit hook below
```

`.pre-commit-config.yaml` runs `ruff-check` then `ruff-format` on staged files on every local
`git commit`. To fix everything by hand (e.g. before the hook is set up):

```bash
ruff check --fix .
ruff format .
```

There's no CI pipeline yet in this project, so the pre-commit hook is currently the only
enforcement mechanism - it depends on `pre-commit install` having been run.

## Testing

```bash
uv run pytest
```

Needs a working `databricks-connect`/CLI profile, same as the Databricks Connect verification
step above - these tests run against real (serverless) compute via `databricks-connect`'s
`DatabricksSession`, not a local Spark session (`databricks-connect` and plain `pyspark` conflict
when both installed, so there's no fully-offline local option here). Set
`DATABRICKS_CONFIG_PROFILE`/`DATABRICKS_CLI_PROFILE` if you're not using the default profile.

Two kinds of tests, both under `tests/` (mirroring `src/pipelines/<layer>/<pipeline_name>/` for
per-pipeline tests, e.g. `tests/pipelines/silver/silver_customers_etl/test_util.py`):

- **`bundle validate` as a regression check** (`tests/test_bundle.py`) - fails on broken
  YAML/resource references; skips cleanly (not a failure) if the CLI or a working profile isn't
  available.
- **Unit tests for extractable pure logic** - most pipeline transformation files aren't safely
  importable outside the Lakeflow runtime (they need a live `spark` global and pipeline graph
  context), so pure logic worth testing lives in a sibling module with no Lakeflow/Spark
  references at module level (e.g. `_util.py` next to `customers.py`) and gets imported and
  tested on its own. See [CONVENTIONS.md](CONVENTIONS.md)'s Testing section for the full pattern
  and why the extractable surface is deliberately small.

## Versioning

The project version lives in `pyproject.toml`'s `[project] version` field. Bump it with
`bump_version.sh` rather than editing the field by hand:

```bash
./bump_version.sh               # patch bump, e.g. 0.0.1 -> 0.0.2
./bump_version.sh --minor       # 0.0.1 -> 0.1.0
./bump_version.sh --major       # 0.0.1 -> 1.0.0
```

See the parent `databricks-core` and `databricks-pipelines` Databricks Agent Skills for CLI
auth/profile setup and Lakeflow Pipelines development patterns.

## Conventions

The naming rules, folder structure, and job/pipeline patterns this project follows - and that
you'd follow to extend it with a real source - are a main asset in their own right, not just
implementation detail. They live in [CONVENTIONS.md](CONVENTIONS.md), separate from this README
(what the project does) and `AGENTS.md` (how an AI agent should work in this repo).

## Backlog

Ideas that are deferred, not scheduled, and not reflected in this project's current behavior
live in [BACKLOG.md](BACKLOG.md), grouped by area - so they stay discoverable without cluttering
this README (which only describes what the project actually does) or `AGENTS.md`.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). Copyright 2026 Maxim Oleznyuk.
