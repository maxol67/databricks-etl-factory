# databricks-etl-factory

This project uses Declarative Automation Bundles (DABs) for deployment.

## For AI Agents: Use Databricks AI Tools

**BEFORE any other action, read the `databricks-core` skill**, then `databricks-pipelines` for
Lakeflow (Declarative) Pipelines work and `databricks-dabs` for bundle config. Without them,
results are often slower and less accurate.

If these skills are not available (Databricks AI Tools are not installed), install them:

```bash
databricks aitools install
```

## What this project is

Not a codebase you deploy and extend indefinitely - a **framework for how to implement ETL on a
Databricks-native data platform**: naming conventions, folder structure, schema-allocation
rules, and job/pipeline patterns, proven out by a working reference implementation (six
example sources). See [`CONVENTIONS.md`](CONVENTIONS.md) for the conventions themselves -
**read it before adding a pipeline, job, or schema**, not just this file. See
[`TUTORIAL.md`](TUTORIAL.md) for the step-by-step, worked-example version of the same
conventions - use it when actually adding a new source, not just when explaining the rules.
See `README.md` for current status and `BACKLOG.md` for deferred ideas, and `_notes/NOTES.md` (if present locally -
gitignored, personal) for the deep design rationale behind each convention.

## Project instructions

- This is a **from-scratch, generic** implementation of the ETL-framework concept above - not a
  port of any specific prior codebase, and not metadata-driven in the classic control-table
  sense (see `BACKLOG.md`'s Config-driven pipeline generation entry for the deferred
  lightweight-config-plus-agent-generation idea, not a scheduled direction). Keep it that way: no
  client-specific names, schemas, or data in tracked
  files. Real/client-specific work belongs in a separate private repo - same boundary drawn by
  other project repos in this series.
- Current state: six hardcoded sources (SAP, a webshop, a CRM,
  Finance's credit-scoring system, MDM's product master data, Product Management's reference
  pricing) as twelve Lakeflow Pipelines in two tiers plus twelve Lakeflow Jobs (automatic
  `trigger_*` roles vs. on-demand-only `refresh_*_full` roles) - see
  `README.md`'s Current status section for the full, up-to-date structure before assuming a
  different pipeline/job layout, and [`CONVENTIONS.md`](CONVENTIONS.md) before adding a new one.
- No config-driven generation layer yet - see `BACKLOG.md`'s Config-driven pipeline generation
  entry before assuming one exists.

<!-- Add further project-specific instructions, coding conventions, or notes below -->
