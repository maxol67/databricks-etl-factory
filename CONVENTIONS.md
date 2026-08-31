# Conventions

This is a main asset of the project, not incidental detail: the naming rules, folder structure,
and job/pipeline patterns below are what someone actually takes from this repo - whether that's
you extending it by hand, or an agent generating pipeline #5 from a lightweight per-source
config (a deferred idea - see `BACKLOG.md`'s Config-driven pipeline generation entry). The six
example sources under `resources/`/`src/` are a worked reference for these conventions, not a
system meant to keep growing on its own.

## Pipeline naming

- **Layer prefix**: every pipeline is prefixed `bronze_`/`silver_`/`gold_`. The Databricks
  Pipelines list has no folders, so the prefix is the only way to group by layer at a glance in
  that flat list.
- **`_etl` suffix**: matches Databricks' own official `bundle-examples` template convention
  exactly (confirmed directly against `lakeflow_pipelines_python_etl.pipeline.yml` and siblings
  in that repo) - not an arbitrary choice.
- **Two tiers, two different grouping axes** - conflating them is what makes a pipeline layout
  illegible:
  - **Tier 1 (source-ingestion)**: one pipeline per *source system*, Bronze only
    (`bronze_sap_etl`, `bronze_webshop_etl`, `bronze_crm_etl`, `bronze_finance_etl`,
    `bronze_mdm_etl`, `bronze_prodman_etl`) - grouped by *who delivers* the data. Independent
    by design: each system's delivery schedule is its own.
  - **Tier 2 (business-domain)**: one pipeline per *business domain*, per layer
    (`silver_sales_etl`, `silver_customers_etl`, `silver_products_etl`,
    `gold_sales_etl`, `gold_shared_customers_etl`, `gold_shared_products_etl`) -
    grouped by *who consumes* the data, reading Tier 1's Bronze tables cross-pipeline via
    fully-qualified names.
- **A pipeline's name suffix always matches its actual schema.** `silver_sales_etl` writes to
  `silver_sales`, `silver_customers_etl` to `silver_customers` - and the two Gold pipelines
  that write to the shared `gold_shared` schema instead of a domain-specific one are named
  `gold_shared_customers_etl`/`gold_shared_products_etl`, not `gold_customers_etl`/
  `gold_products_etl` - the name says "this writes to gold_shared" directly, rather than only
  the pipeline yml's `schema:` field saying so. Same reasoning as the layer prefix itself (the
  flat Pipelines list needs the name to convey structure), one level deeper. `gold_sales_etl`
  is the confirming counter-example: it writes to its own genuine per-domain `gold_sales`
  schema, so it gets the plain `gold_<domain>_etl` form - `gold_shared_` is specifically for
  the shared-schema case, not a blanket Gold naming change.
- Silver and Gold are never the same pipeline, even within one domain - the layer prefix implies
  a hard boundary. What used to be one pipeline's internal, guaranteed-ordered Silver→Gold step
  becomes a cross-pipeline dependency once split this way; a `refresh_*` job (below) is what
  re-establishes that ordering.

## Job naming and roles

Jobs close the ordering/scheduling gaps Lakeflow Pipelines alone can't express - see
"Declarative, but only within a pipeline" below for why that gap exists at all. Three distinct
roles, each with its own name prefix, folder, and trigger type - **the prefix always tells you
whether a job runs automatically or only on demand**, never just a grouping label:

| Role | Prefix | Folder | Trigger | Scope | Example |
|---|---|---|---|---|---|
| **Bronze trigger** | `trigger_bronze_` | `jobs/trigger_bronze/` | `file_arrival` | One per Tier 1 (Bronze) pipeline - runs only that source's Bronze, on new files in its own Staging/Drop path | `trigger_bronze_sap_etl` |
| **Silver(+Gold) trigger** | `trigger_silver_<domain>` / `trigger_silver_gold_<domain>` | `jobs/trigger_silver_gold/` | `table_update` (`condition: ANY_UPDATED`) | Automatically keeps a domain's Tier 2 (Silver, or Silver→Gold) current whenever any of its Bronze sources changes - never re-runs Bronze itself | `trigger_silver_gold_sales`, `trigger_silver_gold_customers` (both Silver→Gold - every domain has a Gold stage now) |
| **Full refresh** | `refresh_<domain>_full` | `jobs/refresh/` | none - on-demand only | Manual, always-runs-everything counterpart (Bronze through Gold) for dev testing, backfills, recovery | `refresh_sales_full`, `refresh_customers_full` |

The two automatic roles both use a `trigger_` prefix (not `refresh_`) precisely so the name
itself says "this fires on its own" - `refresh_` is reserved for the on-demand-only jobs in
`jobs/refresh/`, so that prefix alone now tells you a job never fires by itself.

**Silver(+Gold) trigger names reflect actual scope, not aspirational scope.**
`trigger_silver_gold_customers`/`trigger_silver_gold_products`/`trigger_silver_gold_sales` all
include `gold` because each domain's Gold pipeline actually exists. `trigger_silver_sales` was
this repo's own worked example of the alternative, before `gold_sales_etl` existed: it omitted
`gold` because naming it `trigger_silver_gold_sales` at the time would have overclaimed a stage
it didn't run - and per this same rule, once `gold_sales_etl` was built, the job WAS renamed to
`trigger_silver_gold_sales` (gaining a `run_gold_sales_etl` task depending on its Silver task),
not left with a stale `_silver`-only name. This is a general rule, not a one-off: whenever a
future domain gets a Silver pipeline before its Gold one exists, that domain's trigger job
starts `_silver`-only too, and gets this exact same rename the moment Gold catches up - a job's
name is a claim about what it does, and an outdated claim is worse than no claim.

Why this shape, not one big job per domain:

- A `file_arrival` trigger fires a job's *whole* task graph, never a single task within it - so
  a SAP file landing must not also re-run `bronze_webshop_etl`. Each Tier 1 source needs its own
  single-task trigger job, not one shared job with all sources as tasks.
- A `table_update` trigger only fires on an actual new Delta commit - a Bronze run that finds no
  new files (Auto Loader, checkpointed) writes no new table version, so a no-op Bronze trigger
  run never spuriously re-fires the Silver(+Gold) trigger job downstream.
- `ANY_UPDATED`, not `ALL_UPDATED`, on Silver(+Gold) trigger jobs whenever a domain's Silver step
  reads multiple independent Bronze sources (`silver_sales_etl`'s two per-source tables;
  `silver_customers_etl`'s two independent `append_flow`s, each self-sufficient via a
  stream-static join against the other source's latest snapshot) - each source's own arrival is
  already valid, complete input on its own; waiting on every source (`ALL_UPDATED`) would only
  add latency for whichever arrived first.
- The `_full` job is kept deliberately, not replaced by the trigger jobs - it's the explicit
  reload path when you want Bronze re-run too (dev testing, backfills, recovery), not something
  the automatic path should ever do on its own.

**Trigger pause state is controlled by the deploying target's `presets.trigger_pause_status` in
`databricks.yml`, never by a per-resource `pause_status` inside the job YAML itself.** DABs
precedence is mode defaults → presets → per-resource settings, so a hardcoded
`pause_status: UNPAUSED` in a `trigger_*` job's YAML would override and defeat the `dev`
target's `presets.trigger_pause_status: PAUSED` safety default (see the comment above
`presets:` in `databricks.yml.example`) - confirmed live: deploying with pause state hardcoded
per-resource landed everything unpaused in `dev` regardless of the target preset. None of the
`trigger_*` job YAMLs in this repo set `pause_status` themselves, for exactly this reason -
don't add it back when creating a new one.

## Schema allocation rule

Bronze, Silver, and Gold are not one flat set of schemas, and Silver/Gold don't split the same
way - this is a real Unity Catalog access-control boundary (grants are scoped to schemas, not to
table-name prefixes), not just naming:

- **Bronze**: a single schema, tables prefixed by source system (`bronze.sap_orders`,
  `bronze.crm_customers`). Bronze access is normally uniform (engineers only) regardless of
  source, so splitting it further wouldn't buy real governance value and would just cause schema
  sprawl as sources grow.
  - Each Bronze transformation file declares `SOURCE_NAME`/`ENTITY_NAME` as module-level
    constants and builds the table's `name=`, its Staging/Drop path, and its Auto Loader schema
    location from those two - never as independent string literals. This isn't for readability
    (`sap_orders` is already self-explanatory) - it's so the table name and its storage path
    can't silently drift apart, since both are now computed from the same two values instead of
    typed twice. See `sap_orders.py` for the pattern.
- **Silver**: one schema per **subject area** - what entity the data is about, e.g. `orders` vs.
  `customers` vs. `products` (`silver_sales`, `silver_customers`, `silver_products`), plus a
  `silver_data_quality` schema for rows that fail a Silver check, deliberately kept out of the
  subject-area schemas. This is a real access-control distinction, not just tidiness: Silver's
  subject-area schemas are meant to be safely consumable by downstream readers (analysts, data
  scientists), so a row that failed a hard check can't live alongside them - but `silver_data_quality`
  isn't only for excluded rows: it also holds a copy of rows that triggered a *soft* check and
  still merged normally into the subject-area table, since neither case is reviewable from Silver
  alone (a soft check's outcome, like an Expectation's, isn't a queryable column on the row itself
  - see "How this replaces/extends `@dp.expect`" below). Every subject-area Silver pipeline that
  checks its own input writes its findings there via a fully-qualified table name per source
  Bronze table it reads (`silver_data_quality.sap_orders_findings`,
  `silver_data_quality.crm_customers_findings`, ...) - named `<bronze_table>_findings`, not
  `<bronze_table>_rejected`, since a table holding merged-and-flagged rows alongside excluded
  ones would be misleading if named only for exclusion; named after the Bronze table a row came
  from, not the Silver table it would have joined into, since a two-source join (see
  `silver_customers_etl`/`silver_products_etl`) can flag either source independently.

  Every findings table across every pipeline shares the same common, minimal shape -
  `_driving_table`, `_severity`, `_finding_reason`, `_finding_reason_expr`, `_row_data`,
  `_checked_at` - rather than each mirroring its own source table's business columns:
  `_severity` is `"rejected"` or `"warning"`; `_finding_reason` is the classified outcome (e.g.
  `"non_positive_quantity"`, `"unknown_status"`); `_finding_reason_expr` is the literal
  `REJECT_REASON_EXPR`/`WARNING_REASON_EXPR` text that produced it, kept for audit purposes;
  `_row_data` is the ENTIRE raw Bronze row (every column present, whatever they are) collapsed
  into one `VARIANT` via `to_variant_object(struct(*cols))`, with the column list read off the
  live DataFrame rather than hand-typed - so a findings table never needs updating when a
  source's own schema changes, and every findings table in the repo is queryable the same way.
  A row that's rejected never also gets a separate warning finding for the same event, even if
  it would also have failed a warning check - reject is the terminal, overriding severity (a
  row that's excluded from the main table doesn't need a second, redundant reason it was also
  going to be excluded for). Every Silver pipeline declares a `REJECT_REASON_EXPR`/
  `WARNING_REASON_EXPR` pair per source it checks, even a pipeline with no active warning check
  today (its `WARNING_REASON_EXPR` is just `"CAST(NULL AS STRING)"`) - kept structurally present
  everywhere so a future soft check composes into the existing shape rather than requiring a
  redesign. Each numeric/timestamp field a check actually evaluates uses `try_cast`, not plain
  `.cast` - a plain cast on an unparseable value silently returns `NULL` instead of erroring, and
  a check that only tests e.g. `quantity <= 0` never catches that `NULL` (SQL's three-valued
  logic: `NULL <= 0` is `NULL`, not `TRUE`), so the row would otherwise vanish from every output
  entirely. `try_cast` makes "genuinely missing" and "present but unparseable" resolve to `NULL`
  the same, deterministic way, regardless of the session's ANSI SQL setting. See
  `sap_orders.py`/`customers.py`/`products.py` for the pattern.

  **How this replaces/extends `@dp.expect`/`@dp.expect_or_drop`, not just avoids them:**
  Lakeflow's own Expectations only ever offer three actions - warn (keep the row, log a count),
  drop (exclude the row, log a count), fail (stop the whole update) - and even warn/drop's
  "count" is aggregate-only, in the pipeline's event log/Data Quality UI tab, never the actual
  row. This repo's `_evaluated` view + `REJECT_REASON_EXPR`/`WARNING_REASON_EXPR` pattern is a
  hand-written extension of the *drop* and *warn* actions specifically (not of Expectations as
  an API - `@dp.expect_or_fail` has no equivalent here, and isn't meant to): same per-row boolean
  evaluation `expect_or_drop`/`expect` would do, but routed to a queryable findings row instead
  of only ever a metrics count. A hard check (`REJECT_REASON_EXPR`) is the extended-`drop`
  action: still excludes the row from the subject-area table, exactly like `expect_or_drop`, but
  the excluded row is preserved. A soft check (`WARNING_REASON_EXPR`) is the extended-`warn`
  action: the row still merges normally, exactly like `expect`, but the fact that it warned is
  now a queryable fact - in `silver_data_quality`, not as new columns bolted onto the
  subject-area table itself. That alternative (e.g. `_check_warning`/`_warning_reason` columns
  added directly to `customers`/`products`/...) was considered and rejected: every table would
  carry those columns forever, including ones with no active warning check at all (dead, always-
  default columns on every row); one flat `_warning_reason` column can't represent a row that
  trips two independent warnings at once, the way a terminal reject's single reason can (a
  reject stops mattering once it's excluded - a warning doesn't, so a row could genuinely
  warrant more than one); and a Gold Auto CDC pipeline reading straight off a Silver table
  (`gold_shared_customers_etl` off `silver_customers.customers`) would have to explicitly
  exclude every such housekeeping column from `track_history_column_list`, or risk a warning
  flag flipping getting versioned into SCD Type 2 history as if it were a real business change.
  - The same reviewability problem exists one level up, for the *accepted* rows: when two
    `append_flow`s write into one target (`silver_customers_etl`'s/`silver_products_etl`'s
    `customers`/`products`), a row alone doesn't say which source's event actually produced it,
    since both flows enrich themselves into the identical column shape. Each such flow tags its
    output with a `_driving_table` column (the fully-qualified Bronze table it read, e.g.
    `bronze.mdm_products`) - set from the same `*_BRONZE_TABLE` module-level constant the flow
    already reads from (not a second string literal), same drift-proofing reasoning as
    `SOURCE_NAME`/`ENTITY_NAME` above. **Silver-only, deliberately dropped before Gold**: which
    of two Silver flows produced a row stops being a meaningful question once Gold's Auto CDC
    has merged everything into one conformed dimension timeline, so `dim_customer.py`'s/
    `dim_product.py`'s `*_for_gold` temporary view (see Standard governance metadata columns
    below) explicitly `.drop("_driving_table")` before handing the DataFrame to
    `create_auto_cdc_flow` - not carried into Gold's `schema=` DDL at all.
- **Gold**: one schema per **business domain** - which business function/department consumes it
  (`gold_sales.fact_orders` is this repo's own real example - Databricks' own HR/finance/IT
  split is the general pattern it follows), plus a `gold_shared` schema for conformed
  dimensions used across domains, deliberately domain-*agnostic* rather than itself a domain
  (`gold_shared.dim_customer`).
- "Subject area" (Silver) and "business domain" (Gold) are genuinely different axes, not two
  words for the same idea - `gold_shared` proves this: it's explicitly cross-domain, which would
  be self-contradictory if "domain" meant what Silver's split means. `silver_data_quality` is a
  third kind of exception again: not cross-subject-area like `gold_shared` is cross-domain, but
  cross-cutting in a different sense - every subject-area pipeline feeds it, but none of them
  own it.
- **Silver/Gold tables never fully-qualify their own `name=`/`target=`** (e.g.
  `dp.create_streaming_table(name="customers", ...)`, not `f"{catalog}.silver_customers.customers"`)
  - the pipeline's own `schema:` in its `.pipeline.yml` is the single source of truth for where a
    pipeline's tables land, so re-typing `catalog.schema` in Python would duplicate it a second
    time with nothing keeping the two in sync. Lakeflow's own publishing-mode rule backs this:
    fully-qualify only when reading or writing *outside* the pipeline's default catalog/schema -
    e.g. `silver_customers_etl` reading `bronze.crm_customers` cross-pipeline, or
    `gold_shared_customers_etl` reading `silver_customers.customers` cross-pipeline, both stay
    fully-qualified because those genuinely live in a different pipeline's schema. Writing works
    the same way: every Silver pipeline's `*_findings` tables fully-qualify to
    `{catalog}.silver_data_quality...` since that's outside the pipeline's own default schema too
    - the first case in this repo of a pipeline *writing* cross-schema, not just reading.
  - Same reasoning one level down: each Silver/Gold transformation file declares `ENTITY_NAME`
    as a module-level constant (after `catalog = spark.conf.get(...)`) and uses it for that
    table's own `name=`/`target=` wherever it repeats within the file - never for `source=`,
    which points at a different table entirely. Mirrors Bronze's `SOURCE_NAME`/`ENTITY_NAME`
    pattern above; see `dim_customer.py` or `customers.py` for the pattern.

**Materialized view refresh, as Gold aggregations grow:** a non-deterministic function or a
complex join inside a `@dp.materialized_view` forces a full recompute instead of an incremental
one. If a Gold MV's query grows past a simple aggregation, enable `delta.enableRowTracking` and
`delta.enableDeletionVectors` on its source tables to keep the refresh incremental.

## Standard governance metadata columns

Every business table (Bronze, Silver, Gold - not the `silver_data_quality.*_findings` tables,
which already have `_checked_at` doing this job) carries two technical columns, for data
governance/compliance traceability: "when did this row's data first enter the Lakehouse" and
"when did each layer last write it." Deliberately the simple version, not full per-layer
first-arrival tracking - see the note at the end of this section for what that would take and
why it's not built here.

- **`_ingested_at`**: set exactly once, in Bronze, via `current_timestamp()` at Auto Loader
  write time - added the same way `_source_file`/`_source_file_modified_at` are, in the same
  `.select(...)`. This is a genuine write timestamp, unlike `_source_file_modified_at` (the
  source *file's* mtime, not when the row actually landed). Carried forward **unchanged**
  through every downstream Silver/Gold table - a plain `col("_ingested_at")` passthrough in
  every Silver `.select(...)`/`append_flow`, and a plain column in Gold's `schema=` DDL (not
  touched by the Gold `_for_gold` view below) - so any row anywhere traces back to exactly when
  its data first entered the Lakehouse.
- **`_updated_at`**: Silver and Gold only (Bronze doesn't need a separate one - `_ingested_at`
  already is Bronze's only write event, since Bronze is append-only). Recomputed at **each**
  layer via `current_timestamp()`, reflecting that layer's own last write/merge time for the
  row - not preserved across later updates, just "last touched here":
  - **Silver**: added directly in each `.select(...)`/`append_flow`'s final `.withColumn(...)`,
    same place `_ingested_at` passes through.
  - **Gold**: Auto CDC's `source=` reads a Silver table by name, and that table already has its
    *own* `_updated_at` column - passing it through unchanged would silently make Gold's
    `_updated_at` mean "Silver's last write," not Gold's own. Each Gold transformation file
    therefore defines a small `@dp.temporary_view` (e.g. `customers_for_gold`) that reads the
    Silver table and overwrites `_updated_at` with a fresh `current_timestamp()`, then points
    `create_auto_cdc_flow`'s `source=` at that view instead of the raw Silver table name. See
    `dim_customer.py`/`dim_product.py` for the pattern.
  - **Both SCD Type 2 Gold tables explicitly exclude `_ingested_at`/`_updated_at` from
    `track_history_column_list`** - a column that's always different would otherwise trigger a
    spurious new SCD2 version on every single run.

Not implemented: preserving a row's **first** arrival at Silver/Gold specifically, distinct from
its most recent update, for the merge-based tables (`sap_orders`/`webshop_orders`/`products` via
Auto CDC SCD1, `dim_customer`/`dim_product` via SCD2). That needs looking up the row's existing
state in the target table before writing and coalescing (`coalesce(existing._first_seen_at,
current_timestamp())`) - real added code (an extra batch read + left join) in every merge flow,
not just a column. Deliberately left as a standard, minimal-effort framework instead - a client
who needs that finer-grained lineage can add the coalesce pattern themselves where it matters to
them, rather than every table in this reference implementation paying for it upfront.

### SCD Type 2 backfill start

**Every SCD Type 2 Gold dimension in this project needs TWO `create_auto_cdc_flow`s into the
same target, not one** - an "incremental" flow (the real business/arrival timestamp as
`sequence_by`, ongoing) and a "backfill" flow (`once=True`, `sequence_by` hardcoded to a
far-past sentinel, `SCD2_BACKFILL_START_AT = "1900-01-01T00:00:00Z"` - this exact value,
consistently, everywhere this pattern is used). Without the backfill flow, `__START_AT` for a
key's first-ever version is whatever moment the pipeline happened to first process it - which,
for any source without a genuinely historical `sequence_by` (see `dim_product.py`: Bronze
file-arrival metadata, not a real business timestamp), means every version starts at
"whenever this was first deployed," clustered on essentially one instant. An as-of join
(`fact_orders.py`, or any future fact) against a dimension shaped that way finds NO match at
all for any event whose date predates that instant - not even the earliest known version -
since there's nothing earlier to fall back to. **Confirmed live**: `dim_product` deployed with
only the incremental flow put every product's `__START_AT` at the same first-deploy timestamp;
every sample order (backdated up to 60 days) predated it, so `fact_orders.product_key` came
back NULL for 100% of rows, not just a fraction.

**The backfill flow's source must be a genuinely STREAMING view, never a batch one -
confirmed live, the hard way.** The first attempt used a `spark.read.table` batch snapshot
deduped via `row_number() over (partition by <key> order by <real timestamp> desc) = 1` -
`create_auto_cdc_flow` (APPLY CHANGES INTO) rejects any batch source outright
(`_LEGACY_ERROR_TEMP_121_APPLY_CHANGES_WITH_BATCH_SOURCE`), and this is NOT the same as the
separate, bypassable `pipelines.incompatibleViewCheck` (that flag only covers a shallower
view-registration check; setting it to `false` does not help here - don't bother). `once=True`
controls how often the flow *runs*, not whether its source must be stream-compatible - it
still must be.

That forces the dedup itself to be streaming-compatible too: a window function like
`row_number()` needs bounded/complete data and doesn't work on an unbounded stream, so use
`dropDuplicates([<key>])` instead - which only guarantees "one row survives per key," not
"the latest one." That turns out not to matter: whichever row `dropDuplicates` happens to
keep for a given key is, by construction, IDENTICAL to one of the real events the
"incremental" flow independently processes at that event's own genuine timestamp (both flows
read the same source). Since both flows share the exact same `track_history_column_list`,
Auto CDC's SCD Type 2 dedup collapses the sentinel-dated copy and the real-timestamped copy
into one continuous version regardless of which row `dropDuplicates` happened to pick - the
incremental flow reconstructs the rest of the real chronological history on top of it exactly
as it always did. `sequence_by` for the backfill flow is a **literal expression**
(`lit(SCD2_BACKFILL_START_AT).cast("timestamp")`), not a real column -
`create_auto_cdc_flow`'s `sequence_by` doesn't require one. `once=True` means the flow runs
exactly once per pipeline lifecycle (a `--full-refresh` resets it, which is correct - a full
refresh legitimately wants to reseed from scratch); it does not re-seed on every normal
update. See `dim_customer.py`/`dim_product.py` for the pattern - copy it verbatim (same
constant name and value, same `dropDuplicates` shape) for any future SCD Type 2 Gold
dimension.

## Dimensional modeling naming

- Dimension tables are singular (`dim_customer`, not `dim_customers`); fact tables are plural in
  most cases (`fact_orders`).
- **A fact table carries both the surrogate key and the natural key for any dimension it
  references** (`fact_orders.customer_key` + `.customer_id`, `.product_key` + `.product_id`),
  not the surrogate key alone. This isn't just a join convenience - it's a recovery mechanism.
  A surrogate key is only as trustworthy as whatever generated and attached it at write time; if
  the dimension table or the key-lookup logic itself is ever wrong (a bad join predicate, a
  corrupted or accidentally-rebuilt `dim_customer`, a key-generation bug), a fact that carries
  only `customer_key` has no way to tell a correct key from a wrong one, and no way to fix it
  short of reprocessing from raw history, if that's even still available. The natural key is
  what makes the surrogate key **re-derivable**: with `customer_id` still on the fact, a broken
  `customer_key` can be recomputed from the dimension (or from scratch) after the fact - without
  it, the fact's only link to its dimension is whatever surrogate value got baked in, unverifiable
  and unrecoverable. See `fact_orders.py` for the pattern.

## Staging vs. Drop - not the same pre-Bronze zone

- **Staging** (`<catalog>.staging.staging`, a Unity Catalog Volume) is fed by an external
  cloud-native or on-prem ETL/orchestration tool, for a source with **no native Lakeflow Connect
  connector**. It is deliberately **not** Bronze - raw, ungoverned files, promoted to Bronze via
  Auto Loader. (Databricks' own materials sometimes use "Landing Zone" to mean Bronze itself -
  the opposite of this project's usage; don't collapse the distinction.)
- **Drop** (`<catalog>.drop`, with `system_drop`/`internal_drop`/`customer_drop` volumes) is the
  equivalent pre-Bronze zone for **non-automated arrivals**, split by *who writes*: another
  system pushing files (System), an internal system/employee (Internal), or a genuinely external
  customer (Customer - not yet implemented, see `BACKLOG.md`). Each source reads from
  `drop.<zone>/<source_name>/<object>/` - source name always the top-level folder within the
  zone's volume.

## How to talk about Lakeflow - the declarative claim's real boundary

Lakeflow Pipelines infer the dependency graph and parallelism from what each table/flow actually
reads - no manual loop, unlike classic cloud/on-prem ETL tools' `ForEach`-style loops and
pipeline canvases, which are still imperative control flow under cloud branding. That's real,
and it's the core reason this project exists (see README's "Why this exists").

**Don't overclaim it, though.** This project's own `refresh_*_full`/`trigger_*` Lakeflow Jobs are
the honest boundary of the claim: cross-pipeline sequencing still isn't inferred, only the
dependency graph *within* one pipeline is. The accurate claim, always: Lakeflow pushes the
imperative boundary from every step down to only the cross-pipeline seams - not that it eliminates
imperative orchestration entirely.

## Testing

Most transformation files are NOT safely importable outside the Lakeflow pipeline runtime - their
module-level code (`catalog = spark.conf.get("bundle.catalog")`, `dp.create_streaming_table(...)`,
`@dp.append_flow`-decorated functions) needs a live pipeline graph-building context and a `spark`
global that only exist inside a running pipeline. Importing one of these files directly in a test
process fails.

- **Pure, non-declarative logic that's worth unit testing** (a window-function helper, a
  dedup/validation routine reused more than once) goes in a same-directory sibling module with
  **no `spark`/`dp` references at module level** - e.g. `silver_customers_etl/transformations/_util.py`
  next to `customers.py`, which imports `latest_by` from it. The sibling module is then safely
  importable and testable on its own; the transformation file that has the Lakeflow
  decorators/table reads is not meant to be imported directly in a test. Import it from the test
  the normal way - `from src.pipelines.<layer>.<pipeline_name>.transformations.<module> import
  <name>` - not a `sys.path` hack: this project's editable install (`environment.dependencies:
  --editable ${workspace.file_path}` in every pipeline yml) puts the project root on `sys.path`,
  so the `src....` form resolves cleanly for both pytest and static analysis (Pylance/Pyright),
  confirmed live.
- Don't manufacture unit tests for logic that's genuinely just declarative config (a schema
  string, a `TRACKED_COLUMNS` list, an Auto CDC call, an expectation's SQL predicate string) - a
  renamed column there already breaks `bundle validate`/pipeline execution, and asserting "this
  list equals this list" protects against nothing real. Most of this codebase is exactly that
  kind of thin, declarative code - the extractable-pure-logic surface is small on purpose, and
  padding it with low-value tests is worse than having fewer, real ones.
- Tests use `databricks-connect`'s `DatabricksSession` (a session-scoped `spark` fixture in
  `tests/conftest.py`), not a local pyspark session - `databricks-connect` and plain `pyspark`
  conflict when both are installed in the same environment, and `databricks-connect` is already
  this project's dev dependency for local development (see README's "Local Python environment").
  This means `uv run pytest` needs a working CLI profile/reachable compute to run these, same as
  `databricks-connect test` already does - not a new constraint.
- `tests/` mirrors `src/pipelines/<layer>/<pipeline_name>/` for anything testing that pipeline's
  code (`tests/pipelines/silver/silver_customers_etl/test_util.py`), plus flat top-level files for
  cross-cutting checks (`tests/test_bundle.py` runs `databricks bundle validate` as a regression
  check against YAML/resource-reference breakage - skips rather than fails when no CLI/auth is
  available, so a fresh clone without Databricks credentials configured doesn't get a false
  failure). This means every pipeline's test file is named `test_util.py`, same basename in a
  different directory - `pyproject.toml`'s `[tool.pytest.ini_options]` sets
  `addopts = "--import-mode=importlib"` for exactly this reason (confirmed live: pytest's
  default import mode errors with "import file mismatch" the moment a second same-named
  `test_util.py` exists, since neither test directory has an `__init__.py`) - don't remove that
  setting when only one such file exists yet.
- Databricks' native Lakeflow Pipelines unit-testing framework (Beta, web-editor-only, mocks
  Auto CDC/streaming tables/expectations/append flows via the Pipelines Editor) is deliberately
  not used here yet: it only isolates table-name reads, so Bronze's path-based Auto Loader reads
  would bypass its isolation and hit the real Staging/Drop volumes, and it can't be driven from
  `pytest`/CI at all. Worth revisiting once there's a concrete need to test Auto CDC/expectation
  behavior itself, not just the plain-Python logic around it.

## Folder structure

`resources/` and `src/` both mirror the same layer-first, resource-type-first grouping:

```
resources/
  pipelines/
    bronze/              <pipeline>.pipeline.yml   (one per Tier 1 source)
    silver/               <pipeline>.pipeline.yml   (one per Tier 2 domain, Silver layer)
    gold/                 <pipeline>.pipeline.yml   (one per Tier 2 domain, Gold layer)
  jobs/
    trigger_bronze/       trigger_bronze_<source>_etl.job.yml
    trigger_silver_gold/  trigger_silver_<domain>.job.yml or trigger_silver_gold_<domain>.job.yml
    refresh/              refresh_<domain>_full.job.yml

src/
  pipelines/
    bronze/<pipeline_name>/transformations/<source>_<object>.py
    silver/<pipeline_name>/transformations/<object>.py
    gold/<pipeline_name>/transformations/<object>.py
```

Resource type first (`pipelines/` vs `jobs/`), then role within each - pipelines by layer, jobs
by trigger role (one folder per row of the Job naming table above, so the folder alone tells you
whether a job fires automatically). Since each pipeline is already layer-scoped by construction,
its own `transformations/` folder doesn't need a further schema subfolder underneath it.

**Adding a genuinely new subfolder under `resources/pipelines/` or `resources/jobs/` also needs
a new explicit glob line in `databricks.yml`'s (and `databricks.yml.example`'s) `include:`
list.** It deliberately does NOT use a recursive `resources/**/*.yml` glob - confirmed live that
one validates fine but silently resolves to zero resources for `bundle deploy`/`bundle plan`
(see the comment above `include:` in `databricks.yml.example`). Forgetting this step means
`bundle deploy` sees the new folder's resources as simply absent from desired state and deletes
anything previously deployed under the old, now-incomplete `include:` list.
