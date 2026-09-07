# Tutorial: building your own data platform from this repo

Two parts: bootstrapping your own platform from this repo (once), and adding one real source
through Bronze -> Silver -> Gold (repeated for every source you bring in). Read
[CONVENTIONS.md](CONVENTIONS.md) first - this tutorial applies those rules to a worked
example, it doesn't restate them.

## Part 1: Bootstrap your own platform

1. **Treat this repo as a seed, not a dependency.** Fork or copy it as the starting point for
   your own platform repo - you're not meant to keep pulling updates from this repo into an
   ever-growing one, see "A framework, not a codebase" in `README.md`. Decide whether to keep
   the six example sources (SAP/webshop/CRM/Finance/MDM/prodman) around as a live reference while you
   build your first real source, or strip them immediately - either is fine, but never let
   real/client data land in the same catalog as the examples (see `AGENTS.md`'s
   client-specific-work boundary).

2. **Rename the project identifiers.** This project itself went through exactly this rename
   (`lakeflow_factory` -> `etl_factory`), so the mechanics below are proven, not theoretical:
   - `pyproject.toml`'s `[project] name`
   - `databricks.yml.example`'s `bundle.name` (and `bundle.uuid` - generate a fresh one, don't
     reuse this repo's)
   - `databricks.yml.example`'s `dev`/`prod` target `catalog` variable values
   - re-run `uv sync --group dev` after the `pyproject.toml` rename

   **Name it after this platform instance, not after "ETL" generically.** `etl_factory`
   describes *this* framework's own reference deployment - a fine, unconfusing catalog name
   for it. If you ever spin up a second platform from this same framework (a different
   client, a different domain), give that one its own genuinely different name - reusing
   `etl_factory`-style naming for every instance defeats the point of a per-platform catalog
   and makes catalogs indistinguishable in a workspace hosting more than one.

3. **Create your Unity Catalog catalog.**
   ```bash
   databricks catalogs create --json '{"name": "<your_catalog>_dev"}' --profile <profile>
   ```
   **Caveat, live-verified on this project's own workspace** (Databricks Free Edition,
   Default-Storage-backed metastore): catalog creation via CLI/REST was rejected outright
   ("Please use the UI to create a catalog with Default Storage"). If you hit the same, create
   the catalog once via Catalog Explorer in the workspace UI instead - everything downstream
   (schemas, volumes, tables, grants, deploys) then works fine via CLI as normal.

4. **Create the schemas and volumes**, per `CONVENTIONS.md`'s schema allocation rule - `bronze`
   (single schema for all Tier 1 sources), `staging`/`drop` (only the ones your first source
   actually needs), and one `silver_<subject>`/`gold_<domain>` pair per business area you're
   about to build. You don't need every schema on day one - create each new `silver_`/`gold_`
   schema only when you add the pipeline that needs it, proving the mechanics for one source
   before generalizing to the next. Part 2 below shows the exact commands for one worked
   example.

5. **Point `databricks.yml` at your workspace**, per [Getting started](README.md#getting-started)
   in `README.md` - not repeated here.

## Part 2: Add a source, end to end - a worked example

A genuinely new example, not a copy of the six already in this repo: bringing in a warehouse
management system (WMS) that has **no Lakeflow Connect connector** and pushes its own nightly
inventory-level export files directly - no external ETL tool in between. The inventory concept
doesn't fit the existing `sales`/`customers` domains, so this also walks through introducing a
new business domain, `operations`.

### Step 1: decide routing and business domain

- **Routing**: no connector, and WMS pushes the files itself (not an external tool acting on
  its behalf) -> **System Drop**, same reasoning as the existing `bronze_webshop_etl` example.
  If your source instead has a Lakeflow Connect connector, skip Staging/Drop entirely - the
  connector lands directly to Bronze (see "Target architecture" in `README.md`). If an
  external ETL/orchestration tool is what actually moves the files, use **Staging** instead
  (the `bronze_sap_etl` example), not Drop.
- **Business domain**: inventory doesn't belong under `sales` or `customers` - it's a new
  domain, `operations`. New domain means a new `silver_operations` schema and, once Gold is
  built, a new `gold_operations` schema.

### Step 2: create the schema/volume this source needs

`bronze` already exists (shared across all Tier 1 sources). This source only adds a Drop path:

```bash
databricks volumes create --json '{
  "catalog_name": "<your_catalog>_dev",
  "schema_name": "drop",
  "name": "system_drop",
  "volume_type": "MANAGED"
}' --profile <profile>
```
(Skip this if `drop.system_drop` already exists from an earlier source - it's shared across
every System Drop source, per `CONVENTIONS.md`'s Staging-vs-Drop section.)

### Step 3: Tier 1 - the Bronze pipeline

First, register `wms` in the two source-config files (see CONVENTIONS.md's "Source registry and
Source x Environment connection config" section) - the Bronze transformation file below reads
these at runtime instead of hardcoding its Drop path:

`config/sources.yml` (add this entry to the `sources:` list):

```yaml
  - name: wms
    description: >-
      WMS inventory-level export data, landed into the System Drop volume by WMS itself.
    type: file_drop
```

`config/source_environment.yml` (add this entry):

```yaml
wms:
  dev:
    drop_path: "/Volumes/{catalog}/drop/system_drop/wms/inventory/"
    schema_location: "/Volumes/{catalog}/drop/system_drop/_schemas/wms_inventory_bronze/"
  prod:
    drop_path: "/Volumes/{catalog}/drop/system_drop/wms/inventory/"
    schema_location: "/Volumes/{catalog}/drop/system_drop/_schemas/wms_inventory_bronze/"
```

`resources/pipelines/bronze/bronze_wms_etl.pipeline.yml`:

```yaml
# Tier 1 (source-ingestion) pipeline for WMS: System Drop -> Bronze only. See CONVENTIONS.md
# for the project's schema-allocation rules and README.md for the target architecture.
#
# Tier 1 = grouped by WHO DELIVERS the data. Tier 2's silver_operations_etl owns the
# conformed Silver representation, reading this pipeline's Bronze table cross-pipeline - this
# pipeline has no knowledge of who reads it.

resources:
  pipelines:
    bronze_wms_etl:
      name: bronze_wms_etl_${bundle.target}
      catalog: ${var.catalog}
      schema: bronze
      serverless: true
      continuous: false
      root_path: "../../../src/pipelines/bronze/bronze_wms_etl"

      configuration:
        bundle.catalog: ${var.catalog}
        # Read by wms_inventory.py to look up its entry in config/source_environment.yml -
        # see CONVENTIONS.md's "Source registry and Source x Environment connection config"
        # section.
        bundle.target: ${bundle.target}
        bundle.workspace_file_path: ${workspace.file_path}

      libraries:
        - glob:
            include: ../../../src/pipelines/bronze/bronze_wms_etl/transformations/**

      environment:
        dependencies:
          - --editable ${workspace.file_path}
```

`src/pipelines/bronze/bronze_wms_etl/transformations/wms_inventory.py`:

```python
"""Bronze: raw inventory-level events as landed in the System Drop zone by WMS.

Lands in System Drop, not Staging: WMS pushes its own export files directly (no external
ETL/orchestration tool involved, no Lakeflow Connect connector) - same reasoning as
bronze_webshop_etl's webshop_orders.py. Top-level folder is the source name (`wms/`), per
this project's Staging/Drop convention.

No validation performed here - Bronze preserves the source verbatim. `wms_` is this table's
source-system abbreviation.

drop_path/schema_location come from config/source_environment.yml instead of being built from
literals here - see CONVENTIONS.md's "Source registry and Source x Environment connection
config" section for why, and how pipeline code reads the file via bundle.workspace_file_path.
"""

import yaml
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_NAME = "wms"
ENTITY_NAME = "inventory"

catalog = spark.conf.get("bundle.catalog")
target = spark.conf.get("bundle.target")
workspace_file_path = spark.conf.get("bundle.workspace_file_path")

with open(f"{workspace_file_path}/config/source_environment.yml") as f:
    connection = yaml.safe_load(f)[SOURCE_NAME][target]

drop_path = connection["drop_path"].format(catalog=catalog)
schema_location = connection["schema_location"].format(catalog=catalog)


@dp.table(
    name=f"{SOURCE_NAME}_{ENTITY_NAME}",
    comment="Raw inventory-level events exactly as landed in System Drop by WMS. No validation performed.",
    table_properties={"quality": "bronze"},
)
def wms_inventory():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(drop_path)
        .select(
            "*",
            col("_metadata.file_path").alias("_source_file"),
            col("_metadata.file_modification_time").alias("_source_file_modified_at"),
            current_timestamp().alias("_ingested_at"),
        )
    )
```

`resources/jobs/trigger_bronze/trigger_bronze_wms_etl.job.yml`:

```yaml
# Runs bronze_wms_etl automatically whenever a new file lands in System Drop under WMS's own
# top-level folder. Single-task job - Tier 1 pipelines are independent per source.
#
# No pause_status set here on purpose - trigger pause state is controlled by the deploying
# target's presets.trigger_pause_status in databricks.yml, not per-resource (see
# CONVENTIONS.md's Job naming section). A hardcoded pause_status here would override and defeat
# that target-level dev-safety default.

resources:
  jobs:
    trigger_bronze_wms_etl:
      name: trigger_bronze_wms_etl_${bundle.target}

      trigger:
        file_arrival:
          url: "/Volumes/${var.catalog}/drop/system_drop/wms/inventory/"

      tasks:
        - task_key: run_bronze_wms_etl
          pipeline_task:
            pipeline_id: ${resources.pipelines.bronze_wms_etl.id}
```

### Step 4: Tier 2 - the Silver pipeline

New domain means a new schema first:

```bash
databricks schemas create --json '{
  "name": "silver_operations",
  "catalog_name": "<your_catalog>_dev"
}' --profile <profile>
```

`silver_data_quality` needs the same treatment, unless you've already deployed this repo's six
real sources (whose Silver pipelines already write there, which would have created it) - skip
this if `silver_data_quality` already exists:

```bash
databricks schemas create --json '{
  "name": "silver_data_quality",
  "catalog_name": "<your_catalog>_dev"
}' --profile <profile>
```

`resources/pipelines/silver/silver_operations_etl.pipeline.yml`:

```yaml
# Tier 2 (business-domain) pipeline for Operations: Bronze (cross-pipeline) -> Silver.
#
# Tier 2 = grouped by WHICH BUSINESS DOMAIN consumes the data. Reads bronze_wms_etl's
# Bronze table cross-pipeline.

resources:
  pipelines:
    silver_operations_etl:
      name: silver_operations_etl_${bundle.target}
      catalog: ${var.catalog}
      schema: silver_operations
      serverless: true
      continuous: false
      root_path: "../../../src/pipelines/silver/silver_operations_etl"

      configuration:
        bundle.catalog: ${var.catalog}

      libraries:
        - glob:
            include: ../../../src/pipelines/silver/silver_operations_etl/transformations/**

      environment:
        dependencies:
          - --editable ${workspace.file_path}
```

`src/pipelines/silver/silver_operations_etl/transformations/inventory.py`:

```python
"""Silver: validated, deduplicated inventory levels (see CONVENTIONS.md's schema-allocation
rule for why this lives in silver_operations).

Reads bronze_wms_etl's bronze.wms_inventory cross-pipeline (fully-qualified name). Uses Auto
CDC (SCD Type 1, keyed on sku + warehouse_id): each Bronze row is a new stock-level event for
that key, and Silver keeps only the LATEST known quantity per (sku, warehouse_id), not every
event - a different shape from the orders examples' single-column key, same mechanism.

Two severities, mirroring sap_orders.py's design: REJECT_REASON_EXPR (missing sku/
warehouse_id, invalid quantity) EXCLUDES the row from silver_operations.inventory entirely;
WARNING_REASON_EXPR would flag a row without excluding it - inert here (evaluates to a plain
NULL, no active warning check for this example), but still declared and wired into the
_evaluated view/findings table the same way, so a future soft check composes without
restructuring anything. Both land, side by side, in silver_data_quality.wms_inventory_findings
- one common table for every check this pipeline runs, not two separate mechanisms - see
sap_orders.py's docstring for the full reasoning (reviewability over a metrics-only count,
try_cast for the NULL-on-cast-failure problem, reject-overrides-warning precedence).

The findings table keeps this repo's common, minimal shape (_driving_table, _severity,
_finding_reason, _finding_reason_expr, _row_data, _checked_at) rather than its own business
columns - _row_data captures the entire raw Bronze row as VARIANT, so it never needs updating
if wms_inventory's own schema changes later.

_ingested_at (Bronze's write time) passes through unchanged into `inventory`; _updated_at
(this layer's own write time) is added fresh here - the two standard governance metadata
columns every business table carries, see CONVENTIONS.md's Standard governance metadata
columns section.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import coalesce, col, current_timestamp, expr, lit, struct, to_variant_object, when

catalog = spark.conf.get("bundle.catalog")
ENTITY_NAME = "inventory"
WMS_BRONZE_TABLE = "bronze.wms_inventory"

REJECT_REASON_EXPR = (
    "CASE "
    "WHEN sku IS NULL THEN 'missing_sku' "
    "WHEN warehouse_id IS NULL THEN 'missing_warehouse_id' "
    "WHEN try_cast(quantity_on_hand AS INT) IS NULL THEN 'missing_or_invalid_quantity' "
    "WHEN try_cast(quantity_on_hand AS INT) < 0 THEN 'negative_quantity' "
    "END"
)
WARNING_REASON_EXPR = "CAST(NULL AS STRING)"


@dp.temporary_view(name="wms_inventory_evaluated")
def wms_inventory_evaluated():
    return (
        spark.readStream.table(f"{catalog}.{WMS_BRONZE_TABLE}")
        .withColumn("_rejection_reason", expr(REJECT_REASON_EXPR))
        .withColumn("_warning_reason", expr(WARNING_REASON_EXPR))
    )


@dp.temporary_view(name="wms_inventory_valid")
def wms_inventory_valid():
    return (
        spark.readStream.table("wms_inventory_evaluated")
        .where("_rejection_reason IS NULL")
        .select(
            col("sku").cast("string"),
            col("warehouse_id").cast("string"),
            expr("try_cast(quantity_on_hand AS INT)").alias("quantity_on_hand"),
            col("updated_at").cast("timestamp"),
            col("_ingested_at"),
            current_timestamp().alias("_updated_at"),
        )
    )


dp.create_streaming_table(
    name=ENTITY_NAME,
    comment="Validated, deduplicated inventory levels - one row per (sku, warehouse_id).",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target=ENTITY_NAME,
    source="wms_inventory_valid",
    keys=["sku", "warehouse_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1,
)


@dp.table(
    name=f"{catalog}.silver_data_quality.wms_inventory_findings",
    comment="WMS inventory rows that failed a Silver check (rejected: excluded from "
    "silver_operations.inventory; warning: still merged there) - kept for manual review. "
    "_row_data holds the full raw Bronze row as VARIANT.",
    table_properties={"quality": "silver_data_quality"},
)
def wms_inventory_findings():
    evaluated = spark.readStream.table("wms_inventory_evaluated")
    raw_columns = [c for c in evaluated.columns if c not in ("_rejection_reason", "_warning_reason")]
    is_rejected = col("_rejection_reason").isNotNull()
    return evaluated.where("_rejection_reason IS NOT NULL OR _warning_reason IS NOT NULL").select(
        lit(WMS_BRONZE_TABLE).alias("_driving_table"),
        when(is_rejected, lit("rejected")).otherwise(lit("warning")).alias("_severity"),
        coalesce(col("_rejection_reason"), col("_warning_reason")).alias("_finding_reason"),
        when(is_rejected, lit(REJECT_REASON_EXPR)).otherwise(lit(WARNING_REASON_EXPR)).alias("_finding_reason_expr"),
        to_variant_object(struct(*raw_columns)).alias("_row_data"),
        current_timestamp().alias("_checked_at"),
    )
```

(`name=ENTITY_NAME`/`target=ENTITY_NAME` above, not a fully-qualified `f"{catalog}.silver_operations.inventory"` - this pipeline's own `schema: silver_operations` in its `.pipeline.yml` already makes that the default, so re-typing it here would just be a second place for the two to drift apart. Only the findings-table write is fully-qualified, since `silver_data_quality` is genuinely a different schema - see CONVENTIONS.md's Schema allocation rule.)

### Step 5: Gold

```bash
databricks schemas create --json '{
  "name": "gold_operations",
  "catalog_name": "<your_catalog>_dev"
}' --profile <profile>
```

`gold_shared_customers_etl` (the existing example) builds a **dimension** via Auto CDC SCD
Type 2. This one is deliberately different: a **Materialized View aggregating current state**,
since it's a full-dataset aggregation over Silver, not a change-tracked dimension - see the
`databricks-pipelines` skill's decision tree ("Aggregation across full dataset -> Materialized
View", read via a plain batch `spark.read.table`, no `STREAM`).

Naming note: this pipeline stays `gold_operations_etl`, NOT `gold_shared_operations_etl` -
`gold_shared_` is only for pipelines writing to the actual shared `gold_shared` schema (see
`CONVENTIONS.md`'s Pipeline naming section); this one writes to its own domain-specific
`gold_operations` schema, so the plain `gold_<domain>_etl` form is correct here.

`resources/pipelines/gold/gold_operations_etl.pipeline.yml` follows the same shape as
`gold_shared_customers_etl.pipeline.yml` (`schema: gold_operations`, `root_path` pointing at
`src/pipelines/gold/gold_operations_etl`) - omitted here since it's identical boilerplate to
Step 3/4's pipeline files, just with a different `schema:` value.

`src/pipelines/gold/gold_operations_etl/transformations/fact_inventory_snapshot.py`:

```python
"""Gold: current on-hand inventory snapshot, by warehouse (see CONVENTIONS.md's schema
allocation rule for why this lives in gold_operations).

Reads silver_operations_etl's silver_operations.inventory cross-pipeline via a BATCH read
(spark.read.table, no STREAM) - this aggregates the full current state of the table, not an
append-only event stream, so a Materialized View is the right dataset type here, not a
Streaming Table.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import count, sum as _sum

catalog = spark.conf.get("bundle.catalog")


@dp.materialized_view(
    name="fact_inventory_snapshot",
    comment="Current on-hand inventory quantity per warehouse.",
    table_properties={"quality": "gold"},
)
def fact_inventory_snapshot():
    return (
        spark.read.table(f"{catalog}.silver_operations.inventory")
        .groupBy("warehouse_id")
        .agg(
            count("sku").alias("distinct_skus"),
            _sum("quantity_on_hand").alias("total_quantity_on_hand"),
        )
    )
```

### Step 6: the jobs that keep it current and let you refresh on demand

`resources/jobs/trigger_silver_gold/trigger_silver_gold_operations.job.yml` - named with
**both** layers up front since `gold_operations_etl` exists from the start here. If you build
your Silver pipeline before its Gold one exists yet, name the trigger job `trigger_silver_<domain>`
instead (omit "gold" - it would overclaim a stage that doesn't run), then rename it to
`trigger_silver_gold_<domain>` (and add the Gold task) the moment Gold catches up - see
`CONVENTIONS.md`'s Job naming section, which documents this repo's own sales domain going
through exactly that transition, for why the name must reflect actual scope, not aspirational
scope:

```yaml
resources:
  jobs:
    trigger_silver_gold_operations:
      name: trigger_silver_gold_operations_${bundle.target}

      trigger:
        table_update:
          table_names:
            - "${var.catalog}.bronze.wms_inventory"
          condition: ANY_UPDATED

      tasks:
        - task_key: run_silver_operations_etl
          pipeline_task:
            pipeline_id: ${resources.pipelines.silver_operations_etl.id}

        - task_key: run_gold_operations_etl
          depends_on:
            - task_key: run_silver_operations_etl
          pipeline_task:
            pipeline_id: ${resources.pipelines.gold_operations_etl.id}
```

`resources/jobs/refresh/refresh_operations_full.job.yml` - the on-demand, always-runs-everything
counterpart (dev testing, backfills, recovery):

```yaml
resources:
  jobs:
    refresh_operations_full:
      name: refresh_operations_full_${bundle.target}

      tasks:
        - task_key: run_bronze_wms_etl
          pipeline_task:
            pipeline_id: ${resources.pipelines.bronze_wms_etl.id}

        - task_key: run_silver_operations_etl
          depends_on:
            - task_key: run_bronze_wms_etl
          pipeline_task:
            pipeline_id: ${resources.pipelines.silver_operations_etl.id}

        - task_key: run_gold_operations_etl
          depends_on:
            - task_key: run_silver_operations_etl
          pipeline_task:
            pipeline_id: ${resources.pipelines.gold_operations_etl.id}
```

### Step 7: deploy and validate

No `databricks.yml` edits needed for *this* example - all the new files above land in resources
subfolders (`resources/pipelines/{bronze,silver,gold}/`, `resources/jobs/{trigger_bronze,
trigger_silver_gold,refresh}/`) already listed in `databricks.yml.example`'s explicit `include:`
globs. **That's not automatic in general, though**: `databricks.yml` deliberately does NOT use a
recursive `resources/**/*.yml` glob - confirmed live that it validates fine but silently
resolves to zero resources for `bundle deploy`/`bundle plan` (see the comment above `include:`
in `databricks.yml.example`). If you ever introduce a genuinely new resources subfolder (a new
layer, a new job role), add its own explicit glob line to `include:` first - otherwise `bundle
deploy` will delete every previously-deployed resource without creating replacements, since it
sees the new folder's resources as simply absent from desired state:

```bash
databricks bundle validate -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
databricks bundle run refresh_operations_full -t dev --profile <profile>   # first, full backfill
```

Deploying also unpauses `trigger_bronze_wms_etl` and `trigger_silver_gold_operations`, so from
here on, a new WMS export file keeps `operations` current automatically - no manual refresh
needed day to day. Verify row counts at each layer after the first run, the same way this
repo's own `_notes/NOTES.md` validation log records doing for the six example sources.

### Step 8: update your docs

Once this source is real, update your platform's own `README.md` "Current status" section
(pipeline/job count, domain list) to reflect it - don't let the docs describe only the original
example sources once you've moved past them.
