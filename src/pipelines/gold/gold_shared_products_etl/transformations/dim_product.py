"""Gold: conformed product dimension, SCD Type 2 on reference_price changes only (see
README.md and AGENTS.md for the project's schema-allocation and dimensional-modeling
naming rules - why this lives in gold_shared and is named singular).

product_key is the surrogate key (GENERATED ALWAYS AS IDENTITY, unique per historical
version - not per product); product_id is the natural key from the MDM source.
reference_price is the ONLY tracked column - Product Management's own recommendation (see
bronze_prodman_etl) is what this dimension exists to version; a product_name/category/
subcategory change alone does not create a new version, only updates the current row in
place (SCD Type 1 for those, matching gold_shared_customers_etl's "everything else" behavior).

sequence_by is _source_file_modified_at, not a business timestamp - silver_products_etl's
products table doesn't carry one uniformly across both its source flows (see that
pipeline's own docstring for why).

Reads silver_products_etl's silver_products.products cross-pipeline (fully-qualified
name) - see that pipeline's own comment for the Silver/Gold pipeline-split tradeoff.

TWO Auto CDC flows feed this table, not one - see CONVENTIONS.md's Standard governance
metadata columns section ("SCD Type 2 backfill start") for why every SCD2 Gold dimension in
this project needs this shape, not just this one. This pipeline is the one that actually
surfaced the problem live: _source_file_modified_at is file-arrival metadata (essentially
"now", the same instant for every product in one upload), not a spread-out historical
timestamp the way CRM's updated_at is - confirmed live that with only the "incremental" flow,
__START_AT for every product ended up being the exact same first-deploy moment, so
fact_orders' as-of join found ZERO matching product versions for every single order (all
backdated before that moment). The "backfill" flow (once=True, sequence_by hardcoded to
SCD2_BACKFILL_START_AT) fixes this the same way as dim_customer.py: it seeds a starting state
for every product_id as "since the beginning of time", so an as-of join against any
plausible historical order_date finds a match instead of NULL.

Confirmed live, twice, why the backfill source is built the way it is below - see
dim_customer.py's docstring for the full reasoning: Auto CDC requires a genuinely STREAMING
source, not just an incompatible-view-check bypass (`_LEGACY_ERROR_TEMP_121_APPLY_CHANGES_WITH_BATCH_SOURCE`
fires even with `pipelines.incompatibleViewCheck` disabled), which rules out a
`spark.read.table(...)`-based batch snapshot entirely, which in turn rules out a
window-function dedup (unsupported on an unbounded stream). `dropDuplicates` is the
streaming-compatible alternative - whichever row it keeps for a given product_id is
guaranteed to match one of the real events the "incremental" flow also processes, so
track_history_column_list's dedup still collapses them into one continuous version.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit

catalog = spark.conf.get("bundle.catalog")
ENTITY_NAME = "dim_product"

TRACKED_COLUMNS = ["reference_price"]

# See CONVENTIONS.md's "SCD Type 2 backfill start" - this exact value is the project-wide
# standard for every SCD2 Gold dimension's backfill flow, not a per-pipeline choice.
SCD2_BACKFILL_START_AT = "1900-01-01T00:00:00Z"

dp.create_streaming_table(
    name=ENTITY_NAME,
    comment="Conformed product dimension - SCD Type 2 on reference_price changes, SCD Type 1 on everything else.",
    table_properties={"quality": "gold"},
    schema="""
        product_key BIGINT GENERATED ALWAYS AS IDENTITY,
        product_id STRING,
        product_name STRING,
        category STRING,
        subcategory STRING,
        reference_price DOUBLE,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP,
        _source_file_modified_at TIMESTAMP,
        _ingested_at TIMESTAMP,
        _updated_at TIMESTAMP
    """,
)


@dp.temporary_view(name="products_for_gold")
def products_for_gold():
    # Overwrites Silver's own _updated_at with this layer's write time - Auto CDC merges by
    # column name, so passing Silver's value through unchanged here would silently mean
    # "Gold's _updated_at" and "Silver's _updated_at" are the same value, not each layer's own.
    #
    # Drops _driving_table: that column tags which of silver_products.products' two source
    # flows (from_mdm/from_prodman) produced a row - useful within Silver, meaningless once
    # Gold's Auto CDC has merged everything into one conformed dim_product timeline. Also keeps
    # the source DataFrame's columns matching this table's explicit schema= below exactly - an
    # extra column Auto CDC didn't expect is a schema mismatch, not something to silently carry
    # through.
    return (
        spark.readStream.table(f"{catalog}.silver_products.products")
        .drop("_driving_table")
        .withColumn("_updated_at", current_timestamp())
    )


@dp.temporary_view(name="products_backfill")
def products_backfill():
    """Streaming (Auto CDC requires it - see this file's docstring), deduped to one row per
    product_id via dropDuplicates - whichever row it keeps is fine, see this file's docstring
    for why. The one-time seed for the "backfill" Auto CDC flow below."""
    return (
        spark.readStream.table(f"{catalog}.silver_products.products")
        .dropDuplicates(["product_id"])
        .drop("_driving_table")
        .withColumn("_updated_at", current_timestamp())
    )


dp.create_auto_cdc_flow(
    target=ENTITY_NAME,
    source="products_for_gold",
    keys=["product_id"],
    sequence_by="_source_file_modified_at",
    stored_as_scd_type=2,
    track_history_column_list=TRACKED_COLUMNS,
    name="incremental",
)

dp.create_auto_cdc_flow(
    target=ENTITY_NAME,
    source="products_backfill",
    keys=["product_id"],
    # A literal expression, not a real column - sequence_by doesn't require one, and forcing
    # a hardcoded sentinel is the whole point: this flow's rows should sort BEFORE any
    # genuine _source_file_modified_at value, however old.
    sequence_by=lit(SCD2_BACKFILL_START_AT).cast("timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=TRACKED_COLUMNS,
    once=True,
    name="backfill",
)
