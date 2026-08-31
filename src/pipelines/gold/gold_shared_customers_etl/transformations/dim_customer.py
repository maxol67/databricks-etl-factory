"""Gold: conformed customer dimension, SCD Type 2 on address changes and credit score
(see README.md and AGENTS.md for the project's schema-allocation and dimensional-modeling
naming rules - why this lives in gold_shared and is named singular).

customer_key is the surrogate key (GENERATED ALWAYS AS IDENTITY, unique per historical
version - not per customer); customer_id is the natural key from the CRM source.
credit_score is in TRACKED_COLUMNS (SCD2-versioned, alongside address) because it's
meaningful to know what score was current at a given point in time, e.g. for a
historical risk assessment.

Reads silver_customers_etl's silver_customers.customers cross-pipeline (fully-qualified
name) - see that pipeline's own comment for the Silver/Gold pipeline-split tradeoff.

TWO Auto CDC flows feed this table, not one - see CONVENTIONS.md's Standard governance
metadata columns section ("SCD Type 2 backfill start") for why every SCD2 Gold dimension in
this project needs this shape, not just this one:
- "incremental" (the original flow): sequence_by="updated_at", the real CRM/Finance business
  timestamp - genuine ongoing changes, correctly ordered.
- "backfill" (once=True, runs exactly once): seeds a starting state for every customer_id,
  sequenced by a hardcoded far-past sentinel (SCD2_BACKFILL_START_AT), not today's date.
  Without this, __START_AT for a customer's first-ever version would be whenever this
  pipeline happened to first run - and any fact whose event date predates that (a real
  possibility: silver_customers.customers' own updated_at is realistically historical, but
  nothing guarantees every order/event referencing this customer postdates it) would find NO
  matching dimension version at all under an as-of join, not even the earliest one. The
  backfill flow closes that gap: the sentinel version covers "since the beginning of time" up
  to the customer's first real tracked change, so an as-of join always finds a match for any
  plausible historical event date.

Confirmed live, twice, why the backfill source is built the way it is below:
- Auto CDC (`create_auto_cdc_flow`/APPLY CHANGES INTO) requires a genuinely STREAMING source
  - not just "no incompatible view registered" (`pipelines.incompatibleViewCheck` does NOT
  cover this - that flag only bypasses a shallower view-registration check) but a hard,
  non-overridable engine requirement (`_LEGACY_ERROR_TEMP_121_APPLY_CHANGES_WITH_BATCH_SOURCE`
  even with the view check disabled). A `spark.read.table(...)`-based batch view, however
  deduped, cannot be this flow's source at all.
- That forces `customers_backfill` to dedupe via streaming-compatible `dropDuplicates`, not a
  window function (unsupported on an unbounded stream without a watermark) - which means it
  can only pick WHICHEVER row for a given customer_id happens to arrive first in processing
  order, not deterministically "the latest." This turns out not to matter: whichever row it
  keeps is guaranteed to be IDENTICAL to one of the real events the "incremental" flow also
  processes, at that event's own genuine timestamp - so track_history_column_list's dedup
  collapses the sentinel-dated copy and the real-timestamped copy into one continuous version
  regardless of which specific row dropDuplicates happened to keep, and the incremental flow
  independently reconstructs the rest of the real chronological history on top of it exactly
  as it always did.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit

catalog = spark.conf.get("bundle.catalog")
ENTITY_NAME = "dim_customer"

ADDRESS_COLUMNS = [
    "address_line1",
    "city",
    "state_province",
    "postal_code",
    "country",
    "region",
]
TRACKED_COLUMNS = ADDRESS_COLUMNS + ["credit_score"]

# See CONVENTIONS.md's "SCD Type 2 backfill start" - this exact value is the project-wide
# standard for every SCD2 Gold dimension's backfill flow, not a per-pipeline choice.
SCD2_BACKFILL_START_AT = "1900-01-01T00:00:00Z"

dp.create_streaming_table(
    name=ENTITY_NAME,
    comment="Conformed customer dimension - SCD Type 2 on address changes, SCD Type 1 on everything else.",
    table_properties={"quality": "gold"},
    schema="""
        customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone STRING,
        address_line1 STRING,
        city STRING,
        state_province STRING,
        postal_code STRING,
        country STRING,
        region STRING,
        credit_score INT,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP,
        updated_at TIMESTAMP,
        _ingested_at TIMESTAMP,
        _updated_at TIMESTAMP
    """,
)


@dp.temporary_view(name="customers_for_gold")
def customers_for_gold():
    # Overwrites Silver's own _updated_at with this layer's write time - Auto CDC merges
    # by column name, so passing Silver's value through unchanged here would silently mean
    # "Gold's _updated_at" and "Silver's _updated_at" are the same value, not each layer's own.
    #
    # Drops _driving_table: that column tags which of silver_customers.customers' two source
    # flows (from_crm/from_finance) produced a row - useful within Silver, meaningless once
    # Gold's Auto CDC has merged everything into one conformed dim_customer timeline. Also
    # keeps the source DataFrame's columns matching this table's explicit schema= below exactly
    # - an extra column Auto CDC didn't expect is a schema mismatch, not something to silently
    # carry through.
    return (
        spark.readStream.table(f"{catalog}.silver_customers.customers")
        .drop("_driving_table")
        .withColumn("_updated_at", current_timestamp())
    )


@dp.temporary_view(name="customers_backfill")
def customers_backfill():
    """Streaming (Auto CDC requires it - see this file's docstring), deduped to one row per
    customer_id via dropDuplicates - whichever row it keeps is fine, see this file's
    docstring for why. The one-time seed for the "backfill" Auto CDC flow below."""
    return (
        spark.readStream.table(f"{catalog}.silver_customers.customers")
        .dropDuplicates(["customer_id"])
        .drop("_driving_table")
        .withColumn("_updated_at", current_timestamp())
    )


dp.create_auto_cdc_flow(
    target=ENTITY_NAME,
    source="customers_for_gold",
    keys=["customer_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    track_history_column_list=TRACKED_COLUMNS,
    name="incremental",
)

dp.create_auto_cdc_flow(
    target=ENTITY_NAME,
    source="customers_backfill",
    keys=["customer_id"],
    # A literal expression, not a real column - sequence_by doesn't require one, and forcing
    # a hardcoded sentinel is the whole point: this flow's rows should sort BEFORE any
    # genuine updated_at value, however old.
    sequence_by=lit(SCD2_BACKFILL_START_AT).cast("timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=TRACKED_COLUMNS,
    once=True,
    name="backfill",
)
