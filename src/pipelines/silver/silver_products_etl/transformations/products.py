"""Silver: validated product change events, enriched with reference price (see README.md's
Target architecture and CONVENTIONS.md for the project's schema-allocation rules - why this
lives in `silver_products`, its own subject area, not folded into `silver_sales` or
`silver_customers`).

Deliberately does NOT deduplicate to one row per product_id, unlike silver_sales_etl's
Silver tables. gold_shared_products_etl's dim_product (a separate pipeline - see that pipeline's
own comment for why Silver/Gold are split by layer) needs the FULL history of change
events to build its SCD Type 2 versions - collapsing to current-state-only here (via Auto
CDC or dropDuplicates) would permanently discard the intermediate values Gold needs to
reconstruct history. Silver's job for this table is only to validate, cast, and enrich
each incoming event; Gold is where dedup/versioning happens for this source.

Combines TWO independent Tier 1 sources - bronze_mdm_etl's bronze.mdm_products (product
master attributes) and bronze_prodman_etl's bronze.prodman_product_pricing (reference
price, from Product Management) - by JOIN, not fan-in: these are different attributes of
the same product (not more rows), so a plain mdm_products stream wouldn't be enough - a
price-only change (MDM attributes unchanged) still needs to produce its own Silver row so
Gold's Auto CDC sees it and can version it. That needs TWO flows into the same target, one
driven by each source, each enriching itself with the OTHER source's latest known value at
that moment (a stream-static join, the same pattern silver_customers_etl's customers.py
uses) - not a single stream-stream join, which would need watermarks and wouldn't by
itself guarantee a price-only change surfaces on its own.

Each row carries a `_driving_table` column (the fully-qualified Bronze table whose event
produced this row - `bronze.mdm_products` or `bronze.prodman_product_pricing`) - both flows
write into the same `products` table, so without it there's no way to tell, after the fact,
which upstream event actually produced a given row (both MDM- and prodman-driven rows carry
the full product_name/category/subcategory/reference_price shape, since each enriches itself
with the other source's latest known value). Set from the same MDM_BRONZE_TABLE/
PRODMAN_BRONZE_TABLE constants each flow already reads from, not typed as a second string
literal, so the tag can't silently drift from the table it actually names.

No shared business timestamp across both sources: bronze_prodman_etl's feed carries only
product_id and reference_price, no updated_at of its own (unlike MDM, which has one) - so
_source_file_modified_at (file-arrival metadata, already present on every Bronze table) is
used uniformly as the cross-flow sequencing timestamp instead, the same fallback
silver_sales_etl's order tables use for the identical reason (no source-native timestamp).

Each driving stream is deduplicated on (product_id, _source_file_modified_at) right after
casting - same defensive measure as silver_customers_etl's customers.py takes against
duplicate-row reprocessing artifacts (see that file's docstring); not independently
confirmed live for this pipeline, applied here as a precaution since the risk is the same
shape (Lakeflow re-running and re-emitting an already-seen row from a driving stream).

Two severities per source, mirroring sap_orders.py's design: REJECT_REASON_EXPR (missing
product_id) excludes the row entirely; WARNING_REASON_EXPR would flag a row without
excluding it. Neither MDM nor prodman has an active warning check today - it evaluates to a
plain NULL - but it's still declared and wired into the shared _evaluated view/findings table
the same way sap_orders.py's active WARNING_REASON_EXPR is, so a future soft check composes
without restructuring anything. product_id only casts to STRING, which never fails to parse,
so - unlike sap_orders.py's/customers.py's numeric/timestamp fields - there's no
try_cast/silent-NULL risk to guard against here.

Rows failing validation (missing product_id) are NOT silently dropped: each source's
invalid rows are routed to their own silver_data_quality.<bronze_table>_findings table
(mdm_products_findings / prodman_product_pricing_findings) instead, tagged with WHY -
fully-qualified target names since silver_data_quality is a different schema than this
pipeline's own default (silver_products), see CONVENTIONS.md's Schema allocation rule for
why that's a deliberate cross-cutting exception, same shape as gold_shared.

Every finding table in this repo keeps the same common, minimal shape (_driving_table,
_severity, _finding_reason, _finding_reason_expr, _row_data, _checked_at) rather than
mirroring its own source table's business columns - _row_data captures the ENTIRE raw
Bronze row as VARIANT via to_variant_object(struct(*cols)), computed from the live
DataFrame's own column list, not a hand-maintained one, so it never needs updating when a
source's own schema changes.
"""

from _util import latest_by
from pyspark import pipelines as dp
from pyspark.sql.functions import coalesce, col, current_timestamp, expr, lit, struct, to_variant_object, when

catalog = spark.conf.get("bundle.catalog")
ENTITY_NAME = "products"

MDM_BRONZE_TABLE = "bronze.mdm_products"
PRODMAN_BRONZE_TABLE = "bronze.prodman_product_pricing"

REJECT_REASON_EXPR = "CASE WHEN product_id IS NULL THEN 'missing_product_id' END"
WARNING_REASON_EXPR = "CAST(NULL AS STRING)"


def _latest_mdm_product():
    """Latest known MDM attributes per product_id, as of right now - used as the static
    enrichment side when a price change is the event driving a new Silver row."""
    return latest_by(
        spark.read.table(f"{catalog}.{MDM_BRONZE_TABLE}"), "product_id", "_source_file_modified_at"
    ).select("product_id", "product_name", "category", "subcategory")


def _latest_reference_price():
    """Latest known reference_price per product_id, as of right now - used as the static
    enrichment side when an MDM attribute change is the event driving a new Silver row."""
    return latest_by(
        spark.read.table(f"{catalog}.{PRODMAN_BRONZE_TABLE}"),
        "product_id",
        "_source_file_modified_at",
    ).select(col("product_id"), col("reference_price").cast("double"))


dp.create_streaming_table(
    name=ENTITY_NAME,
    comment="Validated product change events, enriched with reference price - may contain multiple "
    "rows per product_id. _driving_table identifies which Bronze table's event produced each row.",
    table_properties={"quality": "silver"},
)


@dp.temporary_view(name="mdm_products_evaluated")
def mdm_products_evaluated():
    return (
        spark.readStream.table(f"{catalog}.{MDM_BRONZE_TABLE}")
        .withColumn("_rejection_reason", expr(REJECT_REASON_EXPR))
        .withColumn("_warning_reason", expr(WARNING_REASON_EXPR))
    )


@dp.temporary_view(name="prodman_product_pricing_evaluated")
def prodman_product_pricing_evaluated():
    return (
        spark.readStream.table(f"{catalog}.{PRODMAN_BRONZE_TABLE}")
        .withColumn("_rejection_reason", expr(REJECT_REASON_EXPR))
        .withColumn("_warning_reason", expr(WARNING_REASON_EXPR))
    )


@dp.append_flow(target=ENTITY_NAME, name="from_mdm")
def products_from_mdm():
    mdm = (
        spark.readStream.table("mdm_products_evaluated")
        .where("_rejection_reason IS NULL")
        .select(
            col("product_id").cast("string"),
            col("product_name").cast("string"),
            col("category").cast("string"),
            col("subcategory").cast("string"),
            col("_source_file_modified_at"),
            col("_ingested_at"),
        )
        .dropDuplicates(["product_id", "_source_file_modified_at"])
    )
    price = _latest_reference_price()
    return (
        mdm.join(price, "product_id", "left")
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "reference_price",
            "_source_file_modified_at",
            "_ingested_at",
        )
        .withColumn("_driving_table", lit(MDM_BRONZE_TABLE))
        .withColumn("_updated_at", current_timestamp())
    )


@dp.append_flow(target=ENTITY_NAME, name="from_prodman")
def products_from_prodman():
    pricing = (
        spark.readStream.table("prodman_product_pricing_evaluated")
        .where("_rejection_reason IS NULL")
        .select(
            col("product_id").cast("string"),
            col("reference_price").cast("double"),
            col("_source_file_modified_at"),
            col("_ingested_at"),
        )
        .dropDuplicates(["product_id", "_source_file_modified_at"])
    )
    mdm = _latest_mdm_product()
    return (
        pricing.join(mdm, "product_id", "left")
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "reference_price",
            "_source_file_modified_at",
            "_ingested_at",
        )
        .withColumn("_driving_table", lit(PRODMAN_BRONZE_TABLE))
        .withColumn("_updated_at", current_timestamp())
    )


@dp.table(
    name=f"{catalog}.silver_data_quality.mdm_products_findings",
    comment="MDM product rows that failed a Silver check (rejected: excluded from "
    "silver_products.products; warning: still merged there) - kept for manual review. "
    "_row_data holds the full raw Bronze row as VARIANT.",
    table_properties={"quality": "silver_data_quality"},
)
def mdm_products_findings():
    evaluated = spark.readStream.table("mdm_products_evaluated")
    raw_columns = [c for c in evaluated.columns if c not in ("_rejection_reason", "_warning_reason")]
    is_rejected = col("_rejection_reason").isNotNull()
    return evaluated.where("_rejection_reason IS NOT NULL OR _warning_reason IS NOT NULL").select(
        lit(MDM_BRONZE_TABLE).alias("_driving_table"),
        when(is_rejected, lit("rejected")).otherwise(lit("warning")).alias("_severity"),
        coalesce(col("_rejection_reason"), col("_warning_reason")).alias("_finding_reason"),
        when(is_rejected, lit(REJECT_REASON_EXPR)).otherwise(lit(WARNING_REASON_EXPR)).alias("_finding_reason_expr"),
        to_variant_object(struct(*raw_columns)).alias("_row_data"),
        current_timestamp().alias("_checked_at"),
    )


@dp.table(
    name=f"{catalog}.silver_data_quality.prodman_product_pricing_findings",
    comment="Prodman reference-price rows that failed a Silver check (rejected: excluded from "
    "silver_products.products; warning: still merged there) - kept for manual review. "
    "_row_data holds the full raw Bronze row as VARIANT.",
    table_properties={"quality": "silver_data_quality"},
)
def prodman_product_pricing_findings():
    evaluated = spark.readStream.table("prodman_product_pricing_evaluated")
    raw_columns = [c for c in evaluated.columns if c not in ("_rejection_reason", "_warning_reason")]
    is_rejected = col("_rejection_reason").isNotNull()
    return evaluated.where("_rejection_reason IS NOT NULL OR _warning_reason IS NOT NULL").select(
        lit(PRODMAN_BRONZE_TABLE).alias("_driving_table"),
        when(is_rejected, lit("rejected")).otherwise(lit("warning")).alias("_severity"),
        coalesce(col("_rejection_reason"), col("_warning_reason")).alias("_finding_reason"),
        when(is_rejected, lit(REJECT_REASON_EXPR)).otherwise(lit(WARNING_REASON_EXPR)).alias("_finding_reason_expr"),
        to_variant_object(struct(*raw_columns)).alias("_row_data"),
        current_timestamp().alias("_checked_at"),
    )
