"""Silver: validated, deduplicated webshop orders (see README.md's Target architecture
and CONVENTIONS.md for the project's schema-allocation rules - why this lives in
`silver_sales`, not a shared `silver` schema with a `sales_`-prefixed table).

Reads bronze_webshop_etl's bronze.webshop_orders cross-pipeline (fully-qualified name, plain
streaming table read). Kept separate from sap_orders.py rather than unified into one
`orders` table - see that file's docstring for why (deferred fan-in, see BACKLOG.md).

Uses Auto CDC (SCD Type 1, keyed on order_id) for the same reason as sap_orders.py.

Two independent severities (reject: order_id/quantity/amount, excludes the row; warning:
status, doesn't exclude it), both landing in silver_data_quality.webshop_orders_findings - see
sap_orders.py's docstring for the full reasoning (try_cast for the NULL-on-cast-failure
problem, reject-overrides-warning precedence, why this isn't two separate mechanisms).

Every finding table in this repo keeps the same common, minimal shape (_driving_table,
_severity, _finding_reason, _finding_reason_expr, _row_data, _checked_at) - see
sap_orders.py's docstring.

product_id and quantity are plain validated fields, like customer_id - see sap_orders.py's
docstring for why product_id isn't joined against silver_products_etl here, and why amount
isn't validated against quantity * a per-product price.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    expr,
    lit,
    struct,
    to_date,
    to_variant_object,
    when,
)

catalog = spark.conf.get("bundle.catalog")
ENTITY_NAME = "webshop_orders"
VALID_STATUSES = ("pending", "completed", "cancelled", "refunded")
WEBSHOP_BRONZE_TABLE = "bronze.webshop_orders"

REJECT_REASON_EXPR = (
    "CASE "
    "WHEN order_id IS NULL THEN 'missing_order_id' "
    "WHEN try_cast(quantity AS INT) IS NULL THEN 'missing_or_invalid_quantity' "
    "WHEN try_cast(quantity AS INT) <= 0 THEN 'non_positive_quantity' "
    "WHEN try_cast(amount AS DOUBLE) IS NULL THEN 'missing_or_invalid_amount' "
    "WHEN try_cast(amount AS DOUBLE) <= 0 THEN 'non_positive_amount' "
    "END"
)

WARNING_REASON_EXPR = f"CASE WHEN status NOT IN {VALID_STATUSES} THEN 'unknown_status' END"


@dp.temporary_view(name="webshop_orders_evaluated")
def webshop_orders_evaluated():
    return (
        spark.readStream.table(f"{catalog}.{WEBSHOP_BRONZE_TABLE}")
        .withColumn("_rejection_reason", expr(REJECT_REASON_EXPR))
        .withColumn("_warning_reason", expr(WARNING_REASON_EXPR))
    )


@dp.temporary_view(name="webshop_orders_valid")
def webshop_orders_valid():
    return (
        spark.readStream.table("webshop_orders_evaluated")
        .where("_rejection_reason IS NULL")
        .select(
            col("order_id").cast("string"),
            col("customer_id").cast("string"),
            col("product_id").cast("string"),
            to_date(col("order_date")).alias("order_date"),
            expr("try_cast(quantity AS INT)").alias("quantity"),
            expr("try_cast(amount AS DOUBLE)").alias("amount"),
            col("status").cast("string"),
            col("shipping_region").cast("string"),
            col("_source_file"),
            col("_source_file_modified_at"),
            col("_ingested_at"),
            current_timestamp().alias("_updated_at"),
        )
    )


dp.create_streaming_table(
    name=ENTITY_NAME,
    comment="Validated, deduplicated webshop orders - one row per order_id.",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target=ENTITY_NAME,
    source="webshop_orders_valid",
    keys=["order_id"],
    sequence_by="_source_file_modified_at",
    stored_as_scd_type=1,
)


@dp.table(
    name=f"{catalog}.silver_data_quality.webshop_orders_findings",
    comment="Webshop order rows that failed a Silver check (rejected: excluded from "
    "silver_sales.webshop_orders; warning: still merged there) - kept for manual review. "
    "_row_data holds the full raw Bronze row as VARIANT.",
    table_properties={"quality": "silver_data_quality"},
)
def webshop_orders_findings():
    evaluated = spark.readStream.table("webshop_orders_evaluated")
    raw_columns = [c for c in evaluated.columns if c not in ("_rejection_reason", "_warning_reason")]
    is_rejected = col("_rejection_reason").isNotNull()
    return evaluated.where("_rejection_reason IS NOT NULL OR _warning_reason IS NOT NULL").select(
        lit(WEBSHOP_BRONZE_TABLE).alias("_driving_table"),
        when(is_rejected, lit("rejected")).otherwise(lit("warning")).alias("_severity"),
        coalesce(col("_rejection_reason"), col("_warning_reason")).alias("_finding_reason"),
        when(is_rejected, lit(REJECT_REASON_EXPR)).otherwise(lit(WARNING_REASON_EXPR)).alias("_finding_reason_expr"),
        to_variant_object(struct(*raw_columns)).alias("_row_data"),
        current_timestamp().alias("_checked_at"),
    )
