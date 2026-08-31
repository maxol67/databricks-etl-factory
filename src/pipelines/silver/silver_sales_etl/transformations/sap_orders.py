"""Silver: validated, deduplicated SAP orders (see README.md's Target architecture and
CONVENTIONS.md for the project's schema-allocation rules - why this lives in `silver_sales`,
not a shared `silver` schema with a `sales_`-prefixed table).

Reads bronze_sap_etl's bronze.sap_orders cross-pipeline (fully-qualified name). Kept
separate from webshop_orders.py rather than unified into one `orders` table - that needs
a fan-in (dp.create_streaming_table + one @dp.append_flow per source), deliberately
deferred, see BACKLOG.md.

Uses Auto CDC (SCD Type 1, keyed on order_id): Bronze is append-only, so a corrected
order lands as a second row with the same order_id, not an in-place update - Auto CDC
keeps the LATEST row per key instead of whichever arrived first.

Two independent severities, both computed once on the shared "_evaluated" view:
- REJECT_REASON_EXPR (order_id/quantity/amount) EXCLUDES the row from silver_sales.sap_orders
  entirely - the row is unusable, not just unexpected.
- WARNING_REASON_EXPR (status) does NOT exclude the row - it still merges into
  silver_sales.sap_orders normally, same as this table's other rows.
Both severities land, side by side, in silver_data_quality.sap_orders_findings - one common
table for every check this pipeline runs, not two separate mechanisms. A row that's rejected
never also gets a separate "warning" finding for the same event, even if it would also have
failed the warning check - reject is the terminal, overriding severity (see
sap_orders_findings below: _severity/_finding_reason/_finding_reason_expr all resolve
reject-first, warning-only-as-fallback).

try_cast, not plain cast, is used for quantity/amount in BOTH the reject check and the final
select - a plain .cast("int") on an uncastable value (e.g. "five") silently produces NULL
instead of erroring, which - if the reject check only tested "quantity <= 0" - would let a
malformed row slip through BOTH the valid AND the rejected path: NULL <= 0 evaluates to
NULL, not TRUE, in SQL's three-valued logic. try_cast makes a failure-to-parse value resolve
to NULL deterministically, regardless of the session's ANSI SQL setting, so
`try_cast(quantity AS INT) IS NULL` catches it the same way an already-NULL source value is.

Every finding table in this repo keeps the same common, minimal shape (_driving_table,
_severity, _finding_reason, _finding_reason_expr, _row_data, _checked_at) rather than
mirroring its own source table's business columns. _severity is "rejected" or "warning";
_finding_reason is the classified outcome (e.g. "non_positive_quantity", "unknown_status");
_finding_reason_expr is the literal REJECT_REASON_EXPR/WARNING_REASON_EXPR text that produced
it, kept for audit purposes; _row_data captures the ENTIRE raw Bronze row (every column
present, whatever they are) as VARIANT via to_variant_object(struct(*cols)), computed from
the live DataFrame's own column list, not a hand-maintained one - so this table never needs
updating when sap_orders' own schema changes, and every finding table across every pipeline
is queryable the exact same way. See CONVENTIONS.md's Schema allocation rule.

product_id is a plain validated reference, like customer_id - NOT joined against
silver_products_etl's product table here. Order<->product is a fact<->dimension
relationship (Kimball sense), which belongs at Gold - see gold_sales_etl's fact_orders.py for
where that join actually happens - not a Silver-level join. quantity is that same kind of plain field -
Bronze/Silver don't validate amount against quantity * a per-product price (there's no
price on the product record at all - see silver_products_etl's own docstring), only that
each is independently positive.
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
ENTITY_NAME = "sap_orders"
VALID_STATUSES = ("pending", "completed", "cancelled", "refunded")
SAP_BRONZE_TABLE = "bronze.sap_orders"

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


@dp.temporary_view(name="sap_orders_evaluated")
def sap_orders_evaluated():
    return (
        spark.readStream.table(f"{catalog}.{SAP_BRONZE_TABLE}")
        .withColumn("_rejection_reason", expr(REJECT_REASON_EXPR))
        .withColumn("_warning_reason", expr(WARNING_REASON_EXPR))
    )


@dp.temporary_view(name="sap_orders_valid")
def sap_orders_valid():
    return (
        spark.readStream.table("sap_orders_evaluated")
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
    comment="Validated, deduplicated SAP orders - one row per order_id.",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target=ENTITY_NAME,
    source="sap_orders_valid",
    keys=["order_id"],
    sequence_by="_source_file_modified_at",
    stored_as_scd_type=1,
)


@dp.table(
    name=f"{catalog}.silver_data_quality.sap_orders_findings",
    comment="SAP order rows that failed a Silver check (rejected: excluded from "
    "silver_sales.sap_orders; warning: still merged there) - kept for manual review. "
    "_row_data holds the full raw Bronze row as VARIANT.",
    table_properties={"quality": "silver_data_quality"},
)
def sap_orders_findings():
    evaluated = spark.readStream.table("sap_orders_evaluated")
    raw_columns = [c for c in evaluated.columns if c not in ("_rejection_reason", "_warning_reason")]
    is_rejected = col("_rejection_reason").isNotNull()
    return evaluated.where("_rejection_reason IS NOT NULL OR _warning_reason IS NOT NULL").select(
        lit(SAP_BRONZE_TABLE).alias("_driving_table"),
        when(is_rejected, lit("rejected")).otherwise(lit("warning")).alias("_severity"),
        coalesce(col("_rejection_reason"), col("_warning_reason")).alias("_finding_reason"),
        when(is_rejected, lit(REJECT_REASON_EXPR)).otherwise(lit(WARNING_REASON_EXPR)).alias("_finding_reason_expr"),
        to_variant_object(struct(*raw_columns)).alias("_row_data"),
        current_timestamp().alias("_checked_at"),
    )
