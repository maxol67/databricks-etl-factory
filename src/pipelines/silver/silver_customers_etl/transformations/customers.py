"""Silver: validated customer change events, enriched with credit score (see README.md's
Target architecture and CONVENTIONS.md for the project's schema-allocation rules - why this
lives in `silver_customers`, not a shared `silver` schema with a `crm_`-prefixed table).

Deliberately does NOT deduplicate to one row per customer_id, unlike silver_sales_etl's
Silver tables. gold_shared_customers_etl's dim_customer (a separate pipeline - see that
pipeline's own comment for why Silver/Gold are split by layer) needs the FULL history of
change events to build its SCD Type 2 versions - collapsing to current-state-only here
(via Auto CDC or dropDuplicates) would permanently discard the intermediate values Gold
needs to reconstruct history. Silver's job for this table is only to validate, cast, and
enrich each incoming event; Gold is where dedup/versioning happens for this source.

Combines TWO independent Tier 1 sources - bronze_crm_etl's bronze.crm_customers and
bronze_finance_etl's bronze.finance_customer_credit_score - by JOIN, not fan-in: these
are different attributes of the same customer (not more rows), so a plain crm_customers
stream wouldn't be enough - a credit-score-only change (CRM unchanged) still needs to
produce its own Silver row so Gold's Auto CDC sees it and can version it. That needs TWO
flows into the same target, one driven by each source, each enriching itself with the
OTHER source's latest known value at that moment (a stream-static join, the same pattern
used elsewhere for stream-dimension enrichment) - not a single stream-stream join, which
would need watermarks and wouldn't by itself guarantee a credit-score-only change
surfaces on its own.

Each row carries a `_driving_table` column (the fully-qualified Bronze table whose event
produced this row - `bronze.crm_customers` or `bronze.finance_customer_credit_score`) - both
flows write into the same `customers` table, so without it there's no way to tell, after the
fact, which upstream event actually produced a given row (both CRM- and Finance-driven rows
carry the full CRM_COLUMNS + credit_score shape, since each enriches itself with the other
source's latest known value). Set from the same CRM_BRONZE_TABLE/FINANCE_BRONZE_TABLE
constants each flow already reads from, not typed as a second string literal, so the tag
can't silently drift from the table it actually names.

updated_at uses try_cast, not plain cast, in both the reject check and the final select -
same reasoning as sap_orders.py: a plain .cast("timestamp") on an unparseable value silently
returns NULL instead of erroring, and a reject condition that only checked nullness on the
RAW (pre-cast) column wouldn't catch that - the raw value isn't SQL NULL, it just doesn't
parse. try_cast makes a genuinely missing value and an unparseable one resolve to NULL the
same, deterministic way, regardless of the session's ANSI SQL setting.

Each driving stream is deduplicated on (customer_id, updated_at) right after casting -
confirmed live that re-running this pipeline can re-emit a driving stream's already-seen
rows as exact duplicates (observed on customers_from_crm specifically after a second run;
root cause not fully isolated). An identical (customer_id, updated_at) pair is never a
legitimate second event - two real changes always get two different updated_at values - so
this dedup is a safe no-op for genuine history and only removes reprocessing artifacts.

Two independent severities per source, mirroring sap_orders.py's design: *_REJECT_REASON_EXPR
excludes a row from silver_customers.customers entirely; *_WARNING_REASON_EXPR would flag a
row without excluding it. Neither CRM nor Finance has an active warning check today - both
constants evaluate to a plain NULL - but they're still declared and wired into their
_evaluated view/findings table the same way sap_orders.py's active WARNING_REASON_EXPR is, so
a future soft check on either source composes without restructuring anything.

Rows failing validation (missing customer_id, missing/unparseable updated_at, or - CRM only
- an unrecognized region) are NOT silently dropped: each source's invalid rows are routed to
their own silver_data_quality.<bronze_table>_findings table (crm_customers_findings /
finance_customer_credit_score_findings) instead, tagged with WHY - fully-qualified target
names since silver_data_quality is a different schema than this pipeline's own default
(silver_customers), see CONVENTIONS.md's Schema allocation rule for why that's a deliberate
cross-cutting exception, same shape as gold_shared. Every finding table in this repo keeps
the same common, minimal shape (_driving_table, _severity, _finding_reason,
_finding_reason_expr, _row_data, _checked_at) rather than mirroring its own source table's
business columns - _row_data captures the ENTIRE raw Bronze row as VARIANT via
to_variant_object(struct(*cols)), computed from the live DataFrame's own column list, not a
hand-maintained one, so it never needs updating when a source's own schema changes.
"""

from _util import latest_by
from pyspark import pipelines as dp
from pyspark.sql.functions import coalesce, col, current_timestamp, expr, lit, struct, to_variant_object, when

catalog = spark.conf.get("bundle.catalog")
ENTITY_NAME = "customers"
VALID_REGIONS = ("AMER", "EMEA", "APAC")

CRM_BRONZE_TABLE = "bronze.crm_customers"
FINANCE_BRONZE_TABLE = "bronze.finance_customer_credit_score"

CRM_COLUMNS = [
    "first_name",
    "last_name",
    "email",
    "phone",
    "address_line1",
    "city",
    "state_province",
    "postal_code",
    "country",
    "region",
]

CRM_REJECT_REASON_EXPR = (
    "CASE "
    "WHEN customer_id IS NULL THEN 'missing_customer_id' "
    "WHEN try_cast(updated_at AS TIMESTAMP) IS NULL THEN 'missing_or_invalid_updated_at' "
    f"WHEN region IS NULL OR region NOT IN {VALID_REGIONS} THEN 'invalid_region' "
    "END"
)
CRM_WARNING_REASON_EXPR = "CAST(NULL AS STRING)"

FINANCE_REJECT_REASON_EXPR = (
    "CASE "
    "WHEN customer_id IS NULL THEN 'missing_customer_id' "
    "WHEN try_cast(updated_at AS TIMESTAMP) IS NULL THEN 'missing_or_invalid_updated_at' "
    "END"
)
FINANCE_WARNING_REASON_EXPR = "CAST(NULL AS STRING)"


def _latest_crm():
    """Latest known CRM attributes per customer_id, as of right now - used as the static
    enrichment side when a credit-score change is the event driving a new Silver row."""
    return latest_by(spark.read.table(f"{catalog}.{CRM_BRONZE_TABLE}"), "customer_id", "updated_at").select(
        "customer_id", *CRM_COLUMNS
    )


def _latest_credit_score():
    """Latest known credit_score per customer_id, as of right now - used as the static
    enrichment side when a CRM change is the event driving a new Silver row."""
    return latest_by(
        spark.read.table(f"{catalog}.{FINANCE_BRONZE_TABLE}"),
        "customer_id",
        "updated_at",
    ).select(col("customer_id"), col("credit_score").cast("int"))


dp.create_streaming_table(
    name=ENTITY_NAME,
    comment="Validated customer change events, enriched with credit score - may contain multiple "
    "rows per customer_id. _driving_table identifies which Bronze table's event produced each row.",
    table_properties={"quality": "silver"},
)


@dp.temporary_view(name="crm_customers_evaluated")
def crm_customers_evaluated():
    return (
        spark.readStream.table(f"{catalog}.{CRM_BRONZE_TABLE}")
        .withColumn("_rejection_reason", expr(CRM_REJECT_REASON_EXPR))
        .withColumn("_warning_reason", expr(CRM_WARNING_REASON_EXPR))
    )


@dp.temporary_view(name="finance_customer_credit_score_evaluated")
def finance_customer_credit_score_evaluated():
    return (
        spark.readStream.table(f"{catalog}.{FINANCE_BRONZE_TABLE}")
        .withColumn("_rejection_reason", expr(FINANCE_REJECT_REASON_EXPR))
        .withColumn("_warning_reason", expr(FINANCE_WARNING_REASON_EXPR))
    )


@dp.append_flow(target=ENTITY_NAME, name="from_crm")
def customers_from_crm():
    crm = (
        spark.readStream.table("crm_customers_evaluated")
        .where("_rejection_reason IS NULL")
        .select(
            col("customer_id").cast("string"),
            *[col(c).cast("string") for c in CRM_COLUMNS],
            expr("try_cast(updated_at AS TIMESTAMP)").alias("updated_at"),
            col("_ingested_at"),
        )
        .dropDuplicates(["customer_id", "updated_at"])
    )
    credit = _latest_credit_score()
    return (
        crm.join(credit, "customer_id", "left")
        .select("customer_id", *CRM_COLUMNS, "credit_score", "updated_at", "_ingested_at")
        .withColumn("_driving_table", lit(CRM_BRONZE_TABLE))
        .withColumn("_updated_at", current_timestamp())
    )


@dp.append_flow(target=ENTITY_NAME, name="from_finance")
def customers_from_finance():
    credit = (
        spark.readStream.table("finance_customer_credit_score_evaluated")
        .where("_rejection_reason IS NULL")
        .select(
            col("customer_id").cast("string"),
            col("credit_score").cast("int"),
            expr("try_cast(updated_at AS TIMESTAMP)").alias("updated_at"),
            col("_ingested_at"),
        )
        .dropDuplicates(["customer_id", "updated_at"])
    )
    crm = _latest_crm()
    return (
        credit.join(crm, "customer_id", "left")
        .select("customer_id", *CRM_COLUMNS, "credit_score", "updated_at", "_ingested_at")
        .withColumn("_driving_table", lit(FINANCE_BRONZE_TABLE))
        .withColumn("_updated_at", current_timestamp())
    )


@dp.table(
    name=f"{catalog}.silver_data_quality.crm_customers_findings",
    comment="CRM customer rows that failed a Silver check (rejected: excluded from "
    "silver_customers.customers; warning: still merged there) - kept for manual review. "
    "_row_data holds the full raw Bronze row as VARIANT.",
    table_properties={"quality": "silver_data_quality"},
)
def crm_customers_findings():
    evaluated = spark.readStream.table("crm_customers_evaluated")
    raw_columns = [c for c in evaluated.columns if c not in ("_rejection_reason", "_warning_reason")]
    is_rejected = col("_rejection_reason").isNotNull()
    return evaluated.where("_rejection_reason IS NOT NULL OR _warning_reason IS NOT NULL").select(
        lit(CRM_BRONZE_TABLE).alias("_driving_table"),
        when(is_rejected, lit("rejected")).otherwise(lit("warning")).alias("_severity"),
        coalesce(col("_rejection_reason"), col("_warning_reason")).alias("_finding_reason"),
        when(is_rejected, lit(CRM_REJECT_REASON_EXPR))
        .otherwise(lit(CRM_WARNING_REASON_EXPR))
        .alias("_finding_reason_expr"),
        to_variant_object(struct(*raw_columns)).alias("_row_data"),
        current_timestamp().alias("_checked_at"),
    )


@dp.table(
    name=f"{catalog}.silver_data_quality.finance_customer_credit_score_findings",
    comment="Finance credit-score rows that failed a Silver check (rejected: excluded from "
    "silver_customers.customers; warning: still merged there) - kept for manual review. "
    "_row_data holds the full raw Bronze row as VARIANT.",
    table_properties={"quality": "silver_data_quality"},
)
def finance_customer_credit_score_findings():
    evaluated = spark.readStream.table("finance_customer_credit_score_evaluated")
    raw_columns = [c for c in evaluated.columns if c not in ("_rejection_reason", "_warning_reason")]
    is_rejected = col("_rejection_reason").isNotNull()
    return evaluated.where("_rejection_reason IS NOT NULL OR _warning_reason IS NOT NULL").select(
        lit(FINANCE_BRONZE_TABLE).alias("_driving_table"),
        when(is_rejected, lit("rejected")).otherwise(lit("warning")).alias("_severity"),
        coalesce(col("_rejection_reason"), col("_warning_reason")).alias("_finding_reason"),
        when(is_rejected, lit(FINANCE_REJECT_REASON_EXPR))
        .otherwise(lit(FINANCE_WARNING_REASON_EXPR))
        .alias("_finding_reason_expr"),
        to_variant_object(struct(*raw_columns)).alias("_row_data"),
        current_timestamp().alias("_checked_at"),
    )
