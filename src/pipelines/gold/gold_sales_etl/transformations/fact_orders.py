"""Gold: order fact table, one row per order (see CONVENTIONS.md's Dimensional modeling
naming for the fact/dimension naming split, and Schema allocation rule for why this lives in
its own gold_sales schema rather than gold_shared - a fact, unlike dim_customer/dim_product,
is domain-specific, not a conformed cross-domain dimension).

Unifies bronze_sap_etl's and bronze_webshop_etl's two separate Silver order tables
(silver_sales.sap_orders, silver_sales.webshop_orders) into ONE fact - the fan-in
silver_sales_etl itself deliberately deferred (see that pipeline's own docstring) finally
happens here, at Gold, where conforming multiple sources into one shape is exactly the job.
order_source tags which one a row came from ("sap"/"webshop") - a genuine business attribute
(sales channel), not a technical provenance tag like Silver's _driving_table (see
CONVENTIONS.md's Schema allocation rule for why _driving_table itself is Silver-only and
dropped before Gold - a related but distinct concept from this one).

Batch reads throughout (spark.read.table, no STREAM): silver_sales.sap_orders/webshop_orders
are themselves Auto CDC SCD Type 1 merge targets (in-place updates, not append-only), so a
plain streaming read over them isn't valid without skipChangeCommits - and skipChangeCommits
would silently mean a corrected order never reaches this fact, which is exactly the kind of
silent drop this whole project has gone out of its way to avoid elsewhere (see
silver_data_quality's findings tables). A Materialized View sidesteps the problem entirely:
every refresh re-reads current state directly, no streaming-over-merged-table caveat at all.

Carries BOTH the surrogate key and the natural key for each dimension it references
(customer_key + customer_id, product_key + product_id), not the surrogate key alone - per
CONVENTIONS.md's Dimensional modeling naming rule: if the dimension table or the key-lookup
logic itself is ever wrong (a bad join, a corrupted dim_customer, a key-generation bug), the
natural key on the fact is what lets a surrogate key be recomputed/verified from scratch -
losing it would mean the fact's link to its dimensions is only ever as trustworthy as
whatever surrogate key happened to get attached at write time, with no way to check or
recover it afterward.

AS-OF JOIN against both dim_product's and dim_customer's FULL SCD Type 2 history (not just
the current row) - see the databricks-pipelines skill's scd-2-querying reference,
"canonical for revenue-correct gold": each order gets the product/customer attributes that
were actually true AT its own order_date, not today's. LEFT JOIN, not INNER - an order whose
order_date predates every known dimension version still appears in fact_orders, with a NULL
product_key/customer_key, rather than being silently dropped from the fact entirely. This is
a real, not just theoretical, possibility with this repo's own sample data: dim_product's
__START_AT tracks _source_file_modified_at (upload time), not a historical value, while
sample orders are backdated up to 60 days (see sample_data/generate_sample_data.py) - so
plenty of orders will have no dim_product version old enough to match, and will legitimately
carry a NULL product_key until the dimension accumulates more history. order_date is a DATE
(no time-of-day), compared against __START_AT/__END_AT's TIMESTAMP precision - an order and a
same-day dimension version change can therefore match the wrong side of midnight; a known,
inherent limitation of the source data's own grain, not something fixable in this fact table.

_source_file/_source_file_modified_at (Bronze-arrival file metadata) are deliberately NOT
carried into this fact - Gold publishes a cleaner, business-facing shape; _ingested_at/
_updated_at (passed through/recomputed the same way as every other Gold table, see
CONVENTIONS.md's Standard governance metadata columns) are what's kept for traceability.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit

catalog = spark.conf.get("bundle.catalog")


def _all_orders():
    """SAP + webshop orders, unioned into one shape - each already one row per order_id
    (Silver's own Auto CDC dedup), tagged with which channel it came from."""
    sap = spark.read.table(f"{catalog}.silver_sales.sap_orders").withColumn("order_source", lit("sap"))
    webshop = spark.read.table(f"{catalog}.silver_sales.webshop_orders").withColumn("order_source", lit("webshop"))
    return sap.unionByName(webshop)


@dp.materialized_view(
    name="fact_orders",
    comment="One row per order (SAP + webshop unified), enriched with the product/customer "
    "dimension versions that were current as of the order's own order_date. Carries both "
    "surrogate and natural keys for each dimension - see CONVENTIONS.md's Dimensional "
    "modeling naming rule for why.",
    table_properties={"quality": "gold"},
)
def fact_orders():
    orders = _all_orders()
    products = spark.read.table(f"{catalog}.gold_shared.dim_product")
    customers = spark.read.table(f"{catalog}.gold_shared.dim_customer")

    as_of_product = (
        (orders["product_id"] == products["product_id"])
        & (orders["order_date"] >= products["__START_AT"])
        & ((orders["order_date"] < products["__END_AT"]) | products["__END_AT"].isNull())
    )
    with_product = orders.join(products, as_of_product, "left").select(orders["*"], products["product_key"])

    as_of_customer = (
        (with_product["customer_id"] == customers["customer_id"])
        & (with_product["order_date"] >= customers["__START_AT"])
        & ((with_product["order_date"] < customers["__END_AT"]) | customers["__END_AT"].isNull())
    )
    with_customer = with_product.join(customers, as_of_customer, "left").select(
        with_product["*"], customers["customer_key"]
    )

    return with_customer.select(
        "order_id",
        "order_source",
        "customer_key",
        "customer_id",
        "product_key",
        "product_id",
        "order_date",
        "quantity",
        "amount",
        "status",
        "shipping_region",
        "_ingested_at",
        current_timestamp().alias("_updated_at"),
    )
