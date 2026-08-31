"""Bronze: raw customer credit scores as landed in the Internal Drop zone by Finance (see
AGENTS.md for the project's schema-allocation rules).

Lands in Internal Drop, not Staging or System Drop: Finance is an internal
department/system, and this is a different attribute of the same customer entity
bronze_crm_etl's crm_customers.py lands - a different source system, not a different
entity, hence its own Tier 1 pipeline rather than folding into bronze_crm_etl (see
silver_customers_etl.pipeline.yml for how Tier 2 joins the two). Top-level folder is the
source name (`finance/`), per this project's Staging/Drop convention, with a
`customer_credit_score/` subfolder for this specific object.

No validation performed here - Bronze preserves the source verbatim. `finance_` is this
table's source-system abbreviation.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_NAME = "finance"
ENTITY_NAME = "customer_credit_score"

catalog = spark.conf.get("bundle.catalog")
drop_path = f"/Volumes/{catalog}/drop/internal_drop/{SOURCE_NAME}/{ENTITY_NAME}/"
schema_location = f"/Volumes/{catalog}/drop/internal_drop/_schemas/{SOURCE_NAME}_{ENTITY_NAME}_bronze/"


@dp.table(
    name=f"{SOURCE_NAME}_{ENTITY_NAME}",
    comment="Raw customer credit scores exactly as landed in the Internal Drop volume by Finance. No validation performed.",
    table_properties={"quality": "bronze"},
)
def finance_customer_credit_score():
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
