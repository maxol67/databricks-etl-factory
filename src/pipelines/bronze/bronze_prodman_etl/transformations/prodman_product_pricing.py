"""Bronze: raw product reference-price recommendations as landed in the Internal Drop zone
by Product Management (prodman) (see AGENTS.md for the project's schema-allocation rules).

Lands in Internal Drop, not Staging or System Drop: Product Management is an internal
department/system, and this is a different attribute of the same product entity
bronze_mdm_etl's mdm_products.py lands - a different source system, not a different
entity, hence its own Tier 1 pipeline rather than folding into bronze_mdm_etl (see
silver_products_etl.pipeline.yml for how Tier 2 joins the two). Top-level folder is the
source name (`prodman/`), per this project's Staging/Drop convention, with a
`product_pricing/` subfolder for this specific object.

No validation performed here - Bronze preserves the source verbatim. `prodman_` is this
table's source-system abbreviation. Deliberately minimal schema (product_id,
reference_price only, no timestamp of its own) - see silver_products_etl.pipeline.yml for
how Silver copes with that.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_NAME = "prodman"
ENTITY_NAME = "product_pricing"

catalog = spark.conf.get("bundle.catalog")
drop_path = f"/Volumes/{catalog}/drop/internal_drop/{SOURCE_NAME}/{ENTITY_NAME}/"
schema_location = f"/Volumes/{catalog}/drop/internal_drop/_schemas/{SOURCE_NAME}_{ENTITY_NAME}_bronze/"


@dp.table(
    name=f"{SOURCE_NAME}_{ENTITY_NAME}",
    comment="Raw product reference-price recommendations exactly as landed in the Internal Drop volume by Product Management. No validation performed.",
    table_properties={"quality": "bronze"},
)
def prodman_product_pricing():
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
