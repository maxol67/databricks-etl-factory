"""Bronze: raw product master data as landed in Staging by MDM (see README.md's Target
architecture for what Staging is, and AGENTS.md for the project's schema-allocation rules).

Staging, not a Drop zone: MDM has no native Lakeflow Connect connector, so this is exactly
what Staging exists for (an external cloud-native or on-prem ETL/orchestration tool landing
raw files) - same reasoning as bronze_sap_etl's sap_orders.py.

No validation performed here - Bronze preserves the source verbatim. `mdm_` is this
table's source-system abbreviation.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_NAME = "mdm"
ENTITY_NAME = "products"

catalog = spark.conf.get("bundle.catalog")
staging_path = f"/Volumes/{catalog}/staging/staging/{SOURCE_NAME}/{ENTITY_NAME}/"
schema_location = f"/Volumes/{catalog}/staging/staging/_schemas/{SOURCE_NAME}_{ENTITY_NAME}_bronze/"


@dp.table(
    name=f"{SOURCE_NAME}_{ENTITY_NAME}",
    comment="Raw product master data exactly as landed in Staging by MDM. No validation performed.",
    table_properties={"quality": "bronze"},
)
def mdm_products():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(staging_path)
        .select(
            "*",
            col("_metadata.file_path").alias("_source_file"),
            col("_metadata.file_modification_time").alias("_source_file_modified_at"),
            current_timestamp().alias("_ingested_at"),
        )
    )
