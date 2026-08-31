"""Bronze: raw orders as landed in Staging by SAP (see README.md's Target architecture
for what Staging is, and AGENTS.md for the project's schema-allocation rules).

Staging, not a Drop zone: SAP has no native Lakeflow Connect connector, so this is exactly
what Staging exists for (an external cloud-native or on-prem ETL/orchestration tool landing
raw files). Top-level folder is the source name (`sap/`), per this project's Staging/Drop convention -
see bronze_webshop_etl's webshop_orders.py for the same orders concept landing via a
different source system's own top-level folder.

No validation performed here - Bronze preserves the source verbatim. `sap_` is this
table's source-system abbreviation.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_NAME = "sap"
ENTITY_NAME = "orders"

catalog = spark.conf.get("bundle.catalog")
staging_path = f"/Volumes/{catalog}/staging/staging/{SOURCE_NAME}/{ENTITY_NAME}/"
schema_location = f"/Volumes/{catalog}/staging/staging/_schemas/{SOURCE_NAME}_{ENTITY_NAME}_bronze/"


@dp.table(
    name=f"{SOURCE_NAME}_{ENTITY_NAME}",
    comment="Raw orders exactly as landed in Staging by SAP. No validation performed.",
    table_properties={"quality": "bronze"},
)
def sap_orders():
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
