"""Bronze: raw orders as landed in Staging by SAP (see README.md's Target architecture
for what Staging is, and AGENTS.md for the project's schema-allocation rules).

Staging, not a Drop zone: SAP has no native Lakeflow Connect connector, so this is exactly
what Staging exists for (an external cloud-native or on-prem ETL/orchestration tool landing
raw files). Top-level folder is the source name (`sap/`), per this project's Staging/Drop convention -
see bronze_webshop_etl's webshop_orders.py for the same orders concept landing via a
different source system's own top-level folder.

No validation performed here - Bronze preserves the source verbatim. `sap_` is this
table's source-system abbreviation.

staging_path/schema_location come from config/source_environment.yml instead of being built
from literals here - see CONVENTIONS.md's "Source registry and source x environment connection
config" section for why, and how pipeline code reads the file via bundle.workspace_file_path.
"""

import yaml
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_NAME = "sap"
ENTITY_NAME = "orders"

catalog = spark.conf.get("bundle.catalog")
target = spark.conf.get("bundle.target")
workspace_file_path = spark.conf.get("bundle.workspace_file_path")

with open(f"{workspace_file_path}/config/source_environment.yml") as f:
    connection = yaml.safe_load(f)[SOURCE_NAME][target]

staging_path = connection["staging_path"].format(catalog=catalog)
schema_location = connection["schema_location"].format(catalog=catalog)


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
