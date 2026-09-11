"""Bronze: raw customer records as landed in the System Drop zone by CRM (see AGENTS.md for
the project's schema-allocation rules).

Lands in System Drop, not Staging: CRM is another system pushing files itself (no external
ETL tool involved, no Lakeflow Connect connector) - same reasoning as bronze_webshop_etl's
webshop_orders.py.
Top-level folder is the source name (`crm/`), per this project's Staging/Drop convention.

No validation performed here - Bronze preserves the source verbatim. `crm_` is this
table's source-system abbreviation.

drop_path/schema_location come from config/source_environment.yml instead of being built from
literals here - see CONVENTIONS.md's "Source registry and source x environment connection
config" section for why, and how pipeline code reads the file via bundle.workspace_file_path.
Paths are constructed dynamically as: base_path/{SOURCE_NAME}/{ENTITY_NAME}/ - this pattern
allows the same source to deliver multiple entities without YAML changes.
"""

import yaml
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_NAME = "crm"
ENTITY_NAME = "customers"

catalog = spark.conf.get("bundle.catalog")
target = spark.conf.get("bundle.target")
workspace_file_path = spark.conf.get("bundle.workspace_file_path")

with open(f"{workspace_file_path}/config/source_environment.yml") as f:
    connection = yaml.safe_load(f)[SOURCE_NAME][target]

# Construct full paths dynamically using SOURCE_NAME + ENTITY_NAME
base_drop_path = connection["drop_path"].format(catalog=catalog)
base_schema_location = connection["schema_location"].format(catalog=catalog)

drop_path = f"{base_drop_path}/{SOURCE_NAME}/{ENTITY_NAME}/"
schema_location = f"{base_schema_location}/{SOURCE_NAME}_{ENTITY_NAME}_bronze/"


@dp.table(
    name=f"{SOURCE_NAME}_{ENTITY_NAME}",
    comment="Raw customer records exactly as landed in the System Drop volume by CRM. No validation performed.",
    table_properties={"quality": "bronze"},
)
def crm_customers():
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
