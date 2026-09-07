"""Bronze: raw orders as landed in the System Drop zone by the webshop (see AGENTS.md for
the project's schema-allocation rules).

Lands in System Drop, not Staging: the webshop is another system pushing files itself (no
external ETL tool involved, no Lakeflow Connect connector) - same reasoning as
bronze_crm_etl's crm_customers.py.
Top-level folder is the source name (`webshop/`), per this project's Staging/Drop
convention - see bronze_sap_etl's sap_orders.py for the same orders concept landing via a
different source system's own top-level folder. Flows into Bronze automatically via Auto
Loader, same trigger as Staging - no review/approval gate, at least for this source.

No validation performed here - Bronze preserves the source verbatim. `webshop_` is this
table's source-system abbreviation.

drop_path/schema_location come from config/source_environment.yml instead of being built from
literals here - see CONVENTIONS.md's "Source registry and source x environment connection
config" section for why, and how pipeline code reads the file via bundle.workspace_file_path.
"""

import yaml
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_NAME = "webshop"
ENTITY_NAME = "orders"

catalog = spark.conf.get("bundle.catalog")
target = spark.conf.get("bundle.target")
workspace_file_path = spark.conf.get("bundle.workspace_file_path")

with open(f"{workspace_file_path}/config/source_environment.yml") as f:
    connection = yaml.safe_load(f)[SOURCE_NAME][target]

drop_path = connection["drop_path"].format(catalog=catalog)
schema_location = connection["schema_location"].format(catalog=catalog)


@dp.table(
    name=f"{SOURCE_NAME}_{ENTITY_NAME}",
    comment="Raw orders exactly as landed in the System Drop volume by the webshop. No validation performed.",
    table_properties={"quality": "bronze"},
)
def webshop_orders():
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
