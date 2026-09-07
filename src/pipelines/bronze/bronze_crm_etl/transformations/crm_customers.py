"""Bronze: raw customers as landed in the System Drop zone (see AGENTS.md for the
project's schema-allocation rules).

Lands in System Drop, not Staging: the CRM is another system pushing files (not ADF, no
Lakeflow Connect connector), which is exactly System Drop's definition, not Staging's -
see BACKLOG.md's Drop zones entry. Simulated the same way Staging simulates ADF: a plain
Unity Catalog Volume standing in for wherever the CRM would actually push to, with a
per-source-system folder (`crm/`) matching System Drop's shape, and a `customers/`
subfolder within it for this specific object - the CRM could push other objects
(contacts, opportunities, ...) as sibling subfolders later. Flows into Bronze
automatically via Auto Loader, same trigger as Staging - no review/approval gate, at
least for this source.

No validation performed here - Bronze preserves the source verbatim. `crm_` is this
table's source-system abbreviation, standing in for the (synthetic) CRM system this
customer data originates from.

drop_path/schema_location come from config/source_environment.yml instead of being built from
literals here - see CONVENTIONS.md's "Source registry and Source x Environment connection
config" section for why, and how pipeline code reads the file via bundle.workspace_file_path.
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

drop_path = connection["drop_path"].format(catalog=catalog)
schema_location = connection["schema_location"].format(catalog=catalog)


@dp.table(
    name=f"{SOURCE_NAME}_{ENTITY_NAME}",
    comment="Raw customers exactly as landed in the System Drop volume. No validation performed.",
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
