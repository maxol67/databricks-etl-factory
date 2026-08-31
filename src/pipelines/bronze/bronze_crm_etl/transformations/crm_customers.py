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
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_NAME = "crm"
ENTITY_NAME = "customers"

catalog = spark.conf.get("bundle.catalog")
drop_path = f"/Volumes/{catalog}/drop/system_drop/{SOURCE_NAME}/{ENTITY_NAME}/"
schema_location = f"/Volumes/{catalog}/drop/system_drop/_schemas/{SOURCE_NAME}_{ENTITY_NAME}_bronze/"


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
