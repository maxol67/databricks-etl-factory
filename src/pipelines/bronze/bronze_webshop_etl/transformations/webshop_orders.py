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
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_NAME = "webshop"
ENTITY_NAME = "orders"

catalog = spark.conf.get("bundle.catalog")
drop_path = f"/Volumes/{catalog}/drop/system_drop/{SOURCE_NAME}/{ENTITY_NAME}/"
schema_location = f"/Volumes/{catalog}/drop/system_drop/_schemas/{SOURCE_NAME}_{ENTITY_NAME}_bronze/"


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
