"""Pure helpers for silver_products_etl - deliberately no `spark`/`dp` (Lakeflow pipelines)
references at module level, unlike products.py in this same folder. products.py's top level
runs `spark.conf.get(...)` and registers Lakeflow datasets (`dp.create_streaming_table`,
`@dp.append_flow`), both of which require a live pipeline runtime context - importing it
standalone (e.g. from a test) fails. This module has neither, so it's safely importable and
unit-testable on its own; see tests/pipelines/silver/silver_products_etl/test_util.py.

Same helper as silver_customers_etl/transformations/_util.py, duplicated rather than shared -
this repo's convention is one self-contained _util.py per pipeline, not a cross-pipeline shared
module (see CONVENTIONS.md's Testing section).
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number


def latest_by(df: DataFrame, key_col: str, order_col: str) -> DataFrame:
    """Keep only the most recent row per key_col, ranked by order_col descending."""
    w = Window.partitionBy(key_col).orderBy(col(order_col).desc())
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
