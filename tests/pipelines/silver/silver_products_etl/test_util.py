"""Unit tests for silver_products_etl/transformations/_util.py.

Only _util.py is imported here, never products.py - see _util.py's own docstring for why
products.py can't be imported standalone in a test process.

Imported via the normal `src.pipelines...` path (works because this project's editable install
puts the project root, not src/, on sys.path - confirmed live) rather than a sys.path hack, so
this resolves cleanly for both pytest and static analysis (Pylance/Pyright).

Same test as silver_customers_etl's test_util.py, retargeted - _util.py is duplicated per
pipeline (see that module's own docstring), so the test is too.
"""

from pyspark.sql.functions import col

from src.pipelines.silver.silver_products_etl.transformations._util import latest_by


def test_latest_by_keeps_only_the_newest_row_per_key(spark):
    # `version` distinguishes rows without relying on comparing timestamp string
    # representations, which are sensitive to the Spark session's configured timezone.
    df = spark.createDataFrame(
        [
            ("p1", "older", "2026-01-01T00:00:00"),
            ("p1", "newer", "2026-02-01T00:00:00"),
            ("p2", "only", "2026-01-15T00:00:00"),
        ],
        ["product_id", "version", "_source_file_modified_at"],
    ).withColumn("_source_file_modified_at", col("_source_file_modified_at").cast("timestamp"))

    result = latest_by(df, "product_id", "_source_file_modified_at").orderBy("product_id")

    rows = result.select("product_id", "version").collect()
    assert [(r.product_id, r.version) for r in rows] == [("p1", "newer"), ("p2", "only")]


def test_latest_by_drops_the_ranking_helper_column(spark):
    df = spark.createDataFrame([("p1", "2026-01-01T00:00:00")], ["product_id", "_source_file_modified_at"]).withColumn(
        "_source_file_modified_at", col("_source_file_modified_at").cast("timestamp")
    )

    result = latest_by(df, "product_id", "_source_file_modified_at")

    assert "_rn" not in result.columns
