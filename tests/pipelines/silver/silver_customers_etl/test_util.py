"""Unit tests for silver_customers_etl/transformations/_util.py.

Only _util.py is imported here, never customers.py - see _util.py's own docstring for why
customers.py can't be imported standalone in a test process.

Imported via the normal `src.pipelines...` path (works because this project's editable install
puts the project root, not src/, on sys.path - confirmed live) rather than a sys.path hack, so
this resolves cleanly for both pytest and static analysis (Pylance/Pyright).
"""

from pyspark.sql.functions import col

from src.pipelines.silver.silver_customers_etl.transformations._util import latest_by


def test_latest_by_keeps_only_the_newest_row_per_key(spark):
    # `version` distinguishes rows without relying on comparing timestamp string
    # representations, which are sensitive to the Spark session's configured timezone.
    df = spark.createDataFrame(
        [
            ("c1", "older", "2026-01-01T00:00:00"),
            ("c1", "newer", "2026-02-01T00:00:00"),
            ("c2", "only", "2026-01-15T00:00:00"),
        ],
        ["customer_id", "version", "updated_at"],
    ).withColumn("updated_at", col("updated_at").cast("timestamp"))

    result = latest_by(df, "customer_id", "updated_at").orderBy("customer_id")

    rows = result.select("customer_id", "version").collect()
    assert [(r.customer_id, r.version) for r in rows] == [("c1", "newer"), ("c2", "only")]


def test_latest_by_drops_the_ranking_helper_column(spark):
    df = spark.createDataFrame([("c1", "2026-01-01T00:00:00")], ["customer_id", "updated_at"]).withColumn(
        "updated_at", col("updated_at").cast("timestamp")
    )

    result = latest_by(df, "customer_id", "updated_at")

    assert "_rn" not in result.columns
