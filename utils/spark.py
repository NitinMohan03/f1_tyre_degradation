from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "f1-tyre-degradation", extra_packages: list[str] | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if extra_packages:
        builder = builder.config("spark.jars.packages", ",".join(extra_packages))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
