"""
Spark session factory with Delta Lake support.
Reads configuration from app_config.yaml.
"""
import os
from pathlib import Path
from typing import Optional

import yaml
from pyspark.sql import SparkSession


def load_config() -> dict:
    """Load application configuration from YAML file."""
    config_path = Path(__file__).parent / "app_config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_spark_session(app_name: Optional[str] = None, master: Optional[str] = None) -> SparkSession:
    """
    Create and return a configured Spark session.

    Args:
        app_name: Override the application name from config.
        master: Override the Spark master URL from config.

    Returns:
        Configured SparkSession instance.
    """
    config = load_config()
    spark_cfg = config["spark"]

    resolved_app_name = app_name or spark_cfg["app_name"]
    resolved_master = master or os.getenv("SPARK_MASTER", spark_cfg["master"])

    builder = (
        SparkSession.builder
        .appName(resolved_app_name)
        .master(resolved_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.executor.memory", spark_cfg["executor_memory"])
        .config("spark.driver.memory", spark_cfg["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(spark_cfg["shuffle_partitions"]))
        .config("spark.default.parallelism", str(spark_cfg["default_parallelism"]))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.showConsoleProgress", "false")
        # Delta Lake configs
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    )

    # Add Delta Lake JAR packages if environment variable set
    packages = os.getenv(
        "SPARK_PACKAGES",
        "io.delta:delta-spark_2.12:3.1.0"
    )
    if packages:
        builder = builder.config("spark.jars.packages", packages)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Gracefully stop a Spark session."""
    if spark:
        spark.stop()
