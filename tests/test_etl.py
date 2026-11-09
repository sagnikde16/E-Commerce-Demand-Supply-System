"""
Tests for the ETL pipeline.
Uses a local Spark session with a small synthetic dataset.
"""
import os
import sys
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, StringType, StructField, StructType,
)

# Allow importing from project root
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(scope="session")
def spark():
    """Create a minimal local Spark session for testing."""
    session = (
        SparkSession.builder
        .appName("ETLTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_orders(spark):
    """Create a small orders DataFrame for testing."""
    data = [
        ("ORD001", "PROD0001", "Electronics", "CUST00001",
         "2024-01-15", 2, 299.99, 0.0, 599.98, "delivered", "web", "North",
         1, 1, 2024, 3, 0, "Winter"),
        ("ORD002", "PROD0002", "Clothing",    "CUST00002",
         "2024-02-20", 1, 49.99,  0.1, 44.99,  "shipped",   "mobile", "South",
         2, 2, 2024, 8, 0, "Winter"),
        ("ORD003", "PROD0001", "Electronics", "CUST00003",
         "2024-01-16", 3, 299.99, 0.0, 899.97, "delivered", "web", "East",
         1, 1, 2024, 3, 0, "Winter"),
        ("ORD004", "PROD0003", "Books",       "CUST00004",
         "2024-03-10", 1, 19.99,  0.0, 19.99,  "cancelled", "web", "West",
         0, 3, 2024, 10, 1, "Spring"),
        # Duplicate — should be deduped
        ("ORD001", "PROD0001", "Electronics", "CUST00001",
         "2024-01-15", 2, 299.99, 0.0, 599.98, "delivered", "web", "North",
         1, 1, 2024, 3, 0, "Winter"),
    ]
    schema = StructType([
        StructField("order_id",     StringType(),  False),
        StructField("product_id",   StringType(),  False),
        StructField("category",     StringType(),  True),
        StructField("customer_id",  StringType(),  True),
        StructField("order_date",   StringType(),  True),
        StructField("quantity",     IntegerType(), True),
        StructField("unit_price",   DoubleType(),  True),
        StructField("discount",     DoubleType(),  True),
        StructField("total_amount", DoubleType(),  True),
        StructField("status",       StringType(),  True),
        StructField("channel",      StringType(),  True),
        StructField("region",       StringType(),  True),
        StructField("day_of_week",  IntegerType(), True),
        StructField("month",        IntegerType(), True),
        StructField("year",         IntegerType(), True),
        StructField("week_of_year", IntegerType(), True),
        StructField("is_weekend",   IntegerType(), True),
        StructField("season",       StringType(),  True),
    ])
    return spark.createDataFrame(data, schema=schema)


class TestETLTransformations:
    """Test ETL transformation logic."""

    def test_cancelled_orders_filtered(self, spark, sample_orders):
        """Cancelled orders should be excluded from clean dataset."""
        from src.processing.etl_pipeline import ETLPipeline

        # Build a minimal config
        config = {
            "data": {
                "raw_path": "/tmp/test_raw",
                "processed_path": "/tmp/test_proc",
            }
        }
        pipeline = ETLPipeline(spark, config)

        # Cast order_date to DateType first
        df = sample_orders.withColumn("order_date", F.to_date("order_date"))
        clean = pipeline.transform_orders(df)

        statuses = [r["status"] for r in clean.select("status").collect()]
        assert "cancelled" not in statuses, "Cancelled orders should be filtered out"

    def test_duplicate_orders_removed(self, spark, sample_orders):
        """Duplicate order_ids should be deduplicated."""
        from src.processing.etl_pipeline import ETLPipeline

        config = {
            "data": {
                "raw_path": "/tmp/test_raw",
                "processed_path": "/tmp/test_proc",
            }
        }
        pipeline = ETLPipeline(spark, config)
        df = sample_orders.withColumn("order_date", F.to_date("order_date"))
        clean = pipeline.transform_orders(df)

        ids = [r["order_id"] for r in clean.select("order_id").collect()]
        assert len(ids) == len(set(ids)), "Duplicate order IDs should be removed"

    def test_revenue_column_added(self, spark, sample_orders):
        """transform_orders should add a revenue column."""
        from src.processing.etl_pipeline import ETLPipeline

        config = {
            "data": {
                "raw_path": "/tmp/test_raw",
                "processed_path": "/tmp/test_proc",
            }
        }
        pipeline = ETLPipeline(spark, config)
        df = sample_orders.withColumn("order_date", F.to_date("order_date"))
        clean = pipeline.transform_orders(df)

        assert "revenue" in clean.columns, "revenue column should exist after transform"

    def test_no_negative_quantities(self, spark, sample_orders):
        """All quantities in transformed orders should be positive."""
        from src.processing.etl_pipeline import ETLPipeline

        config = {
            "data": {
                "raw_path": "/tmp/test_raw",
                "processed_path": "/tmp/test_proc",
            }
        }
        pipeline = ETLPipeline(spark, config)
        df = sample_orders.withColumn("order_date", F.to_date("order_date"))
        clean = pipeline.transform_orders(df)

        min_qty = clean.agg(F.min("quantity")).collect()[0][0]
        assert min_qty > 0, "No zero or negative quantities allowed"

    def test_quarter_column_added(self, spark, sample_orders):
        """transform_orders should add a quarter column."""
        from src.processing.etl_pipeline import ETLPipeline

        config = {
            "data": {
                "raw_path": "/tmp/test_raw",
                "processed_path": "/tmp/test_proc",
            }
        }
        pipeline = ETLPipeline(spark, config)
        df = sample_orders.withColumn("order_date", F.to_date("order_date"))
        clean = pipeline.transform_orders(df)

        assert "quarter" in clean.columns, "quarter column should be added"
