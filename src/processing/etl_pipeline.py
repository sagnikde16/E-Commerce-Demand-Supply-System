"""
Main ETL pipeline: ingest raw data, clean, transform, and persist to Delta Lake / Parquet.
"""

from pathlib import Path

import yaml
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.spark_config import get_spark_session, stop_spark_session

# ── Schema definitions ───────────────────────────────────────────────────────

ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("order_datetime", TimestampType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("region", StringType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("week_of_year", IntegerType(), True),
        StructField("is_weekend", IntegerType(), True),
        StructField("season", StringType(), True),
    ]
)

INVENTORY_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("warehouse_id", StringType(), False),
        StructField("current_stock", IntegerType(), True),
        StructField("reserved_stock", IntegerType(), True),
        StructField("available_stock", IntegerType(), True),
        StructField("reorder_point", IntegerType(), True),
        StructField("reorder_quantity", IntegerType(), True),
        StructField("needs_reorder", BooleanType(), True),
        StructField("unit_cost", DoubleType(), True),
        StructField("total_value", DoubleType(), True),
    ]
)


class ETLPipeline:
    """
    End-to-end ETL pipeline for the e-commerce demand-supply system.

    Stages:
        1. Extract — read raw Parquet files
        2. Transform — clean, enrich, derive features
        3. Load — write processed data to Delta Lake / Parquet
    """

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.raw = Path(config["data"]["raw_path"])
        self.proc = Path(config["data"]["processed_path"])
        self.proc.mkdir(parents=True, exist_ok=True)

    # ── Extract ──────────────────────────────────────────────────────────────

    def extract_orders(self) -> DataFrame:
        """Read raw orders from Parquet."""
        path = str(self.raw / "orders.parquet")
        logger.info(f"Extracting orders from {path}")
        return self.spark.read.schema(ORDER_SCHEMA).parquet(path)

    def extract_products(self) -> DataFrame:
        """Read raw products from Parquet."""
        path = str(self.raw / "products.parquet")
        logger.info(f"Extracting products from {path}")
        return self.spark.read.parquet(path)

    def extract_inventory(self) -> DataFrame:
        """Read raw inventory from Parquet."""
        path = str(self.raw / "inventory.parquet")
        logger.info(f"Extracting inventory from {path}")
        return self.spark.read.schema(INVENTORY_SCHEMA).parquet(path)

    def extract_market_signals(self) -> DataFrame:
        """Read raw market signals from Parquet."""
        path = str(self.raw / "market_signals.parquet")
        logger.info(f"Extracting market signals from {path}")
        return self.spark.read.parquet(path)

    # ── Transform ────────────────────────────────────────────────────────────

    def transform_orders(self, df: DataFrame) -> DataFrame:
        """
        Clean and enrich orders DataFrame.
        - Drop nulls on key columns
        - Remove cancelled/returned for demand analysis
        - Add derived columns: revenue, profit_margin, days_since_order
        """
        logger.info("Transforming orders …")
        return (
            df.dropna(subset=["order_id", "product_id", "order_date"])
            .filter(F.col("status").isin(["delivered", "shipped", "processing"]))
            .filter(F.col("quantity") > 0)
            .filter(F.col("unit_price") > 0)
            .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
            .withColumn("order_date", F.to_date(F.col("order_date")))
            .withColumn("year", F.year(F.col("order_date")))
            .withColumn("month", F.month(F.col("order_date")))
            .withColumn("day_of_week", F.dayofweek(F.col("order_date")))
            .withColumn("week_of_year", F.weekofyear(F.col("order_date")))
            .withColumn(
                "is_weekend",
                F.when(F.col("day_of_week").isin([1, 7]), F.lit(1)).otherwise(F.lit(0)),
            )
            .withColumn("quarter", F.quarter(F.col("order_date")))
            .withColumn("processed_at", F.current_timestamp())
            .dropDuplicates(["order_id"])
        )

    def transform_inventory(self, inv_df: DataFrame, prod_df: DataFrame) -> DataFrame:
        """
        Enrich inventory with product metadata.
        Flag low-stock and over-stock conditions.
        """
        logger.info("Transforming inventory …")
        enriched = inv_df.join(
            prod_df.select("product_id", "category", "unit_price", "product_name"),
            on="product_id",
            how="left",
        )
        return (
            enriched.withColumn(
                "stock_coverage_ratio",
                F.col("current_stock") / (F.col("reorder_quantity") + F.lit(1)),
            )
            .withColumn(
                "stock_status",
                F.when(F.col("current_stock") == 0, "OUT_OF_STOCK")
                .when(F.col("current_stock") <= F.col("reorder_point"), "LOW_STOCK")
                .when(
                    F.col("current_stock") >= F.col("reorder_quantity") * 3,
                    "OVER_STOCK",
                )
                .otherwise("NORMAL"),
            )
            .withColumn("inventory_value", F.col("current_stock") * F.col("unit_price"))
            .withColumn("processed_at", F.current_timestamp())
        )

    def build_demand_aggregates(self, orders_df: DataFrame) -> DataFrame:
        """
        Aggregate demand metrics at product × date granularity.
        Adds lag features and rolling averages for ML.
        """
        logger.info("Building demand aggregates …")
        from pyspark.sql.window import Window

        daily = orders_df.groupBy(
            "product_id",
            "category",
            "order_date",
            "year",
            "month",
            "week_of_year",
            "day_of_week",
            "is_weekend",
            "season",
        ).agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("revenue").alias("total_revenue"),
            F.avg("unit_price").alias("avg_price"),
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )

        # Window for lag and rolling features
        w_7 = Window.partitionBy("product_id").orderBy("order_date").rowsBetween(-7, -1)
        w_14 = Window.partitionBy("product_id").orderBy("order_date").rowsBetween(-14, -1)
        w_lag7 = Window.partitionBy("product_id").orderBy("order_date").rowsBetween(-7, -7)
        w_lag14 = Window.partitionBy("product_id").orderBy("order_date").rowsBetween(-14, -14)

        return (
            daily.withColumn("rolling_mean_7", F.avg("total_quantity").over(w_7))
            .withColumn("rolling_mean_14", F.avg("total_quantity").over(w_14))
            .withColumn("lag_7", F.sum("total_quantity").over(w_lag7))
            .withColumn("lag_14", F.sum("total_quantity").over(w_lag14))
            .withColumn("rolling_std_7", F.stddev("total_quantity").over(w_7))
            .fillna(
                0,
                subset=[
                    "rolling_mean_7",
                    "rolling_mean_14",
                    "lag_7",
                    "lag_14",
                    "rolling_std_7",
                ],
            )
        )

    def build_supply_metrics(self, inventory_df: DataFrame, orders_df: DataFrame) -> DataFrame:
        """
        Join inventory and orders to compute supply gap, days-of-supply, turnover.
        """
        logger.info("Building supply metrics …")
        avg_daily = orders_df.groupBy("product_id").agg(F.avg("quantity").alias("avg_daily_demand"))

        agg_inv = inventory_df.groupBy("product_id", "category", "product_name").agg(
            F.sum("current_stock").alias("total_stock"),
            F.sum("available_stock").alias("total_available"),
            F.sum("inventory_value").alias("total_inventory_value"),
            F.sum(F.when(F.col("stock_status") == "LOW_STOCK", 1).otherwise(0)).alias("low_stock_warehouses"),
            F.sum(F.when(F.col("stock_status") == "OUT_OF_STOCK", 1).otherwise(0)).alias("out_of_stock_warehouses"),
        )

        return (
            agg_inv.join(avg_daily, on="product_id", how="left")
            .withColumn(
                "days_of_supply",
                F.col("total_available") / (F.col("avg_daily_demand") + F.lit(0.001)),
            )
            .withColumn(
                "supply_risk",
                F.when(F.col("days_of_supply") < 7, "HIGH")
                .when(F.col("days_of_supply") < 14, "MEDIUM")
                .otherwise("LOW"),
            )
            .withColumn("processed_at", F.current_timestamp())
        )

    # ── Load ─────────────────────────────────────────────────────────────────

    def _write_parquet(self, df: DataFrame, name: str, partition_by: list = None) -> None:
        path = str(self.proc / name)
        writer = df.write.mode("overwrite").format("parquet")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        logger.success(f"Written {name} → {path}")

    # ── Run ───────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Execute the full ETL pipeline."""
        logger.info("═" * 60)
        logger.info("Starting ETL pipeline …")
        logger.info("═" * 60)

        # Extract
        raw_orders = self.extract_orders()
        raw_products = self.extract_products()
        raw_inventory = self.extract_inventory()
        raw_signals = self.extract_market_signals()

        # Transform
        clean_orders = self.transform_orders(raw_orders)
        clean_inventory = self.transform_inventory(raw_inventory, raw_products)
        demand_agg = self.build_demand_aggregates(clean_orders)
        supply_metrics = self.build_supply_metrics(clean_inventory, clean_orders)

        # Load
        self._write_parquet(clean_orders, "clean_orders", ["year", "month"])
        self._write_parquet(raw_products, "clean_products")
        self._write_parquet(clean_inventory, "clean_inventory")
        self._write_parquet(raw_signals, "clean_market_signals")
        self._write_parquet(demand_agg, "demand_aggregates", ["year", "month"])
        self._write_parquet(supply_metrics, "supply_metrics")

        logger.success("ETL pipeline complete.")
        self._print_summary(clean_orders, clean_inventory, demand_agg, supply_metrics)

    def _print_summary(self, orders, inventory, demand, supply) -> None:
        logger.info("\n📊 Pipeline Summary")
        logger.info(f"  Orders processed:     {orders.count():>10,}")
        logger.info(f"  Inventory records:    {inventory.count():>10,}")
        logger.info(f"  Demand agg records:   {demand.count():>10,}")
        logger.info(f"  Supply metric rows:   {supply.count():>10,}")


def main():
    """Entry point for spark-submit / CLI execution."""
    config_path = Path(__file__).parent.parent.parent / "config" / "app_config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    spark = get_spark_session()
    try:
        pipeline = ETLPipeline(spark, config)
        pipeline.run()
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
