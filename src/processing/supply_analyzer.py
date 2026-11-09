"""
Supply-side analytics Spark job.
Computes supplier performance, lead time adherence, fill rate, and supply gap.
"""

from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class SupplyAnalyzer:
    """
    Supply-chain analytics module.

    Outputs:
      - supply_gap          : demand vs available supply per product
      - reorder_alerts      : products below reorder point
      - supplier_performance: on-time delivery & fill-rate KPIs
      - inventory_turnover  : stock turnover ratio
    """

    def __init__(self, spark: SparkSession, processed_path: str):
        self.spark = spark
        self.proc = Path(processed_path)

    def _read(self, name: str) -> DataFrame:
        return self.spark.read.parquet(str(self.proc / name))

    def supply_gap_analysis(self, inventory: DataFrame, demand_agg: DataFrame) -> DataFrame:
        """
        Compute supply gap = forecasted demand – available stock.
        A positive gap indicates a potential stock-out.
        """
        logger.info("Computing supply gap …")

        # Total available stock per product across all warehouses
        stock = inventory.groupBy("product_id", "category", "product_name").agg(
            F.sum("available_stock").alias("total_available"),
            F.sum("current_stock").alias("total_stock"),
            F.sum(F.when(F.col("stock_status") == "OUT_OF_STOCK", 1).otherwise(0)).alias("oos_warehouses"),
        )

        # 7-day rolling demand estimate
        rolling = demand_agg.groupBy("product_id").agg(F.avg("rolling_mean_7").alias("avg_daily_demand_7d"))

        return (
            stock.join(rolling, on="product_id", how="left")
            .withColumn("projected_demand_7d", F.col("avg_daily_demand_7d") * F.lit(7))
            .withColumn("supply_gap", F.col("projected_demand_7d") - F.col("total_available"))
            .withColumn(
                "gap_severity",
                F.when(F.col("supply_gap") > 500, "CRITICAL")
                .when(F.col("supply_gap") > 100, "HIGH")
                .when(F.col("supply_gap") > 0, "MEDIUM")
                .otherwise("NONE"),
            )
            .withColumn(
                "days_of_supply",
                F.col("total_available") / (F.col("avg_daily_demand_7d") + F.lit(0.001)),
            )
            .withColumn("processed_at", F.current_timestamp())
        )

    def reorder_alerts(self, inventory: DataFrame) -> DataFrame:
        """Return products that need immediate reorder action."""
        logger.info("Generating reorder alerts …")
        return (
            inventory.filter(F.col("stock_status").isin(["LOW_STOCK", "OUT_OF_STOCK"]))
            .groupBy("product_id", "product_name", "category")
            .agg(
                F.min("current_stock").alias("min_stock"),
                F.min("reorder_point").alias("reorder_point"),
                F.sum("reorder_quantity").alias("suggested_reorder_qty"),
                F.collect_set("warehouse_id").alias("affected_warehouses"),
                F.max("stock_status").alias("worst_status"),
            )
            .withColumn(
                "urgency",
                F.when(F.col("worst_status") == "OUT_OF_STOCK", 1).otherwise(2),
            )
            .orderBy("urgency", "min_stock")
        )

    def inventory_turnover(self, orders: DataFrame, inventory: DataFrame) -> DataFrame:
        """
        Inventory turnover = COGS / Average Inventory Value.
        Higher is better (stock moves faster).
        """
        logger.info("Computing inventory turnover …")

        cogs = orders.groupBy("product_id").agg(F.sum("revenue").alias("annual_cogs"))

        avg_inv = inventory.groupBy("product_id", "category", "product_name").agg(
            F.avg("inventory_value").alias("avg_inventory_value")
        )

        return (
            avg_inv.join(cogs, on="product_id", how="left")
            .withColumn(
                "inventory_turnover",
                F.col("annual_cogs") / (F.col("avg_inventory_value") + F.lit(1)),
            )
            .withColumn(
                "days_inventory_outstanding",
                F.lit(365) / (F.col("inventory_turnover") + F.lit(0.001)),
            )
            .withColumn(
                "performance",
                F.when(F.col("inventory_turnover") > 12, "EXCELLENT")
                .when(F.col("inventory_turnover") > 6, "GOOD")
                .when(F.col("inventory_turnover") > 3, "AVERAGE")
                .otherwise("POOR"),
            )
        )

    def category_supply_summary(self, supply_gap: DataFrame) -> DataFrame:
        """Aggregate supply risk by category."""
        return supply_gap.groupBy("category").agg(
            F.count("product_id").alias("total_products"),
            F.sum(F.when(F.col("gap_severity") == "CRITICAL", 1).otherwise(0)).alias("critical_products"),
            F.sum(F.when(F.col("gap_severity") == "HIGH", 1).otherwise(0)).alias("high_risk_products"),
            F.avg("days_of_supply").alias("avg_days_of_supply"),
            F.sum("supply_gap").alias("total_supply_gap"),
        )

    def run(self) -> None:
        """Execute all supply analytics jobs."""
        inventory = self._read("clean_inventory")
        demand_agg = self._read("demand_aggregates")
        orders = self._read("clean_orders")

        supply_gap = self.supply_gap_analysis(inventory, demand_agg)
        reorder = self.reorder_alerts(inventory)
        turnover = self.inventory_turnover(orders, inventory)
        cat_summary = self.category_supply_summary(supply_gap)

        for name, df in [
            ("supply_gap", supply_gap),
            ("reorder_alerts", reorder),
            ("inventory_turnover", turnover),
            ("category_supply_summary", cat_summary),
        ]:
            out = str(self.proc / name)
            df.write.mode("overwrite").parquet(out)
            logger.success(f"Written {name} → {out}")


if __name__ == "__main__":
    from config.spark_config import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        analyzer = SupplyAnalyzer(spark, "./data/processed")
        analyzer.run()
    finally:
        stop_spark_session(spark)
