"""
Inventory optimization using statistical safety stock and reorder point models.
Integrates demand variability with lead-time uncertainty.
"""
from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class InventoryOptimizer:
    """
    Computes optimal inventory levels:
      - Safety stock (service-level driven)
      - Reorder point (ROP)
      - Economic Order Quantity (EOQ)
      - ABC classification (Pareto analysis)
    """

    Z_SCORE = {0.90: 1.282, 0.95: 1.645, 0.99: 2.326}

    def __init__(self, spark: SparkSession, config: dict):
        self.spark   = spark
        self.cfg     = config["ml"]["inventory"]
        self.proc    = Path(config["data"]["processed_path"])
        self.z_score = self.Z_SCORE.get(self.cfg.get("service_level", 0.95), 1.645)

    def _read(self, name: str) -> DataFrame:
        return self.spark.read.parquet(str(self.proc / name))

    def safety_stock(self, demand_agg: DataFrame) -> DataFrame:
        """
        Safety stock = Z × σ(demand) × √(lead_time)
        where σ(demand) is the standard deviation of daily demand.
        """
        logger.info("Computing safety stock …")
        lead_time = self.cfg.get("lead_time_days", 7)
        z         = self.z_score

        stats = (
            demand_agg
            .groupBy("product_id", "category")
            .agg(
                F.avg("total_quantity").alias("avg_daily_demand"),
                F.stddev("total_quantity").alias("std_daily_demand"),
                F.max("total_quantity").alias("max_daily_demand"),
                F.min("total_quantity").alias("min_daily_demand"),
            )
            .fillna(0, subset=["std_daily_demand"])
        )

        return (
            stats
            .withColumn("lead_time_days", F.lit(lead_time))
            .withColumn("safety_stock",
                        F.round(F.lit(z) * F.col("std_daily_demand")
                                * F.sqrt(F.lit(float(lead_time))), 0).cast("integer"))
            .withColumn("reorder_point",
                        F.round(F.col("avg_daily_demand") * F.lit(lead_time)
                                + F.col("safety_stock"), 0).cast("integer"))
            .withColumn("max_stock_level",
                        F.round(F.col("avg_daily_demand") * F.lit(lead_time * 2)
                                + F.col("safety_stock"), 0).cast("integer"))
        )

    def eoq(self, demand_agg: DataFrame, inventory: DataFrame) -> DataFrame:
        """
        Economic Order Quantity: EOQ = √(2DS/H)
          D = annual demand
          S = ordering cost (assumed fixed at 50 per order)
          H = holding cost per unit per year (assumed 20% of unit cost)
        """
        logger.info("Computing EOQ …")
        ORDERING_COST  = 50.0   # \$ per order
        HOLDING_RATE   = 0.20   # 20% of unit cost per year

        annual_demand = (
            demand_agg
            .groupBy("product_id")
            .agg((F.sum("total_quantity") / F.lit(2)).alias("annual_demand"))  # 2 years of data
        )

        unit_cost = (
            inventory
            .groupBy("product_id")
            .agg(F.avg("unit_cost").alias("unit_cost"))
        )

        return (
            annual_demand
            .join(unit_cost, on="product_id", how="left")
            .withColumn("holding_cost_per_unit",
                        F.col("unit_cost") * F.lit(HOLDING_RATE))
            .withColumn("eoq",
                        F.round(
                            F.sqrt(
                                F.lit(2) * F.col("annual_demand")
                                * F.lit(ORDERING_COST)
                                / (F.col("holding_cost_per_unit") + F.lit(0.001))
                            ), 0
                        ).cast("integer"))
            .withColumn("orders_per_year",
                        F.round(F.col("annual_demand") / (F.col("eoq") + F.lit(1)), 1))
            .withColumn("total_annual_cost",
                        F.round(
                            (F.col("annual_demand") / (F.col("eoq") + F.lit(1))) * F.lit(ORDERING_COST)
                            + (F.col("eoq") / F.lit(2)) * F.col("holding_cost_per_unit"), 2
                        ))
        )

    def abc_classification(self, orders: DataFrame) -> DataFrame:
        """
        ABC analysis — classify products by revenue contribution.
          A: top 80% of revenue
          B: next 15%
          C: remaining 5%
        """
        logger.info("Running ABC classification …")

        revenue = (
            orders
            .groupBy("product_id", "category")
            .agg(F.sum("revenue").alias("total_revenue"))
        )

        total_rev = revenue.agg(F.sum("total_revenue")).collect()[0][0]
        window    = Window.orderBy(F.col("total_revenue").desc())

        return (
            revenue
            .withColumn("rank",             F.rank().over(window))
            .withColumn("cumulative_pct",
                        F.sum("total_revenue").over(window.rowsBetween(Window.unboundedPreceding, 0))
                        / F.lit(total_rev) * F.lit(100))
            .withColumn("abc_class",
                        F.when(F.col("cumulative_pct") <= 80, "A")
                        .when(F.col("cumulative_pct") <= 95, "B")
                        .otherwise("C"))
            .withColumn("abc_strategy",
                        F.when(F.col("abc_class") == "A",
                               "High-priority: frequent replenishment, tight control")
                        .when(F.col("abc_class") == "B",
                               "Medium-priority: periodic review, moderate safety stock")
                        .otherwise("Low-priority: bulk orders, minimal monitoring"))
        )

    def combined_recommendations(
        self, safety_df: DataFrame, eoq_df: DataFrame, abc_df: DataFrame
    ) -> DataFrame:
        """Join all optimization results into a single recommendation table."""
        logger.info("Building combined inventory recommendations …")
        return (
            safety_df
            .join(eoq_df.select("product_id", "eoq", "orders_per_year", "total_annual_cost"),
                  on="product_id", how="left")
            .join(abc_df.select("product_id", "abc_class", "abc_strategy", "total_revenue"),
                  on="product_id", how="left")
            .withColumn("recommended_order_qty",
                        F.greatest("eoq", "reorder_point"))
            .withColumn("processed_at", F.current_timestamp())
        )

    def run(self) -> None:
        demand_agg = self._read("demand_aggregates")
        inventory  = self._read("clean_inventory")
        orders     = self._read("clean_orders")

        safety    = self.safety_stock(demand_agg)
        eoq_df    = self.eoq(demand_agg, inventory)
        abc       = self.abc_classification(orders)
        combined  = self.combined_recommendations(safety, eoq_df, abc)

        for name, df in [
            ("safety_stock",          safety),
            ("eoq_analysis",          eoq_df),
            ("abc_classification",    abc),
            ("inventory_recommendations", combined),
        ]:
            out = str(self.proc / name)
            df.write.mode("overwrite").parquet(out)
            logger.success(f"Written {name} → {out}")


if __name__ == "__main__":
    import yaml
    from config.spark_config import get_spark_session, stop_spark_session
    config_path = Path(__file__).parent.parent.parent / "config" / "app_config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    spark = get_spark_session()
    try:
        optimizer = InventoryOptimizer(spark, config)
        optimizer.run()
    finally:
        stop_spark_session(spark)
