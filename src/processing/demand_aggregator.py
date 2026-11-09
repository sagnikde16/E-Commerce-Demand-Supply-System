"""
Demand aggregation Spark job.
Computes product-level and category-level demand KPIs.
"""

from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class DemandAggregator:
    """
    Builds comprehensive demand analytics from cleaned order data.
    Outputs:
      - daily_demand       : per-product daily totals with features
      - category_demand    : category roll-ups
      - customer_segments  : RFM-based customer segments
      - top_sellers        : top 50 products by revenue
      - regional_demand    : demand split by region
    """

    def __init__(self, spark: SparkSession, processed_path: str):
        self.spark = spark
        self.proc = Path(processed_path)

    def _read(self, name: str) -> DataFrame:
        return self.spark.read.parquet(str(self.proc / name))

    # ── Daily demand per product ──────────────────────────────────────────────

    def daily_demand(self, orders: DataFrame) -> DataFrame:
        """Aggregate to daily product demand."""
        logger.info("Computing daily demand …")
        window = Window.partitionBy("product_id").orderBy("order_date")

        daily = orders.groupBy(
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
            F.sum("quantity").alias("units_sold"),
            F.sum("revenue").alias("revenue"),
            F.avg("unit_price").alias("avg_price"),
            F.countDistinct("order_id").alias("orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )

        return (
            daily.withColumn("units_sold_lag1", F.lag("units_sold", 1).over(window))
            .withColumn("units_sold_lag7", F.lag("units_sold", 7).over(window))
            .withColumn(
                "mom_growth",
                (F.col("units_sold") - F.col("units_sold_lag1")) / (F.col("units_sold_lag1") + F.lit(1)),
            )
            .fillna(0)
        )

    # ── Category roll-ups ─────────────────────────────────────────────────────

    def category_demand(self, orders: DataFrame) -> DataFrame:
        """Monthly demand aggregated by category."""
        logger.info("Computing category demand …")
        return (
            orders.groupBy("category", "year", "month", "season")
            .agg(
                F.sum("quantity").alias("total_units"),
                F.sum("revenue").alias("total_revenue"),
                F.avg("unit_price").alias("avg_price"),
                F.countDistinct("product_id").alias("distinct_products"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.countDistinct("order_id").alias("total_orders"),
            )
            .withColumn(
                "revenue_per_order",
                F.col("total_revenue") / (F.col("total_orders") + F.lit(1)),
            )
        )

    # ── RFM Customer segmentation ─────────────────────────────────────────────

    def customer_rfm(self, orders: DataFrame) -> DataFrame:
        """
        Compute Recency, Frequency, Monetary metrics per customer.
        Score each on 1-5 scale and assign segment label.
        """
        logger.info("Computing RFM customer segments …")
        max_date = orders.agg(F.max("order_date")).collect()[0][0]

        rfm = orders.groupBy("customer_id").agg(
            F.datediff(F.lit(max_date), F.max("order_date")).alias("recency"),
            F.countDistinct("order_id").alias("frequency"),
            F.sum("revenue").alias("monetary"),
        )

        rfm_scored = (
            rfm.withColumn("r_score", F.ntile(5).over(Window.orderBy(F.col("recency").desc())))
            .withColumn("f_score", F.ntile(5).over(Window.orderBy("frequency")))
            .withColumn("m_score", F.ntile(5).over(Window.orderBy("monetary")))
            .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
        )

        return rfm_scored.withColumn(
            "segment",
            F.when(F.col("rfm_score") >= 13, "Champions")
            .when(F.col("rfm_score") >= 10, "Loyal Customers")
            .when(F.col("rfm_score") >= 7, "Potential Loyalists")
            .when(F.col("rfm_score") >= 5, "At Risk")
            .otherwise("Lost"),
        )

    # ── Top sellers ───────────────────────────────────────────────────────────

    def top_sellers(self, orders: DataFrame, n: int = 50) -> DataFrame:
        """Return the top N products by total revenue."""
        logger.info(f"Computing top {n} sellers …")
        window = Window.orderBy(F.col("total_revenue").desc())
        return (
            orders.groupBy("product_id", "category")
            .agg(
                F.sum("quantity").alias("total_units"),
                F.sum("revenue").alias("total_revenue"),
                F.avg("unit_price").alias("avg_price"),
                F.countDistinct("order_id").alias("order_count"),
            )
            .withColumn("rank", F.rank().over(window))
            .filter(F.col("rank") <= n)
        )

    # ── Regional demand ───────────────────────────────────────────────────────

    def regional_demand(self, orders: DataFrame) -> DataFrame:
        """Demand split by region and category."""
        logger.info("Computing regional demand …")
        return orders.groupBy("region", "category", "year", "month").agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("revenue").alias("revenue"),
            F.countDistinct("order_id").alias("orders"),
        )

    # ── Run all ───────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Execute all aggregation jobs and write results."""
        orders = self._read("clean_orders")

        aggs = {
            "daily_demand": self.daily_demand(orders),
            "category_demand": self.category_demand(orders),
            "customer_rfm": self.customer_rfm(orders),
            "top_sellers": self.top_sellers(orders),
            "regional_demand": self.regional_demand(orders),
        }

        for name, df in aggs.items():
            out = str(self.proc / name)
            df.write.mode("overwrite").parquet(out)
            logger.success(f"Written {name} → {out}")


if __name__ == "__main__":
    from config.spark_config import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        agg = DemandAggregator(spark, "./data/processed")
        agg.run()
    finally:
        stop_spark_session(spark)
