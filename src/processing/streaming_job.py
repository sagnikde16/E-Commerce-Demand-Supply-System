"""
Spark Structured Streaming job.
Processes live order events from JSONL files dropped by the Kafka producer simulation.
Writes aggregated micro-batch results to Parquet (or Delta Lake in production).
"""

import os
from pathlib import Path

from loguru import logger
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from config.spark_config import get_spark_session

EVENT_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("channel", StringType(), True),
        StructField("region", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("current_stock", IntegerType(), True),
        StructField("needs_reorder", StringType(), True),
        StructField("search_volume", IntegerType(), True),
        StructField("market_demand_idx", DoubleType(), True),
        StructField("trend_direction", StringType(), True),
        StructField("timestamp", StringType(), True),  # ISO string from producer
    ]
)


def run_streaming(
    input_dir: str = "./data/raw/streaming",
    output_dir: str = "./data/processed/streaming",
    checkpoint_dir: str = "./data/checkpoints",
    trigger_seconds: int = 10,
) -> None:
    """
    Launch Spark Structured Streaming job.

    Reads new JSONL files from input_dir, processes micro-batches,
    and writes results to output_dir.
    """
    Path(input_dir).mkdir(parents=True, exist_ok=True)
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    spark = get_spark_session(app_name="EcommerceStreamingJob")

    logger.info(f"Watching for events in: {input_dir}")

    # ── Read stream ───────────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream.schema(EVENT_SCHEMA)
        .option("multiLine", False)
        .option("maxFilesPerTrigger", 10)
        .json(input_dir)
    )

    # ── Parse and enrich ──────────────────────────────────────────────────────
    enriched = (
        raw_stream.withColumn("event_ts", F.to_timestamp(F.col("timestamp")))
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .withColumn("date", F.to_date(F.col("event_ts")))
        .withColumn("hour", F.hour(F.col("event_ts")))
    )

    # ── Order demand stream ───────────────────────────────────────────────────
    order_stream = (
        enriched.filter(F.col("event_type") == "ORDER_PLACED")
        .withWatermark("event_ts", "1 hour")
        .groupBy(
            F.window("event_ts", "10 minutes"),
            "product_id",
            "region",
            "channel",
        )
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("revenue").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("unit_price").alias("avg_price"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )

    # ── Inventory alert stream ────────────────────────────────────────────────
    inventory_stream = (
        enriched.filter(F.col("event_type") == "INVENTORY_UPDATE")
        .filter(F.col("needs_reorder") == "true")
        .select("product_id", "warehouse_id", "current_stock", "event_ts")
        .withColumn(
            "alert_level",
            F.when(F.col("current_stock") == 0, "OUT_OF_STOCK").otherwise("LOW_STOCK"),
        )
    )

    # ── Write order aggregates ────────────────────────────────────────────────
    order_query = (
        order_stream.writeStream.outputMode("update")
        .format("parquet")
        .option("path", os.path.join(output_dir, "order_aggregates"))
        .option("checkpointLocation", os.path.join(checkpoint_dir, "orders"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    # ── Write inventory alerts ────────────────────────────────────────────────
    inventory_query = (
        inventory_stream.writeStream.outputMode("append")
        .format("parquet")
        .option("path", os.path.join(output_dir, "inventory_alerts"))
        .option("checkpointLocation", os.path.join(checkpoint_dir, "inventory"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    logger.info(f"Order query started: {order_query.id}")
    logger.info(f"Inventory query started: {inventory_query.id}")
    logger.info("Streaming queries running. Awaiting termination …")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_streaming()
