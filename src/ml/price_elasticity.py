"""
Price elasticity of demand analysis using Spark.
Estimates how demand changes with price changes for each product/category.
"""

from pathlib import Path

from loguru import logger
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class PriceElasticityAnalyzer:
    """
    Computes price elasticity of demand (PED) using log-log linear regression.

    PED = % change in quantity / % change in price
    A PED < -1 indicates elastic demand; between -1 and 0 is inelastic.

    Uses Spark MLlib's LinearRegression on log-transformed price and quantity.
    """

    MIN_DATA_POINTS = 50

    def __init__(self, spark: SparkSession, processed_path: str):
        self.spark = spark
        self.proc = Path(processed_path)

    def _read_demand(self) -> DataFrame:
        return self.spark.read.parquet(str(self.proc / "daily_demand"))

    def compute_product_elasticity(self, demand: DataFrame) -> DataFrame:
        """
        Estimate PED for each product using per-product log-log OLS.
        Returns a DataFrame with elasticity coefficients.
        """
        logger.info("Computing product-level price elasticity …")

        # Log transform
        log_df = (
            demand.filter((F.col("avg_price") > 0) & (F.col("units_sold") > 0))
            .withColumn("log_price", F.log("avg_price"))
            .withColumn("log_quantity", F.log("units_sold"))
        )

        # Filter products with sufficient data
        counts = log_df.groupBy("product_id").count().filter(F.col("count") >= self.MIN_DATA_POINTS)
        filtered = log_df.join(counts.select("product_id"), on="product_id", how="inner")

        # Assemble features for MLlib
        assembler = VectorAssembler(inputCols=["log_price"], outputCol="features")
        assembled = assembler.transform(filtered)

        # Train linear regression per product using grouped map
        from pyspark.sql.types import DoubleType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("product_id", StringType(), False),
                StructField("category", StringType(), True),
                StructField("elasticity", DoubleType(), True),
                StructField("r_squared", DoubleType(), True),
                StructField("data_points", DoubleType(), True),
            ]
        )

        def fit_elasticity(pdf):
            """Fit OLS on a pandas group and return elasticity coefficient."""
            import numpy as np
            import pandas as pd

            if len(pdf) < PriceElasticityAnalyzer.MIN_DATA_POINTS:
                return pd.DataFrame(columns=schema.fieldNames())

            x = pdf["log_price"].values
            y = pdf["log_quantity"].values
            x_mat = np.column_stack([np.ones(len(x)), x])

            try:
                coeffs, _, _, _ = np.linalg.lstsq(x_mat, y, rcond=None)
                intercept, slope = coeffs

                # R² calculation
                y_pred = intercept + slope * x
                ss_res = np.sum((y - y_pred) ** 2)
                ss_tot = np.sum((y - np.mean(y)) ** 2)
                r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

                return pd.DataFrame(
                    [
                        {
                            "product_id": pdf["product_id"].iloc[0],
                            "category": pdf["category"].iloc[0],
                            "elasticity": round(float(slope), 4),
                            "r_squared": round(float(r2), 4),
                            "data_points": float(len(pdf)),
                        }
                    ]
                )
            except Exception:
                return pd.DataFrame(columns=schema.fieldNames())

        result = assembled.groupBy("product_id").applyInPandas(fit_elasticity, schema=schema)

        return result.withColumn(
            "demand_type",
            F.when(F.col("elasticity") < -1, "Elastic")
            .when(F.col("elasticity").between(-1, 0), "Inelastic")
            .when(F.col("elasticity") > 0, "Giffen Good")
            .otherwise("Unit Elastic"),
        )

    def category_elasticity(self, demand: DataFrame) -> DataFrame:
        """Aggregate elasticity to category level."""
        logger.info("Computing category elasticity …")

        log_df = (
            demand.filter((F.col("avg_price") > 0) & (F.col("units_sold") > 0))
            .withColumn("log_price", F.log("avg_price"))
            .withColumn("log_quantity", F.log("units_sold"))
        )

        assembler = VectorAssembler(inputCols=["log_price"], outputCol="features")
        assembled = assembler.transform(log_df)

        from pyspark.sql.types import DoubleType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("category", StringType(), False),
                StructField("elasticity", DoubleType(), True),
                StructField("r_squared", DoubleType(), True),
            ]
        )

        def fit_category(pdf):
            import numpy as np
            import pandas as pd

            x = pdf["log_price"].values
            y = pdf["log_quantity"].values
            if len(x) < 10:
                return pd.DataFrame(columns=schema.fieldNames())
            x_mat = np.column_stack([np.ones(len(x)), x])
            try:
                c, _, _, _ = np.linalg.lstsq(x_mat, y, rcond=None)
                slope = c[1]
                y_pred = c[0] + slope * x
                ss_res = np.sum((y - y_pred) ** 2)
                ss_tot = np.sum((y - np.mean(y)) ** 2)
                r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
                return pd.DataFrame(
                    [
                        {
                            "category": pdf["category"].iloc[0],
                            "elasticity": round(float(slope), 4),
                            "r_squared": round(float(r2), 4),
                        }
                    ]
                )
            except Exception:
                return pd.DataFrame(columns=schema.fieldNames())

        return assembled.groupBy("category").applyInPandas(fit_category, schema=schema)

    def pricing_recommendations(self, elasticity_df: DataFrame) -> DataFrame:
        """
        Generate pricing recommendations based on elasticity scores.
        """
        return elasticity_df.withColumn(
            "recommendation",
            F.when(
                F.col("elasticity") < -2,
                "Price is highly elastic — consider reducing price to boost volume",
            )
            .when(
                F.col("elasticity").between(-2, -1),
                "Moderate elasticity — price reductions will increase revenue",
            )
            .when(
                F.col("elasticity").between(-1, -0.5),
                "Inelastic demand — small price increases are safe",
            )
            .when(
                F.col("elasticity") > -0.5,
                "Very inelastic — price increases unlikely to reduce demand",
            )
            .otherwise("Monitor for data anomalies"),
        )

    def run(self) -> None:
        demand = self._read_demand()

        prod_elast = self.compute_product_elasticity(demand)
        cat_elast = self.category_elasticity(demand)
        recs = self.pricing_recommendations(prod_elast)

        for name, df in [
            ("product_elasticity", recs),
            ("category_elasticity", cat_elast),
        ]:
            out = str(self.proc / name)
            df.write.mode("overwrite").parquet(out)
            logger.success(f"Written {name} → {out}")


if __name__ == "__main__":
    from config.spark_config import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        analyzer = PriceElasticityAnalyzer(spark, "./data/processed")
        analyzer.run()
    finally:
        stop_spark_session(spark)
