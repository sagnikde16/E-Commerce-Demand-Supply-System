"""
End-to-End Pipeline Runner.
Runs:
1. Synthetic Data Generation
2. Spark ETL Pipeline
3. Demand Aggregation
4. Supply Chain Analytics
5. ML Demand Forecasting
6. Inventory Optimization
7. Price Elasticity Analysis
"""

import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger

from config.spark_config import get_spark_session, stop_spark_session
from scripts.generate_data import run_generation
from src.ml.demand_forecaster import main as run_forecaster
from src.ml.inventory_optimizer import main as run_inventory_optimizer
from src.ml.price_elasticity import main as run_price_elasticity
from src.processing.demand_aggregator import DemandAggregator
from src.processing.etl_pipeline import main as run_etl
from src.processing.supply_analyzer import SupplyAnalyzer


def run_full_pipeline(records: int = 10_000):
    start_time = time.time()
    logger.info("============================================================")
    logger.info("Starting Full E-Commerce Demand-Supply Spark Pipeline")
    logger.info(f"Target record count: {records:,}")
    logger.info("============================================================")

    # Step 1: Generate synthetic raw data
    logger.info("[1/7] Generating synthetic dataset...")
    run_generation(output_dir="./data/raw", n_orders=records)

    # Step 2: Spark ETL
    logger.info("[2/7] Running PySpark ETL Pipeline...")
    run_etl()

    # Step 3: Demand Aggregations
    logger.info("[3/7] Running Demand Aggregations & Customer RFM...")
    spark = get_spark_session(app_name="EcommerceFullPipeline")
    try:
        DemandAggregator(spark, "./data/processed").run()

        # Step 4: Supply Analysis
        logger.info("[4/7] Running Supply-Side & Inventory Turnover Analysis...")
        SupplyAnalyzer(spark, "./data/processed").run()
    finally:
        stop_spark_session(spark)

    # Step 5: Demand Forecasting (MLlib)
    logger.info("[5/7] Training MLlib Demand Forecaster & Generating Predictions...")
    run_forecaster()

    # Step 6: Inventory Optimization (Safety Stock & EOQ)
    logger.info("[6/7] Computing Safety Stock, EOQ & ABC Classifications...")
    run_inventory_optimizer()

    # Step 7: Price Elasticity
    logger.info("[7/7] Computing Price Elasticity of Demand (Log-Log OLS)...")
    run_price_elasticity()

    elapsed = time.time() - start_time
    logger.success("============================================================")
    logger.success(f"Full pipeline finished successfully in {elapsed:.1f} seconds!")
    logger.success("You can now start the dashboard or API:")
    logger.success("  Streamlit Dashboard: streamlit run src/dashboard/app.py")
    logger.success("  FastAPI Server:      uvicorn src.api.main:app --reload")
    logger.success("============================================================")


if __name__ == "__main__":
    records = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000
    run_full_pipeline(records=records)
