"""
Tests for the ML demand forecaster.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("MLTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def minimal_config():
    return {
        "data": {
            "processed_path": "/tmp/test_proc",
            "model_path": "/tmp/test_models",
        },
        "ml": {
            "demand_forecast": {
                "algorithm": "linear_regression",
                "forecast_horizon_days": 7,
                "train_test_split": 0.8,
                "n_trees": 10,
                "max_depth": 3,
            }
        },
    }


@pytest.fixture
def demand_sample(spark):
    """Create minimal demand aggregates for ML testing."""
    import numpy as np
    import pandas as pd

    rng = np.random.default_rng(42)
    n = 200
    pdf = pd.DataFrame(
        {
            "product_id": ["PROD0001"] * n,
            "category": ["Electronics"] * n,
            "order_date": pd.date_range("2023-01-01", periods=n, freq="D"),
            "year": [2023] * (n // 2) + [2024] * (n // 2),
            "month": [((i % 12) + 1) for i in range(n)],
            "week_of_year": [((i % 52) + 1) for i in range(n)],
            "day_of_week": [i % 7 for i in range(n)],
            "is_weekend": [int(i % 7 >= 5) for i in range(n)],
            "season": ["Winter"] * 50 + ["Spring"] * 50 + ["Summer"] * 50 + ["Autumn"] * 50,
            "total_quantity": rng.integers(1, 50, size=n).astype(float),
            "total_revenue": rng.uniform(100, 5000, size=n),
            "avg_price": rng.uniform(10, 500, size=n),
            "order_count": rng.integers(1, 20, size=n).astype(float),
            "unique_customers": rng.integers(1, 15, size=n).astype(float),
            "rolling_mean_7": rng.uniform(5, 30, size=n),
            "rolling_mean_14": rng.uniform(5, 30, size=n),
            "lag_7": rng.uniform(5, 30, size=n),
            "lag_14": rng.uniform(5, 30, size=n),
            "rolling_std_7": rng.uniform(1, 10, size=n),
        }
    )
    return spark.createDataFrame(pdf)


class TestDemandForecaster:
    """Test the DemandForecaster class."""

    def test_pipeline_builds_without_error(self, spark, minimal_config):
        """The ML pipeline should build without raising exceptions."""
        from src.ml.demand_forecaster import DemandForecaster

        forecaster = DemandForecaster(spark, minimal_config)
        pipeline = forecaster._build_pipeline("linear_regression")
        assert pipeline is not None

    def test_unsupported_algorithm_raises(self, spark, minimal_config):
        """An unsupported algorithm name should raise ValueError."""
        from src.ml.demand_forecaster import DemandForecaster

        forecaster = DemandForecaster(spark, minimal_config)
        with pytest.raises(ValueError, match="Unknown algorithm"):
            forecaster._build_pipeline("unsupported_algo")

    def test_model_trains_and_predicts(self, spark, demand_sample, minimal_config, tmp_path):
        """Model should train on sample data and produce predictions."""
        from src.ml.demand_forecaster import DemandForecaster

        config = dict(minimal_config)
        config["data"] = {
            "processed_path": str(tmp_path / "proc"),
            "model_path": str(tmp_path / "models"),
        }

        # Save sample demand aggregates for the forecaster to load
        agg_path = tmp_path / "proc" / "demand_aggregates"
        demand_sample.write.mode("overwrite").parquet(str(agg_path))

        forecaster = DemandForecaster(spark, config)
        metrics = forecaster.train("linear_regression")

        assert "rmse" in metrics
        assert "r2" in metrics
        assert metrics["rmse"] >= 0
