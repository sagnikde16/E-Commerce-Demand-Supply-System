"""
Apache Airflow DAG for orchestrating the E-Commerce Demand-Supply pipeline.
Runs daily: data generation → ETL → aggregation → supply analysis → ML forecasting.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Default args ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "sagnikde16",
    "depends_on_past":  False,
    "email":            ["sagnikde191@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

PROJECT_DIR = "/opt/ecommerce-demand-supply-spark"

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="ecommerce_demand_supply_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily E-Commerce Demand-Supply ETL + ML Pipeline",
    schedule_interval="0 2 * * *",   # 2 AM daily
    start_date=days_ago(1),
    catchup=False,
    tags=["ecommerce", "spark", "ml", "demand-supply"],
    max_active_runs=1,
) as dag:

    # ── Task 1: Generate fresh data (dev/staging only) ────────────────────────
    generate_data = BashOperator(
        task_id="generate_synthetic_data",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python scripts/generate_data.py --records 10000 --output ./data/raw"
        ),
    )

    # ── Task 2: Run ETL pipeline ──────────────────────────────────────────────
    run_etl = BashOperator(
        task_id="run_etl_pipeline",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.processing.etl_pipeline"
        ),
        execution_timeout=timedelta(hours=1),
    )

    # ── Task 3: Demand aggregation ────────────────────────────────────────────
    demand_agg = BashOperator(
        task_id="demand_aggregation",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.processing.demand_aggregator"
        ),
    )

    # ── Task 4: Supply analysis ───────────────────────────────────────────────
    supply_analysis = BashOperator(
        task_id="supply_analysis",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.processing.supply_analyzer"
        ),
    )

    # ── Task 5: Demand forecasting ────────────────────────────────────────────
    demand_forecast = BashOperator(
        task_id="demand_forecasting",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.ml.demand_forecaster"
        ),
        execution_timeout=timedelta(hours=2),
    )

    # ── Task 6: Inventory optimization ───────────────────────────────────────
    inventory_opt = BashOperator(
        task_id="inventory_optimization",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.ml.inventory_optimizer"
        ),
    )

    # ── Task 7: Price elasticity ───────────────────────────────────────────────
    price_elasticity = BashOperator(
        task_id="price_elasticity_analysis",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.ml.price_elasticity"
        ),
    )

    # ── Task 8: Health check ──────────────────────────────────────────────────
    health_check = BashOperator(
        task_id="api_health_check",
        bash_command="curl -f http://localhost:8000/health || echo 'API not running'",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    (
        generate_data
        >> run_etl
        >> [demand_agg, supply_analysis]
        >> demand_forecast
        >> inventory_opt
        >> price_elasticity
        >> health_check
    )
