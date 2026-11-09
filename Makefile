.PHONY: generate-data run-etl demand-agg supply-analysis train-model inventory-opt \
        price-elasticity api dashboard test lint docker-up docker-down clean help

PYTHON=python
SRC=src

help:
	@echo ""
	@echo "  E-Commerce Demand-Supply System — Makefile"
	@echo "  ─────────────────────────────────────────────────────"
	@echo "  make generate-data       Generate synthetic data"
	@echo "  make run-etl             Run the full ETL pipeline"
	@echo "  make demand-agg          Run demand aggregation job"
	@echo "  make supply-analysis     Run supply chain analytics"
	@echo "  make train-model         Train demand forecasting model"
	@echo "  make inventory-opt       Run inventory optimization"
	@echo "  make price-elasticity    Run price elasticity analysis"
	@echo "  make run-all             Run complete pipeline end-to-end"
	@echo "  make api                 Start FastAPI server"
	@echo "  make dashboard           Start Streamlit dashboard"
	@echo "  make test                Run all tests"
	@echo "  make lint                Run linters"
	@echo "  make docker-up           Start Docker Compose services"
	@echo "  make docker-down         Stop Docker Compose services"
	@echo "  make clean               Remove generated data"
	@echo ""

generate-data:
	$(PYTHON) scripts/generate_data.py --records 50000

run-etl:
	$(PYTHON) -m src.processing.etl_pipeline

demand-agg:
	$(PYTHON) -m src.processing.demand_aggregator

supply-analysis:
	$(PYTHON) -m src.processing.supply_analyzer

train-model:
	$(PYTHON) -m src.ml.demand_forecaster

inventory-opt:
	$(PYTHON) -m src.ml.inventory_optimizer

price-elasticity:
	$(PYTHON) -m src.ml.price_elasticity

run-all: generate-data run-etl demand-agg supply-analysis train-model inventory-opt price-elasticity
	@echo "✅ Full pipeline complete"

api:
	uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

dashboard:
	streamlit run src/dashboard/app.py

stream:
	$(PYTHON) -m src.processing.streaming_job &
	$(PYTHON) -m src.ingestion.kafka_consumer

test:
	pytest tests/ -v --tb=short

test-cov:
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term

lint:
	flake8 src/ tests/ --max-line-length=120 --ignore=E203,W503
	black --check src/ tests/
	isort --check-only src/ tests/

format:
	black src/ tests/
	isort src/ tests/

docker-up:
	docker-compose up --build -d
	@echo "Services starting..."
	@echo "  Spark UI:   http://localhost:8080"
	@echo "  API:        http://localhost:8000/docs"
	@echo "  Dashboard:  http://localhost:8501"
	@echo "  Airflow:    http://localhost:8081"

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

clean:
	rm -rf data/raw/* data/processed/* data/models/* data/checkpoints/*
	rm -rf logs/*.log
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete

install:
	pip install -r requirements.txt
