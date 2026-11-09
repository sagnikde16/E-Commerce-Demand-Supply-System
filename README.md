# 🛒 E-Commerce Demand-Supply System

A distributed, production-grade e-commerce demand-supply analytics platform built with **Apache Spark**, Delta Lake, Kafka, FastAPI, and Streamlit.

---

## 🏗️ Architecture

```
                     ┌──────────────────────────────────────────────────┐
                     │               Data Sources                        │
                     │  Orders | Products | Inventory | Market Signals   │
                     └────────────────────┬─────────────────────────────┘
                                          │
                     ┌────────────────────▼─────────────────────────────┐
                     │          Ingestion Layer (Kafka / CSV)            │
                     └────────────────────┬─────────────────────────────┘
                                          │
                     ┌────────────────────▼─────────────────────────────┐
                     │        Apache Spark Processing Engine             │
                     │  ┌────────────┐  ┌──────────┐  ┌─────────────┐  │
                     │  │ ETL/ELT    │  │ ML Jobs  │  │  Streaming  │  │
                     │  │ Pipelines  │  │(Forecast)│  │  Analytics  │  │
                     │  └────────────┘  └──────────┘  └─────────────┘  │
                     └────────────────────┬─────────────────────────────┘
                                          │
                     ┌────────────────────▼─────────────────────────────┐
                     │         Storage Layer (Delta Lake / Parquet)      │
                     └───────────┬────────────────────┬─────────────────┘
                                 │                    │
              ┌──────────────────▼──────┐  ┌─────────▼──────────────────┐
              │     FastAPI REST API    │  │    Streamlit Dashboard      │
              └─────────────────────────┘  └────────────────────────────┘
```

## 🚀 Features

| Feature | Description |
|---------|-------------|
| 📊 Demand Forecasting | ML-based prediction using Spark MLlib (Random Forest + Linear Regression) |
| 📦 Inventory Optimization | Automatic reorder point & safety stock calculations |
| ⚡ Real-time Streaming | Spark Structured Streaming for live order processing |
| 💲 Price Elasticity | Demand-price sensitivity analysis |
| 🔄 Supply Chain Insights | Supplier performance & lead time analytics |
| 🌐 REST API | FastAPI layer for downstream system integration |
| 📈 Dashboard | Interactive Streamlit visualization UI |
| ✈️ Orchestration | Apache Airflow DAGs for batch pipeline scheduling |
| 🐳 Docker | Full containerized deployment via Docker Compose |
| 🤖 CI/CD | GitHub Actions automated testing & linting |

---

## 🛠️ Tech Stack

- **Distributed Processing**: Apache Spark 3.5 (PySpark)
- **Stream Processing**: Spark Structured Streaming
- **Storage**: Delta Lake, Apache Parquet
- **ML**: Spark MLlib (Random Forest, Linear Regression, ALS)
- **Data Generation**: Faker, NumPy, Pandas
- **API**: FastAPI + Uvicorn
- **Dashboard**: Streamlit + Plotly
- **Orchestration**: Apache Airflow 2.x
- **Containerization**: Docker + Docker Compose
- **Testing**: pytest + pyspark
- **CI/CD**: GitHub Actions
- **Language**: Python 3.10+

---

## 📁 Project Structure

```
ecommerce-demand-supply-spark/
├── config/
│   ├── app_config.yaml          # Application configuration
│   └── spark_config.py          # Spark session factory
├── data/
│   ├── raw/                     # Raw ingested data
│   ├── processed/               # Cleaned & transformed data
│   └── sample/                  # Sample datasets for testing
├── src/
│   ├── ingestion/
│   │   ├── data_generator.py    # Synthetic data generator
│   │   └── kafka_consumer.py    # Kafka consumer (simulated)
│   ├── processing/
│   │   ├── etl_pipeline.py      # Main ETL pipeline
│   │   ├── demand_aggregator.py # Demand aggregation jobs
│   │   ├── supply_analyzer.py   # Supply-side analytics
│   │   └── streaming_job.py     # Structured Streaming job
│   ├── ml/
│   │   ├── demand_forecaster.py # ML demand forecasting
│   │   ├── price_elasticity.py  # Price elasticity model
│   │   └── inventory_optimizer.py # Inventory optimization
│   ├── api/
│   │   ├── main.py              # FastAPI application
│   │   ├── routes/              # API route handlers
│   │   └── schemas.py           # Pydantic schemas
│   ├── dashboard/
│   │   └── app.py               # Streamlit dashboard
│   └── utils/
│       ├── logger.py            # Logging utilities
│       └── helpers.py           # Helper functions
├── airflow/
│   └── dags/
│       └── demand_supply_dag.py # Airflow DAG
├── tests/
│   ├── test_etl.py              # ETL pipeline tests
│   ├── test_ml.py               # ML model tests
│   └── test_api.py              # API endpoint tests
├── notebooks/
│   └── exploration.ipynb        # EDA notebook
├── docker/
│   ├── Dockerfile               # App Dockerfile
│   └── Dockerfile.spark         # Spark worker Dockerfile
├── scripts/
│   ├── run_etl.sh               # ETL run script
│   └── generate_data.py         # Data generation script
├── docker-compose.yml           # Full stack compose
├── requirements.txt             # Python dependencies
├── setup.py                     # Package setup
├── .env.example                 # Environment template
├── .gitignore                   # Git ignore rules
└── Makefile                     # Developer shortcuts
```

---

## ⚡ Quick Start

### Prerequisites
- Python 3.10+
- Java 11+ (for Spark)
- Docker & Docker Compose (optional, for full stack)

### 1. Clone & Install

```bash
git clone https://github.com/sagnikde16/ecommerce-demand-supply-spark.git
cd ecommerce-demand-supply-spark
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your settings
```

### 3. Generate Sample Data

```bash
python scripts/generate_data.py --records 10000
```

### 4. Run ETL Pipeline

```bash
python -m src.processing.etl_pipeline
```

### 5. Train Demand Forecasting Model

```bash
python -m src.ml.demand_forecaster --train
```

### 6. Launch API

```bash
uvicorn src.api.main:app --reload --port 8000
```

### 7. Launch Dashboard

```bash
streamlit run src/dashboard/app.py
```

### 8. Full Stack with Docker

```bash
docker-compose up --build
```

---

## 🐳 Docker Services

| Service | Port | Description |
|---------|------|-------------|
| Spark Master | 8080 | Spark Web UI |
| Spark Worker | — | Processing worker |
| FastAPI | 8000 | REST API |
| Streamlit | 8501 | Dashboard UI |
| Airflow | 8081 | Workflow UI |
| Zookeeper | 2181 | Kafka coordinator |
| Kafka | 9092 | Message broker |

---

## 📊 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/v1/demand/forecast/{product_id}` | Get demand forecast |
| GET | `/api/v1/inventory/status` | Inventory overview |
| GET | `/api/v1/supply/analytics` | Supply chain KPIs |
| POST | `/api/v1/demand/predict` | Batch demand prediction |
| GET | `/api/v1/products/top-sellers` | Top selling products |
| GET | `/api/v1/alerts/reorder` | Products needing reorder |

---

## 🧪 Running Tests

```bash
# All tests
pytest tests/ -v

# ETL tests only
pytest tests/test_etl.py -v

# With coverage
pytest tests/ --cov=src --cov-report=html
```

---

## 📌 Makefile Shortcuts

```bash
make generate-data     # Generate synthetic data
make run-etl           # Run ETL pipeline
make train-model       # Train ML models
make api               # Start FastAPI server
make dashboard         # Start Streamlit dashboard
make test              # Run all tests
make docker-up         # Start all Docker services
make docker-down       # Stop all Docker services
make lint              # Run flake8 linter
```

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

## 👨‍💻 Author

**Sagnik De** — [sagnikde16](https://github.com/sagnikde16)
