"""
API endpoint tests using FastAPI TestClient.
"""
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.main import app

client = TestClient(app)


class TestHealthEndpoint:
    def test_health_ok(self):
        """Health endpoint should return 200 OK."""
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "version" in data

    def test_docs_accessible(self):
        """OpenAPI docs should be accessible."""
        resp = client.get("/docs")
        assert resp.status_code == 200


class TestDemandEndpoints:
    def test_forecast_missing_data_returns_503(self):
        """Should return 503 when processed data is not available."""
        resp = client.get("/api/v1/demand/forecast/PROD0001")
        # Either 503 (data not ready) or 404 (product not found) are acceptable
        assert resp.status_code in [404, 503]

    def test_trends_endpoint_exists(self):
        """Trends endpoint should be reachable (may return 503 without data)."""
        resp = client.get("/api/v1/demand/trends")
        assert resp.status_code in [200, 503]

    def test_batch_predict_requires_body(self):
        """POST /predict without body should return 422."""
        resp = client.post("/api/v1/demand/predict")
        assert resp.status_code == 422


class TestInventoryEndpoints:
    def test_inventory_status(self):
        resp = client.get("/api/v1/inventory/status")
        assert resp.status_code in [200, 503]

    def test_reorder_alerts(self):
        resp = client.get("/api/v1/inventory/alerts/reorder")
        assert resp.status_code in [200, 503]

    def test_abc_analysis(self):
        resp = client.get("/api/v1/inventory/abc")
        assert resp.status_code in [200, 503]


class TestSupplyEndpoints:
    def test_supply_analytics(self):
        resp = client.get("/api/v1/supply/analytics")
        assert resp.status_code in [200, 503]

    def test_turnover(self):
        resp = client.get("/api/v1/supply/turnover")
        assert resp.status_code in [200, 503]


class TestProductsEndpoints:
    def test_top_sellers(self):
        resp = client.get("/api/v1/products/top-sellers")
        assert resp.status_code in [200, 503]

    def test_top_sellers_limit_param(self):
        resp = client.get("/api/v1/products/top-sellers?limit=5")
        assert resp.status_code in [200, 503]
