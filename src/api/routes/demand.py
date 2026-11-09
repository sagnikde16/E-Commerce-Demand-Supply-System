"""
Demand-related API routes.
Reads pre-computed Parquet results (produced by Spark jobs).
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from loguru import logger

from src.api.schemas import (
    DemandForecastItem,
    DemandForecastRequest,
    DemandForecastResponse,
)

router = APIRouter()
PROC = Path("./data/processed")


def _read_parquet(name: str) -> pd.DataFrame:
    path = PROC / name
    if not path.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Dataset '{name}' not found. Run the ETL pipeline first.",
        )
    return pd.read_parquet(str(path))


@router.get("/forecast/{product_id}", response_model=DemandForecastResponse)
async def get_demand_forecast(
    product_id: str,
    horizon_days: int = Query(default=30, ge=1, le=90),
):
    """Get demand forecast for a specific product."""
    try:
        df = _read_parquet("demand_forecasts")
        product_df = df[df["product_id"] == product_id].head(horizon_days)

        if product_df.empty:
            raise HTTPException(status_code=404, detail=f"No forecast for product {product_id}")

        items = [
            DemandForecastItem(
                product_id=row["product_id"],
                category=row.get("category", "Unknown"),
                forecast_date=row["forecast_date"],
                predicted_demand=int(row.get("predicted_demand", 0)),
            )
            for _, row in product_df.iterrows()
        ]

        return DemandForecastResponse(
            product_id=product_id,
            forecasts=items,
            generated_at=datetime.utcnow(),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching forecast for {product_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/predict", response_model=list)
async def batch_predict(request: DemandForecastRequest):
    """Batch demand prediction for multiple products."""
    try:
        df = _read_parquet("demand_forecasts")
        results = []

        for pid in request.product_ids:
            sub = df[df["product_id"] == pid].head(request.horizon_days)
            if not sub.empty:
                results.append(
                    {
                        "product_id": pid,
                        "avg_predicted_demand": round(sub["predicted_demand"].mean(), 1),
                        "total_predicted_demand": int(sub["predicted_demand"].sum()),
                        "forecast_days": len(sub),
                    }
                )

        return results
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends")
async def demand_trends(
    category: Optional[str] = Query(default=None),
    months: int = Query(default=6, ge=1, le=24),
):
    """Get demand trend data for charting."""
    try:
        df = _read_parquet("category_demand")
        if category:
            df = df[df["category"] == category]

        df = df.sort_values(["year", "month"]).tail(months)
        return df.to_dict(orient="records")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
