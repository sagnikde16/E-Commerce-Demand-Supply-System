"""Supply chain analytics API routes."""

from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from src.api.schemas import SupplyAnalyticsResponse, SupplyGapItem

router = APIRouter()
PROC = Path("./data/processed")


def _read(name: str) -> pd.DataFrame:
    path = PROC / name
    if not path.exists():
        raise HTTPException(503, detail=f"Dataset '{name}' not ready. Run ETL first.")
    return pd.read_parquet(str(path))


@router.get("/analytics", response_model=SupplyAnalyticsResponse)
async def supply_analytics(limit: int = Query(default=100, le=500)):
    """Supply gap analysis across all products."""
    df = _read("supply_gap")
    df = df.fillna(0)
    items = [
        SupplyGapItem(
            product_id=str(row["product_id"]),
            category=str(row.get("category", "")),
            total_available=int(row.get("total_available", 0)),
            projected_demand_7d=float(row.get("projected_demand_7d", 0)),
            supply_gap=float(row.get("supply_gap", 0)),
            days_of_supply=float(row.get("days_of_supply", 0)),
            gap_severity=str(row.get("gap_severity", "NONE")),
        )
        for _, row in df.head(limit).iterrows()
    ]

    return SupplyAnalyticsResponse(
        total_products=len(df),
        critical_gaps=int((df["gap_severity"] == "CRITICAL").sum()),
        high_risk=int((df["gap_severity"] == "HIGH").sum()),
        items=items,
    )


@router.get("/turnover")
async def inventory_turnover(limit: int = 50):
    """Inventory turnover ratios by product."""
    df = _read("inventory_turnover")
    return df.head(limit).to_dict(orient="records")


@router.get("/category-summary")
async def category_supply_summary():
    """Supply risk summary aggregated by category."""
    df = _read("category_supply_summary")
    return df.to_dict(orient="records")
