"""Products and top-sellers API routes."""
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from src.api.schemas import TopSellerItem, TopSellersResponse

router = APIRouter()
PROC   = Path("./data/processed")


def _read(name: str) -> pd.DataFrame:
    path = PROC / name
    if not path.exists():
        raise HTTPException(503, detail=f"Dataset '{name}' not ready. Run ETL first.")
    return pd.read_parquet(str(path))


@router.get("/top-sellers", response_model=TopSellersResponse)
async def top_sellers(limit: int = Query(default=20, ge=1, le=100)):
    """Top N products by revenue."""
    df = _read("top_sellers").head(limit)
    items = [
        TopSellerItem(
            rank=int(row.get("rank", i + 1)),
            product_id=str(row["product_id"]),
            category=str(row.get("category", "")),
            total_units=int(row.get("total_units", 0)),
            total_revenue=float(row.get("total_revenue", 0)),
            avg_price=float(row.get("avg_price", 0)),
            order_count=int(row.get("order_count", 0)),
        )
        for i, (_, row) in enumerate(df.iterrows())
    ]
    return TopSellersResponse(items=items)


@router.get("/elasticity")
async def price_elasticity(limit: int = 50):
    """Price elasticity estimates per product."""
    df = _read("product_elasticity")
    return df.head(limit).to_dict(orient="records")


@router.get("/regional-demand")
async def regional_demand(region: str = None, category: str = None):
    """Demand split by region and category."""
    df = _read("regional_demand")
    if region:
        df = df[df["region"] == region]
    if category:
        df = df[df["category"] == category]
    return df.to_dict(orient="records")
