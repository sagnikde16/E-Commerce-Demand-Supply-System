"""Inventory API routes."""
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException

from src.api.schemas import (
    InventoryStatusItem, InventoryStatusResponse,
    ReorderAlertItem, ReorderAlertResponse,
)

router = APIRouter()
PROC   = Path("./data/processed")


def _read(name: str) -> pd.DataFrame:
    path = PROC / name
    if not path.exists():
        raise HTTPException(503, detail=f"Dataset '{name}' not ready. Run ETL first.")
    return pd.read_parquet(str(path))


@router.get("/status", response_model=InventoryStatusResponse)
async def inventory_status(limit: int = 100):
    """Overview of current inventory across all products."""
    df = _read("clean_inventory")
    agg = (
        df.groupby("product_id")
        .agg(
            product_name=("product_name", "first"),
            category=("category", "first"),
            total_stock=("current_stock", "sum"),
            total_available=("available_stock", "sum"),
            stock_status=("stock_status", lambda x: x.mode()[0] if len(x) > 0 else "UNKNOWN"),
            reorder_point=("reorder_point", "max"),
            inventory_value=("inventory_value", "sum"),
        )
        .reset_index()
    )

    items = [
        InventoryStatusItem(**row)
        for _, row in agg.head(limit).iterrows()
    ]

    return InventoryStatusResponse(
        total_products=len(agg),
        out_of_stock=int((agg["stock_status"] == "OUT_OF_STOCK").sum()),
        low_stock=int((agg["stock_status"] == "LOW_STOCK").sum()),
        normal=int((agg["stock_status"] == "NORMAL").sum()),
        items=items,
    )


@router.get("/alerts/reorder", response_model=ReorderAlertResponse)
async def reorder_alerts():
    """Products requiring immediate reorder action."""
    df = _read("reorder_alerts")
    items = []
    for _, row in df.iterrows():
        items.append(ReorderAlertItem(
            product_id=str(row["product_id"]),
            product_name=str(row.get("product_name", "")),
            category=str(row.get("category", "")),
            min_stock=int(row.get("min_stock", 0)),
            reorder_point=int(row.get("reorder_point", 0)),
            suggested_reorder_qty=int(row.get("suggested_reorder_qty", 0)),
            worst_status=str(row.get("worst_status", "")),
            urgency=int(row.get("urgency", 2)),
        ))

    return ReorderAlertResponse(
        total_alerts=len(items),
        critical_alerts=sum(1 for i in items if i.worst_status == "OUT_OF_STOCK"),
        alerts=items,
    )


@router.get("/recommendations")
async def inventory_recommendations(limit: int = 50):
    """Combined EOQ + safety stock + ABC recommendations."""
    df = _read("inventory_recommendations")
    return df.head(limit).to_dict(orient="records")


@router.get("/abc")
async def abc_analysis():
    """ABC inventory classification."""
    df = _read("abc_classification")
    summary = df.groupby("abc_class").agg(
        count=("product_id", "count"),
        total_revenue=("total_revenue", "sum"),
    ).reset_index()
    return summary.to_dict(orient="records")
