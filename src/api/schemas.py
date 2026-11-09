"""
Pydantic schemas for API request/response validation.
"""

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    version: str
    timestamp: datetime


class DemandForecastRequest(BaseModel):
    product_ids: List[str] = Field(..., description="List of product IDs to forecast")
    horizon_days: int = Field(default=30, ge=1, le=90, description="Forecast horizon in days")


class DemandForecastItem(BaseModel):
    product_id: str
    category: str
    forecast_date: date
    predicted_demand: int


class DemandForecastResponse(BaseModel):
    product_id: str
    forecasts: List[DemandForecastItem]
    generated_at: datetime


class InventoryStatusItem(BaseModel):
    product_id: str
    product_name: Optional[str]
    category: Optional[str]
    total_stock: int
    total_available: int
    stock_status: str
    reorder_point: int
    inventory_value: float


class InventoryStatusResponse(BaseModel):
    total_products: int
    out_of_stock: int
    low_stock: int
    normal: int
    items: List[InventoryStatusItem]


class ReorderAlertItem(BaseModel):
    product_id: str
    product_name: Optional[str]
    category: Optional[str]
    min_stock: int
    reorder_point: int
    suggested_reorder_qty: int
    worst_status: str
    urgency: int


class ReorderAlertResponse(BaseModel):
    total_alerts: int
    critical_alerts: int
    alerts: List[ReorderAlertItem]


class SupplyGapItem(BaseModel):
    product_id: str
    category: Optional[str]
    total_available: int
    projected_demand_7d: Optional[float]
    supply_gap: Optional[float]
    days_of_supply: Optional[float]
    gap_severity: Optional[str]


class SupplyAnalyticsResponse(BaseModel):
    total_products: int
    critical_gaps: int
    high_risk: int
    items: List[SupplyGapItem]


class TopSellerItem(BaseModel):
    rank: int
    product_id: str
    category: Optional[str]
    total_units: int
    total_revenue: float
    avg_price: float
    order_count: int


class TopSellersResponse(BaseModel):
    items: List[TopSellerItem]
