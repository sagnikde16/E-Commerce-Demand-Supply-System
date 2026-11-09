"""
Synthetic e-commerce data generator.
Produces realistic orders, products, inventory, and market signal data.
"""
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from faker import Faker
from loguru import logger

fake = Faker()
rng = np.random.default_rng(42)

# ── Product catalogue ────────────────────────────────────────────────────────
CATEGORIES = {
    "Electronics":   {"base_price": (200, 2000), "volatility": 0.3},
    "Clothing":      {"base_price": (20,  300),  "volatility": 0.5},
    "Home & Garden": {"base_price": (15,  500),  "volatility": 0.2},
    "Books":         {"base_price": (5,   80),   "volatility": 0.1},
    "Sports":        {"base_price": (30,  600),  "volatility": 0.4},
    "Toys":          {"base_price": (10,  200),  "volatility": 0.6},
    "Food & Grocery":{"base_price": (2,   100),  "volatility": 0.3},
    "Automotive":    {"base_price": (50,  1000), "volatility": 0.2},
}

SEASONS = {1: "Winter", 2: "Winter", 3: "Spring", 4: "Spring",
           5: "Spring", 6: "Summer", 7: "Summer", 8: "Summer",
           9: "Autumn", 10: "Autumn", 11: "Winter", 12: "Winter"}

SEASONAL_MULTIPLIERS = {
    "Electronics":    {"Winter": 1.5, "Spring": 0.9, "Summer": 0.8, "Autumn": 1.2},
    "Clothing":       {"Winter": 1.3, "Spring": 1.4, "Summer": 1.1, "Autumn": 1.0},
    "Home & Garden":  {"Winter": 0.7, "Spring": 1.5, "Summer": 1.3, "Autumn": 0.9},
    "Books":          {"Winter": 1.2, "Spring": 1.0, "Summer": 0.9, "Autumn": 1.1},
    "Sports":         {"Winter": 0.8, "Spring": 1.3, "Summer": 1.6, "Autumn": 1.0},
    "Toys":           {"Winter": 1.8, "Spring": 0.8, "Summer": 0.9, "Autumn": 1.1},
    "Food & Grocery": {"Winter": 1.1, "Spring": 1.0, "Summer": 1.2, "Autumn": 1.0},
    "Automotive":     {"Winter": 0.9, "Spring": 1.3, "Summer": 1.1, "Autumn": 1.0},
}

SUPPLIERS = [
    {"supplier_id": f"SUP{i:03d}", "name": fake.company(),
     "lead_time_days": random.randint(3, 21),
     "reliability_score": round(random.uniform(0.7, 1.0), 2)}
    for i in range(1, 21)
]


def generate_products(n: int = 200) -> pd.DataFrame:
    """Generate a product catalogue."""
    rows = []
    for i in range(1, n + 1):
        category = random.choice(list(CATEGORIES.keys()))
        cfg = CATEGORIES[category]
        base_price = round(rng.uniform(*cfg["base_price"]), 2)
        supplier = random.choice(SUPPLIERS)
        rows.append({
            "product_id":       f"PROD{i:04d}",
            "product_name":     fake.catch_phrase(),
            "category":         category,
            "subcategory":      fake.word().capitalize(),
            "brand":            fake.company(),
            "sku":              fake.bothify(text="??-####-??").upper(),
            "unit_price":       base_price,
            "cost_price":       round(base_price * rng.uniform(0.4, 0.7), 2),
            "weight_kg":        round(rng.uniform(0.1, 20.0), 2),
            "supplier_id":      supplier["supplier_id"],
            "supplier_name":    supplier["name"],
            "lead_time_days":   supplier["lead_time_days"],
            "reorder_point":    random.randint(50, 300),
            "reorder_quantity": random.randint(100, 1000),
            "created_at":       fake.date_between(start_date="-2y", end_date="-6m"),
        })
    return pd.DataFrame(rows)


def generate_orders(
    products_df: pd.DataFrame,
    n: int = 50_000,
    start_date: str = "2023-01-01",
    end_date: str = "2024-12-31",
) -> pd.DataFrame:
    """Generate synthetic order records with realistic demand patterns."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end   = datetime.strptime(end_date,   "%Y-%m-%d")
    total_days = (end - start).days

    rows = []
    for _ in range(n):
        # Skew towards recent dates (demand growth)
        day_offset = int(rng.beta(2, 1) * total_days)
        order_date = start + timedelta(days=day_offset)

        product = products_df.sample(1).iloc[0]
        category = product["category"]
        season   = SEASONS[order_date.month]
        seasonal_mult = SEASONAL_MULTIPLIERS[category][season]

        # Weekend boost
        weekend_mult = 1.3 if order_date.weekday() >= 5 else 1.0

        # Base quantity with noise
        base_qty = max(1, int(rng.poisson(3) * seasonal_mult * weekend_mult))
        discount = round(rng.choice([0.0, 0.05, 0.10, 0.15, 0.20, 0.25], p=[0.5, 0.15, 0.15, 0.1, 0.07, 0.03]), 2)
        unit_price = round(product["unit_price"] * (1 - discount), 2)

        status_weights = [0.75, 0.10, 0.08, 0.05, 0.02]
        status = rng.choice(["delivered", "shipped", "processing", "cancelled", "returned"],
                            p=status_weights)

        rows.append({
            "order_id":       fake.uuid4(),
            "product_id":     product["product_id"],
            "category":       category,
            "customer_id":    f"CUST{random.randint(1, 10000):05d}",
            "order_date":     order_date.date(),
            "order_datetime": order_date + timedelta(
                hours=random.randint(0, 23), minutes=random.randint(0, 59)
            ),
            "quantity":       base_qty,
            "unit_price":     unit_price,
            "discount":       discount,
            "total_amount":   round(unit_price * base_qty, 2),
            "status":         status,
            "channel":        rng.choice(["web", "mobile", "marketplace"], p=[0.5, 0.35, 0.15]),
            "region":         rng.choice(["North", "South", "East", "West", "Central"]),
            "day_of_week":    order_date.weekday(),
            "month":          order_date.month,
            "year":           order_date.year,
            "week_of_year":   order_date.isocalendar()[1],
            "is_weekend":     int(order_date.weekday() >= 5),
            "season":         season,
        })
    return pd.DataFrame(rows)


def generate_inventory(products_df: pd.DataFrame) -> pd.DataFrame:
    """Generate current inventory snapshot per product and warehouse."""
    warehouses = ["WH-NORTH", "WH-SOUTH", "WH-EAST", "WH-WEST", "WH-CENTRAL"]
    rows = []
    for _, product in products_df.iterrows():
        for wh in warehouses:
            current_stock = random.randint(0, 2000)
            rows.append({
                "product_id":        product["product_id"],
                "warehouse_id":      wh,
                "current_stock":     current_stock,
                "reserved_stock":    random.randint(0, min(50, current_stock)),
                "available_stock":   max(0, current_stock - random.randint(0, 50)),
                "reorder_point":     product["reorder_point"],
                "reorder_quantity":  product["reorder_quantity"],
                "needs_reorder":     current_stock <= product["reorder_point"],
                "last_updated":      fake.date_time_between(start_date="-7d", end_date="now"),
                "unit_cost":         product["cost_price"],
                "total_value":       round(current_stock * product["cost_price"], 2),
            })
    return pd.DataFrame(rows)


def generate_market_signals(
    products_df: pd.DataFrame,
    n: int = 5000,
) -> pd.DataFrame:
    """Generate external market demand signals (web traffic, search trends, etc.)."""
    rows = []
    for _ in range(n):
        product = products_df.sample(1).iloc[0]
        signal_date = fake.date_between(start_date="-90d", end_date="today")
        rows.append({
            "signal_id":         fake.uuid4(),
            "product_id":        product["product_id"],
            "category":          product["category"],
            "signal_date":       signal_date,
            "search_volume":     random.randint(100, 50000),
            "social_mentions":   random.randint(0, 5000),
            "competitor_price":  round(product["unit_price"] * rng.uniform(0.8, 1.3), 2),
            "market_demand_idx": round(rng.uniform(0.5, 2.5), 2),
            "trend_direction":   rng.choice(["up", "down", "stable"], p=[0.4, 0.3, 0.3]),
            "sentiment_score":   round(rng.uniform(-1.0, 1.0), 3),
        })
    return pd.DataFrame(rows)


def generate_suppliers_data() -> pd.DataFrame:
    """Return supplier master data as a DataFrame."""
    return pd.DataFrame(SUPPLIERS)


def save_to_parquet(df: pd.DataFrame, path: str, partition_cols: Optional[list] = None) -> None:
    """Save a DataFrame to Parquet format."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    if partition_cols:
        df.to_parquet(path, partition_cols=partition_cols, index=False, engine="pyarrow")
    else:
        df.to_parquet(path, index=False, engine="pyarrow")
    logger.info(f"Saved {len(df):,} rows → {path}")


def save_to_csv(df: pd.DataFrame, path: str) -> None:
    """Save a DataFrame to CSV format."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df):,} rows → {path}")


def run_generation(output_dir: str = "./data/raw", n_orders: int = 50_000) -> dict:
    """
    Generate all datasets and persist them.

    Returns:
        Dictionary of {dataset_name: pd.DataFrame}.
    """
    logger.info("Starting synthetic data generation …")
    output = Path(output_dir)

    # Products
    logger.info("Generating products …")
    products = generate_products(200)
    save_to_parquet(products, str(output / "products.parquet"))
    save_to_csv(products, str(output / "products.csv"))

    # Orders
    logger.info(f"Generating {n_orders:,} orders …")
    orders = generate_orders(products, n=n_orders)
    save_to_parquet(orders, str(output / "orders.parquet"), partition_cols=["year", "month"])
    save_to_csv(orders, str(output / "orders.csv"))

    # Inventory
    logger.info("Generating inventory snapshots …")
    inventory = generate_inventory(products)
    save_to_parquet(inventory, str(output / "inventory.parquet"))
    save_to_csv(inventory, str(output / "inventory.csv"))

    # Market signals
    logger.info("Generating market signals …")
    signals = generate_market_signals(products, n=5000)
    save_to_parquet(signals, str(output / "market_signals.parquet"))
    save_to_csv(signals, str(output / "market_signals.csv"))

    # Suppliers
    suppliers = generate_suppliers_data()
    save_to_parquet(suppliers, str(output / "suppliers.parquet"))
    save_to_csv(suppliers, str(output / "suppliers.csv"))

    logger.success("Data generation complete.")
    return {
        "products": products,
        "orders": orders,
        "inventory": inventory,
        "market_signals": signals,
        "suppliers": suppliers,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data")
    parser.add_argument("--records", type=int, default=50_000, help="Number of order records")
    parser.add_argument("--output", type=str, default="./data/raw", help="Output directory")
    args = parser.parse_args()
    run_generation(output_dir=args.output, n_orders=args.records)
