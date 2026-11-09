"""
Streamlit Dashboard for E-Commerce Demand-Supply System.
Interactive analytics and visualization powered by Plotly.
"""
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="E-Commerce Demand-Supply Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

PROC = Path("./data/processed")

# ── Helper ────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load(name: str) -> pd.DataFrame:
    """Load a processed Parquet dataset with caching."""
    path = PROC / name
    if not path.exists():
        st.warning(f"Dataset '{name}' not found. Run the ETL pipeline first.")
        return pd.DataFrame()
    return pd.read_parquet(str(path))


def metric_card(col, label: str, value, delta=None, suffix=""):
    """Render a styled metric card."""
    col.metric(label, f"{value:,}{suffix}", delta)


# ── Sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.image(
    "https://img.icons8.com/color/96/000000/shopping-cart--v2.png", width=80
)
st.sidebar.title("⚡ Demand-Supply System")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    ["📊 Overview", "📦 Inventory", "🔮 Demand Forecast",
     "🚚 Supply Chain", "💲 Price Elasticity", "👥 Customer Segments"],
)
st.sidebar.markdown("---")
st.sidebar.info("Powered by **Apache Spark** + Streamlit")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════

if page == "📊 Overview":
    st.title("🛒 E-Commerce Demand-Supply Analytics")
    st.markdown("Real-time analytics dashboard powered by **Apache Spark**.")

    orders    = load("clean_orders")
    inventory = load("clean_inventory")
    top       = load("top_sellers")

    if orders.empty:
        st.error("No data available. Please run the ETL pipeline first.")
        st.code("python -m src.processing.etl_pipeline", language="bash")
        st.stop()

    # ── KPI row ──────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    metric_card(c1, "Total Orders",    len(orders))
    metric_card(c2, "Total Revenue",   f"${orders['revenue'].sum():,.0f}" if "revenue" in orders else "N/A")
    metric_card(c3, "Products",        len(orders["product_id"].unique()) if not orders.empty else 0)
    metric_card(c4, "Avg Order Value", f"${orders['total_amount'].mean():.2f}" if "total_amount" in orders else "N/A")

    st.markdown("---")

    col_left, col_right = st.columns(2)

    # Revenue trend
    with col_left:
        st.subheader("📈 Monthly Revenue Trend")
        if "year" in orders.columns and "month" in orders.columns:
            rev = (
                orders.groupby(["year", "month"])["revenue"]
                .sum()
                .reset_index()
                .assign(period=lambda d: pd.to_datetime(d.assign(day=1)[["year", "month", "day"]]))
                .sort_values("period")
            )
            fig = px.area(rev, x="period", y="revenue",
                          labels={"period": "Month", "revenue": "Revenue ($)"},
                          color_discrete_sequence=["#636EFA"])
            st.plotly_chart(fig, use_container_width=True)

    # Category split
    with col_right:
        st.subheader("🎯 Revenue by Category")
        if "category" in orders.columns:
            cat_rev = orders.groupby("category")["revenue"].sum().reset_index()
            fig = px.pie(cat_rev, values="revenue", names="category",
                         hole=0.4, color_discrete_sequence=px.colors.qualitative.Plotly)
            st.plotly_chart(fig, use_container_width=True)

    # Channel distribution
    st.subheader("📡 Orders by Channel & Region")
    c_left, c_right = st.columns(2)
    with c_left:
        if "channel" in orders.columns:
            ch = orders["channel"].value_counts().reset_index()
            ch.columns = ["channel", "count"]
            fig = px.bar(ch, x="channel", y="count",
                         color="channel", color_discrete_sequence=px.colors.qualitative.Set2)
            st.plotly_chart(fig, use_container_width=True)
    with c_right:
        if "region" in orders.columns:
            rg = orders.groupby("region")["revenue"].sum().reset_index()
            fig = px.bar(rg, x="region", y="revenue",
                         color="region", color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(fig, use_container_width=True)

    # Top products
    if not top.empty:
        st.subheader("🏆 Top 10 Products by Revenue")
        fig = px.bar(
            top.head(10),
            x="product_id", y="total_revenue",
            color="category",
            labels={"total_revenue": "Revenue ($)", "product_id": "Product"},
        )
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: INVENTORY
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "📦 Inventory":
    st.title("📦 Inventory Management")

    inventory = load("clean_inventory")
    reorder   = load("reorder_alerts")
    abc       = load("abc_classification")

    if inventory.empty:
        st.warning("No inventory data. Run ETL pipeline.")
        st.stop()

    # Stock status KPIs
    c1, c2, c3, c4 = st.columns(4)
    if "stock_status" in inventory.columns:
        oos = int((inventory["stock_status"] == "OUT_OF_STOCK").sum())
        low = int((inventory["stock_status"] == "LOW_STOCK").sum())
        nrm = int((inventory["stock_status"] == "NORMAL").sum())
        ovr = int((inventory["stock_status"] == "OVER_STOCK").sum())
        c1.metric("Out of Stock", oos, delta=f"-{oos}", delta_color="inverse")
        c2.metric("Low Stock",    low, delta=f"-{low}", delta_color="inverse")
        c3.metric("Normal",       nrm)
        c4.metric("Overstock",    ovr)

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Stock Status Distribution")
        status_counts = inventory["stock_status"].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        fig = px.pie(status_counts, values="count", names="status",
                     color="status",
                     color_discrete_map={
                         "NORMAL":        "#00CC96",
                         "LOW_STOCK":     "#FFA15A",
                         "OUT_OF_STOCK":  "#EF553B",
                         "OVER_STOCK":    "#636EFA",
                     })
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Inventory Value by Category")
        if "category" in inventory.columns:
            cat_val = inventory.groupby("category")["inventory_value"].sum().reset_index()
            fig = px.bar(cat_val, x="category", y="inventory_value",
                         color="category",
                         labels={"inventory_value": "Value ($)"})
            st.plotly_chart(fig, use_container_width=True)

    # Reorder alerts
    if not reorder.empty:
        st.subheader("🚨 Reorder Alerts")
        st.dataframe(reorder.head(50), use_container_width=True)

    # ABC analysis
    if not abc.empty:
        st.subheader("📊 ABC Classification")
        col_a, col_b = st.columns(2)
        with col_a:
            abc_grp = abc.groupby("abc_class").agg(
                products=("product_id", "count"),
                revenue=("total_revenue", "sum"),
            ).reset_index()
            fig = px.bar(abc_grp, x="abc_class", y="revenue",
                         color="abc_class",
                         color_discrete_map={"A": "#EF553B", "B": "#FFA15A", "C": "#00CC96"},
                         labels={"revenue": "Revenue ($)"})
            st.plotly_chart(fig, use_container_width=True)
        with col_b:
            st.dataframe(abc_grp, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: DEMAND FORECAST
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "🔮 Demand Forecast":
    st.title("🔮 Demand Forecasting")

    forecasts = load("demand_forecasts")
    daily     = load("daily_demand")

    if forecasts.empty:
        st.warning("No forecast data. Run the ML forecasting job.")
        st.code("python -m src.ml.demand_forecaster", language="bash")
        st.stop()

    products = sorted(forecasts["product_id"].unique().tolist())
    selected = st.selectbox("Select Product", products)

    prod_fcst = forecasts[forecasts["product_id"] == selected].sort_values("forecast_date")
    prod_hist = daily[daily["product_id"] == selected].sort_values("order_date") if not daily.empty else pd.DataFrame()

    st.subheader(f"Demand Forecast: {selected}")

    fig = go.Figure()
    if not prod_hist.empty:
        fig.add_trace(go.Scatter(
            x=prod_hist["order_date"], y=prod_hist["units_sold"],
            name="Historical", line=dict(color="#636EFA"),
        ))
    if not prod_fcst.empty:
        fig.add_trace(go.Scatter(
            x=prod_fcst["forecast_date"], y=prod_fcst["predicted_demand"],
            name="Forecast", line=dict(color="#EF553B", dash="dash"),
        ))
    fig.update_layout(
        xaxis_title="Date", yaxis_title="Units",
        legend=dict(x=0, y=1),
    )
    st.plotly_chart(fig, use_container_width=True)

    col_l, col_r = st.columns(2)
    with col_l:
        st.metric("Avg Forecasted Demand",
                  f"{prod_fcst['predicted_demand'].mean():.0f} units/day"
                  if not prod_fcst.empty else "N/A")
    with col_r:
        st.metric("Total Forecasted (30d)",
                  f"{prod_fcst['predicted_demand'].sum():,} units"
                  if not prod_fcst.empty else "N/A")

    st.subheader("Forecast Data")
    st.dataframe(prod_fcst.reset_index(drop=True), use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: SUPPLY CHAIN
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "🚚 Supply Chain":
    st.title("🚚 Supply Chain Analytics")

    supply_gap = load("supply_gap")
    turnover   = load("inventory_turnover")
    cat_supply = load("category_supply_summary")

    if supply_gap.empty:
        st.warning("No supply data. Run the supply analyzer job.")
        st.stop()

    # Supply risk KPIs
    c1, c2, c3 = st.columns(3)
    if "gap_severity" in supply_gap.columns:
        c1.metric("Critical Gaps",  int((supply_gap["gap_severity"] == "CRITICAL").sum()))
        c2.metric("High Risk",      int((supply_gap["gap_severity"] == "HIGH").sum()))
        c3.metric("Avg Days of Supply",
                  f"{supply_gap['days_of_supply'].mean():.1f}" if "days_of_supply" in supply_gap else "N/A")

    st.markdown("---")

    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Supply Gap Severity Distribution")
        sev = supply_gap["gap_severity"].value_counts().reset_index()
        sev.columns = ["severity", "count"]
        fig = px.pie(sev, values="count", names="severity",
                     color="severity",
                     color_discrete_map={
                         "NONE":     "#00CC96",
                         "MEDIUM":   "#FFA15A",
                         "HIGH":     "#FF6692",
                         "CRITICAL": "#EF553B",
                     })
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Days of Supply by Category")
        if not cat_supply.empty and "category" in cat_supply.columns:
            fig = px.bar(cat_supply, x="category", y="avg_days_of_supply",
                         color="category",
                         labels={"avg_days_of_supply": "Avg Days of Supply"})
            st.plotly_chart(fig, use_container_width=True)

    # Inventory turnover
    if not turnover.empty:
        st.subheader("📊 Inventory Turnover by Category")
        if "category" in turnover.columns:
            cat_turn = (
                turnover.groupby("category")["inventory_turnover"]
                .mean().reset_index()
                .sort_values("inventory_turnover", ascending=False)
            )
            fig = px.bar(cat_turn, x="category", y="inventory_turnover",
                         color="inventory_turnover",
                         color_continuous_scale="RdYlGn",
                         labels={"inventory_turnover": "Turnover Ratio"})
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Supply Gap Details")
    st.dataframe(supply_gap.head(100), use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: PRICE ELASTICITY
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "💲 Price Elasticity":
    st.title("💲 Price Elasticity of Demand")

    prod_elast = load("product_elasticity")
    cat_elast  = load("category_elasticity")

    if prod_elast.empty:
        st.warning("No elasticity data. Run price elasticity job.")
        st.stop()

    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Demand Type Distribution")
        if "demand_type" in prod_elast.columns:
            dt = prod_elast["demand_type"].value_counts().reset_index()
            dt.columns = ["demand_type", "count"]
            fig = px.pie(dt, values="count", names="demand_type", hole=0.3)
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Elasticity by Category")
        if not cat_elast.empty:
            fig = px.bar(cat_elast.sort_values("elasticity"),
                         x="category", y="elasticity",
                         color="elasticity",
                         color_continuous_scale="RdYlGn",
                         labels={"elasticity": "Price Elasticity"})
            fig.add_hline(y=-1, line_dash="dash", line_color="red",
                          annotation_text="Elastic / Inelastic boundary")
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Product Elasticity & Pricing Recommendations")
    display_cols = ["product_id", "category", "elasticity", "r_squared",
                    "demand_type", "recommendation"]
    available = [c for c in display_cols if c in prod_elast.columns]
    st.dataframe(prod_elast[available].head(100), use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: CUSTOMER SEGMENTS
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "👥 Customer Segments":
    st.title("👥 Customer RFM Segmentation")

    rfm = load("customer_rfm")

    if rfm.empty:
        st.warning("No RFM data. Run demand aggregation job.")
        st.stop()

    # Segment distribution
    if "segment" in rfm.columns:
        seg_counts = rfm["segment"].value_counts().reset_index()
        seg_counts.columns = ["segment", "count"]

        col_l, col_r = st.columns(2)
        with col_l:
            st.subheader("Customer Segment Distribution")
            fig = px.pie(seg_counts, values="count", names="segment",
                         color_discrete_sequence=px.colors.qualitative.Bold)
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            st.subheader("Segment Summary")
            seg_summary = rfm.groupby("segment").agg(
                customers=("customer_id", "count"),
                avg_recency=("recency", "mean"),
                avg_frequency=("frequency", "mean"),
                avg_monetary=("monetary", "mean"),
            ).reset_index()
            st.dataframe(seg_summary, use_container_width=True)

    # RFM scatter
    st.subheader("RFM 3D Analysis")
    if all(c in rfm.columns for c in ["recency", "frequency", "monetary", "segment"]):
        fig = px.scatter_3d(
            rfm.sample(min(2000, len(rfm))),
            x="recency", y="frequency", z="monetary",
            color="segment",
            opacity=0.7,
            labels={"recency": "Recency (days)", "frequency": "Frequency", "monetary": "Monetary ($)"},
        )
        st.plotly_chart(fig, use_container_width=True)
