"""
Streamlit Dashboard for Clickstream Analytics
Visualizes executive KPIs, conversion funnels, monthly revenue trends,
customer segmentation, peak hourly traffic, top cities, and engine benchmarks.
"""

import json
from pathlib import Path
import pandas as pd
import streamlit as st

# Configure page layout
st.set_page_config(
    page_title="Clickstream Big Data Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown(
    """
    <style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E293B;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.05rem;
        color: #64748B;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 16px;
        text-align: center;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Directories
PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = PROJECT_ROOT / "outputs"


@st.cache_data
def load_data():
    """Loads all batch output files into pandas DataFrames."""
    data = {}
    files = {
        "monthly": OUTPUTS_DIR / "monthly_revenue.csv",
        "top_products": OUTPUTS_DIR / "top_products_per_category.csv",
        "conversion": OUTPUTS_DIR / "conversion_rate.csv",
        "peak_hours": OUTPUTS_DIR / "peak_hours.csv",
        "top_cities": OUTPUTS_DIR / "top_cities.csv",
        "segmentation": OUTPUTS_DIR / "customer_segmentation.csv",
        "benchmark": OUTPUTS_DIR / "benchmark_results.csv",
    }

    all_exist = True
    for key, path in files.items():
        if path.exists():
            data[key] = pd.read_csv(path)
        else:
            all_exist = False
            data[key] = None

    return data, all_exist


data, all_loaded = load_data()

# Sidebar
st.sidebar.image("https://img.icons8.com/fluency/96/data-configuration.png", width=64)
st.sidebar.title("Clickstream Engine")
st.sidebar.markdown("**Architecture:** DuckDB + Polars")
st.sidebar.markdown("**Storage:** Hive-Partitioned Snappy Parquet")
st.sidebar.markdown("**Default Scale:** 5,000,000 rows (Chunked)")
st.sidebar.divider()

if not all_loaded:
    st.error("⚠️ Analytics outputs not found in `outputs/` folder!")
    st.info("Please generate the dataset and run the analytics pipeline first:\n\n"
            "```bash\n"
            "python generate_data.py\n"
            "python analytics.py\n"
            "python benchmark.py\n"
            "```")
    st.stop()

# Header
st.markdown('<div class="main-header">⚡ E-Commerce Clickstream Analytics</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-header">High-performance out-of-core streaming analytics on 5 Million events using DuckDB & Polars</div>',
    unsafe_allow_html=True,
)

# -------------------------------------------------------------
# Top Level KPIs
# -------------------------------------------------------------
conv_df = data["conversion"]
monthly_df = data["monthly"]
cities_df = data["top_cities"]

total_events = int(conv_df["total_views"].iloc[0] + conv_df["total_carts"].iloc[0] + conv_df["total_purchases"].iloc[0])
total_revenue = float(monthly_df["total_revenue"].sum())
total_orders = int(conv_df["total_purchases"].iloc[0])
conv_rate = float(conv_df["event_overall_conversion_pct"].iloc[0])
avg_order_val = total_revenue / total_orders if total_orders > 0 else 0
active_customers = int(conv_df["users_viewed"].iloc[0])

kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)
kpi1.metric("Total Events", f"{total_events:,.0f}")
kpi2.metric("Total Revenue", f"${total_revenue:,.2f}")
kpi3.metric("Total Orders", f"{total_orders:,}")
kpi4.metric("Conversion Rate", f"{conv_rate:.2f}%")
kpi5.metric("Avg Order Value", f"${avg_order_val:.2f}")
kpi6.metric("Active Users", f"{active_customers:,}")

st.divider()

# -------------------------------------------------------------
# Main Tabs
# -------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 Monthly Revenue",
    "🎯 Conversion Funnel",
    "🏆 Top Products & Categories",
    "⏰ Peak Traffic & Cities",
    "👥 Customer Segmentation",
    "⚡ Engine Benchmark",
])

# -------------------------------------------------------------
# Tab 1: Monthly Revenue
# -------------------------------------------------------------
with tab1:
    st.subheader("Monthly Revenue & Order Trends (2-Year Period)")
    col1, col2 = st.columns([2, 1])

    with col1:
        rev_chart_df = monthly_df.set_index("year_month")[["total_revenue"]]
        st.bar_chart(rev_chart_df, color="#2563EB", use_container_width=True)

    with col2:
        st.markdown("**Monthly Performance Summary**")
        orders_chart_df = monthly_df.set_index("year_month")[["total_orders"]]
        st.line_chart(orders_chart_df, color="#10B981", use_container_width=True)

    st.dataframe(
        monthly_df[[
            "year_month", "total_revenue", "total_orders", "total_events", "avg_order_value"
        ]].rename(columns={
            "year_month": "Period",
            "total_revenue": "Revenue ($)",
            "total_orders": "Orders",
            "total_events": "Total Events",
            "avg_order_value": "Avg Order Value ($)",
        }),
        use_container_width=True,
    )

# -------------------------------------------------------------
# Tab 2: Conversion Funnel
# -------------------------------------------------------------
with tab2:
    st.subheader("E-Commerce Conversion Funnel (View -> Cart -> Purchase)")
    fcol1, fcol2 = st.columns(2)

    with fcol1:
        st.markdown("#### Event-Level Funnel")
        event_funnel_df = pd.DataFrame({
            "Stage": ["1. Product Views", "2. Cart Additions", "3. Purchases"],
            "Event Count": [
                int(conv_df["total_views"].iloc[0]),
                int(conv_df["total_carts"].iloc[0]),
                int(conv_df["total_purchases"].iloc[0]),
            ],
            "Step Conversion (%)": [
                "100.0%",
                f"{conv_df['event_view_to_cart_pct'].iloc[0]:.2f}% (View → Cart)",
                f"{conv_df['event_cart_to_purchase_pct'].iloc[0]:.2f}% (Cart → Purchase)",
            ],
        })
        st.dataframe(event_funnel_df, use_container_width=True)
        st.bar_chart(event_funnel_df.set_index("Stage")["Event Count"], color="#6366F1")

    with fcol2:
        st.markdown("#### Unique User Funnel")
        user_funnel_df = pd.DataFrame({
            "Stage": ["1. Active Viewers", "2. Users with Cart", "3. Purchasing Customers"],
            "Unique Users": [
                int(conv_df["users_viewed"].iloc[0]),
                int(conv_df["users_carted"].iloc[0]),
                int(conv_df["users_purchased"].iloc[0]),
            ],
            "Step Conversion (%)": [
                "100.0%",
                f"{conv_df['user_view_to_cart_pct'].iloc[0]:.2f}%",
                f"{conv_df['user_cart_to_purchase_pct'].iloc[0]:.2f}%",
            ],
        })
        st.dataframe(user_funnel_df, use_container_width=True)
        st.bar_chart(user_funnel_df.set_index("Stage")["Unique Users"], color="#EC4899")

# -------------------------------------------------------------
# Tab 3: Top Products & Categories
# -------------------------------------------------------------
with tab3:
    st.subheader("Top 10 Products Ranked by Revenue per Category")
    top_p_df = data["top_products"]

    categories = sorted(top_p_df["category"].unique())
    selected_cat = st.selectbox("Select Product Category:", categories)

    cat_filtered = top_p_df[top_p_df["category"] == selected_cat].sort_values(by="rank")

    pcol1, pcol2 = st.columns([3, 2])
    with pcol1:
        st.bar_chart(
            cat_filtered.set_index("product_id")["total_revenue"],
            color="#F59E0B",
            use_container_width=True,
        )
    with pcol2:
        st.dataframe(
            cat_filtered[["rank", "product_id", "purchase_count", "total_revenue", "avg_unit_price"]].rename(columns={
                "rank": "Rank",
                "product_id": "Product ID",
                "purchase_count": "Units Sold",
                "total_revenue": "Revenue ($)",
                "avg_unit_price": "Avg Price ($)",
            }),
            use_container_width=True,
        )

# -------------------------------------------------------------
# Tab 4: Peak Hours & Cities
# -------------------------------------------------------------
with tab4:
    st.subheader("Traffic Distribution & Geographic Analysis")
    hcol1, hcol2 = st.columns(2)

    with hcol1:
        st.markdown("#### Hourly Traffic & Shopping Activity (0-23 hrs)")
        peak_df = data["peak_hours"].copy()
        peak_df["hour_str"] = peak_df["hour_of_day"].astype(int).map(lambda h: f"{h:02d}:00")
        peak_chart_data = peak_df.set_index("hour_str")[["total_events", "purchase_count"]]
        peak_chart_data.columns = ["Total Events", "Purchases"]
        st.area_chart(peak_chart_data, color=["#6366F1", "#10B981"], use_container_width=True)

    with hcol2:
        st.markdown("#### Top Cities Ranked by Revenue")
        st.bar_chart(cities_df.set_index("city")["total_revenue"], color="#06B6D4", use_container_width=True)

    col_tbl1, col_tbl2 = st.columns(2)
    with col_tbl1:
        st.markdown("##### Hourly Traffic Breakdown")
        st.dataframe(
            peak_df[[
                "hour_str", "total_events", "view_count", "cart_count", "purchase_count", "total_revenue", "hourly_purchase_pct"
            ]].rename(columns={
                "hour_str": "Hour",
                "total_events": "Total Events",
                "view_count": "Views",
                "cart_count": "Carts",
                "purchase_count": "Purchases",
                "total_revenue": "Revenue ($)",
                "hourly_purchase_pct": "Purchase Rate (%)",
            }),
            use_container_width=True,
        )

    with col_tbl2:
        st.markdown("##### City Performance Table")
        st.dataframe(
            cities_df[[
                "city", "total_revenue", "total_orders", "active_users", "avg_order_value", "revenue_per_active_user"
            ]].rename(columns={
                "city": "City",
                "total_revenue": "Total Revenue ($)",
                "total_orders": "Orders",
                "active_users": "Active Users",
                "avg_order_value": "Avg Order ($)",
                "revenue_per_active_user": "Rev / User ($)",
            }),
            use_container_width=True,
        )

# -------------------------------------------------------------
# Tab 5: Customer Segmentation
# -------------------------------------------------------------
with tab5:
    st.subheader("Customer Value Segmentation (RFM Spend Tiers)")
    seg_df = data["segmentation"]

    scol1, scol2 = st.columns([1, 1])
    with scol1:
        st.markdown("#### Customer Share by Segment")
        st.bar_chart(seg_df.set_index("segment")["customer_count"], color="#8B5CF6", use_container_width=True)

    with scol2:
        st.markdown("#### Revenue Contribution by Segment")
        st.bar_chart(seg_df.set_index("segment")["total_segment_revenue"], color="#10B981", use_container_width=True)

    st.dataframe(
        seg_df.rename(columns={
            "segment": "Tier",
            "customer_count": "Customers",
            "pct_of_customers": "Customer Share (%)",
            "total_segment_revenue": "Total Revenue ($)",
            "avg_spend_per_customer": "Avg Customer Spend ($)",
            "avg_orders_per_customer": "Avg Orders / Customer",
        }),
        use_container_width=True,
    )

# -------------------------------------------------------------
# Tab 6: Engine Benchmark
# -------------------------------------------------------------
with tab6:
    st.subheader("Performance Benchmark: Pandas vs DuckDB on Identical Data")
    b_df = data["benchmark"]

    st.markdown("""
    This benchmark compares **in-memory Pandas** against **out-of-core DuckDB** on the **EXACT same data**
    at **1,000,000**, **2,500,000**, and **5,000,000** rows.
    *(10M rows was evaluated and skipped due to host RAM constraints: Pandas at 5M rows already reached ~1.19 GB peak RSS).*
    """)

    # Query selector for chart filtering
    benchmark_queries = sorted(b_df["query"].unique())
    selected_bench_query = st.selectbox("Select Analytical Query to Inspect:", ["All Queries Combined"] + benchmark_queries)

    st.markdown("#### Measured Benchmark Results Table")
    st.dataframe(
        b_df.rename(columns={
            "engine": "Engine",
            "rows": "Rows Processed",
            "query": "Query Name",
            "time": "Wall-Clock Time (s)",
            "peak_memory": "Peak Process RSS (MB)",
        }),
        use_container_width=True,
    )

    # Prepare data for plotting
    plot_df = b_df.copy()
    if selected_bench_query != "All Queries Combined":
        plot_df = plot_df[plot_df["query"] == selected_bench_query]

    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        st.markdown("#### ⏱️ Wall-Clock Execution Time vs Rows")
        time_pivot = plot_df.pivot_table(index="rows", columns="engine", values="time", aggfunc="mean").reset_index()
        time_pivot["rows_str"] = time_pivot["rows"].map(lambda x: f"{x/1_000_000:.1f}M rows")
        time_chart_data = time_pivot.set_index("rows_str")[["duckdb", "pandas"]]
        time_chart_data.columns = ["DuckDB (s)", "Pandas (s)"]
        st.line_chart(time_chart_data, color=["#10B981", "#EF4444"], use_container_width=True)

    with col_chart2:
        st.markdown("#### 💾 Peak Memory (Process RSS) vs Rows")
        mem_pivot = plot_df.pivot_table(index="rows", columns="engine", values="peak_memory", aggfunc="max").reset_index()
        mem_pivot["rows_str"] = mem_pivot["rows"].map(lambda x: f"{x/1_000_000:.1f}M rows")
        mem_chart_data = mem_pivot.set_index("rows_str")[["duckdb", "pandas"]]
        mem_chart_data.columns = ["DuckDB Peak RSS (MB)", "Pandas Peak RSS (MB)"]
        st.line_chart(mem_chart_data, color=["#10B981", "#EF4444"], use_container_width=True)

    st.markdown("---")
    st.markdown("#### 🔬 Honest Analysis & Architecture Takeaways")
    ana_col1, ana_col2 = st.columns(2)

    with ana_col1:
        st.info("""
        **Where Pandas is Faster:**
        - **Low-cardinality in-memory aggregations (Monthly Revenue):** When data is already fully materialized in RAM, Pandas simple groupby on 25 periods runs in **0.08s - 0.33s** vs DuckDB's **0.13s - 0.58s**.
        - Why? DuckDB must decompress snappy blocks from disk across 50 to 250 Parquet files, whereas Pandas queries already-hot RAM.
        """)

    with ana_col2:
        st.success("""
        **Where DuckDB Excels:**
        - **Distinct Counts / High Cardinality (Top Cities):** DuckDB's multi-threaded hash aggregation outperforms Pandas `nunique` by **1.5x to 1.95x** (e.g., 1.98s vs 2.99s at 5M).
        - **Hourly Aggregation (Peak Hours):** DuckDB consistently beats Pandas (0.76s vs 0.89s at 5M).
        - **Memory Scalability:** DuckDB memory stays low (~124MB to 470MB) via out-of-core streaming. Pandas peaked at **1,188.7 MB** at 5M rows, making 10M rows infeasible on low-RAM machines.
        """)

