"""
Clickstream Analytics Pipeline
Executes batch analytical queries using DuckDB + Polars over Hive-partitioned Snappy Parquet.
Saves all query results to both CSV and Parquet formats in outputs/.
"""

import os
import sys
import time
from pathlib import Path
import duckdb
import polars as pl

# Ensure UTF-8 output encoding for console
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import config


def get_duckdb_connection():
    """Initializes DuckDB with optimized multi-threaded, out-of-core memory settings."""
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")
    con.execute("PRAGMA preserve_insertion_order=false;")
    return con


def run_analytics_pipeline(data_path: Path = config.DATA_DIR, output_dir: Path = config.OUTPUTS_DIR):
    print("=" * 70)
    print(" BATCH CLICKSTREAM ANALYTICS ENGINE (DuckDB + Polars)")
    print("=" * 70)

    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_glob = f"{data_path.as_posix()}/**/*.parquet"

    # Verify input data exists
    parquet_files = list(data_path.glob("**/*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {data_path}. Please run generate_data.py first.")

    total_size_mb = sum(f.stat().st_size for f in parquet_files) / (1024 ** 2)
    print(f"[*] Input Data: {len(parquet_files)} partition files ({total_size_mb:.2f} MB compressed)")
    print(f"[*] Dataset glob: {parquet_glob}")
    print(f"[*] Destination: {output_dir}")
    print("=" * 70)

    con = get_duckdb_connection()
    t_start_total = time.time()
    results = {}

    def save_result(df_arrow, name: str):
        """Converts Arrow table to Polars DataFrame and writes CSV and Parquet."""
        pldf = pl.from_arrow(df_arrow)
        csv_path = output_dir / f"{name}.csv"
        parquet_path = output_dir / f"{name}.parquet"
        pldf.write_csv(csv_path)
        pldf.write_parquet(parquet_path, compression="snappy")
        results[name] = pldf
        return pldf

    # -------------------------------------------------------------
    # Query 1: Monthly Revenue & Order Volume
    # -------------------------------------------------------------
    print("\n[Query 1/6] Computing Monthly Revenue Trend...")
    t0 = time.time()
    q1 = f"""
    SELECT
        year,
        month,
        printf('%04d-%02d', year, month) AS year_month,
        COUNT(*) FILTER (WHERE event_type = 'purchase') AS total_orders,
        COUNT(*) AS total_events,
        ROUND(SUM(amount), 2) AS total_revenue,
        ROUND(AVG(amount) FILTER (WHERE event_type = 'purchase'), 2) AS avg_order_value
    FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    GROUP BY year, month
    ORDER BY year, month
    """
    tbl1 = con.sql(q1).to_arrow_table()
    pldf1 = save_result(tbl1, "monthly_revenue")
    print(f"    Completed in {time.time() - t0:.2f}s | {len(pldf1)} monthly periods calculated.")

    # -------------------------------------------------------------
    # Query 2: Top 10 Products Per Category (Ranked by Revenue)
    # -------------------------------------------------------------
    print("\n[Query 2/6] Computing Top 10 Products Per Category...")
    t0 = time.time()
    q2 = f"""
    WITH product_sales AS (
        SELECT
            category,
            product_id,
            COUNT(*) AS purchase_count,
            ROUND(SUM(amount), 2) AS total_revenue,
            ROUND(AVG(amount), 2) AS avg_unit_price
        FROM read_parquet('{parquet_glob}', hive_partitioning=true)
        WHERE event_type = 'purchase'
        GROUP BY category, product_id
    ),
    ranked_products AS (
        SELECT
            category,
            product_id,
            purchase_count,
            total_revenue,
            avg_unit_price,
            DENSE_RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS rank
        FROM product_sales
    )
    SELECT
        category,
        rank,
        product_id,
        purchase_count,
        total_revenue,
        avg_unit_price
    FROM ranked_products
    WHERE rank <= 10
    ORDER BY category, rank
    """
    tbl2 = con.sql(q2).to_arrow_table()
    pldf2 = save_result(tbl2, "top_products_per_category")
    print(f"    Completed in {time.time() - t0:.2f}s | {len(pldf2)} top product entries.")

    # -------------------------------------------------------------
    # Query 3: Conversion Funnel (view -> cart -> purchase)
    # -------------------------------------------------------------
    print("\n[Query 3/6] Computing Conversion Funnel (view -> cart -> purchase)...")
    t0 = time.time()
    q3 = f"""
    WITH funnel_events AS (
        SELECT
            COUNT(*) FILTER (WHERE event_type = 'view') AS total_views,
            COUNT(*) FILTER (WHERE event_type = 'cart') AS total_carts,
            COUNT(*) FILTER (WHERE event_type = 'purchase') AS total_purchases,
            COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'view') AS users_viewed,
            COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'cart') AS users_carted,
            COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'purchase') AS users_purchased
        FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    )
    SELECT
        total_views,
        total_carts,
        total_purchases,
        users_viewed,
        users_carted,
        users_purchased,
        ROUND(total_carts * 100.0 / NULLIF(total_views, 0), 2) AS event_view_to_cart_pct,
        ROUND(total_purchases * 100.0 / NULLIF(total_carts, 0), 2) AS event_cart_to_purchase_pct,
        ROUND(total_purchases * 100.0 / NULLIF(total_views, 0), 2) AS event_overall_conversion_pct,
        ROUND(users_carted * 100.0 / NULLIF(users_viewed, 0), 2) AS user_view_to_cart_pct,
        ROUND(users_purchased * 100.0 / NULLIF(users_carted, 0), 2) AS user_cart_to_purchase_pct,
        ROUND(users_purchased * 100.0 / NULLIF(users_viewed, 0), 2) AS user_overall_conversion_pct
    FROM funnel_events
    """
    tbl3 = con.sql(q3).to_arrow_table()
    pldf3 = save_result(tbl3, "conversion_rate")
    print(f"    Completed in {time.time() - t0:.2f}s | Conversion rates computed.")

    # -------------------------------------------------------------
    # Query 4: Peak Hours Analysis (Hourly Distribution)
    # -------------------------------------------------------------
    print("\n[Query 4/6] Computing Hourly Traffic and Peak Shopping Times...")
    t0 = time.time()
    q4 = f"""
    SELECT
        EXTRACT(HOUR FROM timestamp) AS hour_of_day,
        COUNT(*) AS total_events,
        COUNT(*) FILTER (WHERE event_type = 'view') AS view_count,
        COUNT(*) FILTER (WHERE event_type = 'cart') AS cart_count,
        COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchase_count,
        ROUND(SUM(amount), 2) AS total_revenue,
        ROUND(COUNT(*) FILTER (WHERE event_type = 'purchase') * 100.0 / COUNT(*), 2) AS hourly_purchase_pct
    FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    GROUP BY hour_of_day
    ORDER BY hour_of_day
    """
    tbl4 = con.sql(q4).to_arrow_table()
    pldf4 = save_result(tbl4, "peak_hours")
    print(f"    Completed in {time.time() - t0:.2f}s | 24 hourly buckets generated.")

    # -------------------------------------------------------------
    # Query 5: Top Cities by Revenue & Orders
    # -------------------------------------------------------------
    print("\n[Query 5/6] Computing City Revenue & Performance...")
    t0 = time.time()
    q5 = f"""
    SELECT
        city,
        ROUND(SUM(amount), 2) AS total_revenue,
        COUNT(*) FILTER (WHERE event_type = 'purchase') AS total_orders,
        COUNT(*) AS total_events,
        COUNT(DISTINCT user_id) AS active_users,
        ROUND(AVG(amount) FILTER (WHERE event_type = 'purchase'), 2) AS avg_order_value,
        ROUND(SUM(amount) / COUNT(DISTINCT user_id), 2) AS revenue_per_active_user
    FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    GROUP BY city
    ORDER BY total_revenue DESC
    """
    tbl5 = con.sql(q5).to_arrow_table()
    pldf5 = save_result(tbl5, "top_cities")
    print(f"    Completed in {time.time() - t0:.2f}s | {len(pldf5)} cities analyzed.")

    # -------------------------------------------------------------
    # Query 6: Customer Segmentation (High, Medium, Low Spenders)
    # -------------------------------------------------------------
    print("\n[Query 6/6] Computing Customer Segmentation (High, Medium, Low spenders)...")
    t0 = time.time()
    q6 = f"""
    WITH customer_spend AS (
        SELECT
            user_id,
            city,
            COUNT(*) FILTER (WHERE event_type = 'purchase') AS total_purchases,
            ROUND(SUM(amount), 2) AS total_spend
        FROM read_parquet('{parquet_glob}', hive_partitioning=true)
        GROUP BY user_id, city
    ),
    segmented_customers AS (
        SELECT
            user_id,
            city,
            total_purchases,
            total_spend,
            CASE
                WHEN total_spend >= 1000.0 THEN 'High Spenders (>= $1,000)'
                WHEN total_spend >= 250.0 THEN 'Medium Spenders ($250 - $999)'
                WHEN total_spend > 0.0 THEN 'Low Spenders (< $250)'
                ELSE 'Browsing Only ($0)'
            END AS segment
        FROM customer_spend
    )
    SELECT
        segment,
        COUNT(*) AS customer_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM segmented_customers), 2) AS pct_of_customers,
        ROUND(SUM(total_spend), 2) AS total_segment_revenue,
        ROUND(AVG(total_spend), 2) AS avg_spend_per_customer,
        ROUND(AVG(total_purchases), 2) AS avg_orders_per_customer
    FROM segmented_customers
    GROUP BY segment
    ORDER BY total_segment_revenue DESC
    """
    tbl6 = con.sql(q6).to_arrow_table()
    pldf6 = save_result(tbl6, "customer_segmentation")
    print(f"    Completed in {time.time() - t0:.2f}s | Customer segmentation summary generated.")

    total_pipeline_time = time.time() - t_start_total

    print("\n" + "=" * 70)
    print(" ANALYTICS PIPELINE FINISHED SUCCESSFULLY")
    print(f"[*] Total analytical runtime: {total_pipeline_time:.2f} seconds")
    print(f"[*] Outputs stored in:       {output_dir}")
    print("=" * 70)

    # Display clean preview of results
    print("\n--- [Monthly Revenue Preview] ---")
    print(pldf1.head(5))

    print("\n--- [Conversion Funnel Preview] ---")
    print(pldf3)

    print("\n--- [Customer Segmentation Preview] ---")
    print(pldf6)

    return results


if __name__ == "__main__":
    run_analytics_pipeline()
