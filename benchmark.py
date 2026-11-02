"""
Multi-Scale Benchmark for Clickstream Analytics
Compares Pandas and DuckDB on the EXACT same dataset at 1,000,000, 2,500,000,
and 5,000,000 rows (evaluating 10,000,000 if RAM and disk permit, else safely skipping).

Measures:
1. Wall-clock execution time (median of 3 runs per query)
2. Peak process RSS memory using psutil

Queries evaluated:
- Query 1: Monthly Revenue (Low-cardinality aggregation)
- Query 2: Peak Hours Distribution (Hourly aggregation)
- Query 3: Top Cities by Revenue & Active Users (High-cardinality distinct count)

Saves results to outputs/benchmark_results.csv and outputs/benchmark_results.json.
"""

import gc
import glob
import json
import os
import shutil
import statistics
import sys
import threading
import time
from pathlib import Path

import duckdb
import pandas as pd
import psutil
import pyarrow.parquet as pq

# Force UTF-8 encoding
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import config


class PeakMemoryTracker:
    """Tracks peak process RSS (in Megabytes) during code execution."""

    def __init__(self, interval: float = 0.005):
        self.interval = interval
        self.proc = psutil.Process()
        self.peak_rss_bytes = self.proc.memory_info().rss
        self._stop_event = threading.Event()
        self._thread = None

    def _poll(self):
        while not self._stop_event.is_set():
            try:
                rss = self.proc.memory_info().rss
                if rss > self.peak_rss_bytes:
                    self.peak_rss_bytes = rss
            except Exception:
                pass
            time.sleep(self.interval)

    def __enter__(self):
        gc.collect()
        self.peak_rss_bytes = self.proc.memory_info().rss
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=0.2)
        try:
            rss = self.proc.memory_info().rss
            if rss > self.peak_rss_bytes:
                self.peak_rss_bytes = rss
        except Exception:
            pass

    @property
    def peak_mb(self) -> float:
        return round(self.peak_rss_bytes / (1024 ** 2), 2)


def get_parquet_files_for_scale(data_dir: Path, n_rows: int, chunk_size: int = config.CHUNK_SIZE) -> list[str]:
    """
    Returns the exact list of parquet partition files corresponding to n_rows.
    Chunk 000 is rows [0 - 500k], Chunk 001 is [500k - 1M], etc.
    """
    needed_chunks = (n_rows + chunk_size - 1) // chunk_size
    all_files = sorted(glob.glob(f"{data_dir.as_posix()}/**/*.parquet", recursive=True))
    all_files = [f.replace("\\", "/") for f in all_files]

    selected_files = []
    for f in all_files:
        for c_idx in range(needed_chunks):
            if f"chunk_{c_idx:03d}_" in f:
                selected_files.append(f)
                break

    return sorted(selected_files)


def load_pandas_dataframe(file_list: list[str]) -> pd.DataFrame:
    """Loads exact parquet files into a single Pandas DataFrame with partition fields."""
    dfs = []
    for f in file_list:
        table = pq.read_table(f)
        df_part = table.to_pandas()
        parts = Path(f).parts
        for part in parts:
            if part.startswith("year="):
                df_part["year"] = int(part.split("=")[1])
            elif part.startswith("month="):
                df_part["month"] = int(part.split("=")[1])
        dfs.append(df_part)

    df = pd.concat(dfs, ignore_index=True)
    df["hour"] = df["timestamp"].dt.hour
    return df


def run_benchmark():
    print("=" * 80)
    print(" COMPREHENSIVE BENCHMARK: PANDAS vs DUCKDB ON IDENTICAL DATASETS")
    print("=" * 80)

    data_dir = config.DATA_DIR
    output_dir = config.OUTPUTS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Inspect System Hardware Capacity
    mem = psutil.virtual_memory()
    total_ram_gb = mem.total / (1024 ** 3)
    avail_ram_gb = mem.available / (1024 ** 3)
    free_disk_gb, _ = config.check_disk_space(config.PROJECT_ROOT)

    print(f"[*] Hardware Status:")
    print(f"    - Total RAM:     {total_ram_gb:.2f} GB")
    print(f"    - Available RAM: {avail_ram_gb:.2f} GB")
    print(f"    - Free Disk:     {free_disk_gb:.2f} GB")

    scales = [1_000_000, 2_500_000, 5_000_000]

    # Evaluate 10M capability
    if avail_ram_gb >= 3.0 and free_disk_gb >= 3.0:
        print("[+] Sufficient RAM and disk detected for 10M rows test.")
        scales.append(10_000_000)
    else:
        print(f"[!] WARNING: 10M rows benchmark skipped due to system constraints.")
        print(f"    Reason: Available RAM is {avail_ram_gb:.2f} GB (minimum 3.0 GB required for in-memory Pandas 10M aggregation) and free disk is {free_disk_gb:.2f} GB.")

    # Verify generated parquet files
    parquet_files = list(data_dir.glob("**/*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}. Run generate_data.py first.")

    results = []

    # -------------------------------------------------------------
    # Query Definitions
    # -------------------------------------------------------------
    # Query 1: Monthly Revenue
    def run_pandas_q1(df):
        purchases = df[df["event_type"] == "purchase"]
        return purchases.groupby(["year", "month"]).agg(
            total_orders=("event_type", "count"),
            total_revenue=("amount", "sum"),
            avg_order_value=("amount", "mean"),
        ).reset_index()

    def get_duckdb_q1(file_list_str):
        return f"""
        SELECT
            year,
            month,
            COUNT(*) FILTER (WHERE event_type = 'purchase') AS total_orders,
            ROUND(SUM(amount), 2) AS total_revenue,
            ROUND(AVG(amount) FILTER (WHERE event_type = 'purchase'), 2) AS avg_order_value
        FROM read_parquet({file_list_str}, hive_partitioning=true)
        GROUP BY year, month
        ORDER BY year, month
        """

    # Query 2: Peak Hours
    def run_pandas_q2(df):
        return df.groupby("hour").agg(
            total_events=("event_type", "count"),
            purchases=("event_type", lambda s: (s == "purchase").sum()),
            total_revenue=("amount", "sum"),
        ).reset_index()

    def get_duckdb_q2(file_list_str):
        return f"""
        SELECT
            EXTRACT(HOUR FROM timestamp) AS hour_of_day,
            COUNT(*) AS total_events,
            COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
            ROUND(SUM(amount), 2) AS total_revenue
        FROM read_parquet({file_list_str}, hive_partitioning=true)
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """

    # Query 3: Top Cities (Distinct Count)
    def run_pandas_q3(df):
        return df.groupby("city").agg(
            total_revenue=("amount", "sum"),
            total_orders=("event_type", lambda s: (s == "purchase").sum()),
            total_events=("event_type", "count"),
            active_users=("user_id", "nunique"),
        ).reset_index().sort_values(by="total_revenue", ascending=False)

    def get_duckdb_q3(file_list_str):
        return f"""
        SELECT
            city,
            ROUND(SUM(amount), 2) AS total_revenue,
            COUNT(*) FILTER (WHERE event_type = 'purchase') AS total_orders,
            COUNT(*) AS total_events,
            COUNT(DISTINCT user_id) AS active_users
        FROM read_parquet({file_list_str}, hive_partitioning=true)
        GROUP BY city
        ORDER BY total_revenue DESC
        """

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")

    queries = [
        ("Monthly Revenue", run_pandas_q1, get_duckdb_q1),
        ("Peak Hours", run_pandas_q2, get_duckdb_q2),
        ("Top Cities", run_pandas_q3, get_duckdb_q3),
    ]

    for scale in scales:
        print("\n" + "=" * 80)
        print(f" BENCHMARK SCALE: {scale:,} ROWS")
        print("=" * 80)

        file_list = get_parquet_files_for_scale(data_dir, scale)
        file_list_str = str(file_list)

        # ---------------------------------------------------------
        # Benchmark DuckDB at this scale
        # ---------------------------------------------------------
        print(f"\n[*] Evaluating DuckDB on {scale:,} rows (directly from {len(file_list)} Parquet files)...")
        for q_name, _, duck_q_fn in queries:
            sql_query = duck_q_fn(file_list_str)
            timings = []
            peak_mems = []

            for run_i in range(3):
                with PeakMemoryTracker() as tracker:
                    t0 = time.perf_counter()
                    res = con.sql(sql_query).to_arrow_table()
                    t1 = time.perf_counter()
                timings.append(t1 - t0)
                peak_mems.append(tracker.peak_mb)
                del res
                gc.collect()

            med_time = round(statistics.median(timings), 4)
            peak_mem = round(max(peak_mems), 2)
            print(f"    DuckDB | {q_name:16s} | Time: {med_time:7.4f}s | Peak RSS: {peak_mem:6.2f} MB")

            results.append({
                "engine": "duckdb",
                "rows": scale,
                "query": q_name,
                "time": med_time,
                "peak_memory": peak_mem,
            })

        # ---------------------------------------------------------
        # Benchmark Pandas at this scale
        # ---------------------------------------------------------
        print(f"\n[*] Loading {scale:,} rows into Pandas DataFrame for in-memory comparison...")
        t_load = time.time()
        with PeakMemoryTracker() as load_tracker:
            pandas_df = load_pandas_dataframe(file_list)
        load_duration = time.time() - t_load
        print(f"    Pandas DataFrame loaded in {load_duration:.2f}s (Base RAM: {load_tracker.peak_mb:.1f} MB)")

        for q_name, pd_fn, _ in queries:
            timings = []
            peak_mems = []

            for run_i in range(3):
                with PeakMemoryTracker() as tracker:
                    t0 = time.perf_counter()
                    res = pd_fn(pandas_df)
                    t1 = time.perf_counter()
                timings.append(t1 - t0)
                peak_mems.append(tracker.peak_mb)
                del res
                gc.collect()

            med_time = round(statistics.median(timings), 4)
            peak_mem = round(max(peak_mems), 2)
            print(f"    Pandas | {q_name:16s} | Time: {med_time:7.4f}s | Peak RSS: {peak_mem:6.2f} MB")

            results.append({
                "engine": "pandas",
                "rows": scale,
                "query": q_name,
                "time": med_time,
                "peak_memory": peak_mem,
            })

        # Free Pandas DataFrame to preserve low memory for subsequent iterations
        del pandas_df
        gc.collect()

    # -------------------------------------------------------------
    # Save Results
    # -------------------------------------------------------------
    benchmark_df = pd.DataFrame(results)
    csv_path = output_dir / "benchmark_results.csv"
    json_path = output_dir / "benchmark_results.json"

    benchmark_df.to_csv(csv_path, index=False)
    with open(json_path, "w") as jf:
        json.dump(results, jf, indent=2)

    print("\n" + "=" * 80)
    print(" FINAL BENCHMARK RESULTS TABLE")
    print("=" * 80)
    print(benchmark_df.to_string(index=False))
    print("\n" + "=" * 80)
    print(f"[*] Benchmark CSV saved to:  {csv_path}")
    print(f"[*] Benchmark JSON saved to: {json_path}")
    print("=" * 80)

    return benchmark_df


if __name__ == "__main__":
    run_benchmark()
