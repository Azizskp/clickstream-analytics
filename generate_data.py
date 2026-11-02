"""
Data Generation Script for Clickstream Analytics
Generates synthetic e-commerce clickstream events in memory-safe chunks
and writes Hive-partitioned Snappy-compressed Parquet files (by year and month).
"""

import gc
import os
import sys
import time
from datetime import datetime
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

# Set standard UTF-8 output encoding for cross-platform compatibility
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import config


def generate_clickstream_dataset(n_rows: int = config.N_ROWS, chunk_size: int = config.CHUNK_SIZE):
    print("=" * 65)
    print(" CLICKSTREAM DATA GENERATOR (CHUNKED & LIGHTWEIGHT)")
    print("=" * 65)

    # 1. Disk Space Verification
    free_gb, is_sufficient = config.check_disk_space(config.PROJECT_ROOT, config.MIN_FREE_DISK_GB)
    print(f"[*] Checking available disk space: {free_gb:.2f} GB free")

    if not is_sufficient:
        print(f"[!] WARNING: Free disk space ({free_gb:.2f} GB) is below the recommended threshold of {config.MIN_FREE_DISK_GB} GB!")
        print("    Proceeding with caution; generating data strictly in chunks.")
    else:
        print(f"[+] Disk space check PASSED (> {config.MIN_FREE_DISK_GB} GB available).")

    # 2. Check if output dataset already exists
    data_dir = config.DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)

    existing_parquet_files = list(data_dir.glob("**/*.parquet"))
    if existing_parquet_files:
        print(f"[*] Found {len(existing_parquet_files)} existing parquet partition files in {data_dir}.")
        print("    Data will be appended/partitioned with unique chunk identifiers.")

    print(f"[*] Target dataset size: {n_rows:,} rows")
    print(f"[*] Chunk size: {chunk_size:,} rows per batch")
    print(f"[*] Partition strategy: Hive partitions by [year, month]")
    print(f"[*] Compression codec: {config.PARQUET_COMPRESSION}")
    print(f"[*] Date span: {config.DATE_START} to {config.DATE_END}")

    start_total_time = time.time()
    rng = np.random.default_rng(config.RANDOM_SEED)

    # 3. Pre-create fixed product catalog and user profiles
    print("[*] Initializing catalog and user reference arrays...")
    n_products = config.N_PRODUCTS
    product_ids_list = [f"P{i:04d}" for i in range(1, n_products + 1)]
    product_ids_pa = pa.array(product_ids_list)

    # Deterministic product categories and base price ranges
    cat_names = list(config.CATEGORY_PRICE_RANGES.keys())
    product_category_indices = rng.integers(0, len(cat_names), size=n_products)
    product_categories = np.array(cat_names)[product_category_indices]
    product_categories_pa = pa.array(product_categories)

    product_prices = np.zeros(n_products, dtype=np.float64)
    for i, cat in enumerate(product_categories):
        p_min, p_max = config.CATEGORY_PRICE_RANGES[cat]
        product_prices[i] = round(rng.uniform(p_min, p_max), 2)
    product_prices_pa = pa.array(product_prices)

    # Pre-create user catalog & user-city assignments
    n_users = config.N_USERS
    user_ids_list = [f"U{i:06d}" for i in range(1, n_users + 1)]
    user_ids_pa = pa.array(user_ids_list)

    city_names = config.CITIES
    user_city_indices = rng.integers(0, len(city_names), size=n_users)
    user_cities = np.array(city_names)[user_city_indices]
    user_cities_pa = pa.array(user_cities)

    # Pre-calculate epoch bounds for timestamp generation
    dt_start = datetime.strptime(config.DATE_START, "%Y-%m-%d %H:%M:%S")
    dt_end = datetime.strptime(config.DATE_END, "%Y-%m-%d %H:%M:%S")
    epoch_start = int(dt_start.timestamp())
    epoch_end = int(dt_end.timestamp())

    # Pre-build event types array
    event_types_pa = pa.array(config.EVENT_TYPES)
    event_probs = config.EVENT_PROBABILITIES  # [0.75 view, 0.20 cart, 0.05 purchase]

    total_chunks = (n_rows + chunk_size - 1) // chunk_size
    rows_generated = 0

    print(f"\n[*] Generating {n_rows:,} events across {total_chunks} chunks...")

    for chunk_idx in range(total_chunks):
        t_chunk_start = time.time()
        current_chunk_size = min(chunk_size, n_rows - rows_generated)

        # 1. Random user and product indices for this chunk
        u_idx = rng.integers(0, n_users, size=current_chunk_size)
        p_idx = rng.integers(0, n_products, size=current_chunk_size)

        u_idx_pa = pa.array(u_idx, type=pa.int32())
        p_idx_pa = pa.array(p_idx, type=pa.int32())

        chunk_user_ids = pc.take(user_ids_pa, u_idx_pa)
        chunk_product_ids = pc.take(product_ids_pa, p_idx_pa)
        chunk_categories = pc.take(product_categories_pa, p_idx_pa)
        chunk_cities = pc.take(user_cities_pa, u_idx_pa)

        # 2. Event type distribution: 0: view, 1: cart, 2: purchase
        event_choice_idx = rng.choice(3, size=current_chunk_size, p=event_probs)
        chunk_event_types = pc.take(event_types_pa, pa.array(event_choice_idx, type=pa.int32()))

        # 3. Dynamic Amount calculation:
        # Views and Cart additions have 0.0 monetary amount; Purchases reflect base_price * quantity (1 to 3 items)
        base_prices_sampled = product_prices[p_idx]
        quantities = rng.integers(1, 4, size=current_chunk_size)
        purchase_amounts = np.round(base_prices_sampled * quantities, 2)
        chunk_amounts = np.where(event_choice_idx == 2, purchase_amounts, 0.0)
        chunk_amounts_pa = pa.array(chunk_amounts, type=pa.float64())

        # 4. Timestamp generation across 2-year range
        timestamps_epoch = rng.integers(epoch_start, epoch_end, size=current_chunk_size, dtype=np.int64)
        chunk_timestamps = pa.array(timestamps_epoch, type=pa.timestamp("s"))

        # Extract Year and Month partition keys
        chunk_years = pc.cast(pc.year(chunk_timestamps), pa.int32())
        chunk_months = pc.cast(pc.month(chunk_timestamps), pa.int32())

        # 5. Build Chunk Arrow Table
        chunk_table = pa.table({
            "user_id": chunk_user_ids,
            "product_id": chunk_product_ids,
            "category": chunk_categories,
            "city": chunk_cities,
            "event_type": chunk_event_types,
            "amount": chunk_amounts_pa,
            "timestamp": chunk_timestamps,
            "year": chunk_years,
            "month": chunk_months,
        })

        # 6. Stream write directly to Hive partitioned Parquet dataset
        ds.write_dataset(
            chunk_table,
            base_dir=str(data_dir),
            format="parquet",
            partitioning=["year", "month"],
            partitioning_flavor="hive",
            file_options=ds.ParquetFileFormat().make_write_options(compression=config.PARQUET_COMPRESSION),
            basename_template=f"chunk_{chunk_idx:03d}_{{i}}.parquet",
            existing_data_behavior="overwrite_or_ignore",
        )

        rows_generated += current_chunk_size
        chunk_duration = time.time() - t_chunk_start
        percent_done = (rows_generated / n_rows) * 100

        print(f"    [Chunk {chunk_idx + 1:02d}/{total_chunks:02d}] "
              f"Wrote {current_chunk_size:,} rows ({rows_generated:,}/{n_rows:,} - {percent_done:.1f}%) "
              f"in {chunk_duration:.2f}s")

        # Explicit garbage collection to ensure constant low memory footprint
        del chunk_table, chunk_timestamps, chunk_years, chunk_months, chunk_user_ids, chunk_product_ids
        del chunk_categories, chunk_cities, chunk_event_types, chunk_amounts_pa, u_idx, p_idx
        gc.collect()

    total_time = time.time() - start_total_time
    total_size_bytes = sum(f.stat().st_size for f in data_dir.glob("**/*.parquet"))
    total_size_mb = total_size_bytes / (1024 ** 2)

    print("\n" + "=" * 65)
    print(" GENERATION COMPLETED SUCCESSFULLY")
    print(f"[*] Total rows generated: {rows_generated:,}")
    print(f"[*] Total elapsed time:   {total_time:.2f} seconds ({rows_generated / total_time:,.0f} rows/s)")
    print(f"[*] Total compressed size: {total_size_mb:.2f} MB")
    print(f"[*] Data stored in:       {data_dir}")
    print("=" * 65)


if __name__ == "__main__":
    generate_clickstream_dataset()

