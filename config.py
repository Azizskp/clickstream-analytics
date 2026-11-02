import os
import shutil
from pathlib import Path

# ==========================================
# Clickstream Analytics Configuration
# ==========================================

# Dataset scale (Default: 5,000,000; can be scaled up to 10,000,000)
N_ROWS = 5_000_000

# Chunk size for memory-safe generation and batch streaming (never loads everything into RAM)
CHUNK_SIZE = 500_000

# Parquet storage format options
PARQUET_COMPRESSION = "snappy"

# Minimum free disk space threshold (in Gigabytes)
MIN_FREE_DISK_GB = 2.0

# Directory paths
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data" / "clickstream"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"

# Synthetic Data Generation Settings
RANDOM_SEED = 42
DATE_START = "2024-11-01 00:00:00"
DATE_END = "2026-10-31 23:59:59"

# Catalog and Demographics
N_USERS = 250_000
N_PRODUCTS = 2_500

CATEGORIES = [
    "Electronics",
    "Apparel & Fashion",
    "Home & Kitchen",
    "Books & Media",
    "Sports & Outdoors",
    "Beauty & Health",
    "Automotive",
    "Toys & Games",
]

CITIES = [
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "San Antonio",
    "San Diego",
    "Dallas",
    "Austin",
    "San Jose",
    "Seattle",
]

# Event types and realistic e-commerce conversion funnel distribution
# ~75% view, ~20% cart, ~5% purchase
EVENT_TYPES = ["view", "cart", "purchase"]
EVENT_PROBABILITIES = [0.75, 0.20, 0.05]

# Category price ranges (min_price, max_price)
CATEGORY_PRICE_RANGES = {
    "Electronics": (29.99, 1299.99),
    "Apparel & Fashion": (14.99, 249.99),
    "Home & Kitchen": (19.99, 499.99),
    "Books & Media": (7.99, 69.99),
    "Sports & Outdoors": (19.99, 399.99),
    "Beauty & Health": (9.99, 149.99),
    "Automotive": (24.99, 599.99),
    "Toys & Games": (9.99, 129.99),
}


def check_disk_space(target_path: Path = PROJECT_ROOT, min_gb: float = MIN_FREE_DISK_GB) -> tuple[float, bool]:
    """
    Checks free disk space for the volume containing target_path.
    Returns (free_gb, is_sufficient).
    """
    total, used, free = shutil.disk_usage(str(target_path))
    free_gb = free / (1024 ** 3)
    is_sufficient = free_gb >= min_gb
    return round(free_gb, 2), is_sufficient

