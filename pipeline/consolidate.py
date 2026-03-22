"""
pipeline/consolidate.py
=======================
Reads all platform CSV outputs from data/ and merges them into
3 unified schema files that the Streamlit dashboard reads.

Outputs (written to data/):
  unified_snapshots.csv   — all product snapshots (price, rating, inventory)
  unified_estimates.csv   — all sales estimates
  unified_brands.csv      — all brand aggregates

Run:
  python pipeline/consolidate.py
  python pipeline/consolidate.py --data-dir ./data --out-dir ./data
"""

import os, sys, argparse, csv
from datetime import datetime

import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


# ── Column mapping per platform ──────────────────────────────────────────────

def load_blinkit_snapshots(data_dir: str) -> pd.DataFrame:
    """
    Blinkit data sources:
      - blinkit_sales_estimates.csv  : keyword-based sales estimator output
      - blinkit_category_products.csv: full category browse (all 20 categories)
      - blinkit_snapshots_cache.csv  : inventory tracker snapshots
      - blinkit_inv_snapshots.csv    : inventory tracker snapshots (alt name)
    """
    rows = []
    for fname in ["blinkit_sales_estimates.csv", "blinkit_category_products.csv",
                  "blinkit_snapshots_cache.csv", "blinkit_inv_snapshots.csv"]:
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path, dtype=str)
        for _, r in df.iterrows():
            rows.append({
                "platform":     "blinkit",
                "product_id":   r.get("product_id", ""),
                "name":         r.get("name", ""),
                "brand":        r.get("brand", ""),
                "category":     r.get("category") or r.get("category_name", ""),
                "price":        _float(r.get("price")),
                "mrp":          _float(r.get("mrp")),
                "discount_pct": _float(r.get("discount_pct")),
                "avg_rating":   None,
                "rating_count": _int(r.get("rating_count")),
                "inventory":    _int(r.get("inventory")),
                "is_oos":       _bool(r.get("is_sold_out")),
                "stock_count":  None,
                "location":     r.get("location", ""),
                "scraped_at":   r.get("as_of") or r.get("scraped_at") or r.get("timestamp", ""),
            })
    return pd.DataFrame(rows)


def load_myntra_snapshots(data_dir: str) -> pd.DataFrame:
    rows = []
    for fname in ["myntra_snapshots.csv"]:
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path, dtype=str)
        for _, r in df.iterrows():
            rows.append({
                "platform":     "myntra",
                "product_id":   r.get("product_id", ""),
                "name":         r.get("name", ""),
                "brand":        r.get("brand", ""),
                "category":     r.get("category", ""),
                "price":        _float(r.get("price")),
                "mrp":          _float(r.get("mrp")),
                "discount_pct": _float(r.get("discount_pct")),
                "avg_rating":   _float(r.get("rating")),
                "rating_count": _int(r.get("rating_count")),
                "inventory":    _int(r.get("total_inventory")),
                "is_oos":       _bool(r.get("is_oos")),
                "stock_count":  None,
                "location":     "",
                "scraped_at":   r.get("scraped_at", ""),
            })
    return pd.DataFrame(rows)


def load_amazon_snapshots(data_dir: str) -> pd.DataFrame:
    rows = []
    for fname in ["amazon_snapshots.csv", "amazon_pdp_snapshots.csv"]:
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path, dtype=str)
        for _, r in df.iterrows():
            rows.append({
                "platform":     "amazon",
                "product_id":   r.get("asin", ""),
                "name":         r.get("title", ""),
                "brand":        r.get("brand", ""),
                "category":     r.get("category", ""),
                "price":        _float(r.get("price")),
                "mrp":          _float(r.get("mrp")),
                "discount_pct": _float(r.get("discount_pct")),
                "avg_rating":   _float(r.get("avg_rating")),
                "rating_count": _int(r.get("rating_count")),
                "inventory":    None,
                "is_oos":       _bool(r.get("is_oos")),
                "stock_count":  _int(r.get("stock_count")),
                "location":     "",
                "scraped_at":   r.get("scraped_at", ""),
            })
    return pd.DataFrame(rows)


def load_flipkart_snapshots(data_dir: str) -> pd.DataFrame:
    rows = []
    for fname in ["flipkart_snapshots.csv", "flipkart_pdp_snapshots.csv"]:
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path, dtype=str)
        for _, r in df.iterrows():
            rows.append({
                "platform":     "flipkart",
                "product_id":   r.get("pid", ""),
                "name":         r.get("title", ""),
                "brand":        r.get("brand", ""),
                "category":     r.get("category", ""),
                "price":        _float(r.get("price")),
                "mrp":          _float(r.get("mrp")),
                "discount_pct": _float(r.get("discount_pct")),
                "avg_rating":   _float(r.get("avg_rating")),
                "rating_count": _int(r.get("rating_count")),
                "inventory":    None,
                "is_oos":       _bool(r.get("is_oos")),
                "stock_count":  _int(r.get("stock_count")),
                "location":     "",
                "scraped_at":   r.get("scraped_at", ""),
            })
    return pd.DataFrame(rows)


# ── Estimates ─────────────────────────────────────────────────────────────────

def load_estimates(data_dir: str) -> pd.DataFrame:
    sources = [
        ("blinkit",  "blinkit_sales_estimates.csv",  "product_id", "name",  "brand", "keyword",      "daily_units_est", "monthly_units_est", "confidence", "location", "as_of"),
        ("myntra",   "myntra_sales_estimates.csv",    "product_id", "name",  "brand", "keyword",      "daily_units_est", "monthly_units_est", "confidence", "",         "timestamp"),
        ("amazon",   "amazon_sales_estimates.csv",    "asin",       "title", "brand", "keyword",      "daily_units_est", "monthly_units_est", "confidence", "",         "timestamp"),
        ("flipkart", "flipkart_sales_estimates.csv",  "pid",        "title", "brand", "keyword",      "daily_units_est", "monthly_units_est", "confidence", "",         "timestamp"),
    ]
    all_rows = []
    for platform, fname, pid_col, name_col, brand_col, kw_col, daily_col, monthly_col, conf_col, loc_col, ts_col in sources:
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path, dtype=str)
        for _, r in df.iterrows():
            all_rows.append({
                "platform":         platform,
                "product_id":       r.get(pid_col, ""),
                "name":             r.get(name_col, ""),
                "brand":            r.get(brand_col, ""),
                "keyword":          r.get(kw_col, ""),
                "daily_units_est":  _float(r.get(daily_col)),
                "monthly_units_est":_float(r.get(monthly_col)),
                "confidence":       r.get(conf_col, ""),
                "location":         r.get(loc_col, "") if loc_col else "",
                "scraped_at":       r.get(ts_col, ""),
            })
    return pd.DataFrame(all_rows)


def load_brands(data_dir: str) -> pd.DataFrame:
    sources = [
        ("blinkit",  "blinkit_brand_estimates.csv",  "brand", "product_count", "total_daily_units_est",  "total_monthly_units_est",  "location", "as_of"),
        ("myntra",   "myntra_brand_estimates.csv",    "brand", "product_count", "total_daily_units_est",  "total_monthly_units_est",  "",         "timestamp"),
        ("amazon",   "amazon_brand_estimates.csv",    "brand", "product_count", "total_daily_units_est",  "total_monthly_units_est",  "",         "timestamp"),
        ("flipkart", "flipkart_brand_estimates.csv",  "brand", "product_count", "total_daily_units_est",  "total_monthly_units_est",  "",         "timestamp"),
    ]
    all_rows = []
    for platform, fname, brand_col, count_col, daily_col, monthly_col, loc_col, ts_col in sources:
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path, dtype=str)
        for _, r in df.iterrows():
            all_rows.append({
                "platform":             platform,
                "brand":                r.get(brand_col, ""),
                "product_count":        _int(r.get(count_col)),
                "total_daily_units_est":_float(r.get(daily_col)),
                "total_monthly_units_est":_float(r.get(monthly_col)),
                "location":             r.get(loc_col, "") if loc_col else "",
                "scraped_at":           r.get(ts_col, ""),
            })
    return pd.DataFrame(all_rows)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _float(v) -> float | None:
    try:
        f = float(str(v).replace(",", ""))
        return f if f == f else None  # NaN check
    except (TypeError, ValueError):
        return None

def _int(v) -> int | None:
    try:
        return int(float(str(v).replace(",", "")))
    except (TypeError, ValueError):
        return None

def _bool(v) -> bool:
    return str(v).lower() in ("true", "1", "yes")


# ── Main ──────────────────────────────────────────────────────────────────────

SNAPSHOT_COLS = [
    "platform", "product_id", "name", "brand", "category",
    "price", "mrp", "discount_pct", "avg_rating", "rating_count",
    "inventory", "is_oos", "stock_count", "location", "scraped_at",
]

ESTIMATE_COLS = [
    "platform", "product_id", "name", "brand", "keyword",
    "daily_units_est", "monthly_units_est", "confidence",
    "location", "scraped_at",
]

BRAND_COLS = [
    "platform", "brand", "product_count",
    "total_daily_units_est", "total_monthly_units_est",
    "location", "scraped_at",
]


def run(data_dir: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    print(f"Reading from: {data_dir}")

    # Snapshots
    snap_frames = [
        load_blinkit_snapshots(data_dir),
        load_myntra_snapshots(data_dir),
        load_amazon_snapshots(data_dir),
        load_flipkart_snapshots(data_dir),
    ]
    snap_df = pd.concat([f for f in snap_frames if not f.empty], ignore_index=True)
    snap_df = snap_df[SNAPSHOT_COLS]
    snap_path = os.path.join(out_dir, "unified_snapshots.csv")
    snap_df.to_csv(snap_path, index=False)
    print(f"  unified_snapshots.csv  -> {len(snap_df):,} rows")

    # Estimates
    est_df = load_estimates(data_dir)
    if not est_df.empty:
        est_df = est_df[ESTIMATE_COLS]
    est_path = os.path.join(out_dir, "unified_estimates.csv")
    est_df.to_csv(est_path, index=False)
    print(f"  unified_estimates.csv  -> {len(est_df):,} rows")

    # Brands
    brand_df = load_brands(data_dir)
    if not brand_df.empty:
        brand_df = brand_df[BRAND_COLS]
    brand_path = os.path.join(out_dir, "unified_brands.csv")
    brand_df.to_csv(brand_path, index=False)
    print(f"  unified_brands.csv     -> {len(brand_df):,} rows")

    print("Consolidation complete.")
    return snap_path, est_path, brand_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=DATA_DIR)
    parser.add_argument("--out-dir",  default=DATA_DIR)
    args = parser.parse_args()
    run(args.data_dir, args.out_dir)
