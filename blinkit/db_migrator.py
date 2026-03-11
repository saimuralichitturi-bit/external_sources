"""
db_migrator.py
===============
Migrates CSV data older than N days from GitHub repo to Supabase.
Runs weekly via GitHub Actions.

Setup:
  1. Create a Supabase project at supabase.com
  2. Create tables (schema below)
  3. Add secrets to GitHub repo:
     - SUPABASE_URL = https://xxxx.supabase.co
     - SUPABASE_KEY = your-anon-key

Supabase table schemas (run in Supabase SQL editor):

  CREATE TABLE blinkit_snapshots (
    id BIGSERIAL PRIMARY KEY,
    product_id TEXT, name TEXT, brand TEXT, unit TEXT,
    timestamp TIMESTAMPTZ, inventory INT, price FLOAT, mrp FLOAT,
    is_sold_out BOOLEAN, rating_count INT, offer_tag TEXT,
    location TEXT, inserted_at TIMESTAMPTZ DEFAULT NOW()
  );

  CREATE TABLE blinkit_sales_estimates (
    id BIGSERIAL PRIMARY KEY,
    product_id TEXT, name TEXT, brand TEXT, unit TEXT, category TEXT,
    price FLOAT, mrp FLOAT,
    daily_units_est FLOAT, monthly_units_est FLOAT,
    confidence TEXT, est_method TEXT,
    rating_count INT, organic_sov_pct FLOAT,
    inv_depletion_rate_per_hr FLOAT,
    location TEXT, as_of TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
  );

  CREATE TABLE blinkit_brand_estimates (
    id BIGSERIAL PRIMARY KEY,
    brand TEXT, product_count INT,
    total_daily_units_est FLOAT, total_monthly_units_est FLOAT,
    avg_organic_sov_pct FLOAT, total_rating_count INT,
    location TEXT, as_of TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
  );

  CREATE TABLE blinkit_keyword_sov (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT, timestamp TIMESTAMPTZ, keyword TEXT,
    brand TEXT, appearance_count INT, sov_pct FLOAT,
    avg_position FLOAT, ad_count INT, top3_count INT,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
  );

  CREATE TABLE blinkit_price_alerts (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ, alert_type TEXT,
    product_id TEXT, name TEXT, brand TEXT,
    old_value FLOAT, new_value FLOAT, change_pct FLOAT, note TEXT,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
  );

  CREATE TABLE blinkit_launches (
    id BIGSERIAL PRIMARY KEY,
    first_seen TIMESTAMPTZ, product_id TEXT,
    name TEXT, brand TEXT, unit TEXT,
    price FLOAT, mrp FLOAT, category_name TEXT,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
  );
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone

try:
    import pandas as pd
    from supabase import create_client
    DEPS_OK = True
except ImportError:
    DEPS_OK = False

# CSV file → Supabase table mapping
FILE_TABLE_MAP = {
    "snapshots_cache.csv":   "blinkit_snapshots",
    "sales_estimates.csv":   "blinkit_sales_estimates",
    "brand_estimates.csv":   "blinkit_brand_estimates",
    "keyword_sov.csv":       "blinkit_keyword_sov",
    "price_alerts.csv":      "blinkit_price_alerts",
    "launches.csv":          "blinkit_launches",
    "keyword_snapshots.csv": "blinkit_keyword_sov",
}

# Timestamp column per file
TIMESTAMP_COL = {
    "snapshots_cache.csv":   "timestamp",
    "sales_estimates.csv":   "as_of",
    "brand_estimates.csv":   "as_of",
    "keyword_sov.csv":       "timestamp",
    "price_alerts.csv":      "timestamp",
    "launches.csv":          "first_seen",
    "keyword_snapshots.csv": "timestamp",
}


def load_and_filter(filepath: str, ts_col: str, cutoff: datetime) -> tuple[list, list]:
    """
    Load CSV, split into rows older than cutoff (→ DB) and recent (→ keep).
    Returns (to_migrate, to_keep)
    """
    to_migrate = []
    to_keep = []

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts_raw = row.get(ts_col, "")
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except (ValueError, AttributeError):
                to_keep.append(row)
                continue

            if ts < cutoff:
                to_migrate.append(row)
            else:
                to_keep.append(row)

    return to_migrate, to_keep


def clean_row(row: dict) -> dict:
    """Convert string values to appropriate types for Supabase."""
    cleaned = {}
    for k, v in row.items():
        if v == "" or v is None:
            cleaned[k] = None
        elif v in ("True", "true", "TRUE"):
            cleaned[k] = True
        elif v in ("False", "false", "FALSE"):
            cleaned[k] = False
        else:
            # Try numeric
            try:
                if "." in str(v):
                    cleaned[k] = float(v)
                else:
                    cleaned[k] = int(v)
            except (ValueError, TypeError):
                cleaned[k] = v
    return cleaned


def migrate_file(supabase, filepath: str, table: str, ts_col: str,
                 cutoff: datetime, dry_run: bool = False) -> int:
    """Migrate old rows to Supabase, rewrite file with only recent rows."""
    if not os.path.exists(filepath):
        return 0

    to_migrate, to_keep = load_and_filter(filepath, ts_col, cutoff)

    if not to_migrate:
        print(f"  {os.path.basename(filepath)}: 0 rows to migrate")
        return 0

    print(f"  {os.path.basename(filepath)}: {len(to_migrate)} rows → {table}, {len(to_keep)} rows kept")

    if dry_run:
        print(f"    [DRY RUN] would insert {len(to_migrate)} rows")
        return len(to_migrate)

    # Insert in batches of 500
    batch_size = 500
    inserted = 0
    for i in range(0, len(to_migrate), batch_size):
        batch = [clean_row(r) for r in to_migrate[i:i+batch_size]]
        try:
            result = supabase.table(table).insert(batch).execute()
            inserted += len(batch)
        except Exception as e:
            print(f"    ERROR inserting batch {i//batch_size + 1}: {e}")
            # Don't delete rows if insert failed
            return 0

    print(f"    ✅ Inserted {inserted} rows")

    # Rewrite file with only recent rows
    if to_keep:
        fieldnames = list(to_keep[0].keys())
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(to_keep)
        print(f"    📝 Rewrote {filepath} ({len(to_keep)} recent rows kept)")
    else:
        # All rows migrated — keep header only
        with open(filepath, "r") as f:
            header = f.readline()
        with open(filepath, "w") as f:
            f.write(header)
        print(f"    📝 All rows migrated — file cleared")

    return inserted


def main():
    parser = argparse.ArgumentParser(description="Migrate Blinkit CSV data to Supabase")
    parser.add_argument("--older-than-days", type=int, default=3,
                        help="Migrate rows older than N days (default 3)")
    parser.add_argument("--data-dir", type=str, default="data",
                        help="Directory containing CSV files")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be migrated without doing it")
    args = parser.parse_args()

    if not DEPS_OK:
        print("Missing deps: pip install supabase pandas")
        sys.exit(1)

    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_KEY", "")

    if not supabase_url or not supabase_key:
        print("❌ SUPABASE_URL and SUPABASE_KEY env vars required")
        print("   Add them as GitHub repo secrets")
        sys.exit(1)

    supabase = create_client(supabase_url, supabase_key)
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.older_than_days)

    print(f"\n{'='*55}")
    print(f"DB MIGRATOR — rows older than {args.older_than_days} days")
    print(f"Cutoff: {cutoff.isoformat()}")
    print(f"Data dir: {args.data_dir}")
    if args.dry_run:
        print("MODE: DRY RUN")
    print(f"{'='*55}\n")

    total_migrated = 0

    for filename, table in FILE_TABLE_MAP.items():
        filepath = os.path.join(args.data_dir, filename)
        ts_col = TIMESTAMP_COL.get(filename, "timestamp")
        n = migrate_file(supabase, filepath, table, ts_col, cutoff, args.dry_run)
        total_migrated += n

    print(f"\n✅ Total rows migrated: {total_migrated}")

    # Write migration log
    log_entry = {
        "migrated_at": datetime.now(timezone.utc).isoformat(),
        "older_than_days": args.older_than_days,
        "total_rows_migrated": total_migrated,
        "dry_run": args.dry_run,
    }
    log_file = os.path.join(args.data_dir, "migration_log.json")
    log = []
    if os.path.exists(log_file):
        with open(log_file) as f:
            try:
                log = json.load(f)
            except Exception:
                log = []
    log.append(log_entry)
    with open(log_file, "w") as f:
        json.dump(log[-50:], f, indent=2)  # keep last 50 entries
    print(f"✅ Migration log → {log_file}")


if __name__ == "__main__":
    main()
