"""
myntra_inventory_tracker.py — Track exact per-size inventory depletion over time

Polls the product detail API at a set interval to measure how fast each size
sells. This gives the most reliable sales estimate when run over 2–6 hours.

Usage:
  python myntra/myntra_inventory_tracker.py --product_ids 39272406 12345678 --interval 30
  python myntra/myntra_inventory_tracker.py --keywords "nike tshirts" --interval 60 --pages 2
"""
import argparse, json, os, time
from collections import defaultdict

from myntra_core import (
    search_all_pages, fetch_product_detail,
    append_csv, load_csv_as_dicts, now_str, run_id,
    FASHION_RETURN_RATE,
)

SNAPSHOT_COLS = [
    "run_id", "snapshot_id", "timestamp",
    "product_id", "name", "brand", "category",
    "mrp", "price", "discount_pct",
    "total_exact_inv", "size_inventory", "out_of_stock_sizes", "is_oos",
    "rating_count", "avg_rating",
    "urgency_purchased", "urgency_cart", "urgency_wishlist",
]

SOLD_COLS = [
    "product_id", "name", "brand", "category",
    "mrp", "price", "discount_pct",
    "first_snapshot", "last_snapshot", "elapsed_hours",
    "inv_start", "inv_end", "gross_sold", "net_sold",
    "size_sold_breakdown",
    "daily_gross_est", "daily_net_est", "monthly_net_est",
    "rating_count_start", "rating_count_end", "new_ratings",
]


def collect_product_ids(keywords: list[str], pages: int) -> list[dict]:
    """Search keywords and return list of {product_id, name, brand, ...}."""
    seen = {}
    for kw in keywords:
        print(f"[search] {kw!r}")
        products = search_all_pages(kw, max_pages=pages, delay=0.3)
        for p in products:
            pid = p["product_id"]
            if pid not in seen:
                seen[pid] = p
    return list(seen.values())


def take_snapshot(product_ids: list[str], product_meta: dict, snapshot_num: int) -> list[dict]:
    """Fetch product detail for each product_id and return snapshot rows."""
    rid  = run_id()
    ts   = now_str()
    rows = []
    for pid in product_ids:
        detail = fetch_product_detail(pid)
        if not detail:
            continue
        meta = product_meta.get(pid, {})
        rows.append({
            "run_id":          rid,
            "snapshot_id":     snapshot_num,
            "timestamp":       ts,
            "product_id":      pid,
            "name":            meta.get("name", ""),
            "brand":           meta.get("brand", ""),
            "category":        meta.get("category", ""),
            "mrp":             meta.get("mrp", 0),
            "price":           meta.get("price", 0),
            "discount_pct":    meta.get("discount_pct", 0),
            **detail,
        })
        time.sleep(0.15)
    return rows


def compute_sold(first_snaps: dict, last_snaps: dict) -> list[dict]:
    """Compare first and last snapshots to estimate units sold per product."""
    results = []
    for pid in first_snaps:
        if pid not in last_snaps:
            continue
        f = first_snaps[pid]
        l = last_snaps[pid]

        try:
            t0 = __import__("datetime").datetime.fromisoformat(f["timestamp"])
            t1 = __import__("datetime").datetime.fromisoformat(l["timestamp"])
            elapsed_h = max((t1 - t0).total_seconds() / 3600, 0.001)
        except Exception:
            elapsed_h = 1.0

        inv_start = int(f.get("total_exact_inv") or 0)
        inv_end   = int(l.get("total_exact_inv") or 0)
        gross_sold = max(inv_start - inv_end, 0)
        net_sold   = round(gross_sold * (1 - FASHION_RETURN_RATE), 1)

        # Per-size breakdown
        size_sold = {}
        try:
            sz_start = json.loads(f.get("size_inventory") or "{}")
            sz_end   = json.loads(l.get("size_inventory") or "{}")
            for sz in sz_start:
                sold = max(int(sz_start.get(sz, 0)) - int(sz_end.get(sz, 0)), 0)
                if sold > 0:
                    size_sold[sz] = sold
        except Exception:
            pass

        daily_gross = round(gross_sold / elapsed_h * 24, 1) if elapsed_h else 0
        daily_net   = round(daily_gross * (1 - FASHION_RETURN_RATE), 1)
        monthly_net = round(daily_net * 30, 0)

        results.append({
            "product_id":          pid,
            "name":                f.get("name", ""),
            "brand":               f.get("brand", ""),
            "category":            f.get("category", ""),
            "mrp":                 f.get("mrp", 0),
            "price":               f.get("price", 0),
            "discount_pct":        f.get("discount_pct", 0),
            "first_snapshot":      f["timestamp"],
            "last_snapshot":       l["timestamp"],
            "elapsed_hours":       round(elapsed_h, 2),
            "inv_start":           inv_start,
            "inv_end":             inv_end,
            "gross_sold":          gross_sold,
            "net_sold":            net_sold,
            "size_sold_breakdown": json.dumps(size_sold),
            "daily_gross_est":     daily_gross,
            "daily_net_est":       daily_net,
            "monthly_net_est":     monthly_net,
            "rating_count_start":  int(f.get("rating_count") or 0),
            "rating_count_end":    int(l.get("rating_count") or 0),
            "new_ratings":         max(
                int(l.get("rating_count") or 0) - int(f.get("rating_count") or 0), 0
            ),
        })

    results.sort(key=lambda x: -x["gross_sold"])
    return results


def run(product_ids: list[str], keywords: list[str], pages: int,
        interval_mins: int, runs: int, out_dir: str):

    snap_file = os.path.join(out_dir, "myntra_inv_snapshots.csv")
    sold_file = os.path.join(out_dir, "myntra_inv_sold.csv")

    # Resolve product IDs from keywords if provided
    product_meta = {}
    if keywords and not product_ids:
        found = collect_product_ids(keywords, pages)
        product_ids = [p["product_id"] for p in found]
        product_meta = {p["product_id"]: p for p in found}
        print(f"[keywords] resolved {len(product_ids)} products")
    else:
        product_meta = {pid: {"product_id": pid} for pid in product_ids}

    print(f"\nTracking {len(product_ids)} products | interval={interval_mins}m | runs={runs}")

    first_snaps: dict[str, dict] = {}
    last_snaps:  dict[str, dict] = {}

    for snap_num in range(1, runs + 1):
        print(f"\n[snapshot {snap_num}/{runs}] {now_str()}")
        rows = take_snapshot(product_ids, product_meta, snap_num)
        append_csv(snap_file, rows, SNAPSHOT_COLS)
        print(f"  saved {len(rows)} rows")

        for row in rows:
            pid = row["product_id"]
            if pid not in first_snaps:
                first_snaps[pid] = row
            last_snaps[pid] = row

        if snap_num < runs:
            print(f"  waiting {interval_mins}m for next snapshot...")
            time.sleep(interval_mins * 60)

    # ── Final sold estimate ───────────────────────────────────────────────────
    sold = compute_sold(first_snaps, last_snaps)
    append_csv(sold_file, sold, SOLD_COLS)
    print(f"\n[sold] saved {len(sold)} rows -> {sold_file}")

    print(f"\n{'='*60}")
    print(f"Top products by gross units sold during tracking window:")
    print(f"{'='*60}")
    for s in sold[:15]:
        print(f"  {s['brand'][:20]:<20} {s['name'][:35]:<35} "
              f"sold {s['gross_sold']:>4} ({s['elapsed_hours']:.1f}h)  "
              f"~{s['daily_net_est']:.0f} net/day  sizes:{s['size_sold_breakdown']}")


def main():
    ap = argparse.ArgumentParser(description="Myntra inventory depletion tracker")
    ap.add_argument("--product_ids", nargs="*", default=[],
                    help="Product IDs to track directly")
    ap.add_argument("--keywords", default="",
                    help="Comma-separated keywords to resolve product IDs from search")
    ap.add_argument("--pages",    type=int, default=2,
                    help="Pages per keyword when resolving via search (default: 2)")
    ap.add_argument("--interval", type=int, default=60,
                    help="Minutes between snapshots (default: 60)")
    ap.add_argument("--runs",     type=int, default=4,
                    help="Number of snapshots to take (default: 4)")
    ap.add_argument("--out-dir",  default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"),
                    help="Output directory for CSVs")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]

    run(
        product_ids=args.product_ids,
        keywords=keywords,
        pages=args.pages,
        interval_mins=args.interval,
        runs=args.runs,
        out_dir=args.out_dir,
    )


if __name__ == "__main__":
    main()
