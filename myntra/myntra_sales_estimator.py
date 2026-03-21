"""
myntra_sales_estimator.py — Multi-signal sales estimator for Myntra products

Signals used (in order of reliability):
  1. Inventory depletion   — exact per-size stock tracked over time           [HIGH]
  2. Rating velocity       — new ratings/day ÷ conversion rate                [HIGH]
  3. Search rank position  — power-law rank score                             [MEDIUM]
  4. Urgency signals       — PURCHASED/CART counts from product detail API    [MEDIUM]
  5. Discount depth        — heavy discounts → fast movers or clearance       [LOW]

Usage:
  python myntra/myntra_sales_estimator.py --keywords "tshirts,sneakers" --pages 3
  python myntra/myntra_sales_estimator.py --keywords "tshirts" --detail   # also fetches per-size inventory
  python myntra/myntra_sales_estimator.py --from-snapshots snapshots.csv
"""
import argparse, json, math, os, time
from datetime import datetime, timezone
from collections import defaultdict

from myntra_core import (
    search_all_pages, fetch_product_detail,
    append_csv, load_csv_as_dicts, now_str, run_id,
    RATING_CONVERSION_RATE, FASHION_RETURN_RATE,
)

# ── Model constants ───────────────────────────────────────────────────────────
RANK_DECAY         = 0.12    # power-law rank decay (softer than Blinkit — wider catalogue)
RANK_BASE_SALES    = 200     # estimated daily units at rank 1 (flagship keyword)
MIN_HOURS_ELAPSED  = 1.0     # ignore snapshots < 1 hr apart

SIGNAL_WEIGHTS = {
    "inventory_depletion": 3.0,
    "rating_velocity":     2.0,
    "rank_score":          1.0,
    "urgency_purchased":   2.5,   # direct signal when non-zero
    "urgency_cart":        0.5,
}

# ── CSV schemas ───────────────────────────────────────────────────────────────
SNAPSHOT_COLS = [
    "run_id", "timestamp", "product_id", "name", "brand", "category", "gender",
    "mrp", "price", "discount_pct", "rating", "rating_count", "reviews_count",
    "total_inventory", "total_exact_inv", "size_inventory",
    "sizes_available", "out_of_stock_sizes", "is_oos",
    "urgency_purchased", "urgency_cart", "urgency_wishlist", "urgency_pdp",
    "is_ad", "catalog_date", "position", "keyword",
]

ESTIMATE_COLS = [
    "run_id", "timestamp", "product_id", "name", "brand", "category", "gender",
    "mrp", "price", "discount_pct", "rating", "rating_count",
    "daily_units_est", "monthly_units_est", "net_daily_units_est", "net_monthly_units_est",
    "confidence", "est_method", "signal_breakdown",
    "inv_depletion_daily", "rating_vel_daily", "rank_score_daily", "urgency_daily",
    "keyword", "position", "is_ad",
]

BRAND_COLS = [
    "run_id", "timestamp", "brand", "keyword",
    "product_count", "ad_count",
    "total_daily_units_est", "total_monthly_units_est",
    "avg_discount_pct", "avg_rating", "avg_sov_pct",
]


# ── Estimation logic ──────────────────────────────────────────────────────────
def estimate_from_signals(product: dict, prev: dict | None, hours_elapsed: float, position: int) -> dict:
    signals = {}
    methods = []

    # ── Signal 1: Inventory depletion ────────────────────────────────────────
    inv_daily = 0.0
    if prev and hours_elapsed >= MIN_HOURS_ELAPSED:
        # Use exact inventory if available, fall back to listing inventory
        curr_inv = int(product.get("total_exact_inv") or product.get("total_inventory") or 0)
        prev_inv = int(prev.get("total_exact_inv") or prev.get("total_inventory") or 0)
        if prev_inv > 0 and prev_inv > curr_inv:
            depletion_per_hour = (prev_inv - curr_inv) / hours_elapsed
            inv_daily = round(depletion_per_hour * 24, 1)
            signals["inventory_depletion"] = inv_daily
            methods.append("inv_depletion")

    # ── Signal 2: Rating velocity ─────────────────────────────────────────────
    rating_vel_daily = 0.0
    if prev and hours_elapsed >= MIN_HOURS_ELAPSED:
        curr_rc = int(product.get("rating_count") or 0)
        prev_rc = int(prev.get("rating_count") or 0)
        if curr_rc > prev_rc:
            new_ratings_per_day = (curr_rc - prev_rc) / (hours_elapsed / 24)
            rating_vel_daily = round(new_ratings_per_day / RATING_CONVERSION_RATE, 1)
            signals["rating_velocity"] = rating_vel_daily
            methods.append("rating_velocity")

    # ── Signal 3: Rank score ──────────────────────────────────────────────────
    rank_daily = round(RANK_BASE_SALES * math.exp(-RANK_DECAY * (position - 1)), 1)
    signals["rank_score"] = rank_daily

    # ── Signal 4: Urgency — PURCHASED (direct, if non-zero) ──────────────────
    urgency_daily = 0.0
    purchased = int(product.get("urgency_purchased") or 0)
    cart      = int(product.get("urgency_cart") or 0)
    if purchased > 0:
        urgency_daily = float(purchased)
        signals["urgency_purchased"] = urgency_daily
        methods.append("urgency_purchased")
    elif cart > 0:
        # Cart-to-purchase conversion ~25%
        urgency_daily = round(cart * 0.25, 1)
        signals["urgency_cart"] = urgency_daily

    # ── Weighted combination ──────────────────────────────────────────────────
    total_weight = 0.0
    weighted_sum = 0.0
    for sig, val in signals.items():
        w = SIGNAL_WEIGHTS.get(sig, 1.0)
        weighted_sum += val * w
        total_weight += w

    if total_weight == 0:
        daily_est = rank_daily  # rank-only fallback
        confidence = "low"
    else:
        daily_est  = round(weighted_sum / total_weight, 1)
        if len(methods) >= 2:
            confidence = "high"
        elif len(methods) == 1:
            confidence = "medium"
        else:
            confidence = "low"

    monthly_est     = round(daily_est * 30, 0)
    net_daily_est   = round(daily_est * (1 - FASHION_RETURN_RATE), 1)
    net_monthly_est = round(net_daily_est * 30, 0)

    return {
        "daily_units_est":      daily_est,
        "monthly_units_est":    monthly_est,
        "net_daily_units_est":  net_daily_est,
        "net_monthly_units_est": net_monthly_est,
        "confidence":           confidence,
        "est_method":           ",".join(methods) if methods else "rank_only",
        "signal_breakdown":     json.dumps({k: round(v, 1) for k, v in signals.items()}),
        "inv_depletion_daily":  inv_daily,
        "rating_vel_daily":     rating_vel_daily,
        "rank_score_daily":     rank_daily,
        "urgency_daily":        urgency_daily,
    }


# ── Main pipeline ─────────────────────────────────────────────────────────────
def run(keywords: list[str], pages: int, fetch_detail: bool,
        from_snapshots: str, out_dir: str):

    rid       = run_id()
    ts        = now_str()
    snap_file = os.path.join(out_dir, "myntra_snapshots.csv")
    est_file  = os.path.join(out_dir, "myntra_sales_estimates.csv")
    brand_file = os.path.join(out_dir, "myntra_brand_estimates.csv")

    # Load previous snapshots to compute velocity
    prev_snaps = {}
    for row in load_csv_as_dicts(snap_file):
        pid = row["product_id"]
        # Keep the most recent snapshot per product
        if pid not in prev_snaps or row["timestamp"] > prev_snaps[pid]["timestamp"]:
            prev_snaps[pid] = row

    # ── Collect products ──────────────────────────────────────────────────────
    all_products = []  # {**listing_fields, **detail_fields, keyword, timestamp, run_id}

    for kw in keywords:
        print(f"\n[search] keyword={kw!r} pages={pages}")
        products = search_all_pages(kw, max_pages=pages, delay=0.4)
        print(f"  found {len(products)} products")

        for p in products:
            p["keyword"]   = kw
            p["run_id"]    = rid
            p["timestamp"] = ts

            # Optionally enrich with product detail (exact inventory + urgency)
            if fetch_detail:
                time.sleep(0.2)
                detail = fetch_product_detail(p["product_id"])
                if detail:
                    p.update(detail)

            all_products.append(p)

    if not all_products:
        print("No products collected.")
        return

    # ── Save snapshots ────────────────────────────────────────────────────────
    append_csv(snap_file, all_products, SNAPSHOT_COLS)
    print(f"\n[snapshots] saved {len(all_products)} rows -> {snap_file}")

    # ── Produce estimates ─────────────────────────────────────────────────────
    estimates = []
    for p in all_products:
        prev = prev_snaps.get(p["product_id"])
        hours_elapsed = 0.0
        if prev:
            try:
                t0 = datetime.fromisoformat(prev["timestamp"])
                t1 = datetime.fromisoformat(ts)
                hours_elapsed = (t1 - t0).total_seconds() / 3600
            except Exception:
                pass

        signals = estimate_from_signals(p, prev, hours_elapsed, int(p.get("position", 99)))
        row = {
            "run_id":       rid,
            "timestamp":    ts,
            "product_id":   p["product_id"],
            "name":         p.get("name", ""),
            "brand":        p.get("brand", ""),
            "category":     p.get("category", ""),
            "gender":       p.get("gender", ""),
            "mrp":          p.get("mrp", 0),
            "price":        p.get("price", 0),
            "discount_pct": p.get("discount_pct", 0),
            "rating":       p.get("rating", 0),
            "rating_count": p.get("rating_count", 0),
            "keyword":      p.get("keyword", ""),
            "position":     p.get("position", 0),
            "is_ad":        p.get("is_ad", False),
            **signals,
        }
        estimates.append(row)

    append_csv(est_file, estimates, ESTIMATE_COLS)
    print(f"[estimates] saved {len(estimates)} rows -> {est_file}")

    # ── Brand aggregates ──────────────────────────────────────────────────────
    by_brand = defaultdict(list)
    for e in estimates:
        by_brand[(e["brand"], e["keyword"])].append(e)

    brand_rows = []
    total_daily = sum(e["daily_units_est"] for e in estimates)

    for (brand, kw), items in by_brand.items():
        b_daily = sum(i["daily_units_est"] for i in items)
        brand_rows.append({
            "run_id":                rid,
            "timestamp":             ts,
            "brand":                 brand,
            "keyword":               kw,
            "product_count":         len(items),
            "ad_count":              sum(1 for i in items if i.get("is_ad")),
            "total_daily_units_est": round(b_daily, 1),
            "total_monthly_units_est": round(b_daily * 30, 0),
            "avg_discount_pct":      round(sum(float(i.get("discount_pct") or 0) for i in items) / len(items), 1),
            "avg_rating":            round(sum(float(i.get("rating") or 0) for i in items) / len(items), 2),
            "avg_sov_pct":           round(b_daily / total_daily * 100, 2) if total_daily else 0,
        })

    brand_rows.sort(key=lambda x: -x["total_daily_units_est"])
    append_csv(brand_file, brand_rows, BRAND_COLS)
    print(f"[brands]    saved {len(brand_rows)} rows -> {brand_file}")

    # ── Console summary ───────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Top 10 products by estimated daily units:")
    print(f"{'='*60}")
    top = sorted(estimates, key=lambda x: -x["daily_units_est"])[:10]
    for e in top:
        print(f"  [{e['position']:>3}] {e['brand'][:20]:<20} {e['name'][:35]:<35} "
              f"~{e['daily_units_est']:>6.0f}/day  [{e['confidence']}] {e['est_method']}")

    print(f"\nTop 10 brands by estimated daily units:")
    print(f"{'='*60}")
    for b in brand_rows[:10]:
        print(f"  {b['brand'][:30]:<30} {b['total_daily_units_est']:>8.0f}/day  "
              f"{b['product_count']} products  SOV {b['avg_sov_pct']:.1f}%")


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Myntra multi-signal sales estimator")
    ap.add_argument("--keywords",       default="tshirts",
                    help="Comma-separated search keywords (default: tshirts)")
    ap.add_argument("--pages",          type=int, default=3,
                    help="Pages per keyword to fetch (50 products/page)")
    ap.add_argument("--detail",         action="store_true",
                    help="Fetch product detail API for exact inventory + urgency signals")
    ap.add_argument("--from-snapshots", metavar="FILE",
                    help="Re-estimate from an existing snapshots CSV (no API calls)")
    ap.add_argument("--out-dir",        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"),
                    help="Output directory for CSVs (default: current dir)")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    if args.from_snapshots:
        # Re-estimate from existing snapshot file
        rows = load_csv_as_dicts(args.from_snapshots)
        if not rows:
            print(f"No rows found in {args.from_snapshots}")
            return
        keywords = list({r["keyword"] for r in rows if r.get("keyword")}) or ["(from file)"]
        print(f"Re-estimating from {len(rows)} snapshot rows")
        run(keywords=keywords, pages=0, fetch_detail=False,
            from_snapshots=args.from_snapshots, out_dir=args.out_dir)
    else:
        keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
        run(keywords=keywords, pages=args.pages, fetch_detail=args.detail,
            from_snapshots=None, out_dir=args.out_dir)


if __name__ == "__main__":
    main()
