"""
flipkart_scraper.py — Flipkart scraper + sales estimator

Mirrors amazon_scraper.py and myntra_sales_estimator.py structure.

SIGNALS used for sales estimation:
  1. Rating velocity               — new ratings/day ÷ conversion rate [HIGH]
  2. Search rank position          — power-law proxy                   [MEDIUM]
  3. Stock count ("Only X left")   — fast-mover signal                 [MEDIUM]
  4. Discount depth                — deeper discount → higher volume   [LOW]

Flipkart has no public BSR equivalent, so rating velocity + rank are primary.

OUTPUTS:
  flipkart_snapshots.csv       — raw search card data per run
  flipkart_pdp_snapshots.csv   — full PDP data (seller, stock, all fields)
  flipkart_sales_estimates.csv — per-product sales estimates
  flipkart_brand_estimates.csv — brand-level aggregates

USAGE:
  # Search only (fast)
  python flipkart/flipkart_scraper.py --keywords "protein powder,chips"

  # Search + fetch PDP for top 20 products
  python flipkart/flipkart_scraper.py --keywords "protein powder" --detail --top 20

  # Direct PDP fetch by PID list
  python flipkart/flipkart_scraper.py --pids MOBBG7QGHYDFFHGT,TVSG7Q5FHKTSF3GZ

  # Re-estimate from existing snapshots (no HTTP calls)
  python flipkart/flipkart_scraper.py --from-snapshots data/flipkart_pdp_snapshots.csv
"""

import argparse, json, math, os, sys
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from flipkart_core import (
    fetch_search_all_pages, fetch_pdp,
    append_csv, load_csv_as_dicts,
    now_str, run_id, delay,
)

# ── Calibration constants ──────────────────────────────────────────────────────

RATING_CONVERSION_RATE = 0.03   # ~3% of Flipkart buyers leave a rating (slightly lower than Amazon)
RETURN_RATE            = 0.15   # ~15% return rate
MIN_HOURS_FOR_VELOCITY = 1.0

RANK_BASE_SALES = 120
RANK_DECAY      = 0.09

SIGNAL_WEIGHTS = {
    "rating_velocity": 2.5,
    "stock_signal":    1.5,
    "rank_score":      1.0,
    "discount_signal": 0.5,
}

# ── CSV schemas ────────────────────────────────────────────────────────────────

SNAPSHOT_COLS = [
    "run_id", "timestamp",
    "pid", "url", "title", "brand", "category", "image_url",
    "price", "mrp", "discount_pct", "offer_tag",
    "avg_rating", "rating_count", "review_count", "rating_dist",
    "stock_count", "is_oos",
    "seller_name", "seller_count",
    "bought_past_month",
    "is_ad", "position", "keyword",
    "scraped_at",
]

ESTIMATE_COLS = [
    "run_id", "timestamp",
    "pid", "title", "brand", "category",
    "price", "mrp", "discount_pct",
    "avg_rating", "rating_count",
    "bought_past_month", "stock_count", "is_oos",
    "daily_units_est", "monthly_units_est",
    "net_daily_units_est", "net_monthly_units_est",
    "confidence", "est_method", "signal_breakdown",
    "rating_vel_daily", "stock_signal_daily", "rank_score_daily", "discount_signal_daily",
    "is_ad", "position", "keyword",
]

BRAND_COLS = [
    "run_id", "timestamp", "brand", "keyword",
    "product_count", "ad_count",
    "total_daily_units_est", "total_monthly_units_est",
    "avg_discount_pct", "avg_rating",
    "sov_pct",
]


# ── Signal calculators ─────────────────────────────────────────────────────────

def rank_to_daily(position: int) -> float:
    return round(RANK_BASE_SALES * math.exp(-RANK_DECAY * max(position - 1, 0)), 1)

def stock_to_daily(stock_count: int) -> float:
    if stock_count <= 3:
        return round(stock_count * 3.0, 1)
    elif stock_count <= 10:
        return round(stock_count * 1.5, 1)
    else:
        return round(stock_count * 0.5, 1)

def discount_to_daily(discount_pct: float, rank_daily: float) -> float:
    """Higher discount → higher velocity multiplier (capped at 2x)."""
    if discount_pct <= 0:
        return 0.0
    mult = min(1.0 + discount_pct / 100, 2.0)
    return round(rank_daily * (mult - 1.0), 1)


def estimate(product: dict, prev: dict | None, hours_elapsed: float) -> dict:
    signals = {}
    methods = []

    # Signal 1: Rating velocity
    rating_vel_daily = 0.0
    if prev and hours_elapsed >= MIN_HOURS_FOR_VELOCITY:
        curr_rc = int(product.get("rating_count") or 0)
        prev_rc = int(prev.get("rating_count") or 0)
        if curr_rc > prev_rc:
            new_per_day      = (curr_rc - prev_rc) / (hours_elapsed / 24)
            rating_vel_daily = round(new_per_day / RATING_CONVERSION_RATE, 1)
            signals["rating_velocity"] = rating_vel_daily
            methods.append("rating_velocity")

    # Signal 2: Stock count
    stock_signal_daily = 0.0
    try:
        sc = product.get("stock_count")
        if sc is not None:
            sc = int(sc)
            stock_signal_daily = stock_to_daily(sc)
            signals["stock_signal"] = stock_signal_daily
            methods.append("stock_signal")
    except (TypeError, ValueError):
        pass

    # Signal 3: Search rank (always present)
    position   = int(product.get("position") or 99)
    rank_daily = rank_to_daily(position)
    signals["rank_score"] = rank_daily

    # Signal 4: Discount depth
    discount_signal_daily = 0.0
    try:
        disc = float(product.get("discount_pct") or 0)
        if disc > 5:
            discount_signal_daily = discount_to_daily(disc, rank_daily)
            signals["discount_signal"] = discount_signal_daily
    except (TypeError, ValueError):
        pass

    total_w   = sum(SIGNAL_WEIGHTS.get(s, 1.0) for s in signals)
    weighted  = sum(v * SIGNAL_WEIGHTS.get(s, 1.0) for s, v in signals.items())

    daily_est  = round((weighted / total_w), 1) if total_w else rank_daily
    confidence = "high" if len(methods) >= 2 else ("medium" if len(methods) == 1 else "low")

    monthly_est = round(daily_est * 30, 0)
    net_daily   = round(daily_est * (1 - RETURN_RATE), 1)
    net_monthly = round(net_daily * 30, 0)

    return {
        "daily_units_est":         daily_est,
        "monthly_units_est":       monthly_est,
        "net_daily_units_est":     net_daily,
        "net_monthly_units_est":   net_monthly,
        "confidence":              confidence,
        "est_method":              ",".join(methods) if methods else "rank_only",
        "signal_breakdown":        json.dumps({k: round(v, 1) for k, v in signals.items()}),
        "rating_vel_daily":        rating_vel_daily,
        "stock_signal_daily":      stock_signal_daily,
        "rank_score_daily":        rank_daily,
        "discount_signal_daily":   discount_signal_daily,
    }


# ── Main pipeline ──────────────────────────────────────────────────────────────

def run(keywords: list[str], fetch_detail: bool, top_n: int,
        pids: list[str], out_dir: str):
    rid = run_id()
    ts  = now_str()

    snap_file  = os.path.join(out_dir, "flipkart_snapshots.csv")
    pdp_file   = os.path.join(out_dir, "flipkart_pdp_snapshots.csv")
    est_file   = os.path.join(out_dir, "flipkart_sales_estimates.csv")
    brand_file = os.path.join(out_dir, "flipkart_brand_estimates.csv")

    # Load previous snapshots for velocity
    prev_snaps = {}
    for row in load_csv_as_dicts(pdp_file):
        pid = row.get("pid", "")
        if pid and (pid not in prev_snaps or row["timestamp"] > prev_snaps[pid]["timestamp"]):
            prev_snaps[pid] = row

    print(f"\n{'='*65}")
    print(f"FLIPKART SCRAPER")
    print(f"Run: {rid}")
    print(f"{'='*65}")

    # ── Direct PID mode ───────────────────────────────────────────────────────
    if pids:
        print(f"\n[pdp] Fetching {len(pids)} PIDs")
        pdp_rows = []
        for i, pid in enumerate(pids):
            print(f"  [{i+1}/{len(pids)}] {pid}", end=" ", flush=True)
            p = fetch_pdp(pid)
            if p:
                p.update({"run_id": rid, "timestamp": ts, "keyword": "", "position": 0, "is_ad": False})
                pdp_rows.append(p)
                print(f"[ok] {p.get('title','')[:50]}")
            else:
                print("FAIL")
            delay(2.0, 4.5)
        append_csv(pdp_file, pdp_rows, SNAPSHOT_COLS)
        _write_estimates(pdp_rows, prev_snaps, ts, rid, est_file, brand_file)
        print(f"\n[done] {pdp_file} (+{len(pdp_rows)} rows)")
        return

    # ── Search mode ───────────────────────────────────────────────────────────
    all_products = []
    seen = set()
    for kw in keywords:
        print(f"\n[search] '{kw}'")
        products = fetch_search_all_pages(kw, max_pages=5)
        for p in products:
            if p["pid"] not in seen:
                p.update({"run_id": rid, "timestamp": ts})
                all_products.append(p)
                seen.add(p["pid"])
        delay(2.0, 4.0)

    if not all_products:
        print("No products found.")
        return

    append_csv(snap_file, all_products, SNAPSHOT_COLS)
    print(f"\n[done] {snap_file} (+{len(all_products)} rows)")

    # ── Optional PDP enrichment ───────────────────────────────────────────────
    if fetch_detail:
        top = sorted(all_products,
                     key=lambda x: int(x.get("rating_count") or 0),
                     reverse=True)[:top_n]
        print(f"\n[pdp] Fetching detail for top {len(top)} products...")
        pdp_rows = []
        for i, p in enumerate(top):
            print(f"  [{i+1}/{len(top)}] {p['pid']} {p.get('title','')[:45]}", end=" ", flush=True)
            detail = fetch_pdp(p["pid"])
            if detail:
                detail.update({
                    "run_id": rid, "timestamp": ts,
                    "keyword":  p.get("keyword", ""),
                    "position": p.get("position", 0),
                    "is_ad":    p.get("is_ad", False),
                })
                pdp_rows.append(detail)
                print(f"[ok] stock={detail.get('stock_count','?')} seller={detail.get('seller_name','?')[:20]}")
            else:
                print("FAIL")
            delay(2.0, 5.0)
        append_csv(pdp_file, pdp_rows, SNAPSHOT_COLS)
        print(f"[done] {pdp_file} (+{len(pdp_rows)} rows)")
        _write_estimates(pdp_rows, prev_snaps, ts, rid, est_file, brand_file)
    else:
        _write_estimates(all_products, prev_snaps, ts, rid, est_file, brand_file)


def _write_estimates(products: list[dict], prev_snaps: dict,
                     ts: str, rid: str, est_file: str, brand_file: str):
    estimates = []
    for p in products:
        prev  = prev_snaps.get(p.get("pid", ""))
        hours = 0.0
        if prev:
            try:
                t0    = datetime.fromisoformat(prev["timestamp"])
                t1    = datetime.fromisoformat(ts)
                hours = (t1 - t0).total_seconds() / 3600
            except Exception:
                pass
        sig = estimate(p, prev, hours)
        estimates.append({
            "run_id":         rid,
            "timestamp":      ts,
            "pid":            p.get("pid", ""),
            "title":          p.get("title", ""),
            "brand":          p.get("brand", ""),
            "category":       p.get("category", ""),
            "price":          p.get("price", 0),
            "mrp":            p.get("mrp", 0),
            "discount_pct":   p.get("discount_pct", 0),
            "avg_rating":     p.get("avg_rating", 0),
            "rating_count":   p.get("rating_count", 0),
            "bought_past_month": p.get("bought_past_month", 0),
            "stock_count":    p.get("stock_count", ""),
            "is_oos":         p.get("is_oos", False),
            "is_ad":          p.get("is_ad", False),
            "position":       p.get("position", 0),
            "keyword":        p.get("keyword", ""),
            **sig,
        })

    append_csv(est_file, estimates, ESTIMATE_COLS)

    # Brand aggregates
    by_brand    = defaultdict(list)
    for e in estimates:
        by_brand[(e.get("brand", "Unknown") or "Unknown", e.get("keyword", ""))].append(e)

    total_daily = sum(e["daily_units_est"] for e in estimates)
    brand_rows  = []
    for (brand, kw), items in by_brand.items():
        b_daily = sum(i["daily_units_est"] for i in items)
        brand_rows.append({
            "run_id":                  rid,
            "timestamp":               ts,
            "brand":                   brand,
            "keyword":                 kw,
            "product_count":           len(items),
            "ad_count":                sum(1 for i in items if i.get("is_ad")),
            "total_daily_units_est":   round(b_daily, 1),
            "total_monthly_units_est": round(b_daily * 30, 0),
            "avg_discount_pct":        round(sum(float(i.get("discount_pct") or 0) for i in items) / len(items), 1),
            "avg_rating":              round(sum(float(i.get("avg_rating") or 0) for i in items) / len(items), 2),
            "sov_pct":                 round(b_daily / total_daily * 100, 2) if total_daily else 0,
        })
    brand_rows.sort(key=lambda x: -x["total_daily_units_est"])
    append_csv(brand_file, brand_rows, BRAND_COLS)

    # Console summary
    print(f"\n{'='*75}")
    print(f"{'#':>4} {'Brand':<20} {'Title':<34} {'Bought/mo':>10} {'/Day':>7} {'Conf'}")
    print(f"{'-'*80}")
    for e in sorted(estimates, key=lambda x: -x["daily_units_est"])[:15]:
        bpm = e.get("bought_past_month", 0) or 0
        print(f"  {e['position']:>3} {str(e.get('brand',''))[:18]:<20} "
              f"{str(e.get('title',''))[:32]:<34} "
              f"{bpm:>10,} "
              f"{e['daily_units_est']:>7.0f} {e['confidence']}")
    print(f"\n{'Brand':<28} {'Prods':>5} {'/Day':>8} {'/Month':>10} {'SOV%':>6}")
    print(f"{'-'*60}")
    for b in brand_rows[:12]:
        print(f"  {str(b['brand'])[:26]:<28} {b['product_count']:>5} "
              f"{b['total_daily_units_est']:>8.0f} "
              f"{b['total_monthly_units_est']:>10.0f} "
              f"{b['sov_pct']:>6.1f}%")
    print(f"\n[done] {est_file} (+{len(estimates)} rows)")
    print(f"[done] {brand_file} (+{len(brand_rows)} rows)")


def run_from_snapshots(filepath: str, out_dir: str):
    rows = load_csv_as_dicts(filepath)
    if not rows:
        print(f"No rows in {filepath}")
        return

    by_pid = defaultdict(list)
    for r in rows:
        by_pid[r["pid"]].append(r)

    prev_map, last_map = {}, {}
    for pid, snaps in by_pid.items():
        snaps.sort(key=lambda x: x.get("timestamp", ""))
        last_map[pid] = snaps[-1]
        if len(snaps) >= 2:
            prev_map[pid] = snaps[-2]

    # Fix types from CSV strings
    for p in last_map.values():
        for field in ("stock_count",):
            try:
                p[field] = int(float(p[field])) if p.get(field) else None
            except (ValueError, TypeError):
                p[field] = None
        try:
            p["rating_count"] = int(float(p.get("rating_count") or 0))
        except (ValueError, TypeError):
            p["rating_count"] = 0
        try:
            p["bought_past_month"] = int(float(p.get("bought_past_month") or 0))
        except (ValueError, TypeError):
            p["bought_past_month"] = 0

    rid = run_id()
    ts  = now_str()
    est_file   = os.path.join(out_dir, "flipkart_sales_estimates.csv")
    brand_file = os.path.join(out_dir, "flipkart_brand_estimates.csv")

    _write_estimates(list(last_map.values()), prev_map, ts, rid, est_file, brand_file)


# ── CLI ────────────────────────────────────────────────────────────────────────

ALL_CATEGORIES = [
    "protein powder", "vitamins", "electronics", "laptop", "smartphone",
    "headphones", "speakers", "camera", "watch", "shoes",
    "tshirts", "jeans", "kurta", "saree", "dress",
    "home decor", "kitchen appliances", "cookware", "bedsheets", "furniture",
    "toys", "books", "stationery", "pet food", "baby products",
    "face wash", "shampoo", "sunscreen", "perfume", "hair oil",
    "running shoes", "cricket bat", "yoga mat", "cycling", "gym equipment",
    "chips", "coffee", "tea", "dry fruits", "cooking oil",
]


def main():
    parser = argparse.ArgumentParser(description="Flipkart scraper + sales estimator")
    parser.add_argument("--keywords",       type=str)
    parser.add_argument("--all-categories", action="store_true", help="Scrape all predefined categories")
    parser.add_argument("--pids",           type=str, help="Comma-separated PIDs for direct PDP fetch")
    parser.add_argument("--detail",         action="store_true", help="Fetch PDP for top-N products")
    parser.add_argument("--top",            type=int, default=20)
    parser.add_argument("--from-snapshots", type=str)
    parser.add_argument("--out-dir",        type=str, default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    if args.from_snapshots:
        run_from_snapshots(args.from_snapshots, args.out_dir)
        return

    pids = [p.strip() for p in args.pids.split(",") if p.strip()] if args.pids else []

    if args.all_categories:
        keywords = ALL_CATEGORIES
    else:
        keywords = [k.strip() for k in args.keywords.split(",") if k.strip()] if args.keywords else []

    if not keywords and not pids:
        parser.error("Provide --keywords, --all-categories, --pids, or --from-snapshots")

    run(keywords=keywords, fetch_detail=args.detail,
        top_n=args.top, pids=pids, out_dir=args.out_dir)


if __name__ == "__main__":
    main()
