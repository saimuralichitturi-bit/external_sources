"""
amazon_scraper.py — Amazon.in scraper + sales estimator

Mirrors blinkit_sales_estimator.py and myntra_sales_estimator.py structure.

SIGNALS used for sales estimation:
  1. BSR (Best Sellers Rank)       — Amazon's own hourly-updated rank  [HIGH]
  2. Rating velocity               — new ratings/day ÷ conversion rate [HIGH]
  3. Stock count ("Only X left")   — fast-mover signal                 [MEDIUM]
  4. Search rank position          — power-law proxy                   [LOW]

OUTPUTS:
  amazon_snapshots.csv       — raw search card data per run
  amazon_pdp_snapshots.csv   — full PDP data (BSR, stock, seller, all fields)
  amazon_sales_estimates.csv — per-product sales estimates
  amazon_brand_estimates.csv — brand-level aggregates

USAGE:
  # Search only (fast, no BSR)
  python amazon/amazon_scraper.py --keywords "protein powder,chips"

  # Search + fetch PDP for top 20 products (gets BSR + stock)
  python amazon/amazon_scraper.py --keywords "protein powder" --detail --top 20

  # Direct PDP fetch by ASIN list
  python amazon/amazon_scraper.py --asins B07XJ8C8F5,B08N5WRWNW

  # Re-estimate from existing snapshots (no HTTP calls)
  python amazon/amazon_scraper.py --from-snapshots data/amazon_pdp_snapshots.csv
"""

import argparse, json, math, os, sys
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from amazon_core import (
    fetch_search_all_pages, fetch_pdp,
    append_csv, load_csv_as_dicts,
    now_str, run_id, delay,
)

# ── Calibration constants ─────────────────────────────────────────────────────

RATING_CONVERSION_RATE = 0.04   # ~4% of Amazon.in buyers leave a rating
RETURN_RATE            = 0.10   # ~10% return rate (lower than Myntra)
MIN_HOURS_FOR_VELOCITY = 1.0

# BSR → daily units: daily = base × e^(-decay × bsr)
# Calibrated from public Amazon seller benchmarks for India
BSR_BASELINES = {
    "Electronics":              (500,  0.000080),
    "Clothing":                 (300,  0.000100),
    "Shoes":                    (280,  0.000105),
    "Home & Kitchen":           (400,  0.000090),
    "Health & Personal Care":   (350,  0.000100),
    "Sports":                   (200,  0.000120),
    "Beauty":                   (250,  0.000110),
    "Baby":                     (180,  0.000130),
    "Books":                    (600,  0.000070),
    "Grocery":                  (700,  0.000060),
    "Toys":                     (220,  0.000120),
    "default":                  (300,  0.000100),
}

RANK_BASE_SALES = 150
RANK_DECAY      = 0.10

SIGNAL_WEIGHTS = {
    "bsr":             2.5,
    "rating_velocity": 2.0,
    "stock_signal":    1.5,
    "rank_score":      1.0,
}

# ── CSV schemas ───────────────────────────────────────────────────────────────

SNAPSHOT_COLS = [
    "run_id", "timestamp",
    "asin", "url", "title", "brand", "category", "image_url",
    "price", "mrp", "discount_pct", "offer_tag",
    "avg_rating", "rating_count", "review_count", "rating_dist",
    "bsr", "bsr_category",
    "stock_count", "is_oos",
    "seller_name", "seller_count", "is_amazon_sold", "fulfilled_by_amazon",
    "bought_past_month",
    "is_ad", "position", "keyword",
    "scraped_at",
]

ESTIMATE_COLS = [
    "run_id", "timestamp",
    "asin", "title", "brand", "category",
    "price", "mrp", "discount_pct",
    "avg_rating", "rating_count",
    "bsr", "bsr_category",
    "bought_past_month",
    "stock_count", "is_oos",
    "daily_units_est", "monthly_units_est",
    "net_daily_units_est", "net_monthly_units_est",
    "confidence", "est_method", "signal_breakdown",
    "bsr_daily", "rating_vel_daily", "stock_signal_daily", "rank_score_daily",
    "is_ad", "position", "keyword",
]

BRAND_COLS = [
    "run_id", "timestamp", "brand", "keyword",
    "product_count", "ad_count",
    "total_daily_units_est", "total_monthly_units_est",
    "avg_discount_pct", "avg_rating", "avg_bsr",
    "sov_pct",
]


# ── Signal calculators ────────────────────────────────────────────────────────

def bsr_to_daily(bsr: int, category: str) -> float:
    base, decay = BSR_BASELINES["default"]
    for key, vals in BSR_BASELINES.items():
        if key.lower() in (category or "").lower():
            base, decay = vals
            break
    return round(max(base * math.exp(-decay * bsr), 0.1), 1)

def rank_to_daily(position: int) -> float:
    return round(RANK_BASE_SALES * math.exp(-RANK_DECAY * (position - 1)), 1)

def stock_to_daily(stock_count: int) -> float:
    # "Only X left" → product is moving fast
    if stock_count <= 3:
        return round(stock_count * 3.0, 1)
    elif stock_count <= 10:
        return round(stock_count * 1.5, 1)
    else:
        return round(stock_count * 0.5, 1)


def estimate(product: dict, prev: dict | None, hours_elapsed: float) -> dict:
    signals = {}
    methods = []

    # Signal 1: BSR
    bsr_daily = 0.0
    try:
        bsr = int(product.get("bsr") or 0)
        if bsr > 0:
            bsr_daily = bsr_to_daily(bsr, product.get("bsr_category", ""))
            signals["bsr"] = bsr_daily
            methods.append("bsr")
    except (TypeError, ValueError):
        pass

    # Signal 2: Rating velocity
    rating_vel_daily = 0.0
    if prev and hours_elapsed >= MIN_HOURS_FOR_VELOCITY:
        curr_rc = int(product.get("rating_count") or 0)
        prev_rc = int(prev.get("rating_count") or 0)
        if curr_rc > prev_rc:
            new_per_day = (curr_rc - prev_rc) / (hours_elapsed / 24)
            rating_vel_daily = round(new_per_day / RATING_CONVERSION_RATE, 1)
            signals["rating_velocity"] = rating_vel_daily
            methods.append("rating_velocity")

    # Signal 3: Stock count
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

    # Signal 4: Search rank (always present, lowest weight)
    position = int(product.get("position") or 99)
    rank_daily = rank_to_daily(position)
    signals["rank_score"] = rank_daily

    # FBA multiplier
    fba_mult = 1.15 if product.get("fulfilled_by_amazon") else 1.0

    total_w = sum(SIGNAL_WEIGHTS.get(s, 1.0) for s in signals)
    weighted = sum(v * SIGNAL_WEIGHTS.get(s, 1.0) for s, v in signals.items())

    daily_est  = round((weighted / total_w) * fba_mult, 1) if total_w else rank_daily
    confidence = "high" if len(methods) >= 2 else ("medium" if len(methods) == 1 else "low")

    monthly_est     = round(daily_est * 30, 0)
    net_daily       = round(daily_est * (1 - RETURN_RATE), 1)
    net_monthly     = round(net_daily * 30, 0)

    return {
        "daily_units_est":       daily_est,
        "monthly_units_est":     monthly_est,
        "net_daily_units_est":   net_daily,
        "net_monthly_units_est": net_monthly,
        "confidence":            confidence,
        "est_method":            ",".join(methods) if methods else "rank_only",
        "signal_breakdown":      json.dumps({k: round(v, 1) for k, v in signals.items()}),
        "bsr_daily":             bsr_daily,
        "rating_vel_daily":      rating_vel_daily,
        "stock_signal_daily":    stock_signal_daily,
        "rank_score_daily":      rank_daily,
    }


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(keywords: list[str], fetch_detail: bool, top_n: int,
        asins: list[str], out_dir: str):
    rid = run_id()
    ts  = now_str()

    snap_file  = os.path.join(out_dir, "amazon_snapshots.csv")
    pdp_file   = os.path.join(out_dir, "amazon_pdp_snapshots.csv")
    est_file   = os.path.join(out_dir, "amazon_sales_estimates.csv")
    brand_file = os.path.join(out_dir, "amazon_brand_estimates.csv")

    # Load previous snapshots for velocity
    prev_snaps = {}
    for row in load_csv_as_dicts(pdp_file):
        asin = row.get("asin", "")
        if asin and (asin not in prev_snaps or row["timestamp"] > prev_snaps[asin]["timestamp"]):
            prev_snaps[asin] = row

    print(f"\n{'='*65}")
    print(f"AMAZON.IN SCRAPER")
    print(f"Run: {rid}")
    print(f"{'='*65}")

    # ── Direct ASIN mode ──────────────────────────────────────────────────────
    if asins:
        print(f"\n[pdp] Fetching {len(asins)} ASINs")
        pdp_rows = []
        for i, asin in enumerate(asins):
            print(f"  [{i+1}/{len(asins)}] {asin}", end=" ", flush=True)
            p = fetch_pdp(asin)
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
            if p["asin"] not in seen:
                p.update({"run_id": rid, "timestamp": ts})
                all_products.append(p)
                seen.add(p["asin"])
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
            print(f"  [{i+1}/{len(top)}] {p['asin']} {p.get('title','')[:45]}", end=" ", flush=True)
            detail = fetch_pdp(p["asin"])
            if detail:
                detail.update({
                    "run_id": rid, "timestamp": ts,
                    "keyword": p.get("keyword", ""),
                    "position": p.get("position", 0),
                    "is_ad": p.get("is_ad", False),
                })
                pdp_rows.append(detail)
                print(f"[ok] bsr={detail.get('bsr','?')} stock={detail.get('stock_count','?')}")
            else:
                print("FAIL")
            delay(2.0, 5.0)
        append_csv(pdp_file, pdp_rows, SNAPSHOT_COLS)
        print(f"[done] {pdp_file} (+{len(pdp_rows)} rows)")
        _write_estimates(pdp_rows, prev_snaps, ts, rid, est_file, brand_file)
    else:
        # Estimate from search data only (rank signal only — low confidence)
        _write_estimates(all_products, prev_snaps, ts, rid, est_file, brand_file)


def _write_estimates(products: list[dict], prev_snaps: dict,
                     ts: str, rid: str, est_file: str, brand_file: str):
    estimates = []
    for p in products:
        prev    = prev_snaps.get(p.get("asin", ""))
        hours   = 0.0
        if prev:
            try:
                t0 = datetime.fromisoformat(prev["timestamp"])
                t1 = datetime.fromisoformat(ts)
                hours = (t1 - t0).total_seconds() / 3600
            except Exception:
                pass
        sig = estimate(p, prev, hours)
        estimates.append({
            "run_id": rid, "timestamp": ts,
            "asin":         p.get("asin", ""),
            "title":        p.get("title", ""),
            "brand":        p.get("brand", ""),
            "category":     p.get("category", ""),
            "price":        p.get("price", 0),
            "mrp":          p.get("mrp", 0),
            "discount_pct": p.get("discount_pct", 0),
            "avg_rating":        p.get("avg_rating", 0),
            "rating_count":      p.get("rating_count", 0),
            "bsr":               p.get("bsr", ""),
            "bsr_category":      p.get("bsr_category", ""),
            "bought_past_month": p.get("bought_past_month", 0),
            "stock_count":       p.get("stock_count", ""),
            "is_oos":            p.get("is_oos", False),
            "is_ad":             p.get("is_ad", False),
            "position":     p.get("position", 0),
            "keyword":      p.get("keyword", ""),
            **sig,
        })

    append_csv(est_file, estimates, ESTIMATE_COLS)

    # Brand aggregates
    by_brand = defaultdict(list)
    for e in estimates:
        by_brand[(e.get("brand", "Unknown"), e.get("keyword", ""))].append(e)

    total_daily = sum(e["daily_units_est"] for e in estimates)
    brand_rows  = []
    for (brand, kw), items in by_brand.items():
        b_daily  = sum(i["daily_units_est"] for i in items)
        bsr_vals = [int(i["bsr"]) for i in items
                    if i.get("bsr") and str(i["bsr"]).lstrip('-').isdigit()]
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
            "avg_bsr":                 round(sum(bsr_vals) / len(bsr_vals)) if bsr_vals else "",
            "sov_pct":                 round(b_daily / total_daily * 100, 2) if total_daily else 0,
        })
    brand_rows.sort(key=lambda x: -x["total_daily_units_est"])
    append_csv(brand_file, brand_rows, BRAND_COLS)

    # Console summary
    print(f"\n{'='*75}")
    print(f"{'#':>4} {'Brand':<20} {'Title':<34} {'BSR':>8} {'Bought/mo':>10} {'/Day':>7} {'Conf'}")
    print(f"{'-'*90}")
    for e in sorted(estimates, key=lambda x: -x["daily_units_est"])[:15]:
        bpm = e.get("bought_past_month", 0) or 0
        print(f"  {e['position']:>3} {str(e.get('brand',''))[:18]:<20} "
              f"{str(e.get('title',''))[:32]:<34} "
              f"{str(e.get('bsr','?')):>8} "
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

    by_asin = defaultdict(list)
    for r in rows:
        by_asin[r["asin"]].append(r)

    prev_map, last_map = {}, {}
    for asin, snaps in by_asin.items():
        snaps.sort(key=lambda x: x.get("timestamp", ""))
        last_map[asin] = snaps[-1]
        if len(snaps) >= 2:
            prev_map[asin] = snaps[-2]

    # Fix types from CSV strings
    for p in last_map.values():
        for field in ("bsr", "stock_count"):
            try:
                p[field] = int(float(p[field])) if p.get(field) else None
            except (ValueError, TypeError):
                p[field] = None
        try:
            p["rating_count"] = int(float(p.get("rating_count") or 0))
        except (ValueError, TypeError):
            p["rating_count"] = 0
        try:
            p["fulfilled_by_amazon"] = str(p.get("fulfilled_by_amazon", "")).lower() == "true"
        except Exception:
            p["fulfilled_by_amazon"] = False

    rid = run_id()
    ts  = now_str()
    est_file   = os.path.join(out_dir, "amazon_sales_estimates.csv")
    brand_file = os.path.join(out_dir, "amazon_brand_estimates.csv")

    _write_estimates(list(last_map.values()), prev_map, ts, rid, est_file, brand_file)


# ── CLI ───────────────────────────────────────────────────────────────────────

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
    parser = argparse.ArgumentParser(description="Amazon.in scraper + sales estimator")
    parser.add_argument("--keywords",       type=str)
    parser.add_argument("--all-categories", action="store_true", help="Scrape all predefined categories")
    parser.add_argument("--asins",          type=str, help="Comma-separated ASINs for direct PDP fetch")
    parser.add_argument("--detail",         action="store_true", help="Fetch PDP for top-N products")
    parser.add_argument("--top",            type=int, default=20)
    parser.add_argument("--from-snapshots", type=str)
    parser.add_argument("--out-dir",        type=str,
                        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    if args.from_snapshots:
        run_from_snapshots(args.from_snapshots, args.out_dir)
        return

    asins = [a.strip() for a in args.asins.split(",") if a.strip()] if args.asins else []

    if args.all_categories:
        keywords = ALL_CATEGORIES
    else:
        keywords = [k.strip() for k in args.keywords.split(",") if k.strip()] if args.keywords else []

    if not keywords and not asins:
        parser.error("Provide --keywords, --all-categories, --asins, or --from-snapshots")

    run(keywords=keywords, fetch_detail=args.detail,
        top_n=args.top, asins=asins, out_dir=args.out_dir)


if __name__ == "__main__":
    main()