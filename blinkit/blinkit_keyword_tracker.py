"""
Blinkit Keyword Rank Tracker
=============================
Tracks product positions for a set of keywords over time.
Approximates Share of Voice (SOV) without needing a brands/seller login.

What it captures per keyword per run:
  - Every product returned (all pages, up to limit)
  - Position (1-based rank in search results)
  - is_ad flag (position 1-3 with ad badge = sponsored)
  - Brand, product name, price, inventory, is_sold_out
  - Timestamps for trend analysis

Outputs:
  keyword_snapshots.csv   — every raw result per run
  keyword_sov.csv         — brand-level SOV summary per keyword per run
  keyword_ranks.csv       — specific product rank history over time

HOW IT WORKS:
  Uses curl_cffi (Chrome impersonation) to bypass Cloudflare.
  Searches up to N pages per keyword, extracts all products + positions.

INSTALL:
  pip install curl_cffi pandas --break-system-packages

USAGE:
  # Track keywords every 6 hours
  python blinkit_keyword_tracker.py --keywords "protein powder,chips,cold coffee" --interval 360

  # Single run, custom keyword file (one keyword per line)
  python blinkit_keyword_tracker.py --keyword-file keywords.txt --once

  # Track specific brands
  python blinkit_keyword_tracker.py --keywords "biscuits" --brands "Britannia,Parle,Oreo"

  # More pages per keyword (default=3 = 72 products)
  python blinkit_keyword_tracker.py --keywords "chips" --pages 5
"""

import argparse
import csv
import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime

try:
    from curl_cffi import requests as cf_requests
    CURL_OK = True
except ImportError:
    import requests as cf_requests
    CURL_OK = False
    print("WARNING: curl_cffi not found, using requests (may get Cloudflare blocked)")
    print("Fix: pip install curl_cffi --break-system-packages")

# ── Config ────────────────────────────────────────────────────────────────────

# Location presets — change or pass --locality to override
LOCATIONS = {
    "vijayawada": ("16.5103525", "80.6465468", "2111"),
    "mumbai":     ("19.0760",   "72.8777",    "1"),
    "bangalore":  ("12.9716",   "77.5946",    "4"),
    "delhi":      ("28.6139",   "77.2090",    "2"),
    "hyderabad":  ("17.3850",   "78.4867",    "5"),
}
LAT, LON, LOCALITY = LOCATIONS["vijayawada"]  # default
LIMIT    = 24   # products per page

HEADERS = {
    "content-type":    "application/json",
    "app-version":     "1000000",
    "web-version":     "1000000",
    "web_app_version": "1008010016",
    "app_client":      "consumer_web",
    "lat":             LAT,
    "lon":             LON,
    "locality":        LOCALITY,
}

# Optional: paste your blinkit.com cookies here to get ad-served results
# Get from DevTools → Application → Cookies → blinkit.com
# Format: "cookie_name=value; cookie_name2=value2"
SESSION_COOKIE = ""  # e.g. "gr_1_auth=xxxxx; _gcl_au=xxxxx"
if SESSION_COOKIE:
    HEADERS["cookie"] = SESSION_COOKIE

SNAPSHOT_FILE = "keyword_snapshots.csv"
SOV_FILE      = "keyword_sov.csv"
RANKS_FILE    = "keyword_ranks.csv"

SNAPSHOT_COLS = [
    "run_id", "timestamp", "keyword",
    "position", "is_ad",
    "product_id", "group_id", "name", "brand", "unit",
    "price", "mrp", "discount_pct",
    "inventory", "is_sold_out",
    "merchant_type", "offer_tag",
]

SOV_COLS = [
    "run_id", "timestamp", "keyword",
    "brand",
    "total_positions",       # sum of all positions brand appeared
    "appearance_count",      # how many results featured this brand
    "top3_count",            # results in positions 1-3
    "ad_count",              # sponsored results
    "avg_position",          # lower = better
    "sov_pct",               # appearance_count / total_results * 100
    "ad_sov_pct",            # ad_count / total_ads * 100
]

RANKS_COLS = [
    "run_id", "timestamp", "keyword",
    "product_id", "name", "brand",
    "position", "is_ad",
    "price", "inventory",
]

# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_price(text) -> float:
    if isinstance(text, (int, float)):
        return float(text)
    if not text:
        return 0.0
    return float(re.sub(r"[^\d.]", "", str(text)) or 0)


def parse_snippet(snippet: dict, position: int, keyword: str) -> dict | None:
    d = snippet.get("data", {})

    # Skip header/banner widgets — not actual products
    widget_type = snippet.get("widget_type", "")
    if "Header" in widget_type or "Banner" in widget_type or "header" in widget_type:
        return None

    product_id = d.get("product_id") or d.get("identity", {}).get("id")
    if not product_id:
        return None

    cart_item = (
        d.get("atc_action", {})
         .get("add_to_cart", {})
         .get("cart_item", {})
    )

    name  = cart_item.get("product_name") or d.get("name", {}).get("text", "")
    brand = cart_item.get("brand") or d.get("brand_name", {}).get("text", "")
    unit  = cart_item.get("unit") or d.get("variant", {}).get("text", "")
    price = cart_item.get("price") or parse_price(d.get("normal_price", {}).get("text", ""))
    mrp   = cart_item.get("mrp")   or parse_price(d.get("mrp", {}).get("text", ""))
    inv   = cart_item.get("inventory")
    if inv is None:
        inv = d.get("inventory")

    group_id     = cart_item.get("group_id") or d.get("group_id")
    merchant_type = cart_item.get("merchant_type", "")
    is_sold_out  = bool(d.get("soldout_tag") or (inv is not None and inv == 0))

    # discount
    disc = 0.0
    if mrp and price and mrp > price:
        disc = round((mrp - price) / mrp * 100, 1)

    # offer tag — skip ETA and Ad badges
    offer_tag = ""
    for badge in (d.get("product_badges", []) or snippet.get("product_badges", [])):
        label = badge.get("label", "")
        btype = badge.get("type", "")
        if label and btype not in ("ETA", "OTHERS") and label.lower() not in ("ad", "eta"):
            offer_tag = label
            break

    # is_ad detection — confirmed badges are on d.product_badges
    all_badges = d.get("product_badges", [])
    # DEBUG: uncomment to see raw badges
    # if all_badges: print(f"  DBG badges: {[b.get('label') for b in all_badges]}")
    ad_flag = any(
        b.get("type") == "OTHERS" and b.get("label", "").lower() == "ad"
        for b in all_badges
    )
    if not ad_flag:
        wtype = snippet.get("widget_type", "")
        ad_flag = "sponsor" in wtype.lower() or "promoted" in wtype.lower()

    return {
        "product_id":   str(product_id),
        "group_id":     str(group_id or ""),
        "name":         name,
        "brand":        brand,
        "unit":         unit,
        "price":        price,
        "mrp":          mrp,
        "discount_pct": disc,
        "inventory":    inv if inv is not None else "",
        "is_sold_out":  is_sold_out,
        "merchant_type": merchant_type,
        "offer_tag":    offer_tag,
        "position":     position,
        "is_ad":        ad_flag,
        "keyword":      keyword,
    }


def search_keyword(keyword: str, pages: int = 3) -> list[dict]:
    """Fetch up to pages*LIMIT products for a keyword. Returns parsed product list."""
    all_products = []
    global_position = 1

    for page in range(pages):
        offset = page * LIMIT
        url = (
            f"https://blinkit.com/v1/layout/search"
            f"?q={keyword.replace(' ', '+')}"
            f"&search_type=type_to_search"
            f"&offset={offset}&limit={LIMIT}"
        )
        try:
            if CURL_OK:
                r = cf_requests.post(url, headers=HEADERS, impersonate="chrome120", timeout=15)
            else:
                r = cf_requests.post(url, headers=HEADERS, timeout=15)

            if r.status_code != 200:
                print(f"  [{keyword}] page {page+1} → HTTP {r.status_code}")
                break

            data = r.json()
            snippets = (
                data.get("response", {})
                    .get("snippets", [])
            )

            if not snippets:
                break  # no more results

            page_products = []
            for snip in snippets:
                parsed = parse_snippet(snip, global_position, keyword)
                if parsed:
                    page_products.append(parsed)
                    global_position += 1

            all_products.extend(page_products)

            if len(snippets) < LIMIT:
                break  # last page

            time.sleep(0.5)  # be polite between pages

        except Exception as e:
            print(f"  [{keyword}] page {page+1} error: {e}")
            break

    return all_products


# ── SOV Calculation ───────────────────────────────────────────────────────────

def calc_sov(products: list[dict], keyword: str, run_id: str, ts: str) -> list[dict]:
    """Compute brand-level SOV from product list."""
    total = len(products)
    total_ads = sum(1 for p in products if p["is_ad"])

    brand_data = defaultdict(lambda: {
        "positions": [], "ad_count": 0, "top3_count": 0
    })

    for p in products:
        b = p["brand"] or "Unknown"
        brand_data[b]["positions"].append(p["position"])
        if p["is_ad"]:
            brand_data[b]["ad_count"] += 1
        if p["position"] <= 3:
            brand_data[b]["top3_count"] += 1

    rows = []
    for brand, bd in sorted(brand_data.items(), key=lambda x: len(x[1]["positions"]), reverse=True):
        count = len(bd["positions"])
        rows.append({
            "run_id":           run_id,
            "timestamp":        ts,
            "keyword":          keyword,
            "brand":            brand,
            "appearance_count": count,
            "total_positions":  sum(bd["positions"]),
            "top3_count":       bd["top3_count"],
            "ad_count":         bd["ad_count"],
            "avg_position":     round(sum(bd["positions"]) / count, 1),
            "sov_pct":          round(count / total * 100, 1) if total else 0,
            "ad_sov_pct":       round(bd["ad_count"] / total_ads * 100, 1) if total_ads else 0,
        })

    return rows


# ── CSV Writers ───────────────────────────────────────────────────────────────

def append_csv(filepath: str, rows: list[dict], cols: list[str]):
    write_header = not os.path.exists(filepath)
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        if write_header:
            w.writeheader()
        w.writerows(rows)


# ── Main Run ──────────────────────────────────────────────────────────────────

def run_once(keywords: list[str], pages: int, brand_filter: list[str] | None):
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    ts     = datetime.now().isoformat()

    print(f"\n{'='*60}")
    print(f"Run: {run_id}  |  Keywords: {len(keywords)}  |  Pages/kw: {pages}")
    print(f"{'='*60}")

    all_snapshots = []
    all_sov       = []
    all_ranks     = []

    for kw in keywords:
        print(f"\n→ '{kw}' ...", end=" ", flush=True)
        products = search_keyword(kw, pages=pages)
        print(f"{len(products)} products found")

        if not products:
            continue

        # filter by brand if requested
        if brand_filter:
            filtered = [p for p in products if any(
                bf.lower() in (p["brand"] or "").lower() for bf in brand_filter
            )]
            print(f"  Brand filter → {len(filtered)} products")
        else:
            filtered = products

        # Snapshots (all products)
        snapshot_rows = []
        for p in products:
            row = {"run_id": run_id, "timestamp": ts}
            row.update(p)
            snapshot_rows.append(row)
        all_snapshots.extend(snapshot_rows)

        # SOV
        sov_rows = calc_sov(products, kw, run_id, ts)
        all_sov.extend(sov_rows)

        # Rank history (filtered if brand filter set, else all)
        for p in filtered:
            all_ranks.append({
                "run_id":    run_id,
                "timestamp": ts,
                "keyword":   kw,
                "product_id": p["product_id"],
                "name":      p["name"],
                "brand":     p["brand"],
                "position":  p["position"],
                "is_ad":     p["is_ad"],
                "price":     p["price"],
                "inventory": p["inventory"],
            })

        # Print SOV summary
        print(f"\n  {'Brand':<25} {'Count':>5} {'SOV%':>6} {'AvgPos':>7} {'Ads':>4} {'Top3':>5}")
        print(f"  {'-'*55}")
        for row in sov_rows[:10]:
            print(
                f"  {row['brand']:<25} "
                f"{row['appearance_count']:>5} "
                f"{row['sov_pct']:>6.1f}% "
                f"{row['avg_position']:>7.1f} "
                f"{row['ad_count']:>4} "
                f"{row['top3_count']:>5}"
            )

        time.sleep(1.0)

    # Write CSVs
    if all_snapshots:
        append_csv(SNAPSHOT_FILE, all_snapshots, SNAPSHOT_COLS)
        print(f"\n✅ Snapshots → {SNAPSHOT_FILE} (+{len(all_snapshots)} rows)")

    if all_sov:
        append_csv(SOV_FILE, all_sov, SOV_COLS)
        print(f"✅ SOV       → {SOV_FILE} (+{len(all_sov)} rows)")

    if all_ranks:
        append_csv(RANKS_FILE, all_ranks, RANKS_COLS)
        print(f"✅ Ranks     → {RANKS_FILE} (+{len(all_ranks)} rows)")

    return run_id


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Blinkit Keyword Rank & SOV Tracker")
    parser.add_argument("--keywords", type=str, help="Comma-separated keywords")
    parser.add_argument("--keyword-file", type=str, help="File with one keyword per line")
    parser.add_argument("--brands", type=str, help="Comma-separated brands to track in rank history")
    parser.add_argument("--pages", type=int, default=3, help="Pages per keyword (default=3, 72 products)")
    parser.add_argument("--interval", type=int, default=0, help="Run every N minutes (0 = run once)")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--location", type=str, default="vijayawada",
                        help="Location preset: vijayawada/mumbai/bangalore/delhi/hyderabad")
    args = parser.parse_args()

    # Apply location
    if args.location in LOCATIONS:
        import sys
        lat, lon, loc = LOCATIONS[args.location]
        HEADERS["lat"] = lat
        HEADERS["lon"] = lon
        HEADERS["locality"] = loc
        print(f"Location: {args.location} (lat={lat}, lon={lon}, locality={loc})")
    else:
        print(f"Unknown location '{args.location}', using vijayawada")

    # Build keyword list
    keywords = []
    if args.keywords:
        keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    if args.keyword_file:
        with open(args.keyword_file) as f:
            keywords += [line.strip() for line in f if line.strip()]
    if not keywords:
        # Default demo keywords
        keywords = [
            "protein powder",
            "chips",
            "cold coffee",
            "biscuits",
            "energy drink",
        ]
        print(f"No keywords specified — using defaults: {keywords}")

    brand_filter = None
    if args.brands:
        brand_filter = [b.strip() for b in args.brands.split(",") if b.strip()]
        print(f"Brand filter active: {brand_filter}")

    if args.once or args.interval == 0:
        run_once(keywords, args.pages, brand_filter)
        return

    # Continuous loop
    print(f"Starting continuous tracking every {args.interval} minutes")
    print(f"Keywords: {keywords}")
    print("Press Ctrl+C to stop\n")

    while True:
        try:
            run_once(keywords, args.pages, brand_filter)
            next_run = datetime.now().strftime("%H:%M:%S")
            print(f"\nNext run in {args.interval} minutes (at ~{next_run})")
            time.sleep(args.interval * 60)
        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except Exception as e:
            print(f"Run failed: {e} — retrying in 60s")
            time.sleep(60)


if __name__ == "__main__":
    main()
