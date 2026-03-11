"""
blinkit_launch_detector.py
===========================
Detects new product launches on Blinkit before they get press coverage.
Monitors categories and keywords for product_ids that have never been seen before.

HOW IT WORKS:
  - Maintains a database of known product_ids per category/keyword
  - Each run compares current results against the known set
  - New product_ids = potential new launches
  - Tracks: first_seen date, brand, price, images

OUTPUTS:
  launches.csv           — all detected new products with metadata
  launch_summary.json    — running count of launches per brand
  known_products/        — per-category known product ID databases

USAGE:
  # Monitor snacks + beverages for new launches daily
  python blinkit_launch_detector.py --categories 3,4 --interval 1440

  # Monitor a brand's launches via keyword
  python blinkit_launch_detector.py --keywords "maaza,slice,frooti" --interval 360

  # One-time scan of all categories
  python blinkit_launch_detector.py --all --location mumbai

  # Get launch report for last 30 days
  python blinkit_launch_detector.py --report --days 30
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from blinkit_core import (
    make_headers, post, parse_snippet, append_csv,
    load_csv_as_dicts, now_str, run_id
)

LAUNCH_FILE   = "launches.csv"
SUMMARY_FILE  = "launch_summary.json"
KNOWN_DIR     = "known_products"

LAUNCH_COLS = [
    "first_seen", "run_id",
    "source_type", "source",          # "category" or "keyword"
    "product_id", "group_id",
    "name", "brand", "unit",
    "price", "mrp", "discount_pct", "offer_tag",
    "inventory", "is_sold_out",
    "image_url",
    "blinkit_url",
]

CATEGORIES = {
    1:  "Vegetables & Fruits",
    2:  "Dairy, Bread & Eggs",
    3:  "Cold Drinks & Juices",
    4:  "Snacks & Munchies",
    5:  "Breakfast & Instant Food",
    6:  "Sweet Tooth",
    7:  "Bakery & Biscuits",
    8:  "Tea, Coffee & Health Drinks",
    9:  "Atta, Rice, Oil & Dals",
    10: "Masala, Oil & More",
    11: "Sauces & Spreads",
    12: "Chicken, Meat & Fish",
    16: "Pharma & Wellness",
    17: "Cleaning Essentials",
    19: "Personal Care & Beauty",
}

CATEGORY_SEARCH_TERMS = {
    1: ["vegetables", "fruits"],
    2: ["milk", "bread eggs", "paneer butter"],
    3: ["cold drink", "juice", "energy drink"],
    4: ["chips", "namkeen", "popcorn"],
    5: ["oats", "cornflakes", "instant noodles"],
    6: ["chocolate", "candy sweets"],
    7: ["biscuits", "cookies"],
    8: ["tea", "coffee"],
    9: ["rice", "atta dal"],
    10: ["masala", "spices"],
    11: ["ketchup sauce", "jam"],
    12: ["chicken", "fish"],
    16: ["vitamins", "medicine"],
    17: ["detergent", "cleaner"],
    19: ["shampoo", "face wash"],
}

# ── Known ID store ────────────────────────────────────────────────────────────
def load_known(key: str) -> set:
    os.makedirs(KNOWN_DIR, exist_ok=True)
    path = os.path.join(KNOWN_DIR, f"{key}.json")
    if os.path.exists(path):
        with open(path) as f:
            return set(json.load(f))
    return set()

def save_known(key: str, ids: set):
    os.makedirs(KNOWN_DIR, exist_ok=True)
    path = os.path.join(KNOWN_DIR, f"{key}.json")
    with open(path, "w") as f:
        json.dump(list(ids), f)

def is_first_run(key: str) -> bool:
    path = os.path.join(KNOWN_DIR, f"{key}.json")
    return not os.path.exists(path)

# ── Fetchers ──────────────────────────────────────────────────────────────────
def fetch_by_keyword(keyword: str, headers: dict, pages=3) -> list[dict]:
    products = []
    seen = set()
    for page in range(pages):
        offset = page * 24
        url = (
            f"https://blinkit.com/v1/layout/search"
            f"?q={keyword.replace(' ', '+')}"
            f"&search_type=type_to_search&offset={offset}&limit=24"
        )
        data = post(url, headers)
        if not data:
            break
        snippets = data.get("response", {}).get("snippets", [])
        if not snippets:
            break
        pos = len(products) + 1
        for s in snippets:
            p = parse_snippet(s, pos)
            if p and p["product_id"] not in seen:
                products.append(p)
                seen.add(p["product_id"])
                pos += 1
        if len(snippets) < 24:
            break
        time.sleep(0.4)
    return products

def fetch_by_category(cat_id: int, headers: dict) -> list[dict]:
    products = []
    seen = set()
    terms = CATEGORY_SEARCH_TERMS.get(cat_id, [CATEGORIES.get(cat_id, "").lower()])
    for term in terms:
        new_prods = fetch_by_keyword(term, headers, pages=2)
        for p in new_prods:
            if p["product_id"] not in seen:
                products.append(p)
                seen.add(p["product_id"])
        time.sleep(0.5)
    return products

# ── Launch detection ──────────────────────────────────────────────────────────
def detect_launches(products: list[dict], key: str, source_type: str, source: str,
                    rid: str, ts: str) -> list[dict]:
    known = load_known(key)
    first_run = is_first_run(key)
    current_ids = {p["product_id"] for p in products}

    new_products = []
    if not first_run:
        for p in products:
            if p["product_id"] not in known:
                new_products.append(p)

    # Save updated known set
    save_known(key, known | current_ids)

    if first_run:
        print(f"  First run — seeded {len(current_ids)} known product IDs (no launches reported)")
        return []

    rows = []
    for p in new_products:
        rows.append({
            "first_seen":   ts,
            "run_id":       rid,
            "source_type":  source_type,
            "source":       source,
            "product_id":   p["product_id"],
            "group_id":     p.get("group_id", ""),
            "name":         p["name"],
            "brand":        p["brand"],
            "unit":         p["unit"],
            "price":        p["price"],
            "mrp":          p["mrp"],
            "discount_pct": p["discount_pct"],
            "offer_tag":    p["offer_tag"],
            "inventory":    p.get("inventory", ""),
            "is_sold_out":  p["is_sold_out"],
            "image_url":    p.get("image_url", ""),
            "blinkit_url":  f"https://blinkit.com/prn/{p['name'].lower().replace(' ','-')}/prid/{p['product_id']}",
        })

    return rows

# ── Summary ───────────────────────────────────────────────────────────────────
def update_summary(new_rows: list[dict]):
    summary = {}
    if os.path.exists(SUMMARY_FILE):
        with open(SUMMARY_FILE) as f:
            summary = json.load(f)

    for row in new_rows:
        brand = row["brand"]
        if brand not in summary:
            summary[brand] = {"total_launches": 0, "launches": []}
        summary[brand]["total_launches"] += 1
        summary[brand]["launches"].append({
            "name":       row["name"],
            "product_id": row["product_id"],
            "first_seen": row["first_seen"],
            "price":      row["price"],
        })

    with open(SUMMARY_FILE, "w") as f:
        json.dump(summary, f, indent=2)

# ── Report ────────────────────────────────────────────────────────────────────
def print_report(days: int):
    if not os.path.exists(LAUNCH_FILE):
        print("No launches file found — run detector first.")
        return

    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    rows = load_csv_as_dicts(LAUNCH_FILE)
    rows = [r for r in rows if r.get("first_seen", "") >= cutoff]

    if not rows:
        print(f"No launches detected in last {days} days.")
        return

    brand_counts = defaultdict(list)
    for r in rows:
        brand_counts[r["brand"]].append(r)

    print(f"\n{'='*60}")
    print(f"LAUNCH REPORT — Last {days} days ({len(rows)} total)")
    print(f"{'='*60}")
    print(f"\n{'Brand':<25} {'Launches':>8}  Latest Product")
    print("-" * 70)
    for brand, launches in sorted(brand_counts.items(), key=lambda x: -len(x[1])):
        latest = sorted(launches, key=lambda x: x["first_seen"], reverse=True)[0]
        print(f"  {brand:<25} {len(launches):>7}  {latest['name'][:30]}")

    print(f"\nTop 10 Recent Launches:")
    print("-" * 70)
    for r in sorted(rows, key=lambda x: x["first_seen"], reverse=True)[:10]:
        print(f"  {r['first_seen'][:10]}  {r['brand']:<20} {r['name'][:30]}  ₹{r['price']}")

# ── Main ──────────────────────────────────────────────────────────────────────
def run_once(cat_ids: list[int], keywords: list[str], headers: dict):
    rid = run_id()
    ts  = now_str()
    all_launches = []

    print(f"\n{'='*60}")
    print(f"Run: {rid}")
    print(f"{'='*60}")

    for cat_id in cat_ids:
        cat_name = CATEGORIES.get(cat_id, f"cat_{cat_id}")
        print(f"\n→ Category [{cat_id}] {cat_name} ...", flush=True)
        products = fetch_by_category(cat_id, headers)
        print(f"  {len(products)} products fetched", end="")
        launches = detect_launches(products, f"cat_{cat_id}", "category", cat_name, rid, ts)
        all_launches.extend(launches)
        if launches:
            print(f"  🆕 {len(launches)} NEW!")
        else:
            print()
        time.sleep(0.5)

    for kw in keywords:
        print(f"\n→ Keyword '{kw}' ...", flush=True)
        products = fetch_by_keyword(kw, headers, pages=3)
        print(f"  {len(products)} products fetched", end="")
        key = "kw_" + kw.replace(" ", "_")[:30]
        launches = detect_launches(products, key, "keyword", kw, rid, ts)
        all_launches.extend(launches)
        if launches:
            print(f"  🆕 {len(launches)} NEW!")
        else:
            print()
        time.sleep(0.5)

    if all_launches:
        append_csv(LAUNCH_FILE, all_launches, LAUNCH_COLS)
        update_summary(all_launches)
        print(f"\n🚀 {len(all_launches)} NEW PRODUCT LAUNCHES DETECTED!")
        print(f"\n  {'Brand':<22} {'Name':<35} {'Price':>7}")
        print(f"  {'-'*65}")
        for r in all_launches:
            print(f"  {r['brand']:<22} {r['name'][:33]:<35} ₹{float(r['price']):>6.0f}")
        print(f"\n✅ {LAUNCH_FILE} (+{len(all_launches)} rows)")
        print(f"✅ {SUMMARY_FILE}")
    else:
        print(f"\nNo new launches detected this run.")


def main():
    parser = argparse.ArgumentParser(description="Blinkit New Product Launch Detector")
    parser.add_argument("--categories", type=str, help="Comma-separated category IDs e.g. 3,4")
    parser.add_argument("--all",        action="store_true", help="Monitor all categories")
    parser.add_argument("--keywords",   type=str, help="Comma-separated keywords to monitor")
    parser.add_argument("--location",   type=str, default="mumbai", help="Location (default: mumbai for max coverage)")
    parser.add_argument("--cookie",     type=str, default="")
    parser.add_argument("--interval",   type=int, default=0, help="Repeat every N minutes")
    parser.add_argument("--report",     action="store_true", help="Print launch report and exit")
    parser.add_argument("--days",       type=int, default=30, help="Days for report (default 30)")
    args = parser.parse_args()

    if args.report:
        print_report(args.days)
        return

    cat_ids = []
    if args.all:
        cat_ids = list(CATEGORIES.keys())
    elif args.categories:
        cat_ids = [int(x.strip()) for x in args.categories.split(",")]

    keywords = []
    if args.keywords:
        keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]

    if not cat_ids and not keywords:
        cat_ids = [3, 4, 7]  # default
        print("No targets specified — defaulting to categories 3 (Drinks), 4 (Snacks), 7 (Biscuits)")

    headers = make_headers(args.location, args.cookie)

    if args.interval > 0:
        print(f"Monitoring every {args.interval} min. Ctrl+C to stop.")
        while True:
            try:
                run_once(cat_ids, keywords, headers)
                time.sleep(args.interval * 60)
            except KeyboardInterrupt:
                print("\nStopped.")
                break
    else:
        run_once(cat_ids, keywords, headers)


if __name__ == "__main__":
    main()
