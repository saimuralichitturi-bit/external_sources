"""
blinkit_category_scraper.py
============================
Scrapes ENTIRE categories from Blinkit — not keyword search, but the full
category browse endpoint. Gets every product Blinkit lists in a category.

Category IDs (l0_cat):
  Blinkit uses numeric IDs for categories. Common ones:
  1   = Vegetables & Fruits
  2   = Dairy, Bread & Eggs  
  3   = Cold Drinks & Juices
  4   = Snacks & Munchies
  5   = Breakfast & Instant Food
  6   = Sweet Tooth
  7   = Bakery & Biscuits
  8   = Tea, Coffee & Health Drinks
  9   = Atta, Rice, Oil & Dals
  10  = Masala, Oil & More
  11  = Sauces & Spreads
  12  = Chicken, Meat & Fish
  13  = Organic & Premium
  14  = Paan Corner
  15  = Baby Care
  16  = Pharma & Wellness
  17  = Cleaning Essentials
  18  = Home & Office
  19  = Personal Care & Beauty
  20  = Pet Care

OUTPUT:
  category_products.csv      — full product list per category per run
  category_sov.csv           — brand SOV per category per run
  category_new_products.csv  — products seen for first time

USAGE:
  # Scrape snacks category
  python blinkit_category_scraper.py --category 4 --location mumbai

  # Scrape multiple categories
  python blinkit_category_scraper.py --categories 3,4,7,8 --location bangalore

  # Scrape all known categories
  python blinkit_category_scraper.py --all --location delhi

  # Run every 24 hours
  python blinkit_category_scraper.py --categories 3,4 --interval 1440 --location mumbai
"""

import argparse
import sys
import time
import json
import os
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from blinkit_core import (
    make_headers, parse_snippet, post, append_csv, load_csv_as_dicts,
    now_str, run_id
)

# ── Category map ──────────────────────────────────────────────────────────────
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
    13: "Organic & Premium",
    15: "Baby Care",
    16: "Pharma & Wellness",
    17: "Cleaning Essentials",
    18: "Home & Office",
    19: "Personal Care & Beauty",
    20: "Pet Care",
}

PRODUCT_COLS = [
    "run_id", "timestamp", "category_id", "category_name",
    "position", "product_id", "group_id", "name", "brand", "unit",
    "price", "mrp", "discount_pct", "offer_tag",
    "inventory", "is_sold_out", "product_state", "eta",
    "merchant_type", "is_ad", "image_url",
]

SOV_COLS = [
    "run_id", "timestamp", "category_id", "category_name",
    "brand", "appearance_count", "sov_pct", "avg_position",
    "top3_count", "ad_count", "ad_sov_pct",
]

NEW_COLS = [
    "first_seen", "category_id", "category_name",
    "product_id", "name", "brand", "unit", "price", "mrp",
]

PRODUCT_FILE = "category_products.csv"
SOV_FILE     = "category_sov.csv"
NEW_FILE     = "category_new_products.csv"

# ── Scraper ───────────────────────────────────────────────────────────────────
def scrape_category(cat_id: int, headers: dict, max_pages=20, delay=0.6) -> list[dict]:
    """Scrape all products from a category using listing endpoint."""
    all_products = []
    global_pos = 1
    cat_name = CATEGORIES.get(cat_id, f"Category {cat_id}")

    # Try multiple URL patterns for category browse
    url_patterns = [
        f"https://blinkit.com/v1/layout/listing?l0_cat={cat_id}&offset={{offset}}&limit=24",
        f"https://blinkit.com/v2/listing?l0_cat_id={cat_id}&offset={{offset}}&limit=24",
        f"https://blinkit.com/v1/layout/category?l0_cat={cat_id}&offset={{offset}}&limit=24",
    ]

    working_pattern = None

    for page in range(max_pages):
        offset = page * 24
        got_results = False

        patterns_to_try = [url_patterns[0].format(offset=offset)] if working_pattern \
            else [p.format(offset=offset) for p in url_patterns]

        for url in patterns_to_try:
            data = post(url, headers)
            if not data:
                continue
            snippets = data.get("response", {}).get("snippets", [])
            if snippets:
                working_pattern = url.split("?")[0] + "?" + url.split("?")[1].replace(str(offset), "{offset}")
                page_products = []
                for s in snippets:
                    p = parse_snippet(s, global_pos)
                    if p:
                        p["category_id"] = cat_id
                        p["category_name"] = cat_name
                        page_products.append(p)
                        global_pos += 1
                all_products.extend(page_products)
                got_results = True

                if len(snippets) < 24:
                    print(f"  {cat_name}: {len(all_products)} products (last page at {page+1})")
                    return all_products
                break

        if not got_results:
            if page == 0:
                # Try search-based fallback for category
                print(f"  {cat_name}: listing endpoint failed, trying search fallback...")
                return scrape_category_via_search(cat_id, headers)
            break

        time.sleep(delay)

    print(f"  {cat_name}: {len(all_products)} products ({page+1} pages)")
    return all_products


def scrape_category_via_search(cat_id: int, headers: dict) -> list[dict]:
    """Fallback: use category name as search query."""
    cat_name = CATEGORIES.get(cat_id, "")
    if not cat_name:
        return []

    # Use abbreviated search terms that work well
    search_terms = {
        1: ["vegetables", "fruits", "tomato onion potato"],
        2: ["milk", "bread", "eggs", "butter paneer"],
        3: ["cold drinks", "juice", "water soda"],
        4: ["chips", "namkeen", "popcorn biscuits snacks"],
        5: ["oats", "cornflakes", "instant noodles"],
        6: ["chocolate", "candy", "ice cream"],
        7: ["biscuits", "cookies", "rusk cake"],
        8: ["tea", "coffee", "health drink"],
        9: ["rice", "atta", "dal oil"],
        10: ["masala", "spices", "pickle"],
        11: ["ketchup", "sauce", "jam peanut butter"],
        12: ["chicken", "fish", "mutton"],
        13: ["organic", "premium nuts"],
        15: ["baby diapers", "baby food"],
        16: ["medicine", "vitamins supplements"],
        17: ["detergent", "floor cleaner"],
        18: ["tissue paper", "stationery"],
        19: ["shampoo", "face wash", "moisturizer"],
        20: ["dog food", "cat food"],
    }

    terms = search_terms.get(cat_id, [cat_name.lower()])
    all_products = []
    seen_ids = set()

    for term in terms[:2]:  # limit to 2 terms per category
        url = f"https://blinkit.com/v1/layout/search?q={term.replace(' ', '+')}&search_type=type_to_search&offset=0&limit=24"
        data = post(url, headers)
        if not data:
            continue
        snippets = data.get("response", {}).get("snippets", [])
        pos = len(all_products) + 1
        for s in snippets:
            p = parse_snippet(s, pos)
            if p and p["product_id"] not in seen_ids:
                p["category_id"] = cat_id
                p["category_name"] = cat_name
                all_products.append(p)
                seen_ids.add(p["product_id"])
                pos += 1
        time.sleep(0.5)

    return all_products


# ── SOV calc ──────────────────────────────────────────────────────────────────
def calc_sov(products: list[dict], cat_id: int, cat_name: str, rid: str, ts: str) -> list[dict]:
    total = len(products)
    total_ads = sum(1 for p in products if p.get("is_ad"))
    brand_data = defaultdict(lambda: {"positions": [], "ads": 0, "top3": 0})

    for p in products:
        b = p.get("brand", "Unknown")
        brand_data[b]["positions"].append(p["position"])
        if p.get("is_ad"):
            brand_data[b]["ads"] += 1
        if p["position"] <= 3:
            brand_data[b]["top3"] += 1

    rows = []
    for brand, bd in sorted(brand_data.items(), key=lambda x: len(x[1]["positions"]), reverse=True):
        count = len(bd["positions"])
        rows.append({
            "run_id":           rid,
            "timestamp":        ts,
            "category_id":      cat_id,
            "category_name":    cat_name,
            "brand":            brand,
            "appearance_count": count,
            "sov_pct":          round(count / total * 100, 1) if total else 0,
            "avg_position":     round(sum(bd["positions"]) / count, 1),
            "top3_count":       bd["top3"],
            "ad_count":         bd["ads"],
            "ad_sov_pct":       round(bd["ads"] / total_ads * 100, 1) if total_ads else 0,
        })
    return rows


# ── New product detection ─────────────────────────────────────────────────────
def detect_new_products(products: list[dict], cat_id: int, cat_name: str, ts: str) -> list[dict]:
    """Compare against known product IDs from previous runs."""
    known_file = f"known_products_cat{cat_id}.json"
    known_ids = set()

    if os.path.exists(known_file):
        with open(known_file) as f:
            known_ids = set(json.load(f))

    current_ids = {p["product_id"] for p in products}
    new_ids = current_ids - known_ids

    new_rows = []
    for p in products:
        if p["product_id"] in new_ids:
            new_rows.append({
                "first_seen":    ts,
                "category_id":   cat_id,
                "category_name": cat_name,
                "product_id":    p["product_id"],
                "name":          p["name"],
                "brand":         p["brand"],
                "unit":          p["unit"],
                "price":         p["price"],
                "mrp":           p["mrp"],
            })

    # Save updated known IDs
    with open(known_file, "w") as f:
        json.dump(list(current_ids), f)

    return new_rows


# ── Main ──────────────────────────────────────────────────────────────────────
def run_once(cat_ids: list[int], headers: dict):
    rid = run_id()
    ts  = now_str()
    print(f"\n{'='*60}")
    print(f"Run: {rid} | Categories: {cat_ids}")
    print(f"{'='*60}")

    for cat_id in cat_ids:
        cat_name = CATEGORIES.get(cat_id, f"Category {cat_id}")
        print(f"\n→ [{cat_id}] {cat_name} ...", flush=True)

        products = scrape_category(cat_id, headers)
        if not products:
            print(f"  No products found")
            continue

        # Rows for CSV
        product_rows = [{"run_id": rid, "timestamp": ts, **p} for p in products]
        sov_rows     = calc_sov(products, cat_id, cat_name, rid, ts)
        new_rows     = detect_new_products(products, cat_id, cat_name, ts)

        append_csv(PRODUCT_FILE, product_rows, PRODUCT_COLS)
        append_csv(SOV_FILE,     sov_rows,     SOV_COLS)
        if new_rows:
            append_csv(NEW_FILE, new_rows, NEW_COLS)

        # Print SOV summary
        print(f"\n  {'Brand':<28} {'Count':>5} {'SOV%':>6} {'AvgPos':>7} {'Ads':>4}")
        print(f"  {'-'*55}")
        for row in sov_rows[:10]:
            print(
                f"  {row['brand']:<28} "
                f"{row['appearance_count']:>5} "
                f"{row['sov_pct']:>6.1f}% "
                f"{row['avg_position']:>7.1f} "
                f"{row['ad_count']:>4}"
            )

        if new_rows:
            print(f"\n  🆕 {len(new_rows)} NEW products detected!")
            for r in new_rows[:5]:
                print(f"     {r['brand']} — {r['name']} ({r['unit']}) ₹{r['price']}")

        time.sleep(1.0)

    print(f"\n✅ {PRODUCT_FILE}")
    print(f"✅ {SOV_FILE}")
    print(f"✅ {NEW_FILE}")


def main():
    parser = argparse.ArgumentParser(description="Blinkit Category Full Scraper")
    parser.add_argument("--category",   type=int,  help="Single category ID")
    parser.add_argument("--categories", type=str,  help="Comma-separated category IDs e.g. 3,4,7")
    parser.add_argument("--all",        action="store_true", help="Scrape all known categories")
    parser.add_argument("--location",   type=str,  default="vijayawada")
    parser.add_argument("--cookie",     type=str,  default="", help="Session cookie string")
    parser.add_argument("--interval",   type=int,  default=0,  help="Repeat every N minutes")
    parser.add_argument("--list",       action="store_true", help="List all category IDs and exit")
    args = parser.parse_args()

    if args.list:
        print("\nAvailable categories:")
        for cid, name in CATEGORIES.items():
            print(f"  {cid:>3}  {name}")
        return

    cat_ids = []
    if args.all:
        cat_ids = list(CATEGORIES.keys())
    elif args.categories:
        cat_ids = [int(x.strip()) for x in args.categories.split(",")]
    elif args.category:
        cat_ids = [args.category]
    else:
        cat_ids = [4]  # default: Snacks
        print("No category specified — defaulting to Snacks & Munchies (4)")

    headers = make_headers(args.location, args.cookie)

    if args.interval > 0:
        print(f"Running every {args.interval} min. Ctrl+C to stop.")
        while True:
            try:
                run_once(cat_ids, headers)
                time.sleep(args.interval * 60)
            except KeyboardInterrupt:
                print("\nStopped.")
                break
    else:
        run_once(cat_ids, headers)


if __name__ == "__main__":
    main()
