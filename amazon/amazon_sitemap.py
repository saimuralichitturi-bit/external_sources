"""
amazon_sitemap.py — Discover ASINs from Amazon.in's public sitemap

Sitemap index: https://www.amazon.in/sitemap.xml
Each sub-sitemap contains product URLs with ASINs in the /dp/ path.

OUTPUTS:
  amazon_asin_catalog.csv  — asin, url, category_hint, sitemap_source, discovered_at

USAGE:
  python amazon/amazon_sitemap.py --list
  python amazon/amazon_sitemap.py --filter "clothing,shoes" --out-dir ../data
  python amazon/amazon_sitemap.py --limit 5 --out-dir ../data
  python amazon/amazon_sitemap.py --all --out-dir ../data
"""

import argparse, csv, os, re, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from amazon_core import get_html, extract_asin, now_str, delay

SITEMAP_INDEX = "https://www.amazon.in/sitemap.xml"

CATALOG_COLS = ["asin", "url", "category_hint", "sitemap_source", "discovered_at"]

CATEGORY_HINTS = {
    "electronic":  "Electronics",
    "clothing":    "Clothing & Accessories",
    "shoe":        "Shoes & Handbags",
    "kitchen":     "Home & Kitchen",
    "beauty":      "Beauty",
    "health":      "Health & Personal Care",
    "sport":       "Sports, Fitness & Outdoors",
    "book":        "Books",
    "grocery":     "Grocery & Gourmet Foods",
    "toy":         "Toys & Games",
    "baby":        "Baby Products",
    "automotive":  "Automotive",
    "pet":         "Pet Supplies",
    "office":      "Office Products",
    "industrial":  "Industrial & Scientific",
    "garden":      "Garden & Outdoors",
    "musical":     "Musical Instruments",
}


def infer_category(url: str) -> str:
    u = url.lower()
    for key, label in CATEGORY_HINTS.items():
        if key in u:
            return label
    return "Unknown"


def fetch_index() -> list[dict]:
    html = get_html(SITEMAP_INDEX)
    if not html:
        return []
    entries = []
    for m in re.finditer(r'<sitemap>(.*?)</sitemap>', html, re.DOTALL):
        block = m.group(1)
        loc = re.search(r'<loc>(.*?)</loc>', block)
        mod = re.search(r'<lastmod>(.*?)</lastmod>', block)
        if loc:
            entries.append({
                "url":     loc.group(1).strip(),
                "lastmod": mod.group(1).strip() if mod else "",
            })
    return entries


def fetch_product_urls(sitemap_url: str) -> list[str]:
    html = get_html(sitemap_url)
    if not html:
        return []
    urls = re.findall(r'<loc>(https?://www\.amazon\.in[^<]+)</loc>', html)
    return [u for u in urls if '/dp/' in u]


def load_known_asins(catalog_file: str) -> set:
    if not os.path.exists(catalog_file):
        return set()
    with open(catalog_file, newline="", encoding="utf-8") as f:
        return {row["asin"] for row in csv.DictReader(f) if row.get("asin")}


def save_rows(catalog_file: str, rows: list[dict]):
    if not rows:
        return
    write_header = not os.path.exists(catalog_file)
    with open(catalog_file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CATALOG_COLS, extrasaction="ignore")
        if write_header:
            w.writeheader()
        w.writerows(rows)


def run(filter_keywords: list[str], limit: int, out_dir: str, list_only: bool):
    catalog_file = os.path.join(out_dir, "amazon_asin_catalog.csv")

    print(f"[sitemap] Fetching index: {SITEMAP_INDEX}")
    sitemaps = fetch_index()
    print(f"  {len(sitemaps)} sub-sitemaps found")

    if list_only:
        print(f"\n{'URL':<80}  lastmod")
        print("-" * 95)
        for s in sitemaps[:60]:
            print(f"  {s['url'][:78]:<80}  {s['lastmod']}")
        if len(sitemaps) > 60:
            print(f"  ... and {len(sitemaps) - 60} more")
        return

    if filter_keywords:
        sitemaps = [s for s in sitemaps
                    if any(k.lower() in s["url"].lower() for k in filter_keywords)]
        print(f"  After filter '{','.join(filter_keywords)}': {len(sitemaps)} sitemaps")
        if not sitemaps:
            print("  No match — try --list to see available sitemaps.")
            return

    if limit > 0:
        sitemaps = sitemaps[:limit]

    known     = load_known_asins(catalog_file)
    print(f"  {len(known)} ASINs already in catalog")

    ts        = now_str()
    total_new = 0

    for i, sm in enumerate(sitemaps):
        sm_url   = sm["url"]
        category = infer_category(sm_url)
        print(f"\n[{i+1}/{len(sitemaps)}] {sm_url}")

        product_urls = fetch_product_urls(sm_url)
        print(f"  {len(product_urls)} product URLs | category: {category}")

        new_rows = []
        for url in product_urls:
            asin = extract_asin(url)
            if asin and asin not in known:
                new_rows.append({
                    "asin":           asin,
                    "url":            url,
                    "category_hint":  category,
                    "sitemap_source": sm_url,
                    "discovered_at":  ts,
                })
                known.add(asin)

        save_rows(catalog_file, new_rows)
        total_new += len(new_rows)
        print(f"  +{len(new_rows)} new ASINs (run total: {total_new})")
        delay(1.0, 2.5)

    print(f"\n✅ {catalog_file} (+{total_new} new ASINs this run)")


def main():
    parser = argparse.ArgumentParser(description="Amazon.in sitemap ASIN discovery")
    parser.add_argument("--list",    action="store_true")
    parser.add_argument("--filter",  type=str, default="")
    parser.add_argument("--limit",   type=int, default=0)
    parser.add_argument("--all",     action="store_true")
    parser.add_argument("--out-dir", type=str, default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    filter_keywords = [k.strip() for k in args.filter.split(",") if k.strip()] if args.filter else []
    limit = 0 if args.all else args.limit

    run(filter_keywords=filter_keywords, limit=limit,
        out_dir=args.out_dir, list_only=args.list)


if __name__ == "__main__":
    main()