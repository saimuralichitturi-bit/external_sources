"""
flipkart_sitemap.py — Discover product PIDs from Flipkart's public sitemap

Sitemap index: https://www.flipkart.com/sitemap/sitemap_index.xml
Each sub-sitemap contains product URLs with PIDs in /p/ path or pid= query param.

OUTPUTS:
  flipkart_pid_catalog.csv  — pid, url, category_hint, sitemap_source, discovered_at

USAGE:
  python flipkart/flipkart_sitemap.py --list
  python flipkart/flipkart_sitemap.py --filter "clothing,shoes" --out-dir ../data
  python flipkart/flipkart_sitemap.py --limit 5 --out-dir ../data
  python flipkart/flipkart_sitemap.py --all --out-dir ../data
"""

import argparse, csv, os, re, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flipkart_core import get_html, extract_pid, now_str, delay

SITEMAP_INDEX = "https://www.flipkart.com/sitemap/sitemap_index.xml"

CATALOG_COLS = ["pid", "url", "category_hint", "sitemap_source", "discovered_at"]

CATEGORY_HINTS = {
    "mobiles":       "Mobiles & Accessories",
    "laptop":        "Computers & Laptops",
    "electronic":    "Electronics",
    "clothing":      "Clothing & Accessories",
    "shoe":          "Footwear",
    "fashion":       "Fashion",
    "kitchen":       "Home & Kitchen",
    "beauty":        "Beauty & Personal Care",
    "health":        "Health & Fitness",
    "sport":         "Sports & Outdoors",
    "book":          "Books",
    "grocery":       "Grocery & Gourmet",
    "toy":           "Toys & Games",
    "baby":          "Baby Products",
    "automotive":    "Automotive",
    "furniture":     "Furniture",
    "appliance":     "Large Appliances",
    "jewellery":     "Jewellery",
    "watch":         "Watches",
    "bag":           "Bags & Luggage",
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
    # Also handle flat <loc> lines (some Flipkart sitemap formats)
    if not entries:
        for loc_m in re.finditer(r'<loc>(https?://[^<]+sitemap[^<]*)</loc>', html):
            entries.append({"url": loc_m.group(1).strip(), "lastmod": ""})
    return entries


def fetch_product_urls(sitemap_url: str) -> list[str]:
    html = get_html(sitemap_url)
    if not html:
        return []
    urls = re.findall(r'<loc>(https?://www\.flipkart\.com[^<]+)</loc>', html)
    # Keep only product URLs (contain /p/ path or pid= param)
    return [u for u in urls if '/p/' in u or 'pid=' in u]


def load_known_pids(catalog_file: str) -> set:
    if not os.path.exists(catalog_file):
        return set()
    with open(catalog_file, newline="", encoding="utf-8") as f:
        return {row["pid"] for row in csv.DictReader(f) if row.get("pid")}


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
    catalog_file = os.path.join(out_dir, "flipkart_pid_catalog.csv")

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

    known     = load_known_pids(catalog_file)
    print(f"  {len(known)} PIDs already in catalog")

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
            pid = extract_pid(url)
            if pid and pid not in known:
                new_rows.append({
                    "pid":            pid,
                    "url":            url,
                    "category_hint":  category,
                    "sitemap_source": sm_url,
                    "discovered_at":  ts,
                })
                known.add(pid)

        save_rows(catalog_file, new_rows)
        total_new += len(new_rows)
        print(f"  +{len(new_rows)} new PIDs (run total: {total_new})")
        delay(1.0, 2.5)

    print(f"\n✅ {catalog_file} (+{total_new} new PIDs this run)")


def main():
    parser = argparse.ArgumentParser(description="Flipkart sitemap PID discovery")
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
