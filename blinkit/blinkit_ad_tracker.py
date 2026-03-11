"""
blinkit_ad_tracker.py
======================
Playwright-based ad tracker — uses a REAL headless Chrome browser.
Gets actual sponsored/ad badges that curl_cffi misses.

Captures:
  - Sponsored product positions per keyword
  - Ad SOV (share of voice) per brand
  - Which products are being actively promoted
  - Ad position vs organic position comparison

INSTALL:
  pip install playwright --break-system-packages
  playwright install chromium

USAGE:
  # Basic ad tracking
  python blinkit_ad_tracker.py --keywords "chips,biscuits,cold coffee"

  # With your logged-in session (gets personalized ads)
  python blinkit_ad_tracker.py --keywords "chips" --cookie "gr_1_accessToken=v2::xxx"

  # Run every 2 hours
  python blinkit_ad_tracker.py --keywords "chips,protein powder" --interval 120

  # Compare locations
  python blinkit_ad_tracker.py --keywords "chips" --location mumbai
  python blinkit_ad_tracker.py --keywords "chips" --location delhi

OUTPUTS:
  ad_snapshots.csv    — every product with ad flag per keyword per run
  ad_sov.csv          — brand-level ad SOV per keyword per run
  ad_vs_organic.csv   — sponsored vs organic position comparison
"""

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from blinkit_core import (
    LOCATIONS, append_csv, load_csv_as_dicts, now_str, run_id
)

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    PW_OK = True
except ImportError:
    PW_OK = False

AD_SNAPSHOT_COLS = [
    "run_id", "timestamp", "keyword", "location",
    "position", "is_ad",
    "product_id", "name", "brand", "unit",
    "price", "mrp", "discount_pct", "offer_tag",
    "inventory", "is_sold_out",
]

AD_SOV_COLS = [
    "run_id", "timestamp", "keyword", "location",
    "brand",
    "total_count", "ad_count", "organic_count",
    "sov_pct", "ad_sov_pct",
    "best_ad_position", "best_organic_position",
    "avg_ad_position", "avg_organic_position",
]

AD_VS_ORG_COLS = [
    "run_id", "timestamp", "keyword", "location",
    "product_id", "name", "brand",
    "ad_position", "organic_position", "position_diff",
    "price", "is_discounted",
]

AD_SNAPSHOT_FILE = "ad_snapshots.csv"
AD_SOV_FILE      = "ad_sov.csv"
AD_VS_ORG_FILE   = "ad_vs_organic.csv"


# ── Playwright fetch ──────────────────────────────────────────────────────────
def fetch_search_with_browser(keyword: str, location: str, cookie: str,
                               page_obj, pages=2) -> list[dict]:
    """Use Playwright page to intercept API responses for a keyword search."""
    lat, lon, locality = LOCATIONS.get(location, LOCATIONS["vijayawada"])
    all_products = []
    captured_responses = []

    def handle_response(response):
        url = response.url
        if "v1/layout/search" in url and response.status == 200:
            try:
                data = response.json()
                captured_responses.append(data)
            except Exception:
                pass

    page_obj.on("response", handle_response)

    for pg in range(pages):
        offset = pg * 24
        url = (
            f"https://blinkit.com/v1/layout/search"
            f"?q={keyword.replace(' ', '+')}"
            f"&search_type=type_to_search&offset={offset}&limit=24"
        )

        # Set headers via route interception
        def route_handler(route):
            headers = {
                **route.request.headers,
                "content-type":    "application/json",
                "app-version":     "1000000",
                "web-version":     "1000000",
                "lat":             lat,
                "lon":             lon,
                "locality":        locality,
            }
            if cookie:
                headers["cookie"] = cookie
            route.continue_(headers=headers)

        page_obj.route("**/v1/layout/search**", route_handler)

        try:
            # POST via fetch in browser context
            result = page_obj.evaluate(f"""
                async () => {{
                    const r = await fetch("{url}", {{
                        method: "POST",
                        credentials: "include",
                        headers: {{
                            "content-type": "application/json",
                            "app-version": "1000000",
                            "web-version": "1000000",
                            "lat": "{lat}",
                            "lon": "{lon}",
                            "locality": "{locality}"
                        }}
                    }});
                    return await r.json();
                }}
            """)

            snippets = result.get("response", {}).get("snippets", []) if result else []
            global_pos = len(all_products) + 1

            for s in snippets:
                widget_type = s.get("widget_type", "")
                if any(x in widget_type for x in ["Header", "Banner", "header"]):
                    continue

                d = s.get("data", {})
                pid = d.get("product_id") or d.get("identity", {}).get("id")
                if not pid:
                    continue

                cart_item = d.get("atc_action", {}).get("add_to_cart", {}).get("cart_item", {})
                name  = cart_item.get("product_name") or d.get("name", {}).get("text", "")
                brand = cart_item.get("brand") or d.get("brand_name", {}).get("text", "")
                unit  = cart_item.get("unit") or d.get("variant", {}).get("text", "")
                price = float(cart_item.get("price") or 0)
                mrp   = float(cart_item.get("mrp") or 0)
                inv   = cart_item.get("inventory")
                if inv is None:
                    inv = d.get("inventory")

                disc = round((mrp - price) / mrp * 100, 1) if mrp > price > 0 else 0
                offer_tag = d.get("offer_tag", {}).get("title", {}).get("text", "")
                is_sold_out = bool(d.get("is_sold_out") or inv == 0)

                # Ad detection from real browser response
                badges = d.get("product_badges", [])
                is_ad = any(
                    b.get("type") == "OTHERS" and b.get("label", "").lower() == "ad"
                    for b in badges
                )

                all_products.append({
                    "product_id":   str(pid),
                    "name":         name,
                    "brand":        brand or "Unknown",
                    "unit":         unit,
                    "price":        price,
                    "mrp":          mrp,
                    "discount_pct": disc,
                    "offer_tag":    offer_tag,
                    "inventory":    inv,
                    "is_sold_out":  is_sold_out,
                    "is_ad":        is_ad,
                    "position":     global_pos,
                    "keyword":      keyword,
                })
                global_pos += 1

            if len(snippets) < 24:
                break

        except Exception as e:
            print(f"    Page {pg+1} error: {e}")
            break

        time.sleep(0.5)

    return all_products


# ── SOV calculation ───────────────────────────────────────────────────────────
def calc_ad_sov(products: list[dict], keyword: str, location: str,
                rid: str, ts: str) -> tuple[list, list]:
    total = len(products)
    total_ads = sum(1 for p in products if p["is_ad"])

    brand_data = defaultdict(lambda: {
        "ad_positions": [], "organic_positions": []
    })

    for p in products:
        b = p["brand"]
        if p["is_ad"]:
            brand_data[b]["ad_positions"].append(p["position"])
        else:
            brand_data[b]["organic_positions"].append(p["position"])

    sov_rows = []
    for brand, bd in sorted(
        brand_data.items(),
        key=lambda x: len(x[1]["ad_positions"]) + len(x[1]["organic_positions"]),
        reverse=True
    ):
        ad_pos  = bd["ad_positions"]
        org_pos = bd["organic_positions"]
        total_count = len(ad_pos) + len(org_pos)

        sov_rows.append({
            "run_id":                rid,
            "timestamp":             ts,
            "keyword":               keyword,
            "location":              location,
            "brand":                 brand,
            "total_count":           total_count,
            "ad_count":              len(ad_pos),
            "organic_count":         len(org_pos),
            "sov_pct":               round(total_count / total * 100, 1) if total else 0,
            "ad_sov_pct":            round(len(ad_pos) / total_ads * 100, 1) if total_ads else 0,
            "best_ad_position":      min(ad_pos) if ad_pos else "",
            "best_organic_position": min(org_pos) if org_pos else "",
            "avg_ad_position":       round(sum(ad_pos) / len(ad_pos), 1) if ad_pos else "",
            "avg_organic_position":  round(sum(org_pos) / len(org_pos), 1) if org_pos else "",
        })

    # Ad vs organic comparison — products that appear as both
    # (shouldn't happen per keyword, but useful for brand-level)
    vs_rows = []
    brand_ads = {p["brand"]: p for p in products if p["is_ad"]}
    brand_org = {p["brand"]: p for p in products if not p["is_ad"]}

    for brand in set(brand_ads) & set(brand_org):
        ad_p  = brand_ads[brand]
        org_p = brand_org[brand]
        vs_rows.append({
            "run_id":       rid,
            "timestamp":    ts,
            "keyword":      keyword,
            "location":     location,
            "product_id":   ad_p["product_id"],
            "name":         ad_p["name"],
            "brand":        brand,
            "ad_position":  ad_p["position"],
            "organic_position": org_p["position"],
            "position_diff": org_p["position"] - ad_p["position"],
            "price":         ad_p["price"],
            "is_discounted": ad_p["discount_pct"] > 0,
        })

    return sov_rows, vs_rows


# ── Main ──────────────────────────────────────────────────────────────────────
def run_once(keywords: list[str], location: str, cookie: str, pages: int):
    if not PW_OK:
        print("❌ Playwright not installed.")
        print("Run: pip install playwright --break-system-packages")
        print("     playwright install chromium")
        sys.exit(1)

    rid = run_id()
    ts  = now_str()

    print(f"\n{'='*60}")
    print(f"Run: {rid} | Location: {location} | Keywords: {len(keywords)}")
    print(f"{'='*60}")

    all_snapshots = []
    all_sov       = []
    all_vs_org    = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )

        # Inject cookies if provided
        if cookie:
            lat, lon, locality = LOCATIONS.get(location, LOCATIONS["vijayawada"])
            cookie_list = []
            for part in cookie.split(";"):
                part = part.strip()
                if "=" in part:
                    name, value = part.split("=", 1)
                    cookie_list.append({
                        "name": name.strip(),
                        "value": value.strip(),
                        "domain": ".blinkit.com",
                        "path": "/",
                    })
            if cookie_list:
                context.add_cookies(cookie_list)

        page = context.new_page()

        # Navigate to blinkit first to establish session
        try:
            page.goto("https://blinkit.com", timeout=15000, wait_until="domcontentloaded")
            time.sleep(2)
        except Exception as e:
            print(f"  Initial navigation: {e}")

        for kw in keywords:
            print(f"\n→ '{kw}' ...", end=" ", flush=True)
            products = fetch_search_with_browser(kw, location, cookie, page, pages=pages)
            print(f"{len(products)} products ({sum(1 for p in products if p['is_ad'])} ads)")

            if not products:
                continue

            # Snapshot rows
            for p in products:
                all_snapshots.append({"run_id": rid, "timestamp": ts, "location": location, **p})

            # SOV
            sov_rows, vs_rows = calc_ad_sov(products, kw, location, rid, ts)
            all_sov.extend(sov_rows)
            all_vs_org.extend(vs_rows)

            # Print ad table
            ads = [p for p in products if p["is_ad"]]
            if ads:
                print(f"\n  SPONSORED PRODUCTS:")
                print(f"  {'Pos':>4} {'Brand':<22} {'Name':<35}")
                print(f"  {'-'*65}")
                for p in ads:
                    print(f"  {p['position']:>4} {p['brand']:<22} {p['name'][:33]}")

            print(f"\n  BRAND SOV:")
            print(f"  {'Brand':<25} {'Tot':>4} {'Ads':>4} {'AdSOV%':>7} {'BestAdPos':>10}")
            print(f"  {'-'*55}")
            for row in sov_rows[:8]:
                print(
                    f"  {row['brand']:<25} "
                    f"{row['total_count']:>4} "
                    f"{row['ad_count']:>4} "
                    f"{row['ad_sov_pct']:>7.1f}% "
                    f"{str(row['best_ad_position']):>10}"
                )

            time.sleep(1.0)

        browser.close()

    # Write
    if all_snapshots:
        append_csv(AD_SNAPSHOT_FILE, all_snapshots, AD_SNAPSHOT_COLS)
        print(f"\n✅ {AD_SNAPSHOT_FILE} (+{len(all_snapshots)} rows)")
    if all_sov:
        append_csv(AD_SOV_FILE, all_sov, AD_SOV_COLS)
        print(f"✅ {AD_SOV_FILE} (+{len(all_sov)} rows)")
    if all_vs_org:
        append_csv(AD_VS_ORG_FILE, all_vs_org, AD_VS_ORG_COLS)
        print(f"✅ {AD_VS_ORG_FILE} (+{len(all_vs_org)} rows)")


def main():
    parser = argparse.ArgumentParser(description="Blinkit Ad Tracker (Playwright)")
    parser.add_argument("--keywords",  type=str, required=True, help="Comma-separated keywords")
    parser.add_argument("--location",  type=str, default="mumbai")
    parser.add_argument("--cookie",    type=str, default="")
    parser.add_argument("--pages",     type=int, default=2)
    parser.add_argument("--interval",  type=int, default=0, help="Repeat every N minutes")
    args = parser.parse_args()

    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]

    if args.interval > 0:
        while True:
            try:
                run_once(keywords, args.location, args.cookie, args.pages)
                time.sleep(args.interval * 60)
            except KeyboardInterrupt:
                print("\nStopped.")
                break
    else:
        run_once(keywords, args.location, args.cookie, args.pages)


if __name__ == "__main__":
    main()
