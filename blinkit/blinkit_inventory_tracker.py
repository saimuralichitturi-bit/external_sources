"""
Blinkit Inventory Tracker
=========================
Tracks inventory changes over time to estimate units sold.

Data collected per product per snapshot:
  - inventory         : current stock (0-50, capped at 50)
  - price             : selling price
  - mrp               : max retail price
  - discount_pct      : % discount
  - is_discounted     : bool
  - offer_tag         : any active offer text
  - is_sold_out       : bool
  - product_state     : available / out_of_stock
  - rating            : float
  - rating_count      : number of ratings (proxy for all-time sales)
  - eta_label         : delivery time
  - shelf_life        : product shelf life
  - brand             : brand name
  - variant           : size/unit
  - sections          : section titles on PDP
  - similar_product_ids: competitor product IDs shown on PDP
  - est_sold_since_last: estimated units sold since last snapshot

Usage:
  python blinkit_inventory_tracker.py --product_ids 447847 125240
  python blinkit_inventory_tracker.py --product_ids 447847 125240 --interval 60
  python blinkit_inventory_tracker.py --file product_ids.txt --interval 30
"""

from curl_cffi import requests
import json, csv, re, time, argparse, sys, os
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────
HEADERS = {
    "content-type": "application/json",
    "app_version": "1010101011",
    "web_app_version": "1008010016",
    "lat": "16.5103525",
    "lon": "80.6465468",
    "app_client": "consumer_web",
    "user-agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Mobile Safari/537.36",
    "origin": "https://blinkit.com",
    "accept": "*/*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
}
# ─────────────────────────────────────────────────────────────────────────────

def extract_price(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, dict):
        val = val.get("text", "")
    if isinstance(val, str):
        match = re.search(r'[₹](\d+(?:\.\d+)?)', val)
        if match:
            return float(match.group(1))
        match = re.search(r'(\d+(?:\.\d+)?)', val)
        if match:
            return float(match.group(1))
    return None

def parse_rating_count(text):
    if not text:
        return None
    text = text.strip("()")
    if "lac" in text:
        return int(float(text.replace("lac", "").strip()) * 100000)
    try:
        return int(text.replace(",", ""))
    except:
        return None

def fetch_pdp(product_id):
    url = f"https://blinkit.com/v1/layout/product/{product_id}"
    headers = {**HEADERS, "referer": f"https://blinkit.com/prn/product/prid/{product_id}"}
    try:
        r = requests.post(url, headers=headers, timeout=15, impersonate="chrome120")
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"
        return r.json(), None
    except Exception as e:
        return None, str(e)

def parse_pdp(data, product_id):
    result = {
        "product_id": str(product_id),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    try:
        snippets = data["response"]["snippets"]

        # Index snippets by widget_type for robust parsing
        by_type = {}
        for s in snippets:
            wt = s.get("widget_type", "")
            by_type.setdefault(wt, []).append(s)

        # ── 1. Rating + ETA + shelf life (carousal_list_vr snippet[0]) ──────
        carousel = snippets[0].get("data", {})
        eta_section = carousel.get("eta_rating_data", {})
        if eta_section:
            bar = eta_section.get("rating", {}).get("bar", {})
            result["rating"] = round(bar.get("value", 0), 2)
            rc_text = bar.get("title", {}).get("text", "")
            result["rating_count_text"] = rc_text
            result["rating_count"] = parse_rating_count(rc_text)
            eta_badge = eta_section.get("eta_data", {}).get("badge_data", {})
            result["eta_label"] = eta_badge.get("label", "")

        # Shelf life from overlay
        overlay = carousel.get("overlay_data", {})
        for item in overlay.get("expandable_data", {}).get("expanded_state", {}).get("vertical_item_list", []):
            if "Shelf Life" in item.get("title", {}).get("text", ""):
                result["shelf_life"] = item.get("subtitle", {}).get("text", "")

        # ── 2. Product name + identity (text_right_icons_rating_snippet_type) ─
        for s in by_type.get("text_right_icons_rating_snippet_type", []):
            sd = s.get("data", {})
            identity_id = sd.get("identity", {}).get("id", "")
            if str(identity_id) == str(product_id):
                result["name"] = sd.get("title", {}).get("text", "")
                break
        # fallback: first snippet with a title
        if not result.get("name"):
            for s in snippets:
                t = s.get("data", {}).get("title", {}).get("text", "")
                if t and len(t) > 3:
                    result["name"] = t
                    break

        # ── 3. Brand (crystal_snippet_type_6) ────────────────────────────────
        for s in by_type.get("crystal_snippet_type_6", []):
            sd = s.get("data", {})
            brand = sd.get("title", {}).get("text", "")
            sub = sd.get("subtitle1", {}).get("text", "")
            # brand snippet has "Explore all products" as subtitle
            if "Explore" in sub or "products" in sub.lower():
                result["brand"] = brand
                break
        # fallback: brand from cart_item in footer
        if not result.get("brand"):
            plc = data["response"].get("page_level_components", {})
            footer = plc.get("sticky", {}).get("footer_snippet_models", [])
            for f in footer:
                fd = f.get("snippet", {}).get("data", {})
                # brand in atc_actions cart_item
                atc = fd.get("atc_actions_v2", {}).get("default", [{}])
                for a in atc:
                    cart = a.get("add_to_cart", {}).get("cart_item", {})
                    if cart.get("brand"):
                        result["brand"] = cart["brand"]
                        break

        # ── 4. Price + inventory from sticky footer ───────────────────────────
        plc = data["response"].get("page_level_components", {})
        footer = plc.get("sticky", {}).get("footer_snippet_models", [])
        for f in footer:
            fd = f.get("snippet", {}).get("data", {})
            if "inventory" in fd:
                result["inventory"] = fd["inventory"]
                result["is_sold_out"] = fd.get("is_sold_out", False)
                result["product_state"] = fd.get("product_state", "")
                result["variant"] = fd.get("variant", {}).get("text", "") if isinstance(fd.get("variant"), dict) else fd.get("variant", "")
                result["merchant_type"] = fd.get("merchant_type", "")
            if "normal_price" in fd:
                p = extract_price(fd["normal_price"].get("text", ""))
                if p is not None:
                    result["price"] = p
            if "mrp" in fd:
                m = extract_price(fd["mrp"].get("text", ""))
                if m is not None:
                    result["mrp"] = m
            if fd.get("offer_tag"):
                ot = fd["offer_tag"]
                result["offer_tag"] = ot.get("text", "") if isinstance(ot, dict) else str(ot)

        # mrp fallback: if no mrp found, mrp = price (no discount)
        if not result.get("mrp") and result.get("price"):
            result["mrp"] = result["price"]

        # ── 5. Discount ───────────────────────────────────────────────────────
        p = result.get("price")
        m = result.get("mrp")
        if p and m and m > 0:
            result["discount_pct"] = round((1 - p / m) * 100, 1)
        else:
            result["discount_pct"] = 0.0
        result["is_discounted"] = result["discount_pct"] > 0

        # ── 6. Section titles + similar product IDs (grid_container_vr) ──────
        section_titles = []
        similar_ids = []

        for s in snippets:
            wt = s.get("widget_type", "")
            sd = s.get("data", {})

            # Section headers
            if wt == "image_text_vr_type_header":
                t = sd.get("title", {}).get("text", "")
                if t:
                    section_titles.append(t)

            # Grid items
            if wt == "grid_container_vr":
                for item in sd.get("items", []):
                    pid = item.get("data", {}).get("identity", {}).get("id", "")
                    if pid and pid != str(product_id):
                        similar_ids.append(str(pid))

        result["sections"] = " | ".join(section_titles)
        result["similar_product_ids"] = "|".join(list(dict.fromkeys(similar_ids))[:15])

        # ── 7. Count of similar products sections ─────────────────────────────
        result["similar_count"] = len(list(dict.fromkeys(similar_ids)))

    except Exception as e:
        result["error"] = str(e)

    return result

def estimate_sold(prev, curr):
    """Estimate units sold between two snapshots."""
    try:
        inv_prev = int(prev.get("inventory", 0) or 0)
        inv_curr = int(curr.get("inventory", 0) or 0)
        drop = inv_prev - inv_curr
        if drop < 0:
            return 0, "restocked"
        if inv_prev == 50 and inv_curr == 50:
            return None, "capped_both"
        if inv_prev == 50 and inv_curr < 50:
            return f">={50 - inv_curr}", "capped_prev"
        return drop, "exact"
    except:
        return None, "error"

# ── CSV FIELDS ────────────────────────────────────────────────────────────────
SNAPSHOT_FIELDS = [
    "timestamp", "product_id", "name", "brand", "variant",
    "price", "mrp", "discount_pct", "is_discounted", "offer_tag",
    "inventory", "is_sold_out", "product_state", "merchant_type",
    "rating", "rating_count", "rating_count_text",
    "eta_label", "shelf_life", "similar_count", "sections",
    "similar_product_ids", "error"
]

SOLD_FIELDS = [
    "product_id", "name", "from_time", "to_time", "duration_mins",
    "inv_before", "inv_after", "est_sold", "sold_type",
    "price", "discount_pct", "offer_tag"
]

def write_row(row, fields, output_file):
    file_exists = os.path.exists(output_file)
    with open(output_file, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def main():
    parser = argparse.ArgumentParser(description="Blinkit Inventory Tracker")
    parser.add_argument("--product_ids", nargs="+")
    parser.add_argument("--file", help="Text file with one product_id per line")
    parser.add_argument("--interval", type=int, default=0, help="Repeat every N minutes (0=once)")
    parser.add_argument("--snapshots", default="snapshots.csv")
    parser.add_argument("--sold", default="sold_estimate.csv")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay between requests (sec)")
    args = parser.parse_args()

    product_ids = []
    if args.product_ids:
        product_ids = [str(p) for p in args.product_ids]
    elif args.file:
        with open(args.file, encoding="utf-8-sig") as f:
            product_ids = [l.strip() for l in f if l.strip()]
    else:
        print("Error: provide --product_ids or --file")
        sys.exit(1)

    prev_snapshots = {}
    run = 0

    while True:
        run += 1
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{'='*65}")
        print(f"Run #{run} | {now} | {len(product_ids)} products")
        print(f"{'='*65}")

        for pid in product_ids:
            print(f"  [{pid}] ", end="", flush=True)
            data, err = fetch_pdp(pid)
            if err:
                print(f"ERROR: {err}")
                write_row({"product_id": pid, "timestamp": now, "error": err}, SNAPSHOT_FIELDS, args.snapshots)
                continue

            curr = parse_pdp(data, pid)
            write_row(curr, SNAPSHOT_FIELDS, args.snapshots)

            print(f"{curr.get('name','?')[:28]:28} | "
                  f"brand={curr.get('brand','?'):12} | "
                  f"inv={str(curr.get('inventory','?')):3} | "
                  f"₹{curr.get('price','?')} "
                  f"(mrp ₹{curr.get('mrp','?')}) | "
                  f"disc={curr.get('discount_pct','?')}% | "
                  f"offer={curr.get('offer_tag','') or '-':15} | "
                  f"ratings={curr.get('rating_count_text','?')}")

            if pid in prev_snapshots:
                prev = prev_snapshots[pid]
                est, sold_type = estimate_sold(prev, curr)
                t1 = prev["timestamp"]
                t2 = curr["timestamp"]
                try:
                    fmt = "%Y-%m-%d %H:%M:%S"
                    mins = int((datetime.strptime(t2, fmt) - datetime.strptime(t1, fmt)).total_seconds() / 60)
                except:
                    mins = args.interval

                sold_row = {
                    "product_id": pid,
                    "name": curr.get("name", ""),
                    "from_time": t1,
                    "to_time": t2,
                    "duration_mins": mins,
                    "inv_before": prev.get("inventory", ""),
                    "inv_after": curr.get("inventory", ""),
                    "est_sold": est,
                    "sold_type": sold_type,
                    "price": curr.get("price", ""),
                    "discount_pct": curr.get("discount_pct", ""),
                    "offer_tag": curr.get("offer_tag", ""),
                }
                write_row(sold_row, SOLD_FIELDS, args.sold)
                if est is not None and est != 0:
                    print(f"         ↳ EST SOLD: {est} units ({sold_type}) in {mins} mins")

            prev_snapshots[pid] = curr
            time.sleep(args.delay)

        print(f"\n  Snapshots → {args.snapshots}")
        print(f"  Sold est  → {args.sold}")

        if args.interval <= 0:
            break

        print(f"\n  Sleeping {args.interval} min...")
        time.sleep(args.interval * 60)

if __name__ == "__main__":
    main()
