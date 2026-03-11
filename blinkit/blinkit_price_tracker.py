"""
blinkit_price_tracker.py
=========================
Tracks price, MRP, discount, and inventory for a watchlist of products.
Detects: price drops, price hikes, new discounts, flash sales, restocks.

OUTPUTS:
  price_history.csv     — every price snapshot per product
  price_alerts.csv      — detected price events (drops, hikes, sales)
  price_summary.csv     — current prices with change vs previous run

USAGE:
  # Track specific product IDs
  python blinkit_price_tracker.py --products 447847,125240,98765

  # Track products from a keyword search
  python blinkit_price_tracker.py --keyword "amul butter" --location mumbai

  # Track from a file (one product_id per line)
  python blinkit_price_tracker.py --product-file watchlist.txt

  # Run every 30 minutes
  python blinkit_price_tracker.py --products 447847,125240 --interval 30

  # Alert on any price drop > 5%
  python blinkit_price_tracker.py --products 447847 --alert-threshold 5
"""

import argparse
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from blinkit_core import (
    make_headers, post, parse_snippet, append_csv,
    load_csv_as_dicts, now_str, run_id, parse_price
)

HISTORY_FILE = "price_history.csv"
ALERTS_FILE  = "price_alerts.csv"
SUMMARY_FILE = "price_summary.csv"

HISTORY_COLS = [
    "run_id", "timestamp",
    "product_id", "name", "brand", "unit",
    "price", "mrp", "discount_pct", "offer_tag",
    "inventory", "is_sold_out", "product_state", "eta",
]

ALERT_COLS = [
    "timestamp", "alert_type", "product_id", "name", "brand",
    "old_value", "new_value", "change_pct", "note",
]

SUMMARY_COLS = [
    "timestamp", "product_id", "name", "brand", "unit",
    "price", "mrp", "discount_pct", "offer_tag",
    "inventory", "is_sold_out",
    "price_vs_prev", "price_change_pct",
    "disc_vs_prev", "inv_vs_prev",
]

# ── Fetch product via search ──────────────────────────────────────────────────
def fetch_by_search(keyword: str, headers: dict) -> list[dict]:
    url = (
        f"https://blinkit.com/v1/layout/search"
        f"?q={keyword.replace(' ', '+')}"
        f"&search_type=type_to_search&offset=0&limit=24"
    )
    data = post(url, headers)
    if not data:
        return []
    snippets = data.get("response", {}).get("snippets", [])
    products = []
    pos = 1
    for s in snippets:
        p = parse_snippet(s, pos)
        if p:
            products.append(p)
            pos += 1
    return products


def fetch_by_product_id(product_id: str, headers: dict) -> dict | None:
    """Fetch single product via PDP endpoint."""
    url = f"https://blinkit.com/v1/layout/product/{product_id}"
    data = post(url, headers)
    if not data:
        return None

    snippets = data.get("response", {}).get("snippets", [])
    for s in snippets:
        p = parse_snippet(s, 1)
        if p and str(p.get("product_id")) == str(product_id):
            return p

    # Fallback: parse PDP-specific structure
    for s in snippets:
        d = s.get("data", {})
        pid = d.get("product_id") or d.get("identity", {}).get("id")
        if pid and str(pid) == str(product_id):
            cart_item = d.get("atc_action", {}).get("add_to_cart", {}).get("cart_item", {})
            price = cart_item.get("price", 0)
            mrp   = cart_item.get("mrp", 0)
            inv   = cart_item.get("inventory")
            return {
                "product_id":   str(product_id),
                "name":         cart_item.get("product_name", ""),
                "brand":        cart_item.get("brand", ""),
                "unit":         cart_item.get("unit", ""),
                "price":        float(price or 0),
                "mrp":          float(mrp or 0),
                "discount_pct": round((float(mrp)-float(price))/float(mrp)*100, 1) if mrp and price and float(mrp) > 0 else 0,
                "offer_tag":    d.get("offer_tag", {}).get("title", {}).get("text", ""),
                "inventory":    inv,
                "is_sold_out":  d.get("is_sold_out", False),
                "product_state": d.get("product_state", ""),
                "eta":          d.get("eta_identifier", ""),
            }
    return None


# ── Alert detection ───────────────────────────────────────────────────────────
def detect_alerts(current: dict, previous: dict, threshold: float) -> list[dict]:
    alerts = []
    ts = now_str()
    pid = current["product_id"]
    name = current["name"]
    brand = current["brand"]

    def alert(atype, old, new, note=""):
        change = round((float(new) - float(old)) / float(old) * 100, 1) if float(old) else 0
        alerts.append({
            "timestamp":  ts,
            "alert_type": atype,
            "product_id": pid,
            "name":       name,
            "brand":      brand,
            "old_value":  old,
            "new_value":  new,
            "change_pct": change,
            "note":       note,
        })

    prev_price = float(previous.get("price", 0) or 0)
    curr_price = float(current.get("price", 0) or 0)
    prev_disc  = float(previous.get("discount_pct", 0) or 0)
    curr_disc  = float(current.get("discount_pct", 0) or 0)
    prev_inv   = previous.get("inventory")
    curr_inv   = current.get("inventory")
    prev_sold  = str(previous.get("is_sold_out", "")).lower() == "true"
    curr_sold  = bool(current.get("is_sold_out", False))

    # Price drop
    if prev_price > 0 and curr_price < prev_price:
        drop_pct = (prev_price - curr_price) / prev_price * 100
        if drop_pct >= threshold:
            alert("PRICE_DROP", prev_price, curr_price,
                  f"₹{prev_price:.0f} → ₹{curr_price:.0f} ({drop_pct:.1f}% drop)")

    # Price hike
    if prev_price > 0 and curr_price > prev_price:
        hike_pct = (curr_price - prev_price) / prev_price * 100
        if hike_pct >= threshold:
            alert("PRICE_HIKE", prev_price, curr_price,
                  f"₹{prev_price:.0f} → ₹{curr_price:.0f} (+{hike_pct:.1f}%)")

    # New discount
    if prev_disc == 0 and curr_disc > 0:
        alert("NEW_DISCOUNT", 0, curr_disc,
              f"New {curr_disc:.1f}% discount appeared")

    # Discount removed
    if prev_disc > 0 and curr_disc == 0:
        alert("DISCOUNT_REMOVED", prev_disc, 0,
              f"{prev_disc:.1f}% discount removed")

    # Restock
    if prev_sold and not curr_sold:
        alert("RESTOCK", "out_of_stock", "in_stock",
              f"Back in stock! Inventory: {curr_inv}")

    # Went out of stock
    if not prev_sold and curr_sold:
        alert("OUT_OF_STOCK", "in_stock", "out_of_stock",
              f"Went out of stock")

    # Inventory spike (possible restocking / bulk arrival)
    if prev_inv is not None and curr_inv is not None:
        try:
            prev_inv_f = float(prev_inv)
            curr_inv_f = float(curr_inv)
            if prev_inv_f > 0 and curr_inv_f > prev_inv_f * 2:
                alert("INVENTORY_SPIKE", prev_inv_f, curr_inv_f,
                      f"Inventory jumped {prev_inv_f:.0f} → {curr_inv_f:.0f}")
        except (ValueError, TypeError):
            pass

    return alerts


# ── Main run ──────────────────────────────────────────────────────────────────
def run_once(product_ids: list[str], keyword: str, headers: dict, threshold: float):
    rid = run_id()
    ts  = now_str()

    print(f"\n{'='*60}")
    print(f"Run: {rid} | Products: {len(product_ids) if product_ids else 'from keyword'}")
    print(f"{'='*60}")

    # Load previous run for comparison
    prev_data = {}
    if os.path.exists(HISTORY_FILE):
        history = load_csv_as_dicts(HISTORY_FILE)
        # Get most recent entry per product
        for row in history:
            pid = row.get("product_id", "")
            if pid:
                prev_data[pid] = row  # last write wins = most recent

    # Fetch current data
    current_products = []

    if keyword:
        print(f"Searching: '{keyword}'")
        products = fetch_by_search(keyword, headers)
        current_products.extend(products)
    
    for pid in product_ids:
        print(f"  Fetching {pid}...", end=" ", flush=True)
        p = fetch_by_product_id(pid, headers)
        if p:
            current_products.append(p)
            print(f"✓ {p['name'][:40]} ₹{p['price']}")
        else:
            print("✗ not found")
        time.sleep(0.3)

    if not current_products:
        print("No products fetched.")
        return

    # Process
    history_rows = []
    alert_rows   = []
    summary_rows = []

    print(f"\n{'Product':<35} {'Price':>7} {'MRP':>7} {'Disc':>6} {'Inv':>5} {'Change':>10}")
    print("-" * 70)

    for p in current_products:
        pid = p["product_id"]

        # History row
        history_rows.append({"run_id": rid, "timestamp": ts, **p})

        # Detect alerts vs previous
        if pid in prev_data:
            new_alerts = detect_alerts(p, prev_data[pid], threshold)
            alert_rows.extend(new_alerts)
            prev_price = float(prev_data[pid].get("price", 0) or 0)
            curr_price = float(p.get("price", 0) or 0)
            price_diff = curr_price - prev_price
            price_chg_pct = round(price_diff / prev_price * 100, 1) if prev_price else 0
            prev_disc = float(prev_data[pid].get("discount_pct", 0) or 0)
            curr_disc = float(p.get("discount_pct", 0) or 0)
            prev_inv  = prev_data[pid].get("inventory", "")
            change_str = f"{price_diff:+.0f} ({price_chg_pct:+.1f}%)" if price_diff != 0 else "—"
        else:
            price_diff = 0
            price_chg_pct = 0
            prev_disc = 0
            curr_disc = float(p.get("discount_pct", 0) or 0)
            prev_inv  = ""
            change_str = "NEW"

        # Summary row
        summary_rows.append({
            "timestamp":        ts,
            "product_id":       pid,
            "name":             p["name"],
            "brand":            p["brand"],
            "unit":             p["unit"],
            "price":            p["price"],
            "mrp":              p["mrp"],
            "discount_pct":     p["discount_pct"],
            "offer_tag":        p["offer_tag"],
            "inventory":        p["inventory"],
            "is_sold_out":      p["is_sold_out"],
            "price_vs_prev":    price_diff,
            "price_change_pct": price_chg_pct,
            "disc_vs_prev":     round(curr_disc - prev_disc, 1),
            "inv_vs_prev":      "",
        })

        sold_marker = " 🔴" if p["is_sold_out"] else ""
        print(
            f"  {(p['brand']+' '+p['name'])[:33]:<35} "
            f"₹{p['price']:>6.0f} "
            f"₹{p['mrp']:>6.0f} "
            f"{p['discount_pct']:>5.1f}% "
            f"{str(p['inventory'] or '?'):>5}"
            f"{sold_marker} "
            f"{change_str:>10}"
        )

    # Print alerts
    if alert_rows:
        print(f"\n{'='*60}")
        print(f"🚨 {len(alert_rows)} ALERT(S):")
        for a in alert_rows:
            print(f"  [{a['alert_type']}] {a['brand']} {a['name'][:30]} — {a['note']}")
        print(f"{'='*60}")
    else:
        print(f"\nNo price changes detected.")

    # Write files
    append_csv(HISTORY_FILE, history_rows, HISTORY_COLS)
    if alert_rows:
        append_csv(ALERTS_FILE, alert_rows, ALERT_COLS)

    # Overwrite summary (current state only)
    import csv
    with open(SUMMARY_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(summary_rows)

    print(f"\n✅ {HISTORY_FILE} (+{len(history_rows)} rows)")
    print(f"✅ {ALERTS_FILE} (+{len(alert_rows)} alerts)")
    print(f"✅ {SUMMARY_FILE} (current snapshot)")


def main():
    parser = argparse.ArgumentParser(description="Blinkit Price History Tracker")
    parser.add_argument("--products",         type=str, help="Comma-separated product IDs")
    parser.add_argument("--product-file",     type=str, help="File with one product_id per line")
    parser.add_argument("--keyword",          type=str, help="Search keyword to track all results")
    parser.add_argument("--location",         type=str, default="vijayawada")
    parser.add_argument("--cookie",           type=str, default="")
    parser.add_argument("--interval",         type=int, default=0,   help="Repeat every N minutes")
    parser.add_argument("--alert-threshold",  type=float, default=2.0, help="Min % change to alert (default 2%%)")
    args = parser.parse_args()

    product_ids = []
    if args.products:
        product_ids = [p.strip() for p in args.products.split(",") if p.strip()]
    if args.product_file and os.path.exists(args.product_file):
        with open(args.product_file) as f:
            product_ids += [l.strip() for l in f if l.strip()]

    headers = make_headers(args.location, args.cookie)

    if args.interval > 0:
        print(f"Tracking every {args.interval} min. Ctrl+C to stop.")
        while True:
            try:
                run_once(product_ids, args.keyword, headers, args.alert_threshold)
                time.sleep(args.interval * 60)
            except KeyboardInterrupt:
                print("\nStopped.")
                break
    else:
        run_once(product_ids, args.keyword, headers, args.alert_threshold)


if __name__ == "__main__":
    main()
