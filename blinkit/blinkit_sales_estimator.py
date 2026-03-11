"""
blinkit_sales_estimator.py
===========================
Estimates approximate units sold per product and per company/brand
on Blinkit using MULTIPLE converging signals.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SIGNAL MODEL — How we estimate sales without seller access
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Signal 1 — INVENTORY DEPLETION (most direct)
  Track inventory every N minutes. Drop from 12→8 = 4 units sold.
  Cap = 50 (Blinkit shows "50+" not exact). Capped→lower = ≥X sold.
  Accuracy: HIGH for uncapped, LOW for capped products.

Signal 2 — RATING COUNT VELOCITY (all-time proxy)
  rating_count grows as users rate after purchase.
  ~5-10% of buyers leave ratings (industry average).
  rating_count × 10-20 = rough all-time unit sales.
  Track rating_count change over time → velocity = recent sales rate.

Signal 3 — SEARCH RANK POSITION
  Products with high sales rank higher organically.
  Position 1-3 = Blinkit's algorithm surfaces top sellers.
  Track rank over multiple keywords/times → rank stability = sustained sales.

Signal 4 — CATEGORY SOV (share of voice)
  High organic SOV% = Blinkit's algorithm rewards the product = high sales.
  Brand with 40% SOV in chips = likely 40%+ market share in that category.

Signal 5 — RESTOCK FREQUENCY
  inventory going from low→50 = restocking event.
  Products that restock frequently = high turnover = high sales.
  Track restock events per product per week.

Signal 6 — AD SPEND PROXY
  Brands spending on ads (is_ad=True) are high-volume sellers trying
  to maintain/grow position. Ad presence = confidence signal of volume.

COMBINED SCORE → SALES ESTIMATE BANDS:
  Uses weighted scoring across all signals to output:
  - daily_units_est    : estimated units/day at this dark store
  - monthly_units_est  : monthly estimate
  - confidence         : low / medium / high
  - est_method         : which signals drove the estimate

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTPUTS:
  sales_estimates.csv      — per-product sales estimates
  brand_estimates.csv      — aggregated brand/company estimates
  sales_report.json        — full structured report
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

USAGE:
  # Full pipeline — scrape + estimate (recommended)
  python blinkit_sales_estimator.py --keywords "chips,protein powder" --location mumbai --interval 30 --duration 120

  # From existing inventory snapshots CSV
  python blinkit_sales_estimator.py --from-snapshots snapshots.csv

  # Category-wide brand estimates
  python blinkit_sales_estimator.py --category 4 --location mumbai

  # Full report across multiple categories
  python blinkit_sales_estimator.py --categories 3,4,7,8 --location mumbai --report
"""

import argparse
import csv
import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from blinkit_core import (
    make_headers, post, parse_snippet, append_csv,
    load_csv_as_dicts, now_str, run_id, parse_price
)

try:
    from curl_cffi import requests as cf_requests
    CURL_OK = True
except ImportError:
    import requests as cf_requests
    CURL_OK = False

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONSTANTS & CALIBRATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# % of buyers who leave ratings (conservative estimate for India)
RATING_CONVERSION_RATE = 0.07   # 7% of buyers rate

# Blinkit dark store coverage multiplier
# Vijayawada ~1-2 dark stores, Mumbai ~30+ dark stores
DARK_STORE_MULTIPLIER = {
    "vijayawada": 1.5,
    "mumbai":     32.0,
    "bangalore":  18.0,
    "delhi":      25.0,
    "hyderabad":  12.0,
    "chennai":    8.0,
    "pune":       6.0,
}

# Inventory cap value (Blinkit shows 50 = "50 or more")
INV_CAP = 50

# Min tracking interval to trust inventory delta (minutes)
MIN_INTERVAL_MINS = 10

# Daily active hours for q-commerce (10am-11pm = 13hrs)
ACTIVE_HOURS = 13

ESTIMATE_COLS = [
    "product_id", "name", "brand", "unit", "category",
    "price", "mrp",
    # Signal values
    "inv_depletion_rate_per_hr",   # units/hr from inventory tracking
    "rating_count",                 # current rating count
    "rating_velocity_per_day",      # rating_count increase per day
    "avg_search_rank",              # average rank across keywords
    "organic_sov_pct",              # category SOV %
    "restock_count",                # # restocks observed
    "is_ad",                        # currently running ads
    # Estimates
    "daily_units_est",
    "monthly_units_est",
    "confidence",
    "est_method",
    "location",
    "as_of",
]

BRAND_COLS = [
    "brand",
    "product_count",
    "total_daily_units_est",
    "total_monthly_units_est",
    "avg_organic_sov_pct",
    "total_rating_count",
    "high_confidence_products",
    "top_product",
    "top_product_daily_est",
    "location",
    "as_of",
]

SALES_FILE  = "sales_estimates.csv"
BRAND_FILE  = "brand_estimates.csv"
REPORT_FILE = "sales_report.json"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SIGNAL 1 — INVENTORY DEPLETION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def track_inventory(product_ids: list[str], headers: dict,
                    interval_mins: int = 0, duration_mins: int = 0) -> dict[str, list[dict]]:
    """
    Single-pass snapshot of all products.
    Designed for GitHub Actions — one run per cron trigger, no sleep loop.
    Previous snapshots loaded from snapshots_cache.csv for depletion calc.
    """
    snapshots = defaultdict(list)
    ts = now_str()

    # Load previous snapshots from cache
    cache_file = "snapshots_cache.csv"
    if os.path.exists(cache_file):
        import csv as _csv
        with open(cache_file, newline="", encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                pid = row.get("product_id", "")
                if pid in product_ids:
                    try:
                        row["inventory"] = int(float(row.get("inventory") or -1))
                        if row["inventory"] < 0:
                            row["inventory"] = None
                    except (ValueError, TypeError):
                        row["inventory"] = None
                    snapshots[pid].append(row)

    print(f"\n[Inventory Tracker] {len(product_ids)} products | single-pass snapshot")
    print(f"  Previous cache: {sum(len(v) for v in snapshots.values())} rows loaded")

    # Current snapshot
    new_rows = []
    for pid in product_ids:
        snap = fetch_pdp_snapshot(pid, headers)
        if snap:
            snap["timestamp"] = ts
            snapshots[pid].append(snap)
            new_rows.append(snap)
            inv_str = "50+" if snap.get("inventory") == INV_CAP else str(snap.get("inventory", "?"))
            print(f"  {pid}: inv={inv_str} ₹{snap.get('price',0):.0f} rc={snap.get('rating_count','?')}")
        time.sleep(0.3)

    # Save updated cache (append new rows)
    if new_rows:
        import csv as _csv
        write_header = not os.path.exists(cache_file)
        cache_cols = ["product_id","name","brand","unit","timestamp","inventory",
                      "price","mrp","is_sold_out","rating_count","rating","offer_tag"]
        with open(cache_file, "a", newline="", encoding="utf-8") as f:
            w = _csv.DictWriter(f, fieldnames=cache_cols, extrasaction="ignore")
            if write_header:
                w.writeheader()
            w.writerows(new_rows)
        print(f"  Cache updated → {cache_file} (+{len(new_rows)} rows)")

    return dict(snapshots)


def fetch_pdp_snapshot(product_id: str, headers: dict) -> dict | None:
    url = f"https://blinkit.com/v1/layout/product/{product_id}"
    try:
        if CURL_OK:
            r = cf_requests.post(url, headers=headers, impersonate="chrome120", timeout=15)
        else:
            r = cf_requests.post(url, headers=headers, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception as e:
        print(f"    Error fetching {product_id}: {e}")
        return None

    snippets = data.get("response", {}).get("snippets", [])
    result = {"product_id": str(product_id)}

    for s in snippets:
        d = s.get("data", {})
        cart_item = d.get("atc_action", {}).get("add_to_cart", {}).get("cart_item", {})

        pid = d.get("product_id") or cart_item.get("product_id")
        if not pid:
            continue

        # Basic fields
        if not result.get("name"):
            result["name"] = cart_item.get("product_name") or d.get("name", {}).get("text", "")
        if not result.get("brand"):
            result["brand"] = cart_item.get("brand") or d.get("brand_name", {}).get("text", "")
        if not result.get("unit"):
            result["unit"] = cart_item.get("unit") or d.get("variant", {}).get("text", "")

        inv = cart_item.get("inventory")
        if inv is None:
            inv = d.get("inventory")
        if inv is not None:
            result["inventory"] = int(inv)

        price = cart_item.get("price")
        mrp   = cart_item.get("mrp")
        if price:
            result["price"] = float(price)
        if mrp:
            result["mrp"] = float(mrp)

        result["is_sold_out"] = bool(d.get("is_sold_out"))

        # Rating count — from eta_rating_data
        eta_rating = d.get("eta_rating_data", {})
        if eta_rating:
            rc_text = eta_rating.get("rating_count", {}).get("text", "")
            result["rating_count"] = parse_rating_count_text(rc_text)
            result["rating"] = eta_rating.get("rating", {}).get("bar", 0)

        # Offer tag
        result["offer_tag"] = d.get("offer_tag", {}).get("title", {}).get("text", "")

    return result if result.get("name") else None


def parse_rating_count_text(text: str) -> int | None:
    if not text:
        return None
    text = text.strip("()")
    if "lac" in text.lower():
        return int(float(text.lower().replace("lac", "").strip()) * 100000)
    try:
        return int(text.replace(",", ""))
    except (ValueError, AttributeError):
        return None


def calc_depletion_rate(snapshots: list[dict]) -> dict:
    """
    From a series of inventory snapshots, calculate units/hour depletion.
    Returns dict with depletion stats.
    """
    if len(snapshots) < 2:
        return {"rate_per_hr": None, "total_sold": None, "confidence": "none", "restocks": 0}

    total_sold = 0
    total_mins = 0
    restocks = 0
    capped_intervals = 0
    clean_intervals = 0
    fmt = "%Y-%m-%dT%H:%M:%S"

    for i in range(1, len(snapshots)):
        prev = snapshots[i-1]
        curr = snapshots[i]

        try:
            t1 = datetime.fromisoformat(prev["timestamp"])
            t2 = datetime.fromisoformat(curr["timestamp"])
            mins = (t2 - t1).total_seconds() / 60
        except Exception:
            continue

        if mins < MIN_INTERVAL_MINS:
            continue

        inv_prev = prev.get("inventory")
        inv_curr = curr.get("inventory")

        if inv_prev is None or inv_curr is None:
            continue

        if inv_curr > inv_prev:
            # Restock — inventory went up
            restocks += 1
            continue

        delta = inv_prev - inv_curr
        if delta < 0:
            continue

        if inv_prev >= INV_CAP and inv_curr >= INV_CAP:
            # Both capped — can't tell how many sold
            capped_intervals += 1
            continue
        elif inv_prev >= INV_CAP and inv_curr < INV_CAP:
            # Was capped, now uncapped — sold at least (50 - inv_curr) + unknown overflow
            # Conservative: use visible delta only
            total_sold += (INV_CAP - inv_curr)
            total_mins += mins
            clean_intervals += 1
        else:
            # Clean interval
            total_sold += delta
            total_mins += mins
            clean_intervals += 1

    if total_mins == 0:
        rate = None
        confidence = "low"
    else:
        rate = (total_sold / total_mins) * 60  # units per hour
        confidence = "high" if clean_intervals >= 3 else "medium" if clean_intervals >= 1 else "low"

    return {
        "rate_per_hr": round(rate, 3) if rate is not None else None,
        "total_sold_observed": total_sold,
        "obs_duration_mins": total_mins,
        "clean_intervals": clean_intervals,
        "capped_intervals": capped_intervals,
        "restocks": restocks,
        "confidence": confidence,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SIGNAL 2 — RATING COUNT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def estimate_from_rating_count(rating_count: int | None, price: float = 0) -> dict:
    """
    rating_count × (1/RATING_CONVERSION_RATE) = all-time orders.
    Price-tier adjustment: cheap products get rated less.
    """
    if not rating_count:
        return {"alltime_units": None, "confidence": "none"}

    # Price tier adjustment for rating rate
    if price < 50:
        rate = 0.05   # cheap impulse items rated less
    elif price < 200:
        rate = 0.07
    else:
        rate = 0.10   # expensive items rated more

    alltime = int(rating_count / rate)
    return {
        "alltime_units": alltime,
        "confidence": "medium",
        "note": f"rating_count={rating_count} ÷ {rate} = {alltime} all-time orders"
    }


def calc_rating_velocity(snapshots: list[dict]) -> float | None:
    """Rating count increase per day from snapshots."""
    valid = [(s["timestamp"], s["rating_count"]) for s in snapshots
             if s.get("rating_count") is not None]
    if len(valid) < 2:
        return None
    try:
        t1 = datetime.fromisoformat(valid[0][0])
        t2 = datetime.fromisoformat(valid[-1][0])
        days = (t2 - t1).total_seconds() / 86400
        if days < 0.01:
            return None
        delta_rc = valid[-1][1] - valid[0][1]
        return round(delta_rc / days, 2) if delta_rc > 0 else 0
    except Exception:
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SIGNAL 3+4 — SEARCH RANK + SOV
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def fetch_keyword_ranks(product_ids: set, keywords: list[str],
                        headers: dict) -> dict[str, dict]:
    """
    Search each keyword, record rank and SOV for each product.
    Returns dict of product_id → {avg_rank, appearances, keywords_found, is_ad}
    """
    rank_data = defaultdict(lambda: {"ranks": [], "keywords": [], "is_ad": False})

    for kw in keywords:
        print(f"  Searching '{kw}'...", end=" ", flush=True)
        url = (f"https://blinkit.com/v1/layout/search"
               f"?q={kw.replace(' ', '+')}&search_type=type_to_search&offset=0&limit=24")
        data = post(url, headers)
        if not data:
            print("failed")
            continue
        snippets = data.get("response", {}).get("snippets", [])
        pos = 1
        found = 0
        for s in snippets:
            p = parse_snippet(s, pos)
            if not p:
                continue
            if p["product_id"] in product_ids:
                rank_data[p["product_id"]]["ranks"].append(pos)
                rank_data[p["product_id"]]["keywords"].append(kw)
                if p["is_ad"]:
                    rank_data[p["product_id"]]["is_ad"] = True
                found += 1
            pos += 1
        print(f"{len(snippets)} products ({found} targets found)")
        time.sleep(0.5)

    result = {}
    for pid, d in rank_data.items():
        ranks = d["ranks"]
        result[pid] = {
            "avg_rank": round(sum(ranks) / len(ranks), 1) if ranks else None,
            "best_rank": min(ranks) if ranks else None,
            "appearances": len(ranks),
            "keywords_found": d["keywords"],
            "is_ad": d["is_ad"],
        }
    return result


def fetch_category_sov(product_ids: set, category_keywords: list[str],
                       headers: dict) -> dict[str, float]:
    """Get SOV% per product across category keywords."""
    all_products = []
    for kw in category_keywords:
        url = (f"https://blinkit.com/v1/layout/search"
               f"?q={kw.replace(' ', '+')}&search_type=type_to_search&offset=0&limit=24")
        data = post(url, headers)
        if data:
            snippets = data.get("response", {}).get("snippets", [])
            pos = 1
            for s in snippets:
                p = parse_snippet(s, pos)
                if p:
                    all_products.append(p)
                    pos += 1
        time.sleep(0.4)

    total = len(all_products)
    if total == 0:
        return {}

    from collections import Counter
    counts = Counter(p["product_id"] for p in all_products)
    return {pid: round(count / total * 100, 2) for pid, count in counts.items()}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMBINED ESTIMATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def estimate_daily_sales(
    depletion: dict,
    rating_data: dict,
    rank_data: dict,
    sov_pct: float,
    location: str,
) -> tuple[float | None, str, str]:
    """
    Combine all signals → daily units estimate at this dark store.
    Returns (daily_units, confidence, method_description)
    """
    estimates = []
    methods = []

    # Signal 1: Inventory depletion → units/hr → daily
    rate = depletion.get("rate_per_hr")
    if rate is not None and rate > 0:
        daily_from_inv = rate * ACTIVE_HOURS
        estimates.append(("inv_depletion", daily_from_inv, 3.0))  # highest weight
        methods.append(f"inv_depletion={daily_from_inv:.1f}/day")

    # Signal 2a: Rating velocity → daily orders
    rv = rating_data.get("velocity_per_day")
    if rv and rv > 0:
        # velocity is ratings/day, convert to orders
        daily_from_rv = rv / RATING_CONVERSION_RATE
        # Scale by dark store multiplier (velocity is national)
        ds_mult = DARK_STORE_MULTIPLIER.get(location, 2.0)
        # Rough national → local store conversion
        # Assume ~5000 active dark stores nationally, local gets proportional share
        national_stores = 700  # Blinkit ~700 dark stores nationally
        local_daily = daily_from_rv / national_stores * ds_mult
        estimates.append(("rating_velocity", local_daily, 1.5))
        methods.append(f"rating_velocity={local_daily:.1f}/day")

    # Signal 2b: Rating count → all-time → extrapolate
    alltime = rating_data.get("alltime_units")
    if alltime and not rv:
        # If we have all-time but no velocity, assume product is ~1yr old (365 days)
        # and Blinkit has been strong in this city for ~6mo
        est_age_days = 365
        national_daily = alltime / est_age_days
        ds_mult = DARK_STORE_MULTIPLIER.get(location, 2.0)
        national_stores = 700
        local_daily = national_daily / national_stores * ds_mult
        estimates.append(("rating_count", local_daily, 0.8))
        methods.append(f"rating_count_alltime={local_daily:.1f}/day")

    # Signal 3: Search rank → sales rank proxy
    avg_rank = rank_data.get("avg_rank") if rank_data else None
    if avg_rank:
        # Position 1 ≈ 100 units/day, position 10 ≈ 20, position 24 ≈ 5 (power law)
        # rank_daily = base × e^(-k × rank)
        rank_daily = 80 * math.exp(-0.15 * (avg_rank - 1))
        estimates.append(("search_rank", rank_daily, 1.0))
        methods.append(f"search_rank={rank_daily:.1f}/day")

    # Signal 4: SOV%
    if sov_pct and sov_pct > 0:
        # Category total volume assumption: ~1000 units/day across all products
        # in this dark store for popular categories
        category_total_est = 500
        sov_daily = sov_pct / 100 * category_total_est
        estimates.append(("sov", sov_daily, 0.8))
        methods.append(f"sov={sov_daily:.1f}/day")

    if not estimates:
        return None, "none", "no_signals"

    # Weighted average
    total_weight = sum(w for _, _, w in estimates)
    weighted_sum = sum(v * w for _, v, w in estimates)
    daily_est = weighted_sum / total_weight

    # Confidence based on number of signals and depletion quality
    n_signals = len(estimates)
    dep_conf = depletion.get("confidence", "none")

    if dep_conf == "high" and n_signals >= 3:
        confidence = "high"
    elif dep_conf in ("high", "medium") or n_signals >= 2:
        confidence = "medium"
    else:
        confidence = "low"

    method_str = " | ".join(methods)
    return round(daily_est, 1), confidence, method_str


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN PIPELINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_pipeline(
    keywords: list[str],
    product_ids: list[str],
    location: str,
    headers: dict,
    interval_mins: int,
    duration_mins: int,
    category: str = "",
):
    ts = now_str()
    print(f"\n{'='*65}")
    print(f"BLINKIT SALES ESTIMATOR")
    print(f"Location: {location} | Mode: single-pass (GitHub Actions)")
    print(f"{'='*65}")

    # ── Step 1: Discover products from keywords if no IDs given
    if not product_ids and keywords:
        print(f"\n[Step 1] Discovering products from {len(keywords)} keywords...")
        seen = set()
        discovered = []
        for kw in keywords:
            url = (f"https://blinkit.com/v1/layout/search"
                   f"?q={kw.replace(' ', '+')}&search_type=type_to_search&offset=0&limit=24")
            data = post(url, headers)
            if data:
                for s in data.get("response", {}).get("snippets", []):
                    p = parse_snippet(s, 1)
                    if p and p["product_id"] not in seen:
                        discovered.append(p)
                        seen.add(p["product_id"])
            time.sleep(0.5)
        product_ids = [p["product_id"] for p in discovered]
        product_meta = {p["product_id"]: p for p in discovered}
        print(f"  Discovered {len(product_ids)} unique products")
    else:
        product_meta = {}

    if not product_ids:
        print("No products found.")
        return

    # ── Step 2: Track inventory over time
    print(f"\n[Step 2] Tracking inventory ({duration_mins}min, {interval_mins}min intervals)...")
    snapshot_history = track_inventory(product_ids, headers, interval_mins, duration_mins)

    # ── Step 3: Get keyword ranks
    print(f"\n[Step 3] Fetching search ranks...")
    product_id_set = set(product_ids)
    rank_data = fetch_keyword_ranks(product_id_set, keywords or [""], headers) if keywords else {}

    # ── Step 4: Get SOV
    sov_data = {}
    if keywords:
        print(f"\n[Step 4] Computing category SOV...")
        sov_data = fetch_category_sov(product_id_set, keywords, headers)

    # ── Step 5: Compute estimates
    print(f"\n[Step 5] Computing sales estimates...")
    estimates = []

    for pid in product_ids:
        snaps = snapshot_history.get(pid, [])
        meta = product_meta.get(pid, {})

        # If no meta from discovery, use last snapshot
        if not meta and snaps:
            meta = snaps[-1]

        name  = meta.get("name", "")
        brand = meta.get("brand", "Unknown")
        price = float(meta.get("price") or 0)
        mrp   = float(meta.get("mrp") or 0)
        unit  = meta.get("unit", "")

        # Depletion
        dep = calc_depletion_rate(snaps)

        # Rating signals
        latest_rc = None
        rating_vel = None
        if snaps:
            last_snap = snaps[-1]
            latest_rc = last_snap.get("rating_count")
            rating_vel = calc_rating_velocity(snaps)

        rc_info = estimate_from_rating_count(latest_rc, price)
        rating_info = {
            "alltime_units": rc_info.get("alltime_units"),
            "velocity_per_day": rating_vel,
        }

        # Rank
        prod_rank = rank_data.get(pid, {})
        sov_pct   = sov_data.get(pid, 0.0)

        # Combined estimate
        daily_est, confidence, method = estimate_daily_sales(
            dep, rating_info, prod_rank, sov_pct, location
        )

        monthly_est = round(daily_est * 30, 0) if daily_est else None

        est_row = {
            "product_id":               pid,
            "name":                     name,
            "brand":                    brand,
            "unit":                     unit,
            "category":                 category,
            "price":                    price,
            "mrp":                      mrp,
            "inv_depletion_rate_per_hr": dep.get("rate_per_hr"),
            "rating_count":             latest_rc,
            "rating_velocity_per_day":  rating_vel,
            "avg_search_rank":          prod_rank.get("avg_rank"),
            "organic_sov_pct":          sov_pct,
            "restock_count":            dep.get("restocks", 0),
            "is_ad":                    prod_rank.get("is_ad", False),
            "daily_units_est":          daily_est,
            "monthly_units_est":        monthly_est,
            "confidence":               confidence,
            "est_method":               method,
            "location":                 location,
            "as_of":                    ts,
        }
        estimates.append(est_row)

    # ── Step 6: Brand aggregation
    brand_agg = defaultdict(lambda: {
        "products": [], "daily_total": 0, "monthly_total": 0,
        "rating_count_total": 0, "sov_list": [], "high_conf": 0
    })

    for e in estimates:
        b = e["brand"]
        brand_agg[b]["products"].append(e)
        if e["daily_units_est"]:
            brand_agg[b]["daily_total"] += e["daily_units_est"]
            brand_agg[b]["monthly_total"] += (e["monthly_units_est"] or 0)
        if e["rating_count"]:
            brand_agg[b]["rating_count_total"] += e["rating_count"]
        if e["organic_sov_pct"]:
            brand_agg[b]["sov_list"].append(e["organic_sov_pct"])
        if e["confidence"] == "high":
            brand_agg[b]["high_conf"] += 1

    brand_rows = []
    for brand, agg in sorted(brand_agg.items(), key=lambda x: -x[1]["daily_total"]):
        prods = agg["products"]
        top = max(prods, key=lambda p: p.get("daily_units_est") or 0)
        brand_rows.append({
            "brand":                    brand,
            "product_count":            len(prods),
            "total_daily_units_est":    round(agg["daily_total"], 1),
            "total_monthly_units_est":  round(agg["monthly_total"], 0),
            "avg_organic_sov_pct":      round(sum(agg["sov_list"]) / len(agg["sov_list"]), 2) if agg["sov_list"] else 0,
            "total_rating_count":       agg["rating_count_total"],
            "high_confidence_products": agg["high_conf"],
            "top_product":              top["name"],
            "top_product_daily_est":    top.get("daily_units_est"),
            "location":                 location,
            "as_of":                    ts,
        })

    # ── Print results
    print(f"\n{'='*65}")
    print(f"PRODUCT ESTIMATES — {location.upper()}")
    print(f"{'='*65}")
    print(f"{'Brand':<20} {'Product':<28} {'/Day':>6} {'/Month':>8} {'Conf':<8} {'Method'}")
    print(f"{'-'*90}")
    for e in sorted(estimates, key=lambda x: -(x.get("daily_units_est") or 0)):
        d = e.get("daily_units_est")
        m = e.get("monthly_units_est")
        print(
            f"  {e['brand'][:18]:<20} {e['name'][:26]:<28} "
            f"{f'{d:.0f}' if d else '?':>6} "
            f"{f'{m:.0f}' if m else '?':>8} "
            f"{e['confidence']:<8} "
            f"{(e['est_method'] or '')[:40]}"
        )

    print(f"\n{'='*65}")
    print(f"BRAND ESTIMATES — {location.upper()}")
    print(f"{'='*65}")
    print(f"{'Brand':<25} {'Prods':>5} {'Daily':>8} {'Monthly':>10} {'RatingCt':>10} {'SOV%':>6}")
    print(f"{'-'*70}")
    for b in brand_rows:
        print(
            f"  {b['brand'][:23]:<25} "
            f"{b['product_count']:>5} "
            f"{b['total_daily_units_est']:>8.0f} "
            f"{b['total_monthly_units_est']:>10.0f} "
            f"{b['total_rating_count']:>10} "
            f"{b['avg_organic_sov_pct']:>6.1f}%"
        )

    # ── Write outputs
    append_csv(SALES_FILE, estimates, ESTIMATE_COLS)
    append_csv(BRAND_FILE, brand_rows, BRAND_COLS)

    report = {
        "generated_at": ts,
        "location": location,
        "products_tracked": len(estimates),
        "brands_tracked": len(brand_rows),
        "methodology": {
            "signals": ["inventory_depletion", "rating_count", "rating_velocity", "search_rank", "sov"],
            "rating_conversion_rate": RATING_CONVERSION_RATE,
            "dark_store_multiplier": DARK_STORE_MULTIPLIER.get(location),
            "active_hours_per_day": ACTIVE_HOURS,
            "caveat": (
                "These are ESTIMATES with significant uncertainty. "
                "Inventory depletion signal has highest accuracy when uncapped. "
                "Rating-based estimates assume 7% rating conversion rate. "
                "All figures are for this dark store locality only."
            )
        },
        "brand_summary": brand_rows,
        "product_details": estimates,
    }
    with open(REPORT_FILE, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n✅ {SALES_FILE} (+{len(estimates)} rows)")
    print(f"✅ {BRAND_FILE} (+{len(brand_rows)} rows)")
    print(f"✅ {REPORT_FILE}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FROM EXISTING SNAPSHOTS CSV
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_from_snapshots(snapshots_file: str, location: str):
    """Compute estimates from an existing snapshots.csv (from inventory tracker)."""
    print(f"\nLoading snapshots from {snapshots_file}...")
    rows = load_csv_as_dicts(snapshots_file)
    if not rows:
        print("No data found.")
        return

    # Group by product_id
    by_product = defaultdict(list)
    for r in rows:
        by_product[r["product_id"]].append(r)

    ts = now_str()
    estimates = []
    brand_agg = defaultdict(lambda: {"daily": 0, "monthly": 0, "products": []})

    print(f"  {len(by_product)} products found in snapshot file")
    print(f"\n{'Brand':<20} {'Product':<30} {'/Day':>7} {'/Month':>8} {'Conf'}")
    print("-" * 75)

    for pid, snaps in by_product.items():
        # Sort by timestamp
        snaps = sorted(snaps, key=lambda x: x.get("timestamp", ""))

        # Convert string inventory to int
        for s in snaps:
            try:
                s["inventory"] = int(float(s.get("inventory") or -1))
                if s["inventory"] < 0:
                    s["inventory"] = None
            except (ValueError, TypeError):
                s["inventory"] = None

            try:
                s["rating_count"] = int(float(s.get("rating_count") or 0)) or None
            except (ValueError, TypeError):
                s["rating_count"] = None

        dep = calc_depletion_rate(snaps)
        last = snaps[-1]

        name  = last.get("name", pid)
        brand = last.get("brand", "Unknown")
        price = float(last.get("price") or 0)
        mrp   = float(last.get("mrp") or 0)

        rc = last.get("rating_count")
        rc_info = estimate_from_rating_count(rc, price)
        rv = calc_rating_velocity(snaps)

        rating_info = {
            "alltime_units": rc_info.get("alltime_units"),
            "velocity_per_day": rv,
        }

        daily, confidence, method = estimate_daily_sales(dep, rating_info, {}, 0.0, location)
        monthly = round(daily * 30) if daily else None

        print(
            f"  {brand[:18]:<20} {name[:28]:<30} "
            f"{f'{daily:.0f}' if daily else '?':>7} "
            f"{f'{monthly:.0f}' if monthly else '?':>8} "
            f"  {confidence}"
        )

        row = {
            "product_id": pid, "name": name, "brand": brand,
            "unit": last.get("unit", ""), "category": "",
            "price": price, "mrp": mrp,
            "inv_depletion_rate_per_hr": dep.get("rate_per_hr"),
            "rating_count": rc, "rating_velocity_per_day": rv,
            "avg_search_rank": None, "organic_sov_pct": 0,
            "restock_count": dep.get("restocks", 0), "is_ad": False,
            "daily_units_est": daily, "monthly_units_est": monthly,
            "confidence": confidence, "est_method": method,
            "location": location, "as_of": ts,
        }
        estimates.append(row)
        brand_agg[brand]["daily"] += (daily or 0)
        brand_agg[brand]["monthly"] += (monthly or 0)
        brand_agg[brand]["products"].append(row)

    append_csv(SALES_FILE, estimates, ESTIMATE_COLS)
    print(f"\n✅ {SALES_FILE}")

    print(f"\n{'='*50}")
    print("BRAND TOTALS")
    print(f"{'='*50}")
    print(f"{'Brand':<25} {'Daily':>8} {'Monthly':>10}")
    print("-" * 45)
    for brand, agg in sorted(brand_agg.items(), key=lambda x: -x[1]["daily"]):
        print(f"  {brand:<25} {agg['daily']:>8.0f} {agg['monthly']:>10.0f}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    parser = argparse.ArgumentParser(
        description="Blinkit Sales Estimator — multi-signal units sold approximation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Track chips category for 2hrs, estimate every 30min
  python blinkit_sales_estimator.py --keywords "chips,namkeen" --location mumbai --interval 30 --duration 120

  # From existing inventory snapshots
  python blinkit_sales_estimator.py --from-snapshots snapshots.csv --location vijayawada

  # Specific products
  python blinkit_sales_estimator.py --products 447847,125240 --keywords "butter" --location mumbai --interval 15 --duration 60
        """
    )
    parser.add_argument("--keywords",       type=str, help="Comma-separated search keywords")
    parser.add_argument("--products",       type=str, help="Comma-separated product IDs to track")
    parser.add_argument("--category",       type=str, default="", help="Category label (for tagging)")
    parser.add_argument("--location",       type=str, default="vijayawada")
    parser.add_argument("--cookie",         type=str, default="")
    # --interval and --duration removed: use GitHub Actions cron schedule instead
    parser.add_argument("--from-snapshots", type=str, help="Compute estimates from existing snapshots.csv")
    args = parser.parse_args()

    if args.from_snapshots:
        run_from_snapshots(args.from_snapshots, args.location)
        return

    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()] if args.keywords else []
    product_ids = [p.strip() for p in args.products.split(",") if p.strip()] if args.products else []

    if not keywords and not product_ids:
        print("Provide --keywords or --products (or --from-snapshots)")
        sys.exit(1)

    headers = make_headers(args.location, args.cookie)

    run_pipeline(
        keywords=keywords,
        product_ids=product_ids,
        location=args.location,
        headers=headers,
        interval_mins=0,
        duration_mins=0,
        category=args.category,
    )


if __name__ == "__main__":
    main()
