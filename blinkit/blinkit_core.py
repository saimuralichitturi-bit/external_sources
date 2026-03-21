"""
blinkit_core.py — Shared utilities for all Blinkit scrapers
"""
import re, time, json, csv, os
from datetime import datetime

try:
    from curl_cffi import requests as cf_requests
    CURL_OK = True
except ImportError:
    import requests as cf_requests
    CURL_OK = False

# ── Locations ─────────────────────────────────────────────────────────────────
LOCATIONS = {
    "vijayawada": ("16.5103525", "80.6465468", "2111"),
    "mumbai":     ("19.0760",   "72.8777",    "1"),
    "bangalore":  ("12.9716",   "77.5946",    "4"),
    "delhi":      ("28.6139",   "77.2090",    "2"),
    "hyderabad":  ("17.3850",   "78.4867",    "5"),
    "chennai":    ("13.0827",   "80.2707",    "3"),
    "pune":       ("18.5204",   "73.8567",    "6"),
}

def make_headers(location="vijayawada", session_cookie=""):
    lat, lon, locality = LOCATIONS.get(location, LOCATIONS["vijayawada"])
    h = {
        "content-type":    "application/json",
        "app-version":     "1000000",
        "web-version":     "1000000",
        "web_app_version": "1008010016",
        "app_client":      "consumer_web",
        "lat":             lat,
        "lon":             lon,
        "locality":        locality,
    }
    if session_cookie:
        h["cookie"] = session_cookie
    return h

# ── Parsing ───────────────────────────────────────────────────────────────────
def parse_price(text) -> float:
    if isinstance(text, (int, float)):
        return float(text)
    return float(re.sub(r"[^\d.]", "", str(text or "")) or 0)

def parse_snippet(snippet: dict, position: int) -> dict | None:
    widget_type = snippet.get("widget_type", "")
    # Skip non-product widgets
    if any(x in widget_type for x in ["Header", "Banner", "header", "banner", "Category"]):
        return None

    d = snippet.get("data", {})
    product_id = d.get("product_id") or d.get("identity", {}).get("id")
    if not product_id:
        return None

    cart_item = d.get("atc_action", {}).get("add_to_cart", {}).get("cart_item", {})

    name  = cart_item.get("product_name") or d.get("name", {}).get("text", "")
    brand = cart_item.get("brand") or d.get("brand_name", {}).get("text", "")
    unit  = cart_item.get("unit") or d.get("variant", {}).get("text", "")
    price = cart_item.get("price") or parse_price(d.get("normal_price", {}).get("text", ""))
    mrp   = cart_item.get("mrp")   or parse_price(d.get("mrp", {}).get("text", ""))
    inv   = cart_item.get("inventory")
    if inv is None:
        inv = d.get("inventory")

    group_id     = cart_item.get("group_id") or d.get("group_id")
    merchant_id  = cart_item.get("merchant_id") or d.get("merchant_id", "")
    merchant_type = cart_item.get("merchant_type") or d.get("merchant_type", "")
    is_sold_out  = bool(d.get("is_sold_out") or d.get("soldout_tag") or (inv == 0))
    product_state = d.get("product_state", "")
    eta          = d.get("eta_identifier", "")
    image_url    = cart_item.get("image_url", "")

    disc = 0.0
    if mrp and price and float(mrp) > float(price):
        disc = round((float(mrp) - float(price)) / float(mrp) * 100, 1)

    # Offer tag
    offer_tag = d.get("offer_tag", {}).get("title", {}).get("text", "")

    # Ad detection — badges on d.product_badges
    badges = d.get("product_badges", [])
    is_ad = any(
        b.get("type") == "OTHERS" and b.get("label", "").lower() == "ad"
        for b in badges
    )
    if not is_ad:
        is_ad = "sponsor" in widget_type.lower() or "promoted" in widget_type.lower()

    return {
        "product_id":   str(product_id),
        "group_id":     str(group_id or ""),
        "merchant_id":  str(merchant_id or ""),
        "merchant_type": merchant_type,
        "name":         name,
        "brand":        brand or "Unknown",
        "unit":         unit,
        "price":        float(price or 0),
        "mrp":          float(mrp or 0),
        "discount_pct": disc,
        "offer_tag":    offer_tag,
        "inventory":    inv,
        "is_sold_out":  is_sold_out,
        "product_state": product_state,
        "eta":          eta,
        "image_url":    image_url,
        "is_ad":        is_ad,
        "position":     position,
        "widget_type":  widget_type,
    }

# ── HTTP ──────────────────────────────────────────────────────────────────────
def post(url: str, headers: dict, timeout=20, retries=2) -> dict | None:
    """POST to Blinkit API with automatic retry on failure."""
    for attempt in range(retries + 1):
        try:
            if CURL_OK:
                r = cf_requests.post(url, headers=headers, impersonate="chrome120", timeout=timeout)
            else:
                r = cf_requests.post(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                wait = 5 * (attempt + 1)
                print(f"  [rate limit] waiting {wait}s...")
                time.sleep(wait)
                continue
            if attempt < retries:
                time.sleep(2)
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
            else:
                print(f"  HTTP error: {e}")
    return None

def search_page(keyword: str, offset: int, headers: dict, limit=24) -> list[dict]:
    url = (
        f"https://blinkit.com/v1/layout/search"
        f"?q={keyword.replace(' ', '+')}"
        f"&search_type=type_to_search"
        f"&offset={offset}&limit={limit}"
    )
    data = post(url, headers)
    if not data:
        return []
    snippets = data.get("response", {}).get("snippets", [])
    products = []
    pos = offset + 1
    for s in snippets:
        p = parse_snippet(s, pos)
        if p:
            products.append(p)
            pos += 1
    return products, len(snippets)

def search_all_pages(keyword: str, headers: dict, max_pages=5, delay=0.5) -> list[dict]:
    all_products = []
    global_pos = 1
    for page in range(max_pages):
        offset = page * 24
        url = (
            f"https://blinkit.com/v1/layout/search"
            f"?q={keyword.replace(' ', '+')}"
            f"&search_type=type_to_search"
            f"&offset={offset}&limit=24"
        )
        data = post(url, headers)
        if not data:
            break
        snippets = data.get("response", {}).get("snippets", [])
        if not snippets:
            break
        for s in snippets:
            p = parse_snippet(s, global_pos)
            if p:
                all_products.append(p)
                global_pos += 1
        if len(snippets) < 24:
            break
        time.sleep(delay)
    return all_products

def category_page(l1_id: int, offset: int, headers: dict, limit=24) -> tuple[list, int]:
    url = (
        f"https://blinkit.com/v1/layout/listing"
        f"?l0_cat={l1_id}&offset={offset}&limit={limit}"
    )
    data = post(url, headers)
    if not data:
        return [], 0
    snippets = data.get("response", {}).get("snippets", [])
    products = []
    pos = offset + 1
    for s in snippets:
        p = parse_snippet(s, pos)
        if p:
            products.append(p)
            pos += 1
    return products, len(snippets)

# ── CSV ───────────────────────────────────────────────────────────────────────
def append_csv(filepath: str, rows: list[dict], cols: list[str]):
    if not rows:
        return
    write_header = not os.path.exists(filepath)
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        if write_header:
            w.writeheader()
        w.writerows(rows)

def load_csv_as_dicts(filepath: str) -> list[dict]:
    if not os.path.exists(filepath):
        return []
    with open(filepath, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def now_str() -> str:
    return datetime.now().isoformat(timespec="seconds")

def run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")
