"""
myntra_core.py — Shared utilities for all Myntra scrapers
"""
import re, time, json, csv, os
from datetime import datetime

try:
    from curl_cffi import requests as cf_requests
    CURL_OK = True
except ImportError:
    import requests as cf_requests
    CURL_OK = False

# ── Session (reused across all modules) ───────────────────────────────────────
_session = None

def get_session():
    global _session
    if _session is None:
        if CURL_OK:
            _session = cf_requests.Session(impersonate="chrome120")
        else:
            import requests
            _session = requests.Session()
            _session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            })
        # Warm up session + collect cookies
        _session.get("https://www.myntra.com/", timeout=15)
    return _session

BASE_HEADERS = {
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-IN,en-US;q=0.9,en;q=0.8",
    "Origin":           "https://www.myntra.com",
}

# ── Myntra-specific constants ─────────────────────────────────────────────────
RATING_CONVERSION_RATE = 0.04   # ~4% of buyers leave a rating on fashion
FASHION_RETURN_RATE    = 0.30   # ~30% average return rate on Myntra apparel
REVIEWS_TO_RATINGS     = 0.15   # ~15% of raters also write a review

# ── HTTP helpers ──────────────────────────────────────────────────────────────
def get(url: str, referer: str = "https://www.myntra.com/", timeout=20) -> dict | None:
    session = get_session()
    headers = {**BASE_HEADERS, "Referer": referer}
    try:
        r = session.get(url, headers=headers, timeout=timeout)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                # Server returned HTML (captcha/block page) instead of JSON
                preview = r.text[:120].replace('\n', ' ')
                print(f"  Non-JSON response (likely bot-blocked): {preview}")
                return None
        print(f"  HTTP {r.status_code}: {url}")
    except Exception as e:
        print(f"  HTTP error: {e}")
    return None

# ── Search API ────────────────────────────────────────────────────────────────
def search_page(keyword: str, page: int = 1, rows: int = 50) -> tuple[list[dict], bool]:
    """Returns (products, has_next_page)."""
    offset = (page - 1) * rows
    url = (
        f"https://www.myntra.com/gateway/v2/search/{keyword.replace(' ', '%20')}"
        f"?p={page}&rows={rows}&o={offset}&plaEnabled=false"
    )
    data = get(url, referer=f"https://www.myntra.com/{keyword}")
    if not data:
        return [], False
    products = [parse_listing_product(p, idx + offset + 1)
                for idx, p in enumerate(data.get("products", []))]
    products = [p for p in products if p]
    return products, data.get("hasNextPage", False)

def search_all_pages(keyword: str, max_pages: int = None, delay: float = 0.5) -> list[dict]:
    all_products = []
    page = 1
    while True:
        products, has_next = search_page(keyword, page=page)
        all_products.extend(products)
        if not has_next:
            break
        if max_pages and page >= max_pages:
            break
        page += 1
        time.sleep(delay)
    return all_products

# ── Product detail API ────────────────────────────────────────────────────────
def fetch_product_detail(product_id: int | str) -> dict | None:
    """Returns parsed product detail with exact per-size inventory."""
    url = f"https://www.myntra.com/gateway/v2/product/{product_id}"
    data = get(url, referer=f"https://www.myntra.com/product/{product_id}/buy")
    if not data:
        return None
    return parse_product_detail(data.get("style", {}))

# ── Parsers ───────────────────────────────────────────────────────────────────
def parse_listing_product(p: dict, position: int) -> dict | None:
    product_id = p.get("productId")
    if not product_id:
        return None

    # Per-SKU inventory from listing (simplified integers)
    inv_info   = p.get("inventoryInfo", [])
    total_inv  = sum(sku.get("inventory", 0) for sku in inv_info if sku.get("available"))
    sizes_avail = [sku["label"] for sku in inv_info if sku.get("available")]

    # Catalog date (epoch ms → ISO)
    catalog_ts = p.get("catalogDate", "")
    catalog_date = ""
    if catalog_ts:
        try:
            catalog_date = datetime.fromtimestamp(int(catalog_ts) / 1000).strftime("%Y-%m-%d")
        except Exception:
            pass

    is_ad = bool(p.get("isPLA")) or bool(p.get("adId"))

    return {
        "product_id":    str(product_id),
        "name":          p.get("productName") or p.get("product", ""),
        "brand":         p.get("brand", "Unknown"),
        "category":      p.get("category", ""),
        "article_type":  (p.get("articleType") or {}).get("typeName", ""),
        "gender":        p.get("gender", ""),
        "mrp":           float(p.get("mrp") or 0),
        "price":         float(p.get("price") or 0),
        "discount_pct":  _calc_discount(p.get("mrp"), p.get("price")),
        "rating":        float(p.get("rating") or 0),
        "rating_count":  int(p.get("ratingCount") or 0),
        "total_inventory": total_inv,
        "sizes_available": ",".join(sizes_avail),
        "is_ad":         is_ad,
        "catalog_date":  catalog_date,
        "position":      position,
    }

def parse_product_detail(style: dict) -> dict:
    product_id = style.get("id", "")

    # Exact per-size inventory: sum availableCount across all sellers for each size
    sizes = style.get("sizes", [])
    size_inventory = {}
    total_exact_inv = 0
    out_of_stock_sizes = []
    for sz in sizes:
        label = sz.get("label", "")
        avail = sz.get("available", False)
        seller_data = sz.get("sizeSellerData") or []
        # Sum across all sellers for this size
        count = sum(s.get("availableCount", 0) for s in seller_data)
        size_inventory[label] = count
        total_exact_inv += count
        if not avail:
            out_of_stock_sizes.append(label)

    # Urgency signals
    urgency = {u["type"]: int(u.get("value", 0)) for u in style.get("urgency", [])}

    # Ratings
    ratings_obj = style.get("ratings", {})

    return {
        "product_id":        str(product_id),
        "total_exact_inv":   total_exact_inv,
        "size_inventory":    json.dumps(size_inventory),
        "out_of_stock_sizes": ",".join(out_of_stock_sizes),
        "is_oos":            style.get("flags", {}).get("outOfStock", False),
        "rating_count":      int(ratings_obj.get("totalCount") or 0),
        "avg_rating":        float(ratings_obj.get("averageRating") or 0),
        "reviews_count":     int(ratings_obj.get("reviewsCount") or 0),
        "urgency_purchased": urgency.get("PURCHASED", 0),
        "urgency_cart":      urgency.get("CART", 0),
        "urgency_wishlist":  urgency.get("WISHLIST", 0),
        "urgency_pdp":       urgency.get("PDP", 0),
    }

def _calc_discount(mrp, price) -> float:
    try:
        mrp, price = float(mrp), float(price)
        if mrp > price > 0:
            return round((mrp - price) / mrp * 100, 1)
    except Exception:
        pass
    return 0.0

# ── CSV helpers ───────────────────────────────────────────────────────────────
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
