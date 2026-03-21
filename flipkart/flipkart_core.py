"""
flipkart_core.py — Shared utilities for all Flipkart scrapers

Parsing strategy:
  Flipkart embeds ALL product data in window.__INITIAL_STATE__ JSON inside
  every page (search + PDP). This is more stable than HTML parsing.

  Search page: __INITIAL_STATE__.pageDataV4.page.data.{10002, 10006} → product slots
  PDP page:    __INITIAL_STATE__.pageDataV4.page.data.{10002} → product details
               OR legacy: window._initialData__ or __NEXT_DATA__

Key JSON paths on search:
  slot.widget.data.products[*].productInfo.value → pid, title, price, rating, stock
  slot.widget.data.products[*].productInfo.value.spinningWheelData → inventory signals

Key JSON paths on PDP:
  pageDataV4.page.data.10002.widgets[*].data → product block
  pid in URL: /p/{pid} or productId field
"""

import re, csv, os, time, random, json
from datetime import datetime

try:
    from curl_cffi import requests as cf_requests
    CURL_OK = True
except ImportError:
    import requests as cf_requests
    CURL_OK = False

# ── Session ────────────────────────────────────────────────────────────────────
_session = None

def get_session():
    global _session
    if _session is None:
        if CURL_OK:
            _session = cf_requests.Session(impersonate="chrome120")
        else:
            import requests
            _session = requests.Session()
        _session.get("https://www.flipkart.com/", headers=_headers(), timeout=15)
        time.sleep(random.uniform(1.5, 2.5))
    return _session

def _headers(referer="https://www.flipkart.com/"):
    return {
        "Accept":           "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":  "en-IN,en-US;q=0.9,en;q=0.8",
        "Referer":          referer,
        "DNT":              "1",
    }

def get_html(url: str, referer="https://www.flipkart.com/", timeout=20) -> str | None:
    try:
        r = get_session().get(url, headers=_headers(referer), timeout=timeout)
        if r.status_code == 200:
            return r.text
        if r.status_code == 503:
            print(f"  [503] blocked, backing off 10s")
            time.sleep(10)
        else:
            print(f"  HTTP {r.status_code}: {url}")
    except Exception as e:
        print(f"  HTTP error: {e}")
    return None

def delay(lo=2.0, hi=5.0):
    time.sleep(random.uniform(lo, hi))


# ── URL / PID helpers ──────────────────────────────────────────────────────────

def extract_pid(url: str) -> str | None:
    """Extract Flipkart product ID from URL.  /p/itm... or pid= query param."""
    m = re.search(r'pid=([A-Z0-9]+)', url)
    if m:
        return m.group(1)
    m = re.search(r'/p/([A-Za-z0-9]+)', url)
    return m.group(1) if m else None

def pid_url(pid: str) -> str:
    return f"https://www.flipkart.com/product/p/itemnull?pid={pid}"


# ── Embedded JSON extraction ───────────────────────────────────────────────────

def extract_initial_state(html: str) -> dict:
    """
    Pull page state JSON from Flipkart HTML. Tries multiple embed strategies:
    1. window.__INITIAL_STATE__ (search pages)
    2. window.__PRELOADED_STATE__ (some PDP pages)
    3. <script type="application/json"> largest block (PDP pages)
    4. __NEXT_DATA__ script tag
    Returns parsed dict or {}.
    """
    # Strategy 1 & 2: named window variables
    for var in ["__INITIAL_STATE__", "__PRELOADED_STATE__", "__redux_store__"]:
        m = re.search(
            rf'window\.{re.escape(var)}\s*=\s*(\{{.*?\}});\s*(?:window|</script>)',
            html, re.DOTALL
        )
        if not m:
            m = re.search(
                rf'window\.{re.escape(var)}\s*=\s*(\{{.*\}})',
                html, re.DOTALL
            )
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass

    # Strategy 3: largest <script type="application/json"> block
    best_blob = {}
    best_len  = 0
    for m in re.finditer(
        r'<script[^>]+type="application/json"[^>]*>(.*?)</script>', html, re.DOTALL
    ):
        raw = m.group(1).strip()
        if len(raw) > best_len:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and len(raw) > best_len:
                    best_blob = parsed
                    best_len  = len(raw)
            except json.JSONDecodeError:
                pass
    if best_blob:
        return best_blob

    # Strategy 4: __NEXT_DATA__
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1).strip())
        except json.JSONDecodeError:
            pass

    return {}


def _extract_pdp_json_direct(html: str) -> dict:
    """
    Flipkart PDP: scan ALL <script> blocks for known product fields.
    Returns the largest parseable dict that contains 'finalPrice' or 'sellerName'.
    """
    best = {}
    for m in re.finditer(r'<script[^>]*>(.*?)</script>', html, re.DOTALL):
        raw = m.group(1).strip()
        if len(raw) < 100:
            continue
        if '"finalPrice"' not in raw and '"sellerName"' not in raw and '"productId"' not in raw:
            continue
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and len(raw) > len(str(best)):
                best = parsed
        except json.JSONDecodeError:
            pass
    return best


def _walk_slots(state: dict) -> list[dict]:
    """Walk pageDataV4 → page → data → slots to find product widgets.

    Flipkart structure (2024+):
      state["pageDataV4"]["page"]["data"] = {"10003": [slot_0, slot_1, ...], ...}
      Each slot_N is a dict; some have {"widget": {"data": {"products": [...]}}}
    """
    results = []
    try:
        data = state["pageDataV4"]["page"]["data"]
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    # v is a list of slot dicts — extend (flatten one level)
                    for item in v:
                        if isinstance(item, dict):
                            results.append(item)
                elif isinstance(v, dict):
                    results.append(v)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    results.append(item)
    except (KeyError, TypeError):
        pass
    return results


def _deep_find(obj, *keys):
    """Recursively search nested dicts/lists for a key from `keys`."""
    if isinstance(obj, dict):
        for k in keys:
            if k in obj:
                return obj[k]
        for v in obj.values():
            result = _deep_find(v, *keys)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _deep_find(item, *keys)
            if result is not None:
                return result
    return None


# ── Raw value parsers ──────────────────────────────────────────────────────────

def _price(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(str(val).replace(',', '').replace('₹', '').strip())
    except (ValueError, TypeError):
        return 0.0

def _int(val, default=0) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

def _rating(val) -> float:
    try:
        return round(float(val), 1)
    except (ValueError, TypeError):
        return 0.0

def _rating_count(text) -> int:
    if not text:
        return 0
    s = str(text).lower().strip()
    if 'lakh' in s:
        try:
            return int(float(re.sub(r'[^\d.]', '', s.split('lakh')[0])) * 100000)
        except ValueError:
            return 0
    m = re.search(r'([\d.]+)\s*k', s)
    if m:
        try:
            return int(float(m.group(1)) * 1000)
        except ValueError:
            pass
    try:
        return int(re.sub(r'[^\d]', '', s))
    except ValueError:
        return 0

def _bought_past_month(html: str) -> int:
    """Parse 'X+ bought in past month' if Flipkart shows it."""
    m = re.search(
        r'([\d,]+(?:\.\d+)?[kK]?)\+?\s*(?:sold|bought)\s+in\s+(?:the\s+)?past\s+month',
        html, re.IGNORECASE
    )
    if m:
        return _rating_count(m.group(1))
    return 0


# ── Search page parser ─────────────────────────────────────────────────────────

def _extract_products_from_slot(slot) -> list[dict]:
    """Try to pull product list from a single page slot."""
    products = []
    if not isinstance(slot, dict):
        return products

    # Common paths Flipkart uses for product listing slots
    for path in [
        ["widget", "data", "products"],
        ["data", "products"],
        ["products"],
    ]:
        node = slot
        for key in path:
            if isinstance(node, dict):
                node = node.get(key)
            else:
                node = None
                break
        if isinstance(node, list) and node:
            products = node
            break
    return products


def parse_search_page(html: str, keyword: str) -> list[dict]:
    """
    Parse Flipkart search results.
    Primary: window.__INITIAL_STATE__ JSON.
    Fallback: HTML card regex.
    """
    state = extract_initial_state(html)
    products = []
    seen = set()
    position = 1

    if state:
        slots = _walk_slots(state)
        for slot in slots:
            for prod_node in _extract_products_from_slot(slot):
                item = _parse_search_product_node(prod_node, keyword, position)
                if item and item["pid"] not in seen:
                    products.append(item)
                    seen.add(item["pid"])
                    position += 1

    # Fallback: HTML regex for pid and basic fields
    if not products:
        for m in re.finditer(r'data-id="([^"]+)"', html):
            pid = m.group(1)
            if pid in seen:
                continue
            block_start = max(0, m.start() - 200)
            block_end   = min(len(html), m.start() + 3000)
            block = html[block_start:block_end]

            title = ""
            tm = re.search(r'<div class="[^"]*KzDlHZ[^"]*"[^>]*>([^<]+)<', block)
            if tm:
                title = tm.group(1).strip()

            price = 0.0
            pm = re.search(r'₹([\d,]+)', block)
            if pm:
                price = _price(pm.group(1))

            rating = 0.0
            rm = re.search(r'([\d.]+)\s*★', block)
            if rm:
                rating = _rating(rm.group(1))

            if title or price:
                products.append({
                    "pid":          pid,
                    "url":          f"https://www.flipkart.com/product/p/itemnull?pid={pid}",
                    "title":        title,
                    "brand":        "",
                    "category":     "",
                    "image_url":    "",
                    "price":        price,
                    "mrp":          price,
                    "discount_pct": 0.0,
                    "offer_tag":    "",
                    "avg_rating":   rating,
                    "rating_count": 0,
                    "review_count": 0,
                    "rating_dist":  "",
                    "stock_count":  None,
                    "is_oos":       False,
                    "seller_name":  "",
                    "seller_count": 0,
                    "bought_past_month": 0,
                    "is_ad":        False,
                    "position":     position,
                    "keyword":      keyword,
                    "scraped_at":   now_str(),
                })
                seen.add(pid)
                position += 1

    return products


def _parse_search_product_node(node: dict, keyword: str, position: int) -> dict | None:
    """Parse one product node from __INITIAL_STATE__ search results (2024+ schema)."""
    pi   = node.get("productInfo", {})
    info = pi.get("value", {})           # productInfo.value
    params = pi.get("action", {}).get("params", {})

    # PID — prefer info.id (same as params.productId), fallback to params
    pid = info.get("id") or params.get("productId") or info.get("pid") or ""
    if not pid:
        return None

    # Titles — productInfo.value.titles
    titles_obj = info.get("titles", {}) if isinstance(info.get("titles"), dict) else {}
    title = (titles_obj.get("title") or titles_obj.get("newTitle") or
             info.get("title") or info.get("name") or "")
    brand = (titles_obj.get("superTitle") or info.get("brand") or
             info.get("brandName") or "")

    # Price — productInfo.value.pricing.prices
    price, mrp = 0.0, 0.0
    pricing = info.get("pricing", {})
    if isinstance(pricing, dict):
        prices_list = pricing.get("prices") or []
        for pr in prices_list:
            if isinstance(pr, dict):
                if pr.get("strikeOff"):
                    mrp = _price(pr.get("value", 0))
                else:
                    price = _price(pr.get("value", 0))
        if not price:
            price = _price(pricing.get("finalPrice") or pricing.get("price") or 0)
        if not mrp:
            mrp = _price(pricing.get("mrp") or pricing.get("originalPrice") or 0)
    if not mrp:
        mrp = price
    discount_pct = round((mrp - price) / mrp * 100, 1) if mrp > price > 0 else 0.0

    # Rating — productInfo.value.rating
    rating_obj   = info.get("rating") or {}
    if not isinstance(rating_obj, dict):
        rating_obj = {}
    avg_rating   = _rating(rating_obj.get("average") or 0)
    rating_count = _rating_count(str(rating_obj.get("count") or
                                     rating_obj.get("roundOffCount") or 0))
    review_count = _int(rating_obj.get("reviewCount") or 0)

    # Stock — productInfo.value.availability
    avail = info.get("availability") or {}
    is_oos = (avail.get("displayState", "").upper() not in ("IN_STOCK", "AVAILABLE")
              and avail.get("displayState") is not None
              and avail.get("displayState", "") != "")

    # Category — productInfo.value.analyticsData
    analytics = info.get("analyticsData") or {}
    category  = analytics.get("subCategory") or analytics.get("category") or ""

    # Image — productInfo.value.media.images[0].url
    image_url = ""
    media = info.get("media") or {}
    if isinstance(media, dict):
        imgs = media.get("images") or []
        if imgs and isinstance(imgs[0], dict):
            raw_url = imgs[0].get("url", "")
            # Replace Flipkart CDN template vars with default size
            image_url = raw_url.replace("{@width}", "416").replace("{@height}", "416")

    # URL — productInfo.value.baseUrl (already starts with /)
    base_url = info.get("baseUrl") or ""
    url = f"https://www.flipkart.com{base_url}" if base_url else \
          f"https://www.flipkart.com/product/p/itemnull?pid={pid}"

    is_ad = "adInfo" in node

    return {
        "pid":          pid,
        "url":          url,
        "title":        title,
        "brand":        brand,
        "category":     category,
        "image_url":    image_url,
        "price":        price,
        "mrp":          mrp,
        "discount_pct": discount_pct,
        "offer_tag":    "",
        "avg_rating":   avg_rating,
        "rating_count": rating_count,
        "review_count": review_count,
        "rating_dist":  "",
        "stock_count":  None,
        "is_oos":       is_oos,
        "seller_name":  "",
        "seller_count": 0,
        "bought_past_month": 0,
        "is_ad":        is_ad,
        "position":     position,
        "keyword":      keyword,
        "scraped_at":   now_str(),
    }


# ── PDP parser ─────────────────────────────────────────────────────────────────

def parse_pdp(html: str, pid: str) -> dict | None:
    """
    Parse Flipkart product detail page.
    Layer 1: window.__INITIAL_STATE__ / __PRELOADED_STATE__ JSON (deep search).
    Layer 2: application/json script blocks (direct JSON scan).
    Layer 3: JSON-LD Product schema.
    Layer 4: HTML regex with current + legacy Flipkart class names.
    """
    p = {
        "pid":          pid,
        "url":          pid_url(pid),
        "title":        "",
        "brand":        "",
        "category":     "",
        "image_url":    "",
        "price":        0.0,
        "mrp":          0.0,
        "discount_pct": 0.0,
        "offer_tag":    "",
        "avg_rating":   0.0,
        "rating_count": 0,
        "review_count": 0,
        "rating_dist":  "",
        "stock_count":  None,
        "is_oos":       False,
        "seller_name":  "",
        "seller_count": 0,
        "bought_past_month": 0,
        "scraped_at":   now_str(),
    }

    # Layer 1: __INITIAL_STATE__ / __PRELOADED_STATE__
    state = extract_initial_state(html)
    if state:
        _parse_pdp_from_state(state, p)

    # Layer 2: direct JSON scan for PDP-specific fields
    if not p["price"] or not p["seller_name"]:
        direct = _extract_pdp_json_direct(html)
        if direct:
            _fill_from_raw_json(direct, p)

    # Layer 3: JSON-LD
    for m in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                         html, re.DOTALL):
        try:
            blob = json.loads(m.group(1).strip())
            items = blob if isinstance(blob, list) else [blob]
            for item in items:
                if not isinstance(item, dict) or item.get("@type") != "Product":
                    continue
                if not p["title"]:
                    p["title"] = item.get("name", "")
                if not p["brand"]:
                    b = item.get("brand", {})
                    p["brand"] = b.get("name", "") if isinstance(b, dict) else str(b or "")
                if not p["image_url"]:
                    img = item.get("image", "")
                    p["image_url"] = img[0] if isinstance(img, list) and img else str(img or "")
                agg = item.get("aggregateRating", {})
                if isinstance(agg, dict) and not p["avg_rating"]:
                    p["avg_rating"]   = _rating(agg.get("ratingValue", 0))
                    p["rating_count"] = _int(agg.get("ratingCount", 0))
                    p["review_count"] = _int(agg.get("reviewCount", 0))
                offers = item.get("offers", {})
                if isinstance(offers, list) and offers:
                    offers = offers[0]
                if isinstance(offers, dict) and not p["price"]:
                    p["price"] = _price(offers.get("price", 0))
                    avail = offers.get("availability", "")
                    if avail:
                        p["is_oos"] = "OutOfStock" in avail or "Discontinued" in avail
        except (json.JSONDecodeError, Exception):
            continue

    # Layer 4: HTML regex — current Flipkart class names (2024-25) + legacy fallbacks
    # Title: VU-ZEz (current), yhB1nd, B_NuCI (legacy)
    if not p["title"]:
        for cls in [r'VU-ZEz', r'yhB1nd', r'B_NuCI', r'_35KyD6']:
            m = re.search(rf'class="[^"]*{cls}[^"]*"[^>]*>([^<]{{10,300}})<', html)
            if m:
                p["title"] = m.group(1).strip()
                break

    # Price: Nx9bqj (current), _30jeq3 (legacy)
    if not p["price"]:
        for cls in [r'Nx9bqj', r'_30jeq3', r'_16Jk6d']:
            m = re.search(rf'class="[^"]*{cls}[^"]*"[^>]*>₹([\d,]+)<', html)
            if m:
                p["price"] = _price(m.group(1))
                break
    if not p["price"]:
        m = re.search(r'"finalPrice"\s*:\s*([\d.]+)', html)
        if m:
            p["price"] = float(m.group(1))

    # MRP: yRaY8j (current), _3I9_wc (legacy)
    if not p["mrp"]:
        for cls in [r'yRaY8j', r'_3I9_wc', r'_3auTQe']:
            m = re.search(rf'class="[^"]*{cls}[^"]*"[^>]*>₹([\d,]+)<', html)
            if m:
                p["mrp"] = _price(m.group(1))
                break
    if not p["mrp"]:
        m = re.search(r'"mrp"\s*:\s*([\d.]+)', html)
        if m:
            p["mrp"] = float(m.group(1))

    # Rating: XQDdHH (current), _3LWZlK (legacy)
    if not p["avg_rating"]:
        for cls in [r'XQDdHH', r'_3LWZlK', r'ipqd2Y']:
            m = re.search(rf'class="[^"]*{cls}[^"]*"[^>]*>([\d.]+)<', html)
            if m:
                p["avg_rating"] = _rating(m.group(1))
                break

    # Rating count
    if not p["rating_count"]:
        m = re.search(r'([\d,]+(?:\s*(?:lakh|k))?)\s+Ratings?', html, re.IGNORECASE)
        if m:
            p["rating_count"] = _rating_count(m.group(1))
    if not p["rating_count"]:
        m = re.search(r'"ratingCount"\s*:\s*(\d+)', html)
        if m:
            p["rating_count"] = int(m.group(1))

    # Review count
    if not p["review_count"]:
        m = re.search(r'([\d,]+)\s+Reviews?', html, re.IGNORECASE)
        if m:
            p["review_count"] = _int(re.sub(r'[^\d]', '', m.group(1)))
    if not p["review_count"]:
        m = re.search(r'"reviewCount"\s*:\s*(\d+)', html)
        if m:
            p["review_count"] = int(m.group(1))

    # OOS
    if not p["is_oos"]:
        p["is_oos"] = bool(re.search(
            r'Currently Unavailable|Out of Stock|Sold Out|notify me', html, re.IGNORECASE
        ))

    # Stock count — Flipkart shows these urgency messages for low-stock items
    if p["stock_count"] is None:
        for stock_pat in [
            r'[Oo]nly\s+(\d+)\s+left',                          # "Only 3 left"
            r'[Hh]urry[!,]?\s+[Oo]nly\s+(\d+)',                 # "Hurry! Only 2 left"
            r'(\d+)\s+[Ll]eft[^a-zA-Z]',                        # "5 left"
            r'"limitedStockCount"\s*:\s*(\d+)',                  # JSON in page
            r'"availableCount"\s*:\s*(\d+)',
            r'"remainingCount"\s*:\s*(\d+)',
            r'"remainingStock"\s*:\s*(\d+)',
        ]:
            m = re.search(stock_pat, html)
            if m:
                try:
                    p["stock_count"] = int(m.group(1))
                    break
                except (ValueError, TypeError):
                    pass

    # Seller — multiple Flipkart HTML patterns (class names change between designs)
    if not p["seller_name"]:
        for pat in [
            # Broadest: "Sold by" followed by one or more HTML tags then text
            r'Sold\s+by\s*(?:<[^>]+>)+([^<]{2,80})<',
            # Direct text after "Sold by" (no tags)
            r'Sold\s+by\s+([A-Za-z0-9 &.,\'-]{3,60})(?:<|[\r\n])',
            # JSON keys
            r'"sellerName"\s*:\s*"([^"]{2,80})"',
            r'"sellerDisplayName"\s*:\s*"([^"]{2,80})"',
            r'"merchantName"\s*:\s*"([^"]{2,80})"',
            # Legacy class-based
            r'class="[^"]*_2jiOoL[^"]*"[^>]*>([^<]{2,80})<',
        ]:
            m = re.search(pat, html, re.IGNORECASE | re.DOTALL)
            if m:
                candidate = m.group(1).strip()
                if _is_valid_str(candidate, min_len=2, max_len=80):
                    p["seller_name"] = candidate
                    break

    # Seller count from "X more sellers"
    if not p["seller_count"]:
        m = re.search(r'(\d+)\s+(?:more\s+)?sellers?', html, re.IGNORECASE)
        if m:
            p["seller_count"] = int(m.group(1)) + 1

    # Brand fallback
    if not p["brand"]:
        m = re.search(r'"brand"\s*:\s*"([^"]{2,60})"', html)
        if m:
            p["brand"] = m.group(1)

    # Bought past month
    p["bought_past_month"] = _bought_past_month(html)

    # Discount
    if p["mrp"] > p["price"] > 0:
        p["discount_pct"] = round((p["mrp"] - p["price"]) / p["mrp"] * 100, 1)
    if not p["mrp"] and p["price"]:
        p["mrp"] = p["price"]

    return p


def _is_valid_str(v, min_len=2, max_len=300) -> bool:
    """Return True if v is a non-empty string that doesn't look like CSS/hex/ID."""
    if not isinstance(v, str):
        return False
    v = v.strip()
    if not (min_len <= len(v) <= max_len):
        return False
    # Reject CSS colors (#RRGGBB), hex IDs, pure digits, URLs
    if re.match(r'^#[0-9A-Fa-f]{3,8}$', v):
        return False
    if re.match(r'^[0-9A-Fa-f]{8,}$', v):
        return False
    if re.match(r'^https?://', v):
        return False
    return True


def _fill_from_raw_json(obj: dict, p: dict):
    """Fill missing PDP fields using deep key search in any JSON blob."""
    if not p["price"]:
        v = _deep_find(obj, "finalPrice", "sellingPrice")
        if v and not isinstance(v, (dict, list)):
            p["price"] = _price(v)
    if not p["mrp"]:
        v = _deep_find(obj, "mrp", "maximumRetailPrice", "originalPrice")
        if v and not isinstance(v, (dict, list)):
            p["mrp"] = _price(v)
    if not p["title"]:
        # Prefer "productTitle" — "title" often contains category/page names
        v = _deep_find(obj, "productTitle")
        if not _is_valid_str(v, min_len=15):
            v = _deep_find(obj, "title")
        if _is_valid_str(v, min_len=15):  # Real product titles are at least 15 chars
            p["title"] = v.strip()
    if not p["brand"]:
        v = _deep_find(obj, "brandName")   # prefer brandName over "brand" to avoid CSS matches
        if not v:
            v = _deep_find(obj, "brand")
        if _is_valid_str(v, min_len=2, max_len=60):
            p["brand"] = v.strip()
    if not p["avg_rating"]:
        for rk in ["averageRating", "ratingValue"]:
            v = _deep_find(obj, rk)
            if v and not isinstance(v, (dict, list)):
                rv = _rating(v)
                # Valid Flipkart ratings are 1.0–5.0 and typically have decimals
                if 1.5 <= rv <= 5.0:
                    p["avg_rating"] = rv
                    break
    if not p["rating_count"]:
        v = _deep_find(obj, "ratingCount", "totalRatings", "ratingsCount")
        if v:
            p["rating_count"] = _int(v)
    if not p["seller_name"]:
        v = _deep_find(obj, "sellerName", "sellerDisplayName", "merchantName")
        if _is_valid_str(v, min_len=2, max_len=80):
            p["seller_name"] = v.strip()
    # limitedStockCount — Flipkart sets this when stock is low
    if p.get("stock_count") is None:
        v = _deep_find(obj, "limitedStockCount", "availableCount", "stockCount",
                       "remainingCount", "remainingStock")
        if v is not None:
            try:
                count = int(v)
                if count > 0:
                    p["stock_count"] = count
            except (TypeError, ValueError):
                pass


def _parse_pdp_from_state(state: dict, p: dict):
    """Fill p in-place from __INITIAL_STATE__ PDP data using deep field search."""
    # Use _deep_find to locate known fields regardless of nesting
    _fill_from_raw_json(state, p)

    # Also walk slots for structured product info
    slots = _walk_slots(state)
    for slot in slots:
        for path in [
            ["widget", "data", "products"],
            ["data", "products"],
            ["widget", "data"],
            ["data"],
        ]:
            node = slot
            for key in path:
                node = node.get(key) if isinstance(node, dict) else None
                if node is None:
                    break

            if isinstance(node, list) and node:
                node = node[0]
            if not isinstance(node, dict):
                continue

            # Try productInfo.value → productInfo → node directly
            info = node
            for sub in [["productInfo", "value"], ["productInfo"], []]:
                n = node
                for key in sub:
                    n = n.get(key, {}) if isinstance(n, dict) else {}
                if n and isinstance(n, dict) and (n.get("pid") or n.get("title")):
                    info = n
                    break

            if not info.get("pid") and not info.get("title"):
                continue

            if not p["title"]:
                p["title"] = info.get("title") or ""
            if not p["brand"]:
                p["brand"] = info.get("brand") or info.get("brandName") or ""
            if not p["category"]:
                p["category"] = info.get("category") or ""

            price_obj = info.get("pricing") or info.get("priceInfo") or {}
            if price_obj and not p["price"]:
                p["price"] = _price(price_obj.get("finalPrice") or price_obj.get("price") or 0)
                p["mrp"]   = _price(price_obj.get("mrp") or price_obj.get("originalPrice") or p["price"])

            rating_obj = info.get("rating") or info.get("ratingDetails") or {}
            if rating_obj and not p["avg_rating"]:
                p["avg_rating"]   = _rating(rating_obj.get("average") or rating_obj.get("rating") or 0)
                p["rating_count"] = _rating_count(str(
                    rating_obj.get("count") or rating_obj.get("ratingCount") or 0))
                p["review_count"] = _int(rating_obj.get("reviewCount") or 0)

            stock_obj = info.get("stockInfo") or info.get("availability") or {}
            if stock_obj and not p["is_oos"]:
                p["is_oos"] = not bool(stock_obj.get("available", True))
                # limitedStockCount is set by Flipkart for near-OOS items
                lsc = (stock_obj.get("limitedStockCount") or
                       stock_obj.get("availableCount") or
                       stock_obj.get("remainingCount"))
                if lsc is not None and p["stock_count"] is None:
                    try:
                        p["stock_count"] = int(lsc)
                    except (TypeError, ValueError):
                        pass

            imgs = info.get("images") or []
            if isinstance(imgs, list) and imgs and not p["image_url"]:
                p["image_url"] = imgs[0] if isinstance(imgs[0], str) else ""

            seller_info = info.get("sellerInfo") or {}
            if seller_info and not p["seller_name"]:
                p["seller_name"]  = seller_info.get("sellerName") or seller_info.get("name") or ""
                p["seller_count"] = _int(seller_info.get("otherSellers", 0)) + 1

            dist_obj = info.get("ratingHistogram") or {}
            if dist_obj and not p["rating_dist"]:
                p["rating_dist"] = json.dumps({str(k): v for k, v in dist_obj.items()})

            return


# ── Fetch helpers ──────────────────────────────────────────────────────────────

def fetch_search_page(keyword: str, page: int = 1) -> tuple[list[dict], bool]:
    kw  = keyword.replace(' ', '+')
    url = f"https://www.flipkart.com/search?q={kw}" + (f"&page={page}" if page > 1 else "")
    html = get_html(url, referer=f"https://www.flipkart.com/search?q={kw}")
    if not html:
        return [], False
    products = parse_search_page(html, keyword)
    has_next = bool(re.search(r'class="[^"]*_1LKTO3[^"]*"[^>]*>Next<', html) or
                    re.search(r'page=' + str(page + 1), html))
    return products, has_next

def fetch_search_all_pages(keyword: str, max_pages: int = 5) -> list[dict]:
    all_products = []
    for page in range(1, max_pages + 1):
        print(f"  '{keyword}' page {page}...", end=" ", flush=True)
        products, has_next = fetch_search_page(keyword, page)
        print(f"{len(products)} products")
        all_products.extend(products)
        if not has_next or not products:
            break
        delay(2.0, 4.5)
    return all_products

def fetch_aod_stock(pid: str) -> dict:
    """
    Flipkart pincode-based stock check — unauthenticated XHR.
    Uses a major metro pincode (110001 = New Delhi) to check delivery availability.

    Returns dict:
      {
        "general_stock":  bool,   # product available somewhere in FK network
        "pincode_stock":  bool,   # deliverable to checked pincode
        "stock_count":    int | None,  # limitedStockCount if exposed
      }
    """
    # Common major pincodes to try (Delhi, Mumbai, Bangalore)
    PINCODES = ["110001", "400001", "560001"]
    result = {"general_stock": None, "pincode_stock": None, "stock_count": None}

    for pincode in PINCODES:
        try:
            url = f"https://www.flipkart.com/api/3/page/fetch"
            product_url = f"/product/p/itemnull?pid={pid}&pincode={pincode}"
            r = get_session().get(
                url,
                params={"url": product_url, "pincode": pincode},
                headers={**_headers(), "X-Requested-With": "XMLHttpRequest",
                          "Accept": "application/json"},
                timeout=15,
            )
            if r.status_code != 200:
                continue
            data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            if not data:
                continue

            # Parse known fields
            gs = _deep_find(data, "general_stock", "inStock", "available")
            ps = _deep_find(data, "pincode_stock", "deliverable", "pincodeDeliverable")
            lsc = _deep_find(data, "limitedStockCount", "availableCount", "remainingCount")

            if gs is not None:
                result["general_stock"] = bool(gs)
            if ps is not None:
                result["pincode_stock"] = bool(ps)
            if lsc is not None:
                try:
                    result["stock_count"] = int(lsc)
                except (TypeError, ValueError):
                    pass
            break  # got a valid response
        except Exception:
            continue

    return result


def fetch_pdp(pid: str) -> dict | None:
    html = get_html(pid_url(pid))
    if not html:
        return None
    p = parse_pdp(html, pid)
    if p is None:
        return None

    # Try pincode stock check for additional signals
    if p["stock_count"] is None:
        aod = fetch_aod_stock(pid)
        if aod["stock_count"] is not None:
            p["stock_count"] = aod["stock_count"]
        # Update is_oos from pincode check if not already set
        if not p["is_oos"] and aod["general_stock"] is False:
            p["is_oos"] = True

    return p


# ── CSV helpers ────────────────────────────────────────────────────────────────

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
