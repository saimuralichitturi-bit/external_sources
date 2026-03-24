"""
amazon_core.py — Shared utilities for all Amazon.in scrapers

Parsing strategy per field:
  1. Try embedded JSON blobs Amazon puts in every page (more stable than HTML)
  2. Fall back to HTML regex if JSON path yields nothing

Key JSON sources Amazon embeds:
  - <script type="application/ld+json">   → Product schema: title, brand, rating, image, price
  - twister-js-init-dpx-data              → buybox price, availability, variants
  - aod-desktop-cache                     → all seller offers (seller name, FBA, count)
  - "asinList" in script                  → ordered ASIN list for search results
"""

import re, csv, os, time, random, json
from datetime import datetime

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from curl_cffi import requests as cf_requests
    CURL_OK = True
except ImportError:
    import requests as cf_requests
    CURL_OK = False

try:
    from pipeline.proxy_manager import get_proxy, mark_failed as mark_proxy_failed
    PROXY_OK = True
except ImportError:
    PROXY_OK = False
    def get_proxy(): return None
    def mark_proxy_failed(_): pass

# ── Session ───────────────────────────────────────────────────────────────────
_session = None

def get_session():
    global _session
    if _session is None:
        if CURL_OK:
            _session = cf_requests.Session(impersonate="chrome120")
        else:
            import requests
            _session = requests.Session()
        _session.get("https://www.amazon.in/", headers=_headers(), timeout=15)
        time.sleep(random.uniform(1.5, 2.5))
    return _session

def _headers(referer="https://www.amazon.in/"):
    return {
        "Accept":           "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":  "en-IN,en-US;q=0.9,en;q=0.8",
        "Referer":          referer,
        "DNT":              "1",
    }

def get_html(url: str, referer="https://www.amazon.in/", timeout=20):
    for attempt in range(1, 4):
        proxy = get_proxy()
        try:
            r = get_session().get(url, headers=_headers(referer), timeout=timeout, proxies=proxy)
            if r.status_code == 200:
                return r.text
            mark_proxy_failed(proxy)
            if r.status_code in (503, 403):
                print(f"  [{r.status_code}] blocked (attempt {attempt}/3), rotating proxy...")
                time.sleep(3 * attempt)
            else:
                print(f"  HTTP {r.status_code}: {url}")
                return None
        except Exception as e:
            mark_proxy_failed(proxy)
            print(f"  HTTP error (attempt {attempt}/3): {e}")
            time.sleep(3 * attempt)
    return None

def delay(lo=2.0, hi=5.0):
    time.sleep(random.uniform(lo, hi))


# ── AOD (All Offers Display) stock probe ──────────────────────────────────────

def fetch_aod_stock(asin: str) -> dict:
    """
    Fetch seller/offer signals from the public offer-listing page.

    URL: /gp/offer-listing/{ASIN}?ie=UTF8&condition=new
    This page is publicly accessible (no AJAX auth needed) and shows:
      - All sellers with prices and FBA flags
      - "Only N left in stock" low-stock warnings
      - Number of offers

    Note: The old /gp/aod/ajax endpoint no longer responds (404 as of 2024).
    The offer-listing page is the reliable replacement.

    Returns dict:
      {
        "stock_count":    int | None,   # only if low-stock message found
        "seller_count":   int,          # number of offer rows visible
        "aod_price":      float,        # lowest offer price seen
        "has_fba":        bool,         # at least one Prime/FBA offer
        "is_oos":         bool,         # "unavailable" text detected
      }
    """
    url = f"https://www.amazon.in/gp/offer-listing/{asin}?ie=UTF8&condition=new"
    headers = _headers(referer=f"https://www.amazon.in/dp/{asin}")

    result = {
        "stock_count":  None,
        "seller_count": 0,
        "aod_price":    0.0,
        "has_fba":      False,
        "is_oos":       False,
    }

    try:
        r = get_session().get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            return result
        html = r.text
    except Exception:
        return result

    if not html or len(html) < 500:
        return result

    # Low-stock count: "Only N left in stock", "Only N left", "Hurry — only N left"
    m = re.search(r'[Oo]nly\s+(\d+)\s+left', html)
    if m:
        result["stock_count"] = int(m.group(1))

    # OOS
    if re.search(r'currently unavailable|out of stock|no offers', html, re.IGNORECASE):
        result["is_oos"] = True

    # Seller count — count offer rows on the listing page
    # Amazon offer-listing uses olpOffer or a-row olp-listing patterns
    seller_hits = re.findall(
        r'class="[^"]*(?:olpOffer|olp-listing|a-row[^"]*offer)[^"]*"', html
    )
    if not seller_hits:
        # fallback: count seller name links
        seller_hits = re.findall(r'class="[^"]*olpSellerName[^"]*"', html)
    result["seller_count"] = max(len(seller_hits), result["seller_count"])

    # Also check for seller count stated in text: "X offers from Rs."
    m = re.search(r'(\d+)\s+(?:new\s+)?(?:offers?|results?)', html, re.IGNORECASE)
    if m and result["seller_count"] == 0:
        result["seller_count"] = int(m.group(1))

    # FBA / Prime badge
    result["has_fba"] = bool(
        re.search(r'Prime|Fulfilled by Amazon|Ships from.*?Amazon', html, re.IGNORECASE)
    )

    # Lowest price across all visible offers
    vals = re.findall(r'[^\d](\d{2,6}(?:\.\d{1,2})?)\s*(?:<|\n)', html)
    price_candidates = []
    for raw in re.findall(r'class="a-offscreen">\u20b9([\d,]+(?:\.\d+)?)<', html):
        price_candidates.append(_price(raw))
    # Also try olpOfferPrice spans
    for raw in re.findall(r'olpOfferPrice[^>]*>\s*\u20b9\s*([\d,]+(?:\.\d+)?)', html):
        price_candidates.append(_price(raw))
    prices = [p for p in price_candidates if p > 0]
    if prices:
        result["aod_price"] = min(prices)

    return result


# ── ASIN helpers ──────────────────────────────────────────────────────────────
def extract_asin(url: str):
    m = re.search(r'/dp/([A-Z0-9]{10})', url)
    return m.group(1) if m else None

def asin_url(asin: str) -> str:
    return f"https://www.amazon.in/dp/{asin}"

# ── JSON extraction ───────────────────────────────────────────────────────────

def extract_json_blobs(html: str) -> dict:
    """
    Pull named JSON blobs Amazon embeds in the page.
    Returns { blob_name: parsed_dict }.
    """
    blobs = {}

    # 1. JSON-LD blocks
    for i, m in enumerate(re.finditer(
        r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
        html, re.DOTALL
    )):
        try:
            blobs[f"jsonld_{i}"] = json.loads(m.group(1).strip())
        except json.JSONDecodeError:
            pass

    # 2. twister price/availability data — try multiple Amazon variable names
    for tw_pat in [
        r'var\s+dataToReturn\s*=\s*(\{.*?\});\s*\n',
        r'"twister-js-init-dpx-data"[^>]*>\s*(\{.*?\})\s*</script>',
        r'P\.when\(["\']twister["\'][^)]*\)\s*\.execute\s*\(\s*function[^{]*\{[^{]*(\{.*?"availability".*?\})',
    ]:
        m = re.search(tw_pat, html, re.DOTALL)
        if m:
            try:
                blobs["twister_data"] = json.loads(m.group(1))
                break
            except json.JSONDecodeError:
                pass

    # 3. AOD (all offers display) cache — Amazon sometimes embeds offer data here
    m = re.search(r'id="aod-desktop-cache"[^>]*>([^<]{10,})</div>', html)
    if m:
        try:
            blobs["aod"] = json.loads(m.group(1).strip())
        except json.JSONDecodeError:
            pass
    # Also try aod-pinned-offer block
    if "aod" not in blobs:
        m = re.search(r'id="aod-pinned-offer"[^>]*>(.*?)</div>', html, re.DOTALL)
        if m:
            inner = m.group(1)
            # Extract individual fields from inner HTML
            aod_data = {}
            sm = re.search(r'id="sellerProfileTriggerId"[^>]*>([^<]+)<', inner)
            if sm:
                aod_data["sellerName"] = sm.group(1).strip()
            pm = re.search(r'a-offscreen">\u20b9([\d,]+(?:\.\d+)?)<', inner)
            if pm:
                aod_data["price"] = _price(pm.group(1))
            aod_data["fba"] = bool(re.search(r'Fulfilled by Amazon|Prime', inner, re.IGNORECASE))
            if aod_data:
                blobs["aod"] = aod_data

    return blobs


def find_jsonld_product(blobs: dict) -> dict:
    """Return the Product schema.org blob, or {}."""
    for key, blob in blobs.items():
        if not key.startswith("jsonld_"):
            continue
        if isinstance(blob, dict) and blob.get("@type") == "Product":
            return blob
        if isinstance(blob, list):
            for item in blob:
                if isinstance(item, dict) and item.get("@type") == "Product":
                    return item
    return {}


# ── Raw value parsers ─────────────────────────────────────────────────────────

def _price(text) -> float:
    if not text:
        return 0.0
    try:
        return float(re.sub(r'[^\d.]', '', str(text).replace(',', '')))
    except ValueError:
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
    # Handle "1K+", "2.5k", etc.
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
    """Parse 'X+ bought in past month' Amazon badge — closest to a sold count."""
    m = re.search(
        r'([\d,]+(?:\.\d+)?[kK]?)\+?\s*(?:bought|purchased)\s+in\s+(?:the\s+)?past\s+month',
        html, re.IGNORECASE
    )
    if not m:
        # newer format: data attribute or aria-label
        m = re.search(r'boughtInPastMonth[^>]*>\s*([\d,]+[kK+]*)\s*\+?\s*bought', html, re.IGNORECASE)
    if m:
        return _rating_count(m.group(1))
    return 0

def _rating(text) -> float:
    if not text:
        return 0.0
    m = re.search(r'(\d+\.?\d*)\s*out of', str(text))
    if m:
        return float(m.group(1))
    try:
        return float(re.search(r'[\d.]+', str(text)).group())
    except (AttributeError, ValueError):
        return 0.0

def _bsr(text) -> int | None:
    if not text:
        return None
    m = re.search(r'#?([\d,]+)', str(text))
    try:
        return int(m.group(1).replace(',', '')) if m else None
    except (ValueError, AttributeError):
        return None


# ── PDP parser ────────────────────────────────────────────────────────────────

def parse_pdp(html: str, asin: str) -> dict:
    """
    Parse a product detail page. JSON-first, HTML fallback per field.
    Returns all fields needed for products + pricing + reviews + inventory parquet tables.
    """
    p = {
        "asin":               asin,
        "url":                asin_url(asin),
        "title":              "",
        "brand":              "",
        "category":           "",
        "image_url":          "",
        "price":              0.0,
        "mrp":                0.0,
        "discount_pct":       0.0,
        "offer_tag":          "",
        "avg_rating":         0.0,
        "rating_count":       0,
        "review_count":       0,
        "rating_dist":        "",
        "bsr":                None,
        "bsr_category":       "",
        "stock_count":        None,
        "is_oos":             False,
        "seller_name":        "",
        "seller_count":       0,
        "is_amazon_sold":     False,
        "fulfilled_by_amazon": False,
        "bought_past_month":  0,
        "scraped_at":         now_str(),
    }

    blobs  = extract_json_blobs(html)
    jsonld = find_jsonld_product(blobs)

    # ── Title ─────────────────────────────────────────────────────────────────
    p["title"] = jsonld.get("name", "")
    if not p["title"]:
        m = re.search(r'id="productTitle"[^>]*>\s*(.*?)\s*</span>', html, re.DOTALL)
        if m:
            p["title"] = re.sub(r'\s+', ' ', m.group(1)).strip()

    # ── Brand ─────────────────────────────────────────────────────────────────
    _SKIP_BRANDS = {"brand", "visit the", "store", "n/a", "generic", ""}

    def _clean_brand(s: str) -> str:
        """Strip Amazon store page noise from brand strings."""
        s = (s or "").strip()
        # "Visit the Yogabar Store" -> "Yogabar"
        m2 = re.match(r'Visit\s+the\s+(.+?)\s+(?:Store|Brand|Page|Shop)\s*$', s, re.IGNORECASE)
        if m2:
            s = m2.group(1).strip()
        # "Brand: Yogabar" -> "Yogabar"
        m2 = re.match(r'Brand:\s*(.+)', s, re.IGNORECASE)
        if m2:
            s = m2.group(1).strip()
        return s

    def _valid_brand(s):
        s = _clean_brand(s or "")
        return bool(s) and s.lower() not in _SKIP_BRANDS and len(s) < 80

    # 1. JSON-LD
    brand_obj = jsonld.get("brand", {})
    raw_brand = brand_obj.get("name", "") if isinstance(brand_obj, dict) else str(brand_obj or "")
    raw_brand = _clean_brand(raw_brand)
    if _valid_brand(raw_brand):
        p["brand"] = raw_brand

    # 2. Most reliable: product details table (po-brand row → po-break-word value)
    if not p["brand"]:
        m = re.search(
            r'class="[^"]*po-brand[^"]*".*?class="[^"]*po-break-word[^"]*">([^<]{2,60})<',
            html, re.DOTALL
        )
        if m:
            cleaned = _clean_brand(m.group(1))
            if _valid_brand(cleaned):
                p["brand"] = cleaned

    # 3. bylineInfo — Amazon brand store link
    if not p["brand"]:
        for brand_pat in [
            r'id="bylineInfo"[^>]*>.*?(?:Visit the|Brand:)\s*<[^>]*>([^<]{2,60})<',
            r'<a[^>]*id="bylineInfo"[^>]*>([^<]{2,60})<',
            r'id="bylineInfo"[^>]*>.*?<a[^>]*>([^<]{2,60})</a>',
        ]:
            m = re.search(brand_pat, html, re.DOTALL)
            if m:
                cleaned = _clean_brand(m.group(1))
                if _valid_brand(cleaned):
                    p["brand"] = cleaned
                    break

    # 4. Hidden brand input
    if not p["brand"]:
        m = re.search(r'id="brand"[^>]*value="([^"]+)"', html)
        if m:
            cleaned = _clean_brand(m.group(1))
            if _valid_brand(cleaned):
                p["brand"] = cleaned

    # 5. JSON key fallback
    if not p["brand"]:
        m = re.search(r'"brand"\s*:\s*"([^"]{2,60})"', html)
        if m:
            cleaned = _clean_brand(m.group(1))
            if _valid_brand(cleaned):
                p["brand"] = cleaned

    # ── Rating ────────────────────────────────────────────────────────────────
    agg = jsonld.get("aggregateRating", {})
    if agg:
        p["avg_rating"]   = float(agg.get("ratingValue", 0) or 0)
        p["rating_count"] = int(agg.get("ratingCount", 0) or 0)
        p["review_count"] = int(agg.get("reviewCount", 0) or 0)
    if not p["avg_rating"]:
        m = re.search(r'id="acrPopover"[^>]*title="([^"]+)"', html)
        if m:
            p["avg_rating"] = _rating(m.group(1))
    if not p["rating_count"]:
        m = re.search(r'id="acrCustomerReviewText"[^>]*>([^<]+)<', html)
        if m:
            p["rating_count"] = _rating_count(m.group(1))
    if not p["review_count"]:
        m = re.search(r'"reviewCount"\s*:\s*"?(\d+)"?', html)
        if m:
            p["review_count"] = int(m.group(1))

    # Rating distribution (HTML only — no JSON source)
    dist = {}
    for star in range(1, 6):
        m = re.search(rf'{star} star.*?(\d+)%', html, re.DOTALL)
        if m:
            dist[str(star)] = int(m.group(1))
    if dist:
        p["rating_dist"] = json.dumps(dist)

    # ── Price ─────────────────────────────────────────────────────────────────
    # twister blob first
    tw = blobs.get("twister_data", {})
    if tw:
        p["price"] = _price(tw.get("priceAmount") or tw.get("price") or tw.get("buyingPrice") or "")

    # JSON-LD offers
    if not p["price"]:
        offers = jsonld.get("offers", {})
        if isinstance(offers, list) and offers:
            offers = offers[0]
        if isinstance(offers, dict):
            p["price"] = _price(offers.get("price", ""))

    # HTML fallback
    if not p["price"]:
        vals = re.findall(r'class="a-offscreen">₹([\d,]+(?:\.\d+)?)<', html)
        prices = [_price(v) for v in vals if _price(v) > 0]
        if prices:
            p["price"] = min(prices)
    if not p["price"]:
        m = re.search(r'id="priceblock_ourprice"[^>]*>.*?₹([\d,]+)', html, re.DOTALL)
        if m:
            p["price"] = _price(m.group(1))

    # ── MRP ───────────────────────────────────────────────────────────────────
    if not p["mrp"]:
        offers = jsonld.get("offers", {})
        if isinstance(offers, dict):
            p["mrp"] = _price(offers.get("highPrice", ""))
    if not p["mrp"]:
        m = re.search(r'M\.R\.P\.[^₹]*₹([\d,]+(?:\.\d+)?)', html)
        if m:
            p["mrp"] = _price(m.group(1))
    if not p["mrp"]:
        m = re.search(r'id="listPrice"[^>]*>.*?₹([\d,]+)', html, re.DOTALL)
        if m:
            p["mrp"] = _price(m.group(1))
    if not p["mrp"] and p["price"]:
        p["mrp"] = p["price"]

    if p["mrp"] > p["price"] > 0:
        p["discount_pct"] = round((p["mrp"] - p["price"]) / p["mrp"] * 100, 1)

    # Coupon/offer tag
    m = re.search(r'couponBadge[^>]*>([^<]+)<', html)
    if m:
        p["offer_tag"] = m.group(1).strip()

    # ── Image ─────────────────────────────────────────────────────────────────
    img = jsonld.get("image", "")
    if isinstance(img, list) and img:
        img = img[0]
    p["image_url"] = img or ""
    if not p["image_url"]:
        m = re.search(r'id="imgTagWrapperId".*?data-a-dynamic-image="({[^"]+})"', html, re.DOTALL)
        if m:
            try:
                img_map = json.loads(m.group(1).replace('&quot;', '"'))
                if img_map:
                    best = max(img_map.items(),
                               key=lambda x: x[1][0] * x[1][1]
                               if isinstance(x[1], list) and len(x[1]) >= 2 else 0)
                    p["image_url"] = best[0]
            except Exception:
                pass

    # ── Category ──────────────────────────────────────────────────────────────
    crumbs = re.findall(r'class="a-link-normal a-color-tertiary"[^>]*>([^<]+)<', html)
    if crumbs:
        p["category"] = " > ".join(c.strip() for c in crumbs[:4])

    # ── BSR (HTML only — no reliable JSON source) ─────────────────────────────
    m = re.search(r'id="SalesRank"[^>]*>.*?#([\d,]+)\s+in\s+([^<\(]+)', html, re.DOTALL)
    if m:
        p["bsr"]          = _bsr(m.group(1))
        p["bsr_category"] = m.group(2).strip()
    if not p["bsr"]:
        m = re.search(r'Best Sellers Rank.*?#([\d,]+)\s+in\s+<a[^>]*>([^<]+)<', html, re.DOTALL)
        if m:
            p["bsr"]          = _bsr(m.group(1))
            p["bsr_category"] = m.group(2).strip()

    # ── Stock ─────────────────────────────────────────────────────────────────
    # twister availability JSON first
    avail_json = tw.get("availability") or tw.get("availabilityType") or ""
    if avail_json:
        p["is_oos"] = "unavailable" in str(avail_json).lower()

    # HTML: check multiple availability containers Amazon uses
    for avail_pat in [
        r'id="availability"[^>]*>.*?<span[^>]*>([^<]+)</span>',
        r'id="outOfStock"[^>]*>.*?<span[^>]*>([^<]+)</span>',
        r'class="[^"]*a-color-price[^"]*"[^>]*>\s*(Currently unavailable[^<]*)<',
        r'availability-string[^>]*>\s*([^<]{5,80})<',
    ]:
        m = re.search(avail_pat, html, re.DOTALL)
        if m:
            avail_text = m.group(1).strip().lower()
            if not avail_json:
                p["is_oos"] = bool(re.search(r'unavailable|out of stock|not available', avail_text))
            m2 = re.search(r'only (\d+) left', avail_text)
            if m2:
                p["stock_count"] = int(m2.group(1))
            break

    # add-to-cart absent = definitely OOS
    if not p["is_oos"] and not re.search(r'id="add-to-cart-button"', html):
        if re.search(r'Currently unavailable|out of stock', html, re.IGNORECASE):
            p["is_oos"] = True

    # ── Bought in past month (sold count proxy) ───────────────────────────────
    p["bought_past_month"] = _bought_past_month(html)

    # ── Seller info ───────────────────────────────────────────────────────────
    # AOD blob (from aod-desktop-cache or aod-pinned-offer in PDP HTML)
    aod = blobs.get("aod", {})
    if aod:
        offers_list = aod.get("offers") or aod.get("allOffers") or []
        if offers_list:
            p["seller_count"] = len(offers_list)
            first = offers_list[0] if isinstance(offers_list[0], dict) else {}
            p["seller_name"]         = first.get("sellerName") or first.get("merchantName") or ""
            p["fulfilled_by_amazon"] = "amazon" in str(first.get("shipsFrom", "")).lower()
            p["is_amazon_sold"]      = "amazon" in str(p["seller_name"]).lower()
        # Also handle flat format from aod-pinned-offer fallback
        elif aod.get("sellerName"):
            p["seller_name"]         = aod["sellerName"]
            p["fulfilled_by_amazon"] = bool(aod.get("fba"))
            p["is_amazon_sold"]      = "amazon" in p["seller_name"].lower()
    # HTML fallback — multiple patterns for seller name
    if not p["seller_name"]:
        for seller_pat in [
            r'id="sellerProfileTriggerId"[^>]*>([^<]+)<',
            r'<a[^>]*id="sellerProfileTriggerId"[^>]*>([^<]+)<',
            r'Sold by[^<]*<[^>]*>([^<]{2,60})<',
            r'merchant-info[^>]*>.*?Sold by[^<]*<a[^>]*>([^<]{2,60})<',
        ]:
            m = re.search(seller_pat, html, re.DOTALL)
            if m:
                name = m.group(1).strip()
                if name and name.lower() not in ("amazon", ""):
                    p["seller_name"] = name
                    break
                elif name:
                    p["seller_name"] = name
                    break
    if not p["fulfilled_by_amazon"]:
        p["fulfilled_by_amazon"] = bool(
            re.search(r'Ships from.*?Amazon|Fulfilled by Amazon', html, re.IGNORECASE)
        )
    if not p["is_amazon_sold"]:
        p["is_amazon_sold"] = bool(re.search(r'Sold by.*?Amazon', html, re.IGNORECASE))
    if not p["seller_count"]:
        # "X new offers from Rs." or "See all X offers"
        for cnt_pat in [
            r'(\d+)\s+(?:new\s+)?(?:offers?|results?)\s+from',
            r'See\s+all\s+(\d+)\s+(?:buying\s+options|offers?)',
            r'(\d+)\s+(?:new|used).*?offer',
        ]:
            m = re.search(cnt_pat, html, re.IGNORECASE)
            if m:
                p["seller_count"] = int(m.group(1))
                break

    return p


# ── Search page parser ────────────────────────────────────────────────────────

def parse_search_page(html: str, keyword: str) -> list[dict]:
    """
    Parse a search results page. HTML card parsing — JSON path for search
    only yields an ordered ASIN list with no field data, so HTML is primary.

    Amazon HTML puts data-asin BEFORE data-component-type in the tag, so we
    scan for the opening <div> of each search card using both attribute orders.
    """
    products = []
    position = 1
    seen_asins = set()

    # Match the opening <div> tag for each search result card.
    # Amazon uses either attribute order depending on page variant.
    card_re = re.compile(
        r'<div\b[^>]*?data-asin="([A-Z0-9]{10})"[^>]*?data-component-type="s-search-result"[^>]*>'
        r'|'
        r'<div\b[^>]*?data-component-type="s-search-result"[^>]*?data-asin="([A-Z0-9]{10})"[^>]*>',
        re.DOTALL
    )

    for match in card_re.finditer(html):
        asin = match.group(1) or match.group(2)
        if not asin or asin == "0000000000" or asin in seen_asins:
            continue
        seen_asins.add(asin)

        block = html[match.start(): match.start() + 5000]

        # Title — try multiple Amazon 2024+ patterns
        title = ""
        for title_pat in [
            r'<h2[^>]*aria-label="([^"]{5,300})"',
            r'<h2[^>]*>.*?<span[^>]*>([^<]{5,300})</span>',
            r'class="[^"]*a-text-normal[^"]*"[^>]*>([^<]{5,300})<',
            r'<span[^>]*class="[^"]*a-size-medium[^"]*"[^>]*>([^<]{5,200})<',
        ]:
            m = re.search(title_pat, block, re.DOTALL)
            if m:
                title = re.sub(r'\s+', ' ', m.group(1)).strip()
                break

        # Price
        price = 0.0
        m = re.search(r'a-price-whole">([^<]+)<.*?a-price-fraction">([^<]*)<', block, re.DOTALL)
        if m:
            price = _price(m.group(1) + "." + (m.group(2) or "0"))
        if not price:
            m = re.search(r'a-offscreen">₹([\d,]+(?:\.\d+)?)<', block)
            if m:
                price = _price(m.group(1))

        # MRP
        mrp = 0.0
        m = re.search(r'a-text-price.*?a-offscreen">₹([\d,]+(?:\.\d+)?)<', block, re.DOTALL)
        if m:
            mrp = _price(m.group(1))
        if not mrp:
            mrp = price

        discount_pct = round((mrp - price) / mrp * 100, 1) if mrp > price > 0 else 0.0

        # Rating
        rating = 0.0
        m = re.search(r'([\d.]+) out of 5', block)
        if m:
            rating = float(m.group(1))

        # Rating count
        rating_count = 0
        m = re.search(r'aria-label="([\d,]+(?:\s+lakh)?)\s+ratings?"', block)
        if m:
            rating_count = _rating_count(m.group(1))

        is_ad = bool(re.search(r'Sponsored|s-sponsored-label', block, re.IGNORECASE))

        image_url = ""
        m = re.search(r's-image"[^>]*src="([^"]+)"', block)
        if m:
            image_url = m.group(1)

        if True:  # always include — filter empty rows downstream
            products.append({
                "asin":               asin,
                "url":                asin_url(asin),
                "title":              title,
                "brand":              "",
                "category":           "",
                "image_url":          image_url,
                "price":              price,
                "mrp":                mrp,
                "discount_pct":       discount_pct,
                "offer_tag":          "",
                "avg_rating":         rating,
                "rating_count":       rating_count,
                "review_count":       0,
                "rating_dist":        "",
                "bsr":                None,
                "bsr_category":       "",
                "stock_count":        None,
                "is_oos":             False,
                "seller_name":        "",
                "seller_count":       0,
                "is_amazon_sold":     False,
                "fulfilled_by_amazon": False,
                "bought_past_month":  0,
                "is_ad":              is_ad,
                "position":           position,
                "keyword":            keyword,
                "scraped_at":         now_str(),
            })
            position += 1

    # Fallback: if nothing matched (Amazon changed markup), grab /dp/ ASINs from links
    if not products:
        for m in re.finditer(r'href="/([^"]+)/dp/([A-Z0-9]{10})(?:/[^"]*)?["\?]', html):
            asin = m.group(2)
            if asin == "0000000000" or asin in seen_asins:
                continue
            seen_asins.add(asin)
            # Extract a 2000-char window around the link for field parsing
            block = html[max(0, m.start() - 500): m.start() + 2000]
            title = ""
            tm = re.search(r'<span[^>]*>([A-Z][^<]{10,120})</span>', block)
            if tm:
                title = re.sub(r'\s+', ' ', tm.group(1)).strip()
            price = 0.0
            pm = re.search(r'₹\s*([\d,]+(?:\.\d+)?)', block)
            if pm:
                price = _price(pm.group(1))
            rating = 0.0
            rm = re.search(r'([\d.]+)\s+out of\s+5', block)
            if rm:
                rating = float(rm.group(1))
            if title or price:
                products.append({
                    "asin": asin, "url": asin_url(asin), "title": title,
                    "brand": "", "category": "", "image_url": "",
                    "price": price, "mrp": price, "discount_pct": 0.0,
                    "offer_tag": "", "avg_rating": rating, "rating_count": 0,
                    "review_count": 0, "rating_dist": "", "bsr": None,
                    "bsr_category": "", "stock_count": None, "is_oos": False,
                    "seller_name": "", "seller_count": 0,
                    "is_amazon_sold": False, "fulfilled_by_amazon": False,
                    "bought_past_month": 0, "is_ad": False,
                    "position": position, "keyword": keyword, "scraped_at": now_str(),
                })
                position += 1

    return products


# ── Fetch helpers ─────────────────────────────────────────────────────────────

def fetch_search_page(keyword: str, page: int = 1) -> tuple[list[dict], bool]:
    kw  = keyword.replace(' ', '+')
    url = f"https://www.amazon.in/s?k={kw}" + (f"&page={page}" if page > 1 else "")
    html = get_html(url, referer=f"https://www.amazon.in/s?k={kw}")
    if not html:
        return [], False
    # Only flag CAPTCHA when the page is a short challenge form (no products).
    # Full pages (>100KB) that mention "captcha" in JS are NOT blocked.
    if len(html) < 100_000 and re.search(
        r'Type the characters|robot check|<form[^>]*captcha', html, re.IGNORECASE
    ):
        print("  [CAPTCHA/BLOCKED] Amazon is challenging the request -- wait 10min or use VPN")
        return [], False
    products = parse_search_page(html, keyword)
    has_next = bool(
        re.search(r'class="s-pagination-next"', html) and
        not re.search(r's-pagination-next s-pagination-disabled', html)
    )
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

def fetch_pdp(asin: str) -> dict | None:
    html = get_html(asin_url(asin))
    if not html:
        return None
    p = parse_pdp(html, asin)
    if p is None:
        return None

    # Always call AOD endpoint — it's unauthenticated and gives seller count
    # plus stock_count when Amazon shows a low-stock warning
    aod = fetch_aod_stock(asin)

    # Only override stock_count if AOD found a real number
    if aod["stock_count"] is not None:
        p["stock_count"] = aod["stock_count"]

    # Merge seller count — take the higher of PDP or AOD (AOD sees all sellers)
    if aod["seller_count"] > p.get("seller_count", 0):
        p["seller_count"] = aod["seller_count"]

    # Fill FBA flag if PDP missed it
    if not p.get("fulfilled_by_amazon") and aod["has_fba"]:
        p["fulfilled_by_amazon"] = True

    # OOS from AOD if PDP missed it
    if not p.get("is_oos") and aod["is_oos"]:
        p["is_oos"] = True

    return p


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