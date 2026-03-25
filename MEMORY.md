give# MEMORY.md — Blinkit Market Intelligence Project

> Auto-maintained knowledge base. Last updated by project setup.

---

## Project Overview

Reverse-engineered Blinkit consumer API toolkit.
**Goal:** Estimate units sold per product/brand without seller portal access.
**Repo:** `github.com/saimuralichitturi-bit/external_sources`
**Owner:** saimuralichitturi-bit | Vijayawada, AP

---

## Architecture

```
GitHub Actions (cron every 2hr)
    ↓ snapshot
blinkit/ scrapers
    ↓ write CSVs
data/ directory (in repo)
    ↓ after 3 days
Supabase (persistent DB)
```

---

## Working APIs (Confirmed)

### Search API
```
POST https://blinkit.com/v1/layout/search
     ?q=<keyword>&search_type=type_to_search&offset=0&limit=24

Required headers:
  content-type: application/json
  app-version: 1000000
  web-version: 1000000
  lat: 16.5103525        ← Vijayawada
  lon: 80.6465468
  locality: 2111
```

### PDP API (product detail)
```
POST https://blinkit.com/v1/layout/product/<product_id>

Same headers + web_app_version: 1008010016, app_client: consumer_web
```

### Response Structure
```python
data["response"]["snippets"]   # list of product widgets
snippet["widget_type"]         # skip "ImageTextViewRendererTypeHeader" etc
snippet["data"]["product_id"]
snippet["data"]["atc_action"]["add_to_cart"]["cart_item"]
  → product_name, brand, unit, price, mrp, inventory, group_id
snippet["data"]["product_badges"]
  → [{"type":"OTHERS","label":"Ad",...}]  ← ad detection
  → [{"type":"ETA","label":"ETA",...}]    ← delivery time
snippet["data"]["offer_tag"]["title"]["text"]  ← "5% OFF" etc
snippet["data"]["eta_rating_data"]["rating_count"]["text"]  ← "(4,587)"
snippet["data"]["is_sold_out"]
snippet["data"]["inventory"]   ← 0-50, capped at 50 (50 = "50+")
```

---

## Sales Estimation Model

### Signal Weights
| Signal | Weight | Accuracy |
|--------|--------|----------|
| Inventory depletion rate | 3.0 | HIGH when inv < 50 |
| Rating velocity (ratings/day) | 1.5 | MEDIUM |
| Rating count (all-time) | 0.8 | LOW |
| Search rank position | 1.0 | MEDIUM |
| Category SOV % | 0.8 | LOW |

### Key Constants
```python
RATING_CONVERSION_RATE = 0.07   # 7% of buyers rate (India)
INV_CAP = 50                     # Blinkit caps at 50
ACTIVE_HOURS = 13                # 10am-11pm daily
NATIONAL_DARK_STORES = 700       # Blinkit ~700 stores nationally

DARK_STORE_MULTIPLIER = {
    "mumbai":     32.0,
    "delhi":      25.0,
    "bangalore":  18.0,
    "hyderabad":  12.0,
    "chennai":    8.0,
    "pune":       6.0,
    "vijayawada": 1.5,
}
```

### Formula
```python
# Inventory depletion → daily
rate_per_hr = (inv_drop / interval_mins) * 60
daily_from_inv = rate_per_hr * ACTIVE_HOURS

# Rating velocity → daily local
daily_from_rv = (rating_velocity_per_day / RATING_CONVERSION_RATE) / NATIONAL_DARK_STORES * multiplier

# Search rank → daily (power law)
rank_daily = 80 * exp(-0.15 * (avg_rank - 1))

# SOV → daily
sov_daily = sov_pct / 100 * 500  # 500 = category daily volume est

# Combined weighted average
daily_est = sum(value * weight) / sum(weights)
monthly_est = daily_est * 30
```

---

## Files & What They Do

### Scrapers (`blinkit/`)
| File | Purpose | Output |
|------|---------|--------|
| `blinkit_core.py` | Shared base — HTTP, parsing, CSV | — |
| `blinkit_sales_estimator.py` | **Main** — multi-signal units sold | sales_estimates.csv, brand_estimates.csv |
| `blinkit_inventory_tracker.py` | PDP snapshots, depletion | snapshots.csv, sold_estimate.csv |
| `blinkit_category_scraper.py` | Full category + brand SOV | category_products.csv, category_sov.csv |
| `blinkit_keyword_tracker.py` | Keyword rank + SOV | keyword_snapshots.csv, keyword_sov.csv |
| `blinkit_price_tracker.py` | Price history + alerts | price_history.csv, price_alerts.csv |
| `blinkit_launch_detector.py` | New product detection | launches.csv |
| `blinkit_ad_tracker.py` | Playwright real ad badges | ad_snapshots.csv, ad_sov.csv |
| `db_migrator.py` | CSV → Supabase (3-day archival) | migration_log.json |

### Data (`data/`)
All CSVs written here by GitHub Actions. Kept for 3 days then migrated to Supabase.

---

## GitHub Actions Workflow

**File:** `.github/workflows/blinkit_pipeline.yml`
**Schedule:** Every 2 hours (`0 */2 * * *`)
**DB migration:** Weekly Sunday midnight

### What each run does:
1. `blinkit_sales_estimator.py` — single snapshot + estimate from cache
2. `blinkit_keyword_tracker.py` — SOV snapshot
3. `blinkit_category_scraper.py` — category product map
4. `blinkit_price_tracker.py` — price check + alerts
5. Copy all CSVs → `data/`
6. Commit data to repo

### Secrets needed (Settings → Secrets → Actions):
- `SUPABASE_URL` — your Supabase project URL
- `SUPABASE_KEY` — Supabase anon/service key

---

## Supabase Tables

```sql
blinkit_snapshots          -- inventory snapshots per product
blinkit_sales_estimates    -- daily/monthly unit estimates
blinkit_brand_estimates    -- brand-level aggregates
blinkit_keyword_sov        -- keyword SOV per brand per run
blinkit_price_alerts       -- price change events
blinkit_launches           -- new product detections
```

---

## Known Limitations & Bugs Fixed

### Ad Detection
- **Issue:** `curl_cffi` requests don't get ad badges — API strips them for non-browser requests
- **Root cause:** Blinkit serves sponsored results only to authenticated browser sessions
- **Partial fix:** `blinkit_ad_tracker.py` uses Playwright (real Chromium) to get real badges
- **Workaround:** Rank position proxy — products at pos 1-3 with low organic relevance = likely ad

### Inventory Cap
- Blinkit shows max 50 for inventory — `inventory == 50` means "50 or more"
- When both snapshots show 50 → can't determine depletion → marked `capped_both`
- When previous=50, current<50 → minimum delta known → `capped_prev` type

### Header Widget Skip
- Search results include `ImageTextViewRendererTypeHeader` at position 1 (banner, not product)
- Fixed: skip any snippet where `widget_type` contains "Header" or "Banner"

### Session Cookie
- `gr_1_accessToken` is the auth token (not `gr_1_auth`)
- Format: `gr_1_accessToken=v2%3A%3Axxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
- Short-lived — expires within hours

---

## Location Configs

| Key | City | lat | lon | locality |
|-----|------|-----|-----|---------|
| vijayawada | Vijayawada AP | 16.5103525 | 80.6465468 | 2111 |
| mumbai | Mumbai | 19.0760 | 72.8777 | 1 |
| bangalore | Bangalore | 12.9716 | 77.5946 | 4 |
| delhi | Delhi | 28.6139 | 77.2090 | 2 |
| hyderabad | Hyderabad | 17.3850 | 78.4867 | 5 |
| chennai | Chennai | 13.0827 | 80.2707 | 3 |
| pune | Pune | 18.5204 | 73.8567 | 6 |

---

## Category IDs

| ID | Name |
|----|------|
| 1 | Vegetables & Fruits |
| 2 | Dairy, Bread & Eggs |
| 3 | Cold Drinks & Juices |
| 4 | Snacks & Munchies |
| 5 | Breakfast & Instant Food |
| 6 | Sweet Tooth |
| 7 | Bakery & Biscuits |
| 8 | Tea, Coffee & Health Drinks |
| 9 | Atta, Rice, Oil & Dals |
| 10 | Masala, Oil & More |
| 11 | Sauces & Spreads |
| 12 | Chicken, Meat & Fish |
| 16 | Pharma & Wellness |
| 19 | Personal Care & Beauty |

---

## Quick Commands

```bash
# Sales estimate — wait for 2+ runs to get depletion signal
python blinkit_sales_estimator.py --keywords "chips,protein powder" --location mumbai

# From existing snapshots (instant)
python blinkit_sales_estimator.py --from-snapshots snapshots.csv --location vijayawada

# Category SOV
python blinkit_category_scraper.py --categories 3,4 --location mumbai

# Price watch
python blinkit_price_tracker.py --products 447847,125240 --interval 60

# Launch detection (run twice — first seeds DB)
python blinkit_launch_detector.py --categories 3,4 --location mumbai

# Real ad badges (needs playwright install)
python blinkit_ad_tracker.py --keywords "chips" --location mumbai
```

---

## Setup from Scratch

```bash
# 1. Clone
git clone https://github.com/saimuralichitturi-bit/external_sources
cd external_sources/blinkit

# 2. Install
pip install curl_cffi

# 3. Test
python blinkit_sales_estimator.py --keywords "chips" --location mumbai

# 4. For Playwright ads
pip install playwright && playwright install chromium
```

---

## Cloudflare Bypass

Uses `curl_cffi` with `impersonate="chrome120"` — mimics Chrome TLS fingerprint.
Standard `requests` library gets blocked immediately.

```python
from curl_cffi import requests as cf_requests
r = cf_requests.post(url, headers=headers, impersonate="chrome120", timeout=15)
```

---

## Next Steps / Ideas

- [ ] Add Zepto API reverse engineering (same architecture as Blinkit)
- [ ] Add Swiggy Instamart API
- [ ] Build dashboard (Streamlit/Grafana) on top of Supabase data
- [ ] Add Telegram alert bot when price drops > 5% on tracked products
- [ ] Calibrate rating conversion rate with ground truth data
- [ ] Add multi-city parallel tracking (run same keyword in 5 cities simultaneously)
