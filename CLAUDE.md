# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Project Vision (Master Plan)

**Multi-platform e-commerce market intelligence system** for India's top 4 platforms:
- **Blinkit** (grocery/instant delivery) — already built
- **Myntra** (fashion) — scraper built, needs enhancement
- **Amazon.in** — TO BUILD
- **Flipkart** — TO BUILD

**Goal:** Scrape inventory, pricing, ratings, reviews daily → store as 4 Parquet files on Google Drive → Angular dashboard with search + inventory tracking.

---

## Planned Directory Structure (Target State)

```
external_sources/
├── blinkit/                    # DONE — 9 modules
├── myntra/                     # DONE — 3 modules
├── amazon/                     # TODO
│   ├── amazon_core.py
│   ├── amazon_scraper.py
│   └── amazon_sitemap.py
├── flipkart/                   # TODO
│   ├── flipkart_core.py
│   ├── flipkart_scraper.py
│   └── flipkart_sitemap.py
├── pipeline/                   # TODO
│   ├── to_parquet.py           # CSV → Parquet converter
│   └── gdrive_upload.py        # Google Drive uploader
├── dashboard/                  # TODO — Angular 17+ app
│   ├── src/
│   │   ├── app/
│   │   │   ├── search/         # Search component
│   │   │   ├── product/        # Product detail + inventory chart
│   │   │   └── tracking/       # Inventory tracking view
│   │   └── services/
│   │       └── data.service.ts # Reads Parquet/JSON from Drive
│   └── package.json
├── data/                       # Local CSV cache (committed)
├── .github/workflows/
│   ├── blinkit_daily.yml       # TODO — 6 AM IST, all 7 locations
│   ├── myntra_daily.yml        # TODO — 6 AM IST
│   ├── amazon_daily.yml        # TODO — 6 AM IST
│   └── flipkart_daily.yml      # TODO — 6 AM IST
└── CLAUDE.md
```

---

## 4 Parquet Files Schema (Google Drive Storage)

All stored on Google Drive. Appended daily. Partitioned by `scraped_date`.

### 1. `products.parquet` — Master product catalog
| Column | Type | Notes |
|---|---|---|
| platform | str | blinkit / myntra / amazon / flipkart |
| product_id | str | Platform-specific ID |
| name | str | |
| brand | str | |
| category | str | |
| subcategory | str | |
| gender | str | Myntra only |
| url | str | Product page URL |
| image_url | str | |
| catalog_date | date | When first listed |
| scraped_at | datetime | |

### 2. `inventory.parquet` — Time-series stock snapshots
| Column | Type | Notes |
|---|---|---|
| platform | str | |
| product_id | str | |
| snapshot_at | datetime | |
| location | str | Blinkit: city name. Others: null |
| total_inventory | int | Total units across all sizes/SKUs |
| size_inventory | str | JSON: `{"S": 10, "M": 5, ...}` |
| is_oos | bool | Out of stock |
| oos_sizes | str | Comma-separated OOS sizes |
| seller_count | int | Myntra/Amazon/Flipkart |

### 3. `pricing.parquet` — Price history
| Column | Type | Notes |
|---|---|---|
| platform | str | |
| product_id | str | |
| scraped_at | datetime | |
| price | float | Selling price |
| mrp | float | MRP |
| discount_pct | float | |
| offer_tag | str | Coupon/offer text |
| location | str | Blinkit only |

### 4. `reviews.parquet` — Ratings & reviews over time
| Column | Type | Notes |
|---|---|---|
| platform | str | |
| product_id | str | |
| scraped_at | datetime | |
| avg_rating | float | |
| rating_count | int | Total ratings |
| review_count | int | Written reviews |
| rating_dist | str | JSON: `{"1":4,"2":3,"3":8,"4":7,"5":45}` |
| pdp_views | int | Myntra urgency.PDP (real-time views) |
| urgency_purchased | int | Myntra urgency.PURCHASED |
| urgency_cart | int | Myntra urgency.CART |

---

## GitHub Actions Schedule

All workflows trigger at **6 AM IST = 00:30 UTC** daily.

```yaml
on:
  schedule:
    - cron: '30 0 * * *'   # 6 AM IST
  workflow_dispatch:
```

**Required GitHub Secrets:**
- `GDRIVE_SERVICE_ACCOUNT_JSON` — Google service account credentials JSON
- `GDRIVE_FOLDER_ID` — Google Drive folder ID for parquet files
- `SUPABASE_URL`, `SUPABASE_KEY` — existing Blinkit DB (optional)

**Blinkit workflow** must loop over ALL 7 locations: `vijayawada,mumbai,bangalore,delhi,hyderabad,chennai,pune`

---

## Platform API Details

### Blinkit (DONE)
- Base: `https://blinkit.com/v1/layout/`
- Auth: location headers (lat/lon/locality) + `curl_cffi` chrome120 impersonation
- Inventory: capped at 50 (50 = "50 or more")
- All 7 locations must be scraped separately

### Myntra (DONE — myntra/)
- Search: `GET https://www.myntra.com/gateway/v2/search/{keyword}?p=1&rows=50&o=0&plaEnabled=false`
- Detail: `GET https://www.myntra.com/gateway/v2/product/{product_id}`
- Auth: warm session via homepage GET (auto-sets cookies), then `curl_cffi` chrome120
- Key fields: `inventoryInfo` (per-SKU), `ratingCount`, `isPLA` (ad flag), `urgency.PDP` (live page views — non-zero for popular products)
- `urgency.PURCHASED/CART/WISHLIST` = 0 normally, non-zero only during sales events

### Amazon.in (TODO)
- Sitemap index: `https://www.amazon.in/sitemap.xml`
- Product pages: `https://www.amazon.in/dp/{ASIN}`
- Key data locations in HTML:
  - Rating: `#acrPopover` or JSON-LD `aggregateRating`
  - Review count: `#acrCustomerReviewText`
  - Price: `#priceblock_ourprice` or `.a-price .a-offscreen`
  - BSR: `#SalesRank` or `productDetails_db_sections`
  - Stock: `#availability span` (shows "Only X left in stock")
  - ASIN: in URL or `#ASIN` hidden field
- Bot protection: aggressive. Use `curl_cffi` chrome120 + random delays 2-5s + rotate UA

### Flipkart (TODO)
- Sitemap: `https://www.flipkart.com/sitemap/sitemap_index.xml`
- Has internal JSON API similar to Myntra embedded in page (`window.__INITIAL_STATE__`)
- Search: `https://www.flipkart.com/search?q={keyword}&page={n}` — scrape JSON from `<script id="is_bot">` or embedded state
- Product detail: parse `window.__INITIAL_STATE__` from product HTML
- Key fields: `pid` (product ID), `rating`, `ratingCount`, `price`, `mrp`, stock signals

---

## Myntra Modules (myntra/)

### `myntra_core.py`
- `get_session()` — singleton curl_cffi session, auto-warms cookies on first call
- `search_page(keyword, page, rows)` → `(products, has_next)`
- `search_all_pages(keyword, max_pages, delay)` → `list[dict]`
- `fetch_product_detail(product_id)` → parsed detail with exact per-size inventory
- `parse_listing_product(p, position)` — parses search result product object
- `parse_product_detail(style)` — parses detail API response
- Constants: `RATING_CONVERSION_RATE=0.04`, `FASHION_RETURN_RATE=0.30`

### `myntra_sales_estimator.py`
```bash
python myntra/myntra_sales_estimator.py --keywords "tshirts,sneakers" --pages 3
python myntra/myntra_sales_estimator.py --keywords "tshirts" --pages 3 --detail   # exact inventory
python myntra/myntra_sales_estimator.py --from-snapshots data/myntra_snapshots.csv
```
Outputs: `myntra_snapshots.csv`, `myntra_sales_estimates.csv`, `myntra_brand_estimates.csv`

Signal weights: inventory_depletion=3.0, rating_velocity=2.0, rank_score=1.0, urgency_purchased=2.5

### `myntra_inventory_tracker.py`
```bash
python myntra/myntra_inventory_tracker.py --keywords "nike tshirts" --interval 60 --runs 6
python myntra/myntra_inventory_tracker.py --product_ids 39272406 12345678 --interval 30
```
Outputs: `myntra_inv_snapshots.csv`, `myntra_inv_sold.csv`

---

## Blinkit Modules (blinkit/)

| Module | Role |
|---|---|
| `blinkit_core.py` | Shared HTTP client, location configs, API headers, CSV I/O, response parsing |
| `blinkit_sales_estimator.py` | Main orchestrator — multi-signal estimation, brand aggregates |
| `blinkit_inventory_tracker.py` | Repeated PDP snapshots to measure stock depletion |
| `blinkit_category_scraper.py` | Paginates full category listings |
| `blinkit_keyword_tracker.py` | Search rank + brand share-of-voice |
| `blinkit_price_tracker.py` | Price change tracking + alerts |
| `blinkit_launch_detector.py` | New product detection |
| `blinkit_ad_tracker.py` | Playwright-based real sponsored badge detection |
| `db_migrator.py` | CSV → Supabase migration |

```bash
python blinkit/blinkit_sales_estimator.py --keywords "chips,protein powder" --location mumbai
python blinkit/blinkit_category_scraper.py --categories 3,4,7,8 --location delhi
```

Locations: `vijayawada`, `mumbai`, `bangalore`, `delhi`, `hyderabad`, `chennai`, `pune`
Category IDs: 3=Cold Drinks, 4=Snacks, 7=Bakery, 8=Tea/Coffee, 1=Vegetables, 2=Dairy, 5=Breakfast, 9=Atta/Rice, 12=Meat, 16=Pharma, 19=Personal Care

---

## Pipeline (TODO — pipeline/)

### `to_parquet.py`
- Reads all platform CSVs from `data/`
- Normalizes to unified schema
- Appends to 4 parquet files using `pyarrow`
- Deduplicates by `(platform, product_id, snapshot_at)`

### `gdrive_upload.py`
- Uses `google-api-python-client` with service account
- Uploads/updates 4 parquet files in a specific Drive folder
- Folder structure on Drive:
  ```
  Market Intelligence/
  ├── products.parquet
  ├── inventory.parquet
  ├── pricing.parquet
  └── reviews.parquet
  ```

```bash
pip install google-auth google-auth-httplib2 google-api-python-client pyarrow pandas
```

---

## Angular Dashboard (TODO — dashboard/)

**Stack:** Angular 17+, standalone components, Angular Material, Chart.js (ng2-charts)

**Key components:**
- `SearchComponent` — search bar, calls all 4 platforms, unified results grid
- `ProductDetailComponent` — product info, price history chart, inventory chart
- `InventoryTrackingComponent` — shows inventory over time per product/location
- `DataService` — fetches JSON/Parquet data (pre-converted to JSON for dashboard)

**Data serving approach:**
- Pipeline also exports `products.json`, `latest_inventory.json` alongside parquet
- Dashboard reads these static JSON files from a GitHub Pages or Drive public URL

```bash
cd dashboard
npm install
ng serve         # dev
ng build         # prod build → dist/
```

---

## Implementation Order (Next Steps)

1. **`pipeline/to_parquet.py`** — CSV → 4 Parquet files converter
2. **`pipeline/gdrive_upload.py`** — Google Drive uploader
3. **`.github/workflows/blinkit_daily.yml`** — Blinkit all-locations daily at 6 AM IST
4. **`.github/workflows/myntra_daily.yml`** — Myntra daily
5. **`amazon/amazon_scraper.py`** — Amazon.in scraper
6. **`flipkart/flipkart_scraper.py`** — Flipkart scraper
7. **`.github/workflows/amazon_daily.yml`** — Amazon daily
8. **`.github/workflows/flipkart_daily.yml`** — Flipkart daily
9. **`dashboard/`** — Angular app

---

## Current Working State

- `blinkit/` — fully working, runs via GitHub Actions every 2 hours
- `myntra/` — working scrapers, outputs to `data/`
  - `data/myntra_snapshots.csv` — 100 rows (2 runs: tshirts + sneakers)
  - `data/myntra_sales_estimates.csv` — estimates with rank-only confidence (needs 2nd run for velocity)
  - `data/myntra_brand_estimates.csv` — brand aggregates
- `data/` — local CSV cache committed to git
- Google Drive: NOT YET CONNECTED
- GitHub Actions for Myntra/Amazon/Flipkart: NOT YET CREATED
- Angular dashboard: NOT YET CREATED
