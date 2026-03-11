# external_sources

Blinkit market intelligence toolkit — estimates units sold per product/brand
using reverse-engineered consumer APIs. No seller portal access required.

See **[MEMORY.md](MEMORY.md)** for full technical documentation.

## Quick Start

```bash
pip install curl_cffi
python blinkit/blinkit_sales_estimator.py --keywords "chips" --location mumbai
```

## Data Pipeline

GitHub Actions runs every 2 hours → snapshots stored in `data/` → migrated to Supabase after 3 days.
