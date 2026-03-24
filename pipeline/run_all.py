"""
pipeline/run_all.py
===================
Single entry point for the daily scrape pipeline.
Run directly on the Oracle VM via cron — no GitHub Actions needed.

Cron setup (6 AM IST = 00:30 UTC):
  crontab -e
  30 0 * * * cd /home/ubuntu/external_sources && python3 pipeline/run_all.py >> /home/ubuntu/logs/daily_pipeline.log 2>&1

Env vars required (add to /home/ubuntu/.env or export in crontab):
  R2_WORKER_URL
  R2_API_KEY
"""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

ROOT = Path(__file__).parent.parent


def run(cmd: list[str], **kwargs) -> subprocess.Popen:
    """Start a subprocess, inherit env so R2 creds are available."""
    return subprocess.Popen(cmd, cwd=ROOT, env=os.environ.copy(), **kwargs)


def main():
    start = datetime.now()
    print(f"\n{'='*60}")
    print(f"Daily Pipeline — {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    procs = []

    # Blinkit — all categories, all 7 locations in parallel
    for loc in ["vijayawada", "mumbai", "bangalore", "delhi", "hyderabad", "chennai", "pune"]:
        procs.append(run([
            sys.executable, "blinkit/blinkit_category_scraper.py",
            "--all", "--location", loc,
        ]))
        print(f"  [started] blinkit @ {loc}")

    # Myntra — all fashion categories
    procs.append(run([
        sys.executable, "myntra/myntra_sales_estimator.py",
        "--all-categories", "--out-dir", "data",
    ]))
    print(f"  [started] myntra all-categories")

    # Amazon — all categories
    procs.append(run([
        sys.executable, "amazon/amazon_scraper.py",
        "--all-categories", "--out-dir", "data",
    ]))
    print(f"  [started] amazon all-categories")

    # Flipkart — all categories
    procs.append(run([
        sys.executable, "flipkart/flipkart_scraper.py",
        "--all-categories", "--out-dir", "data",
    ]))
    print(f"  [started] flipkart all-categories")

    print(f"\n  Waiting for {len(procs)} jobs to finish...\n")

    for p in procs:
        p.wait()

    # Upload to R2
    print("\n  Uploading to Cloudflare R2...")
    result = subprocess.run(
        [sys.executable, "pipeline/r2_sync.py", "--upload", "--data-dir", "data"],
        cwd=ROOT, env=os.environ.copy(),
    )

    elapsed = (datetime.now() - start).seconds // 60
    print(f"\n{'='*60}")
    print(f"✅ Done in {elapsed} min | R2 exit={result.returncode}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
