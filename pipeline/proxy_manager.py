"""
pipeline/proxy_manager.py
=========================
Rotating proxy pool for Myntra, Amazon, Flipkart scrapers.

Sources (in priority order):
  1. PROXY_LIST env var — comma-separated  "ip:port,ip:port,..."
  2. proxies.txt file in project root      — one proxy per line
  3. ProxyScrape free API                  — auto-fetched, refreshed when pool exhausted

Usage:
  from pipeline.proxy_manager import get_proxy, mark_failed

  proxy = get_proxy()           # {"http": "http://ip:port", "https": "http://ip:port"}
  response = session.get(url, proxies=proxy)
  if response.status_code in (403, 407, 503):
      mark_failed(proxy)
      proxy = get_proxy()       # get next one
"""

import os
import random
import threading
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

# ── Config ────────────────────────────────────────────────────────────────────

_pool: list[str] = []          # "ip:port" strings
_failed: set[str] = set()
_idx = 0
_lock = threading.Lock()
_last_refresh = 0.0


# ── Pool management ───────────────────────────────────────────────────────────


def _load_from_env_or_file() -> list[str]:
    # Env var takes priority
    raw = os.environ.get("PROXY_LIST", "")
    if raw:
        return [p.strip() for p in raw.split(",") if p.strip()]

    # proxies.txt in project root
    path = Path(__file__).parent.parent / "proxies.txt"
    if path.exists():
        return [line.strip() for line in path.read_text().splitlines() if line.strip()]

    return []


def _refresh():
    global _pool, _failed, _idx, _last_refresh
    proxies = _load_from_env_or_file()
    # Only use proxies if explicitly configured — free public proxies are too unreliable
    if not proxies:
        print("  [proxy] no PROXY_LIST or proxies.txt found — running direct (no proxy)")
    random.shuffle(proxies)
    _pool = proxies
    _failed.clear()
    _idx = 0
    _last_refresh = time.time()
    if proxies:
        print(f"  [proxy] pool loaded: {len(_pool)} proxies")


def _ensure_pool():
    global _pool
    with _lock:
        available = [p for p in _pool if p not in _failed]
        # Refresh if pool is empty, mostly dead, or stale (>1 hour)
        if not available or len(available) < 3 or (time.time() - _last_refresh) > 3600:
            _refresh()


# ── Public API ────────────────────────────────────────────────────────────────

def get_proxy() -> dict | None:
    """
    Return the next proxy as a dict ready for requests/curl_cffi.
    Returns None if no proxies are available (caller should proceed without proxy).
    """
    global _idx
    _ensure_pool()

    with _lock:
        available = [p for p in _pool if p not in _failed]
        if not available:
            return None
        proxy = available[_idx % len(available)]
        _idx += 1

    return {
        "http":  f"http://{proxy}",
        "https": f"http://{proxy}",
    }


def mark_failed(proxy: dict | None):
    """Mark a proxy as dead so it won't be reused."""
    if not proxy:
        return
    raw = proxy.get("https", "").replace("http://", "")
    if raw:
        with _lock:
            _failed.add(raw)
