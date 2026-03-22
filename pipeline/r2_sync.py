"""
pipeline/r2_sync.py
===================
Upload / download unified CSVs via the Cloudflare R2 Worker API.

The Worker (r2_storage_script.js) exposes:
  PUT  /<filename>   — upload a file (body = file bytes)
  GET  /usage        — storage usage stats
  DELETE /<filename> — delete a file

Auth: X-API-Key header.

Required env vars (set in .env locally, GitHub Secrets in CI):
  R2_WORKER_URL   — e.g. https://your-worker.your-account.workers.dev
  R2_API_KEY      — the API key set in the Worker script

Usage:
  python pipeline/r2_sync.py --upload   --data-dir data
  python pipeline/r2_sync.py --usage
"""

import os, sys, argparse
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

UPLOAD_FILES = [
    "unified_snapshots.csv",
    "unified_estimates.csv",
    "unified_brands.csv",
]


def _config():
    url = os.environ.get("R2_WORKER_URL", "").rstrip("/")
    key = os.environ.get("R2_API_KEY", "")
    if not url or not key:
        print(
            "ERROR: R2_WORKER_URL and R2_API_KEY must be set.\n"
            "  Local: add them to your .env file\n"
            "  GitHub Actions: add them as repository Secrets"
        )
        sys.exit(1)
    return url, key


def _session():
    try:
        from curl_cffi import requests
        return requests, True
    except ImportError:
        import requests
        return requests, False


def upload_file(filename: str, local_path: str, worker_url: str, api_key: str) -> bool:
    req, is_cffi = _session()
    with open(local_path, "rb") as f:
        data = f.read()

    url     = f"{worker_url}/{filename}"
    headers = {
        "X-API-Key":       api_key,
        "Content-Type":    "text/csv",
        "Content-Length":  str(len(data)),
    }

    try:
        if is_cffi:
            r = req.put(url, data=data, headers=headers, timeout=60,
                        impersonate="chrome120")
        else:
            r = req.put(url, data=data, headers=headers, timeout=60)

        if r.status_code == 200:
            info = r.json()
            print(f"  [R2] Uploaded {filename}  "
                  f"(used {info.get('usedGB','?')} GB, "
                  f"remaining {info.get('remainingGB','?')} GB)")
            return True
        else:
            print(f"  [R2] ERROR {r.status_code} uploading {filename}: {r.text[:200]}")
            return False
    except Exception as e:
        print(f"  [R2] Exception uploading {filename}: {e}")
        return False


def upload_all(data_dir: str):
    worker_url, api_key = _config()
    print(f"Uploading to R2: {worker_url}")
    ok = 0
    for fname in UPLOAD_FILES:
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            print(f"  [skip] {fname} not found in {data_dir}")
            continue
        if upload_file(fname, path, worker_url, api_key):
            ok += 1
    print(f"Upload complete: {ok}/{len(UPLOAD_FILES)} files.")


def get_usage():
    worker_url, api_key = _config()
    req, is_cffi = _session()
    headers = {"X-API-Key": api_key}
    try:
        if is_cffi:
            r = req.get(f"{worker_url}/usage", headers=headers,
                        timeout=15, impersonate="chrome120")
        else:
            r = req.get(f"{worker_url}/usage", headers=headers, timeout=15)
        info = r.json()
        print(f"R2 Storage Usage:")
        print(f"  Used      : {info.get('usedGB','?')} GB")
        print(f"  Remaining : {info.get('remainingGB','?')} GB")
        print(f"  Limit     : {info.get('limitGB','?')} GB")
        print(f"  Percent   : {info.get('percentUsed','?')}")
    except Exception as e:
        print(f"  [R2] Error fetching usage: {e}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ROOT = Path(__file__).parent.parent
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload",   action="store_true", help="Upload unified CSVs to R2")
    parser.add_argument("--usage",    action="store_true", help="Show R2 storage usage")
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    args = parser.parse_args()

    if args.usage:
        get_usage()
    elif args.upload:
        upload_all(args.data_dir)
    else:
        parser.print_help()
