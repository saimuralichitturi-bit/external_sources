"""
pipeline/gdrive_sync.py
=======================
Upload / download files from Google Drive.

Supports two auth modes:
  1. OAuth2 (personal Gmail) — recommended for personal accounts
  2. Service account (Google Workspace Shared Drives only)

── First-time OAuth2 setup ─────────────────────────────────────────────────
  1. Go to https://console.cloud.google.com/
  2. Create a project → Enable Google Drive API
  3. OAuth consent screen → External → add your Gmail as test user
  4. Credentials → Create → OAuth client ID → Desktop app → Download JSON
  5. Save the downloaded file as  client_secrets.json  in the project root
  6. Run once to authenticate:
       python pipeline/gdrive_sync.py --auth
     A browser tab opens → sign in → token.json is saved automatically
  7. All future runs (including Task Scheduler) use token.json silently

── Usage ───────────────────────────────────────────────────────────────────
  python pipeline/gdrive_sync.py --upload   --data-dir data --folder-id ID
  python pipeline/gdrive_sync.py --download --data-dir data --folder-id ID
  python pipeline/gdrive_sync.py --full     --data-dir data --folder-id ID
"""

import os, sys, json, argparse
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

ROOT        = Path(__file__).parent.parent
TOKEN_FILE  = ROOT / "token.json"
CLIENT_FILE = ROOT / "client_secrets.json"
SCOPES      = ["https://www.googleapis.com/auth/drive"]

UPLOAD_FILES = [
    "unified_snapshots.csv",
    "unified_estimates.csv",
    "unified_brands.csv",
]


# ── Credentials ───────────────────────────────────────────────────────────────

def _get_oauth2_credentials():
    """Load stored OAuth2 user credentials, refreshing if expired."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    creds = None

    # Option A: token JSON from env var (for GitHub Actions)
    token_raw = os.environ.get("GOOGLE_TOKEN_JSON", "")
    if token_raw:
        creds = Credentials.from_authorized_user_info(json.loads(token_raw), SCOPES)

    # Option B: token.json file (local / Task Scheduler)
    elif TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        TOKEN_FILE.write_text(creds.to_json())

    return creds if (creds and creds.valid) else None


def _run_oauth2_flow():
    """Open browser for first-time OAuth2 consent and save token.json."""
    from google_auth_oauthlib.flow import InstalledAppFlow

    if not CLIENT_FILE.exists():
        print(
            "ERROR: client_secrets.json not found.\n"
            "  1. Go to https://console.cloud.google.com/\n"
            "  2. APIs & Services > Credentials > Create OAuth client ID (Desktop app)\n"
            "  3. Download JSON and save as client_secrets.json in the project root."
        )
        sys.exit(1)

    flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_FILE), SCOPES)
    creds = flow.run_local_server(port=0)
    TOKEN_FILE.write_text(creds.to_json())
    print(f"Auth complete. token.json saved to {TOKEN_FILE}")
    return creds


def _get_credentials():
    """
    Credential resolution order:
      1. OAuth2 token (personal Gmail) — token.json or GOOGLE_TOKEN_JSON env var
      2. Service account — GOOGLE_CREDENTIALS_JSON env var or credentials.json
    """
    # Try OAuth2 first
    creds = _get_oauth2_credentials()
    if creds:
        return creds

    # Fall back to service account
    from google.oauth2 import service_account
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if raw:
        info = json.loads(raw)
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

    for fname in ["credentials.json", "service_account.json"]:
        path = ROOT / fname
        if path.exists():
            return service_account.Credentials.from_service_account_file(
                str(path), scopes=SCOPES
            )

    print(
        "ERROR: No credentials found.\n"
        "  For personal Gmail: run  python pipeline/gdrive_sync.py --auth\n"
        "  For service account: set GOOGLE_CREDENTIALS_JSON env var."
    )
    sys.exit(1)


def _get_drive_service():
    from googleapiclient.discovery import build
    return build("drive", "v3", credentials=_get_credentials())


# ── Drive helpers ─────────────────────────────────────────────────────────────

def _find_file_id(service, folder_id: str, filename: str) -> str | None:
    resp = service.files().list(
        q=f"name='{filename}' and '{folder_id}' in parents and trashed=false",
        fields="files(id, name)",
        spaces="drive",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def upload_file(service, local_path: str, folder_id: str, mimetype="text/csv") -> str:
    from googleapiclient.http import MediaFileUpload

    filename = os.path.basename(local_path)
    media    = MediaFileUpload(local_path, mimetype=mimetype, resumable=False)
    existing = _find_file_id(service, folder_id, filename)

    if existing:
        service.files().update(
            fileId=existing,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        print(f"  [Drive] Updated  {filename}")
        return existing
    else:
        meta = {"name": filename, "parents": [folder_id]}
        f = service.files().create(
            body=meta,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        print(f"  [Drive] Uploaded {filename} (id={f['id']})")
        return f["id"]


def download_file(service, folder_id: str, filename: str, dest_path: str) -> bool:
    import io
    from googleapiclient.http import MediaIoBaseDownload

    fid = _find_file_id(service, folder_id, filename)
    if not fid:
        print(f"  [Drive] Not found: {filename}")
        return False

    buf = io.BytesIO()
    dl  = MediaIoBaseDownload(buf, service.files().get_media(fileId=fid))
    done = False
    while not done:
        _, done = dl.next_chunk()

    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
    Path(dest_path).write_bytes(buf.getvalue())
    print(f"  [Drive] Downloaded {filename} -> {dest_path}")
    return True


def upload_all(data_dir: str, folder_id: str):
    service = _get_drive_service()
    print(f"Uploading to Drive folder: {folder_id}")
    for fname in UPLOAD_FILES:
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            print(f"  [skip] {fname} not found")
            continue
        upload_file(service, path, folder_id)


def download_all(data_dir: str, folder_id: str):
    service = _get_drive_service()
    print(f"Downloading from Drive folder: {folder_id}")
    os.makedirs(data_dir, exist_ok=True)
    for fname in UPLOAD_FILES:
        download_file(service, folder_id, fname, os.path.join(data_dir, fname))


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--auth",      action="store_true", help="Run OAuth2 browser flow (first-time setup)")
    parser.add_argument("--upload",    action="store_true", help="Upload unified CSVs to Drive")
    parser.add_argument("--download",  action="store_true", help="Download unified CSVs from Drive")
    parser.add_argument("--full",      action="store_true", help="Consolidate + upload")
    parser.add_argument("--data-dir",  default=str(ROOT / "data"))
    parser.add_argument("--folder-id", default=os.environ.get("GDRIVE_FOLDER_ID", ""))
    args = parser.parse_args()

    if args.auth:
        _run_oauth2_flow()
        sys.exit(0)

    if not args.folder_id:
        print("ERROR: Provide --folder-id or set GDRIVE_FOLDER_ID env var")
        sys.exit(1)

    if args.full or args.upload:
        if args.full:
            sys.path.insert(0, str(ROOT))
            from pipeline.consolidate import run as consolidate
            consolidate(args.data_dir, args.data_dir)
        upload_all(args.data_dir, args.folder_id)

    if args.download:
        download_all(args.data_dir, args.folder_id)

    if not any([args.auth, args.upload, args.download, args.full]):
        parser.print_help()
