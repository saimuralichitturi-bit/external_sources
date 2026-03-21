"""
pipeline/gdrive_sync.py
=======================
Upload / download files from Google Drive using a service account.

Setup:
  1. Create a Google Cloud service account with Drive API enabled.
  2. Download the credentials JSON and save as credentials.json (gitignored).
  3. Share your Google Drive folder with the service account email.
  4. Set GDRIVE_FOLDER_ID in .env or pass --folder-id.

Usage:
  # Upload unified CSVs to Drive
  python pipeline/gdrive_sync.py --upload --data-dir ./data --folder-id YOUR_FOLDER_ID

  # Download latest CSVs from Drive
  python pipeline/gdrive_sync.py --download --data-dir ./data --folder-id YOUR_FOLDER_ID

  # Full pipeline: consolidate + upload
  python pipeline/gdrive_sync.py --full --data-dir ./data --folder-id YOUR_FOLDER_ID
"""

import os, sys, json, argparse
from pathlib import Path

# Load .env if present
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

# Credentials resolution order:
#   1. GOOGLE_CREDENTIALS_JSON env var (JSON string — for GitHub Actions/Streamlit secrets)
#   2. credentials.json file in project root
#   3. service_account.json file in project root
def _get_credentials():
    from google.oauth2 import service_account

    scopes = ["https://www.googleapis.com/auth/drive"]

    # Option 1: JSON string from environment (GitHub Actions secret / Streamlit Cloud)
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if raw:
        info = json.loads(raw)
        return service_account.Credentials.from_service_account_info(info, scopes=scopes)

    # Option 2: local file
    for fname in ["credentials.json", "service_account.json"]:
        path = Path(__file__).parent.parent / fname
        if path.exists():
            return service_account.Credentials.from_service_account_file(str(path), scopes=scopes)

    raise FileNotFoundError(
        "No Google credentials found. Set GOOGLE_CREDENTIALS_JSON env var "
        "or place credentials.json in the project root."
    )


def _get_drive_service():
    from googleapiclient.discovery import build
    creds = _get_credentials()
    return build("drive", "v3", credentials=creds)


def _find_file_id(service, folder_id: str, filename: str) -> str | None:
    """Find a file by name in the given Drive folder. Returns file ID or None."""
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
    """Upload or update a file on Google Drive. Returns the file ID.

    Works with both regular My Drive folders (shared with the service account)
    and Shared Drives (Team Drives). For regular My Drive, the folder must be
    shared with the service account email as Editor.
    Note: Use a Shared Drive folder to avoid service-account storage quota limits.
    """
    from googleapiclient.http import MediaFileUpload

    filename = os.path.basename(local_path)
    media = MediaFileUpload(local_path, mimetype=mimetype, resumable=False)
    existing_id = _find_file_id(service, folder_id, filename)

    if existing_id:
        # Update existing file content
        service.files().update(
            fileId=existing_id,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        print(f"  [Drive] Updated  {filename} (id={existing_id})")
        return existing_id
    else:
        # Create new file
        meta = {"name": filename, "parents": [folder_id]}
        f = service.files().create(
            body=meta,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        fid = f["id"]
        print(f"  [Drive] Uploaded {filename} (id={fid})")
        return fid


def download_file(service, folder_id: str, filename: str, dest_path: str) -> bool:
    """Download a named file from Drive into dest_path. Returns True on success."""
    import io
    from googleapiclient.http import MediaIoBaseDownload

    fid = _find_file_id(service, folder_id, filename)
    if not fid:
        print(f"  [Drive] Not found: {filename}")
        return False

    request = service.files().get_media(fileId=fid)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = dl.next_chunk()

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "wb") as f:
        f.write(buf.getvalue())
    print(f"  [Drive] Downloaded {filename} -> {dest_path}")
    return True


def upload_all(data_dir: str, folder_id: str):
    """Upload all unified CSVs from data_dir to Google Drive."""
    service = _get_drive_service()
    for fname in UPLOAD_FILES:
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            print(f"  [skip] {fname} not found in {data_dir}")
            continue
        upload_file(service, path, folder_id)


def download_all(data_dir: str, folder_id: str):
    """Download all unified CSVs from Google Drive to data_dir."""
    service = _get_drive_service()
    os.makedirs(data_dir, exist_ok=True)
    for fname in UPLOAD_FILES:
        download_file(service, folder_id, fname, os.path.join(data_dir, fname))


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload",    action="store_true", help="Upload unified CSVs to Drive")
    parser.add_argument("--download",  action="store_true", help="Download unified CSVs from Drive")
    parser.add_argument("--full",      action="store_true", help="Consolidate + upload")
    parser.add_argument("--data-dir",  default=str(Path(__file__).parent.parent / "data"))
    parser.add_argument("--folder-id", default=os.environ.get("GDRIVE_FOLDER_ID", ""))
    args = parser.parse_args()

    if not args.folder_id:
        print("ERROR: Provide --folder-id or set GDRIVE_FOLDER_ID env var")
        sys.exit(1)

    if args.full or args.upload:
        if args.full:
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from pipeline.consolidate import run as consolidate
            consolidate(args.data_dir, args.data_dir)
        print(f"\nUploading to Drive folder: {args.folder_id}")
        upload_all(args.data_dir, args.folder_id)

    if args.download:
        print(f"Downloading from Drive folder: {args.folder_id}")
        download_all(args.data_dir, args.folder_id)

    if not any([args.upload, args.download, args.full]):
        parser.print_help()
