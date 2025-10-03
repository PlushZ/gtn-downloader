import os
import requests
import shutil
import time

TSV_FILE = os.path.join(os.path.dirname(__file__), "files_to_download.tsv")
TMP_BASE = "/tmp"  # temp dir for partial downloads


def safe_download_http(url, dest_path, retries=1, backoff=5):
    """Download a file over HTTP with retries, writing to temp then moving into place"""
    tmp_local = os.path.join(TMP_BASE, os.path.basename(dest_path))
    os.makedirs(os.path.dirname(tmp_local), exist_ok=True)

    for attempt in range(retries):
        try:
            print(f"➡️ Downloading: {url}")
            with requests.get(url, stream=True, timeout=(5, 30)) as r:
                r.raise_for_status()

                downloaded = 0
                last_reported = 0

                with open(tmp_local, "wb") as f:
                    for chunk in r.iter_content(chunk_size=256 * 1024):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if downloaded - last_reported >= 100 * 1024 * 1024:
                                print(f"📥 {downloaded / (1024**2):.1f} MB...")
                                last_reported = downloaded

            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.move(tmp_local, dest_path)
            print(f"✅ Done: {dest_path} ({downloaded / (1024**2):.1f} MB)")
            return "Downloaded", os.path.getsize(dest_path)

        except Exception as e:
            print(f"⚠️ Error downloading {url} (attempt {attempt+1}): {e}")
            if os.path.exists(tmp_local):
                os.remove(tmp_local)
            time.sleep(backoff)

    return "Error", 0


def normalize_path(path: str) -> str:
    """Ensure all files go under /mnt/onedata/GTN data/..."""
    if path.startswith("/mnt/onedata/GTN---Material"):
        return path.replace(
            "/mnt/onedata/GTN---Material",
            "/mnt/onedata/GTN data/GTN---Material",
            1
        )
    return path


def main():
    if not os.path.exists(TSV_FILE):
        print(f"❌ Missing {TSV_FILE}")
        return

    with open(TSV_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            try:
                path, url = line.split("\t")
            except ValueError:
                print(f"⚠️ Skipping malformed line: {line}")
                continue

            # Normalize path to point inside "GTN data"
            path = normalize_path(path)

            # Check if file already exists and is non-zero
            if os.path.isfile(path) and os.path.getsize(path) > 0:
                print(f"⏩ Skipped (exists): {path}")
                continue

            status, size = safe_download_http(url, path)
            print(f"📊 {path}\t{url}\t{status}\t{size}")


if __name__ == "__main__":
    main()
