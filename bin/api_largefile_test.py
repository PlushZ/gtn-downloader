#!/usr/bin/env python3
import os
import requests
import time

# --- CONFIG ---
PROVIDER = "plg-cyfronet-01.datahub.egi.eu"
SPACE_ID = "6de387b1f76aec7e8b925fd1ab92f032chacd1"
ROOT_ID = "0000000000584FBD677569642373706163655F3664653338376231663736616563376538623932356664316162393266303332636861636431233664653338376231663736616563376538623932356664316162393266303332636861636431"
TOKEN = os.environ.get("ONEPROVIDER_REST_ACCESS_TOKEN")
TMP_DIR = "/tmp/gtn-large-test"
URL = "https://zenodo.org/api/files/ed51565b-3e53-4636-8410-2adf4414a36e/GSM461179.fastqsanger"
# ----------------


def create_directory(parent_id, name):
    """Create folder in Onedata (idempotent)."""
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children?name={name}&type=DIR"
    headers = {"X-Auth-Token": TOKEN}
    r = requests.post(url, headers=headers)
    if r.status_code == 201:
        print(f"📁 Created dir: {name}")
        return r.json()["fileId"]
    elif r.status_code == 400 and "eexist" in r.text:
        print(f"ℹ️ Directory exists: {name}")
        # get existing folder id
        get_url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children"
        g = requests.get(get_url, headers=headers)
        for c in g.json().get("children", []):
            if c.get("name") == name:
                return c.get("fileId")
    else:
        print(f"❌ Failed to create dir '{name}': {r.status_code} - {r.text}")
        return None


def download_large_file(url, local_path):
    """Stream download with progress output."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    print(f"⬇️ Downloading: {url}")
    with requests.get(url, stream=True, timeout=(10, 120)) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        downloaded = 0
        last_mb = 0
        start = time.time()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=4 * 1024 * 1024):  # 4 MB
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded - last_mb >= 100 * 1024 * 1024:
                        print(f"📥 {downloaded / (1024**2):.1f} MB of {total / (1024**2):.1f} MB")
                        last_mb = downloaded
        print(f"✅ Download complete ({downloaded / (1024**2):.1f} MB) in {time.time()-start:.1f}s")
    return local_path


def upload_to_onedata(parent_id, local_path):
    """Upload a large file to Onedata (streamed POST)."""
    dest_name = os.path.basename(local_path)
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children?name={dest_name}"
    headers = {"X-Auth-Token": TOKEN, "Content-Type": "application/octet-stream"}

    print(f"📤 Uploading {dest_name} to Onedata...")
    start = time.time()
    with open(local_path, "rb") as f:
        r = requests.post(url, headers=headers, data=f)
    dur = time.time() - start

    if r.status_code == 201:
        print(f"✅ Upload successful ({os.path.getsize(local_path)/(1024**2):.1f} MB, {dur:.1f}s)")
    else:
        print(f"❌ Upload failed: {r.status_code} - {r.text}")


def main():
    if not TOKEN:
        print("❌ Missing ONEPROVIDER_REST_ACCESS_TOKEN")
        return

    os.makedirs(TMP_DIR, exist_ok=True)

    print("🔍 Preparing sandbox folder: test-ci-folder")
    sandbox_id = create_directory(ROOT_ID, "test-ci-folder")
    if not sandbox_id:
        print("❌ Cannot access test-ci-folder.")
        return

    filename = os.path.basename(URL)
    local_path = os.path.join(TMP_DIR, filename)

    try:
        download_large_file(URL, local_path)
        upload_to_onedata(sandbox_id, local_path)
    except Exception as e:
        print(f"⚠️ Error: {e}")


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"⏱️ Total run time: {time.time() - start:.1f}s")
