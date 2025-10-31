#!/usr/bin/env python3
import os
import requests
import shutil
import time
from urllib.parse import urlparse, unquote, quote

# --- CONFIG ---
PROVIDER = "plg-cyfronet-01.datahub.egi.eu"
SPACE_ID = "6de387b1f76aec7e8b925fd1ab92f032chacd1"
ROOT_ID   = "0000000000584FBD677569642373706163655F3664653338376231663736616563376538623932356664316162393266303332636861636431233664653338376231663736616563376538623932356664316162393266303332636861636431"
TOKEN     = os.environ.get("ONEPROVIDER_REST_ACCESS_TOKEN")
TMP_DIR   = "/tmp/onedata_upload_test"
# ----------------

def ensure_tmp_dir():
    if os.path.exists(TMP_DIR):
        shutil.rmtree(TMP_DIR, ignore_errors=True)
    os.makedirs(TMP_DIR, exist_ok=True)

def download_file(url, dest_path, stream=True, timeout=(10,3600)):
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    print(f"➡️ Downloading: {url}")
    with requests.get(url, stream=stream, timeout=timeout) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=512*1024):
                if chunk:
                    f.write(chunk)
    print(f"✅ Downloaded: {dest_path} ({os.path.getsize(dest_path)/(1024**2):.1f} MB)")
    return dest_path

def upload_to_onedata(parent_id, local_path):
    filename = os.path.basename(local_path)
    safe_name = quote(filename, safe='')
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children?name={safe_name}"
    headers = {"X-Auth-Token": TOKEN, "Content-Type": "application/octet-stream"}
    print(f"➡️ Uploading: {local_path} → {filename}")
    with open(local_path, "rb") as f:
        r = requests.post(url, headers=headers, data=f, timeout=(15,3600))
    if r.status_code == 201:
        print(f"✅ Upload successful: {filename}")
        return True
    else:
        print(f"❌ Upload failed ({r.status_code}): {r.text}")
        return False

def main():
    if not TOKEN:
        print("❌ Missing ONEPROVIDER_REST_ACCESS_TOKEN environment variable")
        return

    ensure_tmp_dir()

    # Files to test
    files = [
        ("https://zenodo.org/record/14365542/files/RNA-Seq_Reads_1.fastqsanger.gz",   "RNA-Seq_Reads_1.fastqsanger.gz"),
        ("https://zenodo.org/record/14377365/files/iedb_novel_peptide_x_hla_table-strong.tabular", "iedb_novel_peptide_x_hla_table-strong.tabular")
    ]

    for url, fname in files:
        tmp_path = os.path.join(TMP_DIR, fname)
        try:
            download_file(url, tmp_path)
        except Exception as e:
            print(f"⚠️ Download error for {fname}: {e}")
            continue

        success = upload_to_onedata(ROOT_ID, tmp_path)
        if not success:
            print(f"⚠️ Failed to upload {fname}")
        # Clean up local file after upload attempt
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    print("✅ Test script finished.")

if __name__ == "__main__":
    main()
