#!/usr/bin/env python3
import os
import re
import yaml
import ftplib
import shutil
import requests
from urllib.parse import urlparse, unquote
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError

# ========================== CONFIG ==========================
PROVIDER = "plg-cyfronet-01.datahub.egi.eu"
SPACE_ID = "6de387b1f76aec7e8b925fd1ab92f032chacd1"
ROOT_ID = "0000000000584FBD677569642373706163655F3664653338376231663736616563376538623932356664316162393266303332636861636431233664653338376231663736616563376538623932356664316162393266303332636861636431"
TOKEN = os.environ.get("ONEPROVIDER_REST_ACCESS_TOKEN")
TMP_DIR = "/tmp/gtn-api-upload"
# ============================================================


# ---------- Utility helpers ----------

def sanitize_name(name: str) -> str:
    """Replace unsafe characters for folder names."""
    return re.sub(r'[\\/:\*,?"<>|%.#!@$&\'\(\)\[\]{} ]', '-', str(name or ""))


def sanitize_filename(name: str) -> str:
    """Replace only forbidden characters for filenames."""
    name = str(name or "")
    return re.sub(r'[^A-Za-z0-9._\-]', '-', name)



def get_safe_filename_from_url(url):
    """Extract correct filename from URL, handling Zenodo /content links."""
    parsed = urlparse(url)
    parts = parsed.path.split("/")
    if parts[-1] == "content" and len(parts) > 1:
        filename = parts[-2]
    else:
        filename = parts[-1]
    return filename


def ensure_tmp_clean():
    """Ensure /tmp folder is fresh for each CI run."""
    if os.path.exists(TMP_DIR):
        shutil.rmtree(TMP_DIR, ignore_errors=True)
    os.makedirs(TMP_DIR, exist_ok=True)


# ---------- Onedata API helpers ----------

def get_children(parent_id):
    """Return list of child entries (dicts) under given Onedata folder ID."""
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children"
    headers = {"X-Auth-Token": TOKEN}
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code == 200:
        return r.json().get("children", [])
    return []


def get_child_id(parent_id, name):
    """Return fileId of a child by name (folder or file)."""
    for child in get_children(parent_id):
        if child.get("name") == name:
            return child.get("fileId")
    return None


def create_directory(parent_id, name):
    """Create a folder in Onedata (idempotent)."""
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children?name={name}&type=DIR"
    headers = {"X-Auth-Token": TOKEN}
    r = requests.post(url, headers=headers)
    if r.status_code == 201:
        print(f"📁 Created dir: {name}")
        return r.json()["fileId"]
    elif r.status_code == 400 and "eexist" in r.text:
        print(f"ℹ️ Directory exists: {name}")
        return get_child_id(parent_id, name)
    else:
        print(f"❌ Failed to create dir '{name}': {r.status_code} - {r.text}")
        return None


def file_exists(parent_id, name):
    """Check if a file with given name already exists in parent folder."""
    return any(child.get("name") == name for child in get_children(parent_id))


def upload_file(parent_id, local_path, dest_name):
    """Upload one file into Onedata folder."""
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children?name={dest_name}"
    headers = {"X-Auth-Token": TOKEN, "Content-Type": "application/octet-stream"}
    try:
        with open(local_path, "rb") as f:
            r = requests.post(url, headers=headers, data=f, timeout=(10, 600))
        if r.status_code == 201:
            print(f"✅ Uploaded {dest_name} ({os.path.getsize(local_path)} bytes)")
            return True
        elif r.status_code == 400 and "eexist" in r.text:
            print(f"ℹ️ File already exists on Onedata: {dest_name}")
            return True
        else:
            print(f"❌ Upload failed ({r.status_code}): {r.text}")
            return False
    except Exception as e:
        print(f"⚠️ Upload error for {dest_name}: {e}")
        return False


# ---------- Download handlers ----------

def download_http(download_url, dest_path):
    """Download a file via HTTP/HTTPS."""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    print(f"➡️ HTTP download: {download_url}")
    with requests.get(download_url, stream=True, timeout=(10, 60)) as r:
        if r.status_code in (403, 404):
            print(f"🚫 Skipping ({r.status_code}): {download_url}")
            return None
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            downloaded = 0
            for chunk in r.iter_content(chunk_size=512 * 1024):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded >= 100 * 1024 * 1024:
                        print(f"📥 Downloaded {downloaded / (1024**2):.1f} MB...")
                        downloaded = 0
    print(f"✅ Downloaded: {dest_path}")
    return dest_path


def download_ftp(download_url, dest_path):
    """Download a file via FTP."""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    print(f"➡️ FTP download: {download_url}")
    ftp_host = download_url.split("://")[1].split("/")[0]
    ftp_path = "/".join(download_url.split("/")[3:])
    try:
        ftp = ftplib.FTP(ftp_host)
        ftp.login()
        with open(dest_path, "wb") as f:
            ftp.retrbinary(f"RETR {ftp_path}", f.write)
        ftp.quit()
        print(f"✅ FTP downloaded: {dest_path}")
        return dest_path
    except Exception as e:
        print(f"⚠️ FTP failed: {e}")
        return None


def download_file(download_url, dest_path):
    """Wrapper: select HTTP or FTP downloader."""
    if download_url.startswith("ftp://"):
        return download_ftp(download_url, dest_path)
    else:
        return download_http(download_url, dest_path)


# ---------- File upload handler ----------

def handle_file_upload(url, parent_id):
    """Download file to tmp, upload to Onedata, then clean up."""
    raw_name = get_safe_filename_from_url(url)
    filename = sanitize_filename(raw_name)

    # Skip if file already exists
    if file_exists(parent_id, filename):
        print(f"⏩ Skipping (already exists): {filename}")
        return

    tmp_path = os.path.join(TMP_DIR, filename)
    local_file = download_file(url, tmp_path)
    if not local_file:
        return

    if upload_file(parent_id, local_file, filename):
        os.remove(local_file)
        print(f"🧹 Removed temp file: {local_file}")
    else:
        print(f"⚠️ Failed upload for {filename}")


# ---------- YAML parsing and processing ----------

def process_yaml(yaml_path, parent_id):
    """Parse a YAML and mirror its structure + upload all files."""
    print(f"➡️ Processing {yaml_path}")
    with open(yaml_path, "r") as f:
        try:
            data = yaml.safe_load(f)
        except Exception as e:
            print(f"⚠️ Invalid YAML {yaml_path}: {e}")
            return

    dest_name = sanitize_name(data.get("destination", {}).get("name", "Unknown"))
    dest_id = create_directory(parent_id, dest_name)
    if not dest_id:
        return

    for topic in data.get("items", []):
        topic_name = sanitize_name(topic.get("name", "Unnamed-Topic"))
        topic_id = create_directory(dest_id, topic_name)
        if not topic_id:
            continue

        for tutorial in topic.get("items", []):
            tut_name = sanitize_name(tutorial.get("name", "Tutorial"))
            tut_id = create_directory(topic_id, tut_name)
            if not tut_id:
                continue

            items = tutorial.get("items", [])
            has_direct_urls = any("url" in i for i in items)

            # --- Case 1: direct URLs under tutorial ---
            if has_direct_urls:
                for item in items:
                    url = item.get("url")
                    if url:
                        handle_file_upload(url, tut_id)
                continue

            # --- Case 2: nested DOI-level structure ---
            for doi in items:
                doi_name = sanitize_name(doi.get("name", ""))
                doi_id = create_directory(tut_id, doi_name)
                if not doi_id:
                    continue
                for item in doi.get("items", []):
                    url = item.get("url")
                    if url:
                        handle_file_upload(url, doi_id)


# ---------- MAIN ----------

def main():
    if not TOKEN:
        print("❌ Missing ONEPROVIDER_REST_ACCESS_TOKEN")
        return

    ensure_tmp_clean()

    print("🔍 Preparing main folder: GTN data")
    sandbox_id = ROOT_ID  # use GTN data root

    for root, _, files in os.walk("training-material"):
        for file in files:
            if file == "data-library.yaml":
                yaml_path = os.path.join(root, file)
                process_yaml(yaml_path, sandbox_id)

    print("✅ Finished full upload run.")


if __name__ == "__main__":
    main()
