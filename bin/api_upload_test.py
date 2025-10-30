#!/usr/bin/env python3
import os
import requests
import yaml
import re
from urllib.parse import urlparse, unquote

# --- CONFIG ---
PROVIDER = "plg-cyfronet-01.datahub.egi.eu"
SPACE_ID = "6de387b1f76aec7e8b925fd1ab92f032chacd1"
ROOT_ID = "0000000000584FBD677569642373706163655F3664653338376231663736616563376538623932356664316162393266303332636861636431233664653338376231663736616563376538623932356664316162393266303332636861636431"
TOKEN = os.environ.get("ONEPROVIDER_REST_ACCESS_TOKEN")
TMP_DIR = "/tmp/gtn-test"
# ----------------

def download_file(url, local_path):
    """Download one file to local path"""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    print(f"⬇️ Downloading {url}")
    with requests.get(url, stream=True, timeout=(5, 30)) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=256 * 1024):
                if chunk:
                    f.write(chunk)
    print(f"✅ Downloaded to {local_path}")
    return local_path


def create_directory(parent_id, name):
    """Create directory under parent (idempotent)"""
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children?name={name}&type=DIR"
    headers = {"X-Auth-Token": TOKEN}
    r = requests.post(url, headers=headers)
    if r.status_code == 201:
        print(f"📁 Created dir {name}")
        return r.json()["fileId"]
    elif r.status_code == 400 and "eexist" in r.text:
        print(f"ℹ️ Directory {name} exists, using it")
        return get_child_id(parent_id, name)
    else:
        print(f"❌ Failed to create dir {name}: {r.status_code} - {r.text}")
        return None


def get_child_id(parent_id, name):
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children"
    headers = {"X-Auth-Token": TOKEN}
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        for c in r.json().get("children", []):
            if c.get("name") == name:
                return c.get("fileId")
    return None


def upload_to_onedata(parent_id, local_path, dest_name):
    """Upload file into Onedata folder"""
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children?name={dest_name}"
    headers = {
        "X-Auth-Token": TOKEN,
        "Content-Type": "application/octet-stream",
    }
    print(f"📤 Uploading {local_path} → {dest_name}")
    with open(local_path, "rb") as f:
        r = requests.post(url, headers=headers, data=f)
    if r.status_code == 201:
        print(f"✅ Uploaded successfully: {r.json()['fileId']}")
    elif r.status_code == 400 and "eexist" in r.text:
        print(f"ℹ️ File {dest_name} already exists.")
    else:
        print(f"❌ Upload failed: {r.status_code} - {r.text}")


def find_first_yaml_file(project_dir):
    """Return path of first data-library.yaml"""
    for root, _, files in os.walk(project_dir):
        for file in files:
            if file == "data-library.yaml":
                return os.path.join(root, file)
    return None


def extract_first_url(yaml_path):
    """Find first URL in data-library.yaml"""
    with open(yaml_path, "r") as f:
        data = yaml.safe_load(f)
    def find_url(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "url" and isinstance(v, str):
                    return v
                res = find_url(v)
                if res:
                    return res
        elif isinstance(obj, list):
            for v in obj:
                res = find_url(v)
                if res:
                    return res
        return None
    return find_url(data)


def sanitize_name(name):
    return re.sub(r'[\\/:\*,?"<>|%.#!@$&\'\(\)\[\]{} ]', '-', name)


def main():
    if not TOKEN:
        print("❌ Missing ONEPROVIDER_REST_ACCESS_TOKEN")
        return

    yaml_path = find_first_yaml_file("training-material")
    if not yaml_path:
        print("❌ No data-library.yaml found.")
        return

    print(f"➡️ Using YAML: {yaml_path}")
    url = extract_first_url(yaml_path)
    if not url:
        print("❌ No URL found in YAML.")
        return

    print(f"🌐 Found test URL: {url}")
    filename = sanitize_name(os.path.basename(urlparse(unquote(url)).path)) or "testfile"
    local_path = os.path.join(TMP_DIR, filename)

    try:
        download_file(url, local_path)
    except Exception as e:
        print(f"⚠️ Download failed: {e}")
        return

    # Create test folder in Onedata and upload
    folder_id = create_directory(ROOT_ID, "test-ci-folder")
    if not folder_id:
        print("❌ Cannot proceed without folder ID.")
        return

    upload_to_onedata(folder_id, local_path, filename)
    print("✅ Test completed successfully.")


if __name__ == "__main__":
    main()
