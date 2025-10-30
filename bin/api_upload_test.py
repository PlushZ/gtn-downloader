#!/usr/bin/env python3
import os
import yaml
import re
import requests
import time
from urllib.parse import urlparse, unquote

# --- CONFIG ---
PROVIDER = "plg-cyfronet-01.datahub.egi.eu"
SPACE_ID = "6de387b1f76aec7e8b925fd1ab92f032chacd1"
ROOT_ID = "0000000000584FBD677569642373706163655F3664653338376231663736616563376538623932356664316162393266303332636861636431233664653338376231663736616563376538623932356664316162393266303332636861636431"
TOKEN = os.environ.get("ONEPROVIDER_REST_ACCESS_TOKEN")
TMP_DIR = "/tmp/gtn-api-test"
# ----------------


def sanitize_name(name):
    """Replace unsafe characters with hyphens."""
    return re.sub(r'[\\/:\*,?"<>|%.#!@$&\'\(\)\[\]{} ]', '-', str(name or ""))


# -------------------- ONEDATA HELPERS --------------------

def get_child_id(parent_id, name):
    """Return fileId of a child directory/file if exists under parent."""
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children"
    headers = {"X-Auth-Token": TOKEN}
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        for c in r.json().get("children", []):
            if c.get("name") == name:
                return c.get("fileId")
    return None


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
        return get_child_id(parent_id, name)
    else:
        print(f"❌ Failed to create dir '{name}': {r.status_code} - {r.text}")
        return None


def upload_file(parent_id, local_path, dest_name):
    """Upload one file into Onedata folder."""
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children?name={dest_name}"
    headers = {"X-Auth-Token": TOKEN, "Content-Type": "application/octet-stream"}
    with open(local_path, "rb") as f:
        r = requests.post(url, headers=headers, data=f)
    if r.status_code == 201:
        print(f"✅ Uploaded {dest_name} ({os.path.getsize(local_path)} bytes)")
        return True
    elif r.status_code == 400 and "eexist" in r.text:
        print(f"ℹ️ File already exists: {dest_name}")
        return True
    else:
        print(f"❌ Upload failed: {r.status_code} - {r.text}")
        return False


# -------------------- DOWNLOAD --------------------

def download_file(url, dest_path):
    """Download one file to tmp dir."""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    print(f"⬇️ Downloading {url}")
    with requests.get(url, stream=True, timeout=(5, 30)) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=512 * 1024):
                if chunk:
                    f.write(chunk)
    print(f"✅ Downloaded {dest_path}")
    return dest_path


# -------------------- YAML PARSING --------------------

def process_yaml(yaml_path, parent_id):
    """Parse one data-library.yaml and mirror folder structure inside test-ci-folder."""
    print(f"➡️ Processing {yaml_path}")
    with open(yaml_path, "r") as f:
        try:
            data = yaml.safe_load(f)
        except Exception as e:
            print(f"⚠️ Skipping invalid YAML {yaml_path}: {e}")
            return False

    destination_name = sanitize_name(data.get("destination", {}).get("name", "Unknown"))
    dest_id = create_directory(parent_id, destination_name)
    if not dest_id:
        return False

    topics = data.get("items", [])
    for topic in topics:
        topic_name = sanitize_name(topic.get("name", "Unnamed-Topic"))
        topic_id = create_directory(dest_id, topic_name)
        if not topic_id:
            continue

        for tutorial in topic.get("items", []):
            tut_name = sanitize_name(tutorial.get("name", "Tutorial"))
            tut_id = create_directory(topic_id, tut_name)
            if not tut_id:
                continue

            for doi in tutorial.get("items", []):
                doi_name = sanitize_name(doi.get("name", "DOI"))
                doi_id = create_directory(tut_id, doi_name)
                if not doi_id:
                    continue

                for item in doi.get("items", []):
                    url = item.get("url")
                    if not url:
                        continue
                    try:
                        # only handle first file for CI test
                        filename = sanitize_name(os.path.basename(urlparse(unquote(url)).path))
                        tmp_path = os.path.join(TMP_DIR, filename)
                        download_file(url, tmp_path)
                        upload_file(doi_id, tmp_path, filename)
                        print("✅ Test upload complete — stopping after first file.")
                        return True
                    except Exception as e:
                        print(f"⚠️ Error downloading/uploading {url}: {e}")
                        continue
    return False


# -------------------- MAIN --------------------

def main():
    if not TOKEN:
        print("❌ Missing ONEPROVIDER_REST_ACCESS_TOKEN")
        return

    os.makedirs(TMP_DIR, exist_ok=True)

    print("🔍 Preparing sandbox folder: test-ci-folder")
    sandbox_id = create_directory(ROOT_ID, "test-ci-folder")
    if not sandbox_id:
        print("❌ Cannot create or access test-ci-folder.")
        return

    for root, _, files in os.walk("training-material"):
        for file in files:
            if file == "data-library.yaml":
                yaml_path = os.path.join(root, file)
                if process_yaml(yaml_path, sandbox_id):
                    print("🎉 Success: first file uploaded in test-ci-folder, stopping.")
                    return

    print("⚠️ No valid files found to upload.")


if __name__ == "__main__":
    start = time.time()
    main()
    print(f"⏱️ Done in {time.time() - start:.1f}s")
