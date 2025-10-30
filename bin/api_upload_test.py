#!/usr/bin/env python3
import os
import yaml
import re
import requests
from urllib.parse import urlparse, unquote

# --- CONFIG ---
PROVIDER = "plg-cyfronet-01.datahub.egi.eu"
SPACE_ID = "6de387b1f76aec7e8b925fd1ab92f032chacd1"  
TOKEN = os.environ.get("ONEPROVIDER_REST_ACCESS_TOKEN")
# ----------------


def sanitize_name(name):
    return re.sub(r'[\\/:\*,?"<>|%.#!@$&\'\(\)\[\]{} ]', "-", name)


def upload_to_onedata(local_path, dest_path):
    """Upload file to Onedata REST API"""
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{SPACE_ID}/{dest_path.lstrip('/')}"
    headers = {"X-Auth-Token": TOKEN}

    with open(local_path, "rb") as f:
        print(f"➡️ Uploading {local_path} → {dest_path}")
        r = requests.put(url, headers=headers, data=f)

    if r.status_code in (200, 201):
        print(f"✅ Uploaded ({r.status_code}): {dest_path}")
    else:
        print(f"❌ Upload failed: {r.status_code} - {r.text}")
    r.raise_for_status()


def download_file(download_url, tmp_file):
    """Simple HTTP/FTP downloader for one file"""
    print(f"➡️ Downloading {download_url}")
    if download_url.startswith("ftp://"):
        from ftplib import FTP
        ftp_host = download_url.split("://")[1].split("/")[0]
        ftp = FTP(ftp_host)
        ftp.login()
        ftp.cwd("/".join(download_url.split("/")[3:-1]))
        with open(tmp_file, "wb") as f:
            ftp.retrbinary("RETR " + download_url.split("/")[-1], f.write)
        ftp.quit()
    else:
        with requests.get(download_url, stream=True, timeout=(5, 30)) as r:
            r.raise_for_status()
            with open(tmp_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=256 * 1024):
                    if chunk:
                        f.write(chunk)
    print(f"✅ Downloaded {tmp_file}")


def process_yaml(yaml_path):
    """Parse YAML and find the first file URL"""
    with open(yaml_path, "r") as f:
        data = yaml.safe_load(f)

    destination_name = sanitize_name(data["destination"]["name"])
    for topic in data.get("items", []):
        for tutorial in topic.get("items", []):
            for doi in tutorial.get("items", []):
                for entry in doi.get("items", []):
                    url = entry.get("url")
                    if url:
                        return destination_name, topic["name"], tutorial["name"], doi["name"], url
    return None


def main():
    if not TOKEN:
        print("❌ Missing ONEPROVIDER_REST_ACCESS_TOKEN")
        return

    project_dir = "training-material"
    print(f"🔍 Searching for data-library.yaml inside {project_dir} ...")

    for root, _, files in os.walk(project_dir):
        if "data-library.yaml" in files:
            yaml_path = os.path.join(root, "data-library.yaml")
            print(f"➡️ Found {yaml_path}")
            result = process_yaml(yaml_path)
            if not result:
                continue

            destination, topic, tutorial, doi, url = result
            download_url = unquote(url)
            filename = os.path.basename(urlparse(download_url).path)
            tmp_file = f"/tmp/{filename}"
            download_file(download_url, tmp_file)

            dest_path = f"GTN data/{sanitize_name(destination)}/{sanitize_name(topic)}/{sanitize_name(tutorial)}/{sanitize_name(doi)}/{filename}"
            upload_to_onedata(tmp_file, dest_path)
            print("✅ Test complete — stopped after first upload.")
            return

    print("❌ No valid URLs found.")


if __name__ == "__main__":
    main()
