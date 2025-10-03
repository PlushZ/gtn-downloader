import os
import yaml
import re
import argparse
import requests
import time
import ftplib
import shutil
from urllib.parse import urlparse, unquote
from requests.exceptions import RequestException
from http.client import IncompleteRead


TMP_BASE = "/tmp"  # temporary local dir for partial downloads


# --- Load forbidden list (auto from script dir) ---
def load_forbidden_list():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    forbidden_path = os.path.join(script_dir, "list_forbidden.txt")
    if not os.path.exists(forbidden_path):
        return set()
    with open(forbidden_path, "r") as f:
        return set(line.strip() for line in f if line.strip())


# --- Onedata health check ---
def ensure_onedata_alive(mountpoint="/mnt/onedata"):
    test_file = os.path.join(mountpoint, ".healthcheck")
    try:
        with open(test_file, "w") as f:
            f.write("ok")
        os.remove(test_file)
        return True
    except Exception as e:
        print(f"❌ Onedata mount not available: {e}")
        return False


# --- HTTP download ---
def safe_download_http(download_url, dest_path, retries=1, backoff=5):
    tmp_local = os.path.join(TMP_BASE, os.path.basename(dest_path))
    os.makedirs(os.path.dirname(tmp_local), exist_ok=True)
    e = None

    if download_url.startswith("wget "):
        download_url = download_url.replace("wget ", "").strip()

    for attempt in range(retries):
        try:
            if not ensure_onedata_alive():
                return "Oneclient disconnected", 0

            print(f"➡️ Starting HTTP download: {download_url}")
            with requests.get(download_url, stream=True, timeout=(5, 30)) as response:
                response.raise_for_status()

                total_size = response.headers.get("Content-Length")
                if total_size:
                    print(f"ℹ️ Expected size: {int(total_size) / (1024**3):.2f} GiB")

                downloaded = 0
                last_reported = 0

                with open(tmp_local, "wb") as f:
                    for chunk in response.iter_content(chunk_size=256 * 1024):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if downloaded - last_reported >= 100 * 1024 * 1024:
                                print(f"📥 Downloaded {downloaded / (1024**2):.1f} MB...")
                                last_reported = downloaded

                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                shutil.move(tmp_local, dest_path)
                print(f"✅ Finished: {dest_path} ({downloaded / (1024**2):.1f} MB)")
                return "Downloaded", os.path.getsize(dest_path)

        except (RequestException, IncompleteRead, IOError) as err:
            e = err
            print(f"⚠️ HTTP failed: {err}")
            if os.path.exists(tmp_local):
                os.remove(tmp_local)
            time.sleep(backoff)

    return f"Error downloading {download_url}: {e}", 0


# --- FTP download ---
def safe_download_ftp(download_url, dest_path, retries=1, backoff=5):
    tmp_local = os.path.join(TMP_BASE, os.path.basename(dest_path))
    os.makedirs(os.path.dirname(tmp_local), exist_ok=True)
    e = None

    for attempt in range(retries):
        try:
            if not ensure_onedata_alive():
                return "Oneclient disconnected", 0

            ftp_host = download_url.split("://")[1].split("/")[0]
            ftp = ftplib.FTP(ftp_host)
            ftp.login()
            ftp.cwd("/".join(download_url.split("/")[3:-1]))

            print(f"➡️ Starting FTP: {download_url}")
            downloaded = 0
            last_reported = 0

            with open(tmp_local, "wb") as file:
                def callback(chunk):
                    nonlocal downloaded, last_reported
                    file.write(chunk)
                    downloaded += len(chunk)
                    if downloaded - last_reported >= 100 * 1024 * 1024:
                        print(f"📥 FTP {downloaded / (1024**2):.1f} MB...")
                        last_reported = downloaded

                ftp.retrbinary("RETR " + download_url.split("/")[-1], callback)

            ftp.quit()
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.move(tmp_local, dest_path)
            print(f"✅ Finished FTP: {dest_path} ({downloaded / (1024**2):.1f} MB)")
            return "Downloaded", os.path.getsize(dest_path)

        except Exception as err:
            e = err
            print(f"⚠️ FTP failed: {err}")
            if os.path.exists(tmp_local):
                os.remove(tmp_local)
            time.sleep(backoff)

    return f"Error FTP {download_url}: {e}", 0


# --- Helpers ---
def get_safe_filename_from_url(url, output_path):
    parsed = urlparse(url)
    parts = parsed.path.split("/")
    if parts[-1] == "content" and len(parts) > 1:
        filename = parts[-2]
    else:
        filename = parts[-1]
    return os.path.join(output_path, filename)


def process_urls(output_path, items, summary_file, forbidden):
    for entry in items:
        url = entry.get("url", "")
        if not url:
            continue

        download_url = unquote(url).strip()
        if download_url.startswith("wget "):
            download_url = download_url.replace("wget ", "").strip()

        # Forbidden check
        if download_url in forbidden:
            print(f"⛔ Skipping forbidden URL: {download_url}")
            update_urls_file(output_path, url, "Forbidden/NotFound", 0, summary_file)
            continue

        filename = get_safe_filename_from_url(download_url, output_path)
        os.makedirs(output_path, exist_ok=True)

        if os.path.isfile(filename) and os.path.getsize(filename) > 0:
            status = "Skipped (exists)"
            file_size = os.path.getsize(filename)
        else:
            if download_url.startswith("ftp://"):
                status, file_size = safe_download_ftp(download_url, filename)
            else:
                status, file_size = safe_download_http(download_url, filename)

        update_urls_file(output_path, url, status, file_size, summary_file)


def update_urls_file(output_path, url, status, file_size, summary_file):
    with open(summary_file, "a") as summary:
        summary.write(f"{output_path}\t{url}\t{status}\t{file_size}\n")


def sanitize_name(name):
    return re.sub(r'[\\/:\*,?"<>|%.#!@$&\'\(\)\[\]{} ]', "-", name)


def write_summary_header(summary_file):
    os.makedirs(os.path.dirname(summary_file), exist_ok=True)
    with open(summary_file, "w") as file:
        file.write("Path\tURL\tStatus\tFile Size\n")


def calculate_overall_size(summary_file):
    overall_size = 0
    with open(summary_file, "r") as file:
        next(file)
        for line in file:
            _, _, _, file_size_str = line.strip().split("\t")
            overall_size += int(file_size_str)
    with open(summary_file, "r+") as file:
        content = file.read()
        file.seek(0, 0)
        file.write(f"Overall\t\t\t{overall_size}\n{content}")


def process_yaml(yaml_path, output_dir, summary_file, forbidden):
    with open(yaml_path, "r") as file:
        data = yaml.safe_load(file)

    destination_name = sanitize_name(data["destination"]["name"])
    topics = data["items"]

    for topic in topics:
        topic_name = sanitize_name(topic["name"])
        topic_path = os.path.join(output_dir, destination_name, topic_name)
        process_urls(topic_path, topic.get("items", []), summary_file, forbidden)

        for tutorial in topic.get("items", []):
            tutorial_name = sanitize_name(tutorial["name"])
            tutorial_path = os.path.join(topic_path, tutorial_name)
            process_urls(tutorial_path, tutorial.get("items", []), summary_file, forbidden)

            for doi in tutorial.get("items", []):
                doi_name = sanitize_name(doi.get("name", ""))
                doi_path = os.path.join(tutorial_path, doi_name)
                os.makedirs(doi_path, exist_ok=True)
                process_urls(doi_path, doi.get("items", []), summary_file, forbidden)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", dest="project_dir", required=True)
    parser.add_argument("--output", dest="output_dir", required=True)
    args = parser.parse_args()

    forbidden = load_forbidden_list()
    print(f"ℹ️ Loaded forbidden list with {len(forbidden)} URLs")

    summary_file = os.path.join(args.output_dir, "download-summary.tsv")
    write_summary_header(summary_file)

    # Test: only one YAML file 
    #yaml_path = os.path.join(
    #   args.project_dir,
    #   "topics/variant-analysis/tutorials/beacon_cnv_query/data-library.yaml"
    #)
    #print(f"➡️ Processing single YAML (test mode): {yaml_path}")
    #process_yaml(yaml_path, args.output_dir, summary_file, forbidden)

    for root, dirs, files in os.walk(args.project_dir):
        for file in files:
            if file == "data-library.yaml":
                yaml_path = os.path.join(root, file)
                print(f"➡️ Processing {yaml_path}")
                process_yaml(yaml_path, args.output_dir, summary_file, forbidden)

    calculate_overall_size(summary_file)
    print(f"✅ Finished. Summary at {summary_file}")


if __name__ == "__main__":
    main()
