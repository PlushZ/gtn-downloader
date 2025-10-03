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


BASE_DIR = os.environ.get("GITHUB_WORKSPACE", os.getcwd())
CACHE_BASE = os.path.join(BASE_DIR, "cache")
TMP_BASE   = os.path.join(BASE_DIR, "tmp")


def safe_download_http(download_url, dest_path, retries=3, backoff=10):
    tmp_local = os.path.join(TMP_BASE, os.path.basename(dest_path))
    os.makedirs(os.path.dirname(tmp_local), exist_ok=True)
    e = None

    if download_url.startswith("wget "):
        download_url = download_url.replace("wget ", "").strip()

    for attempt in range(retries):
        try:
            print(f"➡️ Starting HTTP download: {download_url}")
            with requests.get(download_url, stream=True, timeout=(10, 600)) as response:
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
                print(f"✅ Finished download: {dest_path} ({downloaded / (1024**2):.1f} MB)")
                return "Downloaded", os.path.getsize(dest_path)

        except (RequestException, IncompleteRead, IOError) as err:
            e = err
            print(f"⚠️ HTTP download failed (attempt {attempt+1}/{retries}): {err}")
            if os.path.exists(tmp_local):
                os.remove(tmp_local)
            time.sleep(backoff * (attempt + 1))

    return f"Error downloading file from {download_url}: {e}", 0


def safe_download_ftp(download_url, dest_path, retries=3, backoff=10):
    tmp_local = os.path.join(TMP_BASE, os.path.basename(dest_path))
    os.makedirs(os.path.dirname(tmp_local), exist_ok=True)
    e = None

    for attempt in range(retries):
        try:
            ftp_host = download_url.split("://")[1].split("/")[0]
            ftp = ftplib.FTP(ftp_host)
            ftp.login()
            ftp.cwd("/".join(download_url.split("/")[3:-1]))

            print(f"➡️ Starting FTP download: {download_url}")
            downloaded = 0
            last_reported = 0

            with open(tmp_local, "wb") as file:
                def callback(chunk):
                    nonlocal downloaded, last_reported
                    file.write(chunk)
                    downloaded += len(chunk)
                    if downloaded - last_reported >= 100 * 1024 * 1024:
                        print(f"📥 FTP Downloaded {downloaded / (1024**2):.1f} MB...")
                        last_reported = downloaded

                ftp.retrbinary("RETR " + download_url.split("/")[-1], callback)

            ftp.quit()
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.move(tmp_local, dest_path)
            print(f"✅ Finished FTP download: {dest_path} ({downloaded / (1024**2):.1f} MB)")
            return "Downloaded", os.path.getsize(dest_path)

        except Exception as err:
            e = err
            print(f"⚠️ FTP download failed (attempt {attempt+1}/{retries}): {err}")
            if os.path.exists(tmp_local):
                os.remove(tmp_local)
            time.sleep(backoff * (attempt + 1))

    return f"Error downloading file from {download_url}: {e}", 0


def get_safe_filename_from_url(url, output_path):
    parsed = urlparse(url)
    parts = parsed.path.split("/")

    if parts[-1] == "content" and len(parts) > 1:
        filename = parts[-2]
    else:
        filename = parts[-1]

    return os.path.join(output_path, filename)


def process_urls(output_path, items, summary_file, skip_list):
    for entry in items:
        url = entry.get("url", "")
        if url:
            download_url = unquote(url).strip()
            if download_url.startswith("wget "):
                download_url = download_url.replace("wget ", "").strip()

            cache_filename = get_safe_filename_from_url(download_url, output_path)

            os.makedirs(os.path.dirname(cache_filename), exist_ok=True)

            # 🔹 Check skip list (files already in Onedata)
            rel_path = os.path.relpath(cache_filename, CACHE_BASE)
            if rel_path in skip_list:
                status = "Download skipped (Already in Onedata)"
                file_size = 0
            elif os.path.exists(cache_filename) and os.path.getsize(cache_filename) > 0:
                status = "Download skipped (Already in local cache)"
                file_size = os.path.getsize(cache_filename)
            else:
                if download_url.startswith("ftp://"):
                    status, file_size = safe_download_ftp(download_url, cache_filename)
                else:
                    status, file_size = safe_download_http(download_url, cache_filename)

            update_urls_file(output_path, url, status, file_size, summary_file)


def update_urls_file(output_path, url, status, file_size, summary_file):
    with open(summary_file, "a") as summary:
        summary.write(f"{output_path}\t{url}\t{status}\t{file_size}\n")


def sanitize_name(name):
    return re.sub(r'[\\/:\*,?"<>|%.#!@$&\'\(\)\[\]{} ]', "-", name)


def write_summary_header(summary_file):
    summary_dir = os.path.dirname(summary_file)
    os.makedirs(summary_dir, exist_ok=True)

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


def process_yaml(yaml_path, output_dir, summary_file, skip_list):
    with open(yaml_path, "r") as file:
        data = yaml.safe_load(file)

    destination_name = sanitize_name(data["destination"]["name"])
    topics = data["items"]

    for topic in topics:
        topic_name = sanitize_name(topic["name"])
        topic_path = os.path.join(CACHE_BASE, destination_name, topic_name)

        process_urls(topic_path, topic.get("items", []), summary_file, skip_list)

        tutorials = topic["items"]
        for tutorial in tutorials:
            tutorial_name = sanitize_name(tutorial["name"])
            tutorial_path = os.path.join(topic_path, tutorial_name)

            process_urls(tutorial_path, tutorial.get("items", []), summary_file, skip_list)

            dois = tutorial.get("items", [])
            for doi in dois:
                doi_name = sanitize_name(doi.get("name", ""))
                doi_path = os.path.join(tutorial_path, doi_name)

                os.makedirs(doi_path, exist_ok=True)
                process_urls(doi_path, doi.get("items", []), summary_file, skip_list)


def main():
    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", dest="project_dir", required=True, help="training-material directory")
    parser.add_argument("--skip-list", dest="skip_list", required=False, help="file with list of already existing files")
    args = parser.parse_args()

    local_summary_file = os.path.join(TMP_BASE, "download-summary.tsv")
    os.makedirs(TMP_BASE, exist_ok=True)
    write_summary_header(local_summary_file)

    skip_list = set()
    if args.skip_list and os.path.exists(args.skip_list):
        with open(args.skip_list) as f:
            skip_list = set(line.strip() for line in f if line.strip())
        print(f"ℹ️ Loaded skip list with {len(skip_list)} files already in Onedata")


    # Test: only one YAML file 
    yaml_path = os.path.join(
       args.project_dir,
       "topics/variant-analysis/tutorials/beacon_cnv_query/data-library.yaml"
    )
    print(f"➡️ Processing single YAML (test mode): {yaml_path}")
    process_yaml(yaml_path, CACHE_BASE, local_summary_file, skip_list)


    #for root, dirs, files in os.walk(args.project_dir):
    #    for file in files:
    #        if file == "data-library.yaml":
    #            yaml_path = os.path.join(root, file)
    #            print(f"➡️ Processing YAML: {yaml_path}")
    #            process_yaml(yaml_path, CACHE_BASE, local_summary_file, skip_list)

    calculate_overall_size(local_summary_file)
    print(f"✅ Summary created at: {local_summary_file}")

    end_time = time.time()
    print(f"Script run time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
