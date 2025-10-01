import os
import yaml
import re
import argparse
import requests
from urllib.parse import urlparse, unquote
import time
import ftplib
import shutil
from requests.exceptions import RequestException
from urllib3.exceptions import IncompleteRead


# NEW: helper function for safe HTTP downloads
def safe_download_http(download_url, dest_path, retries=3, backoff=10):
    tmp_local = os.path.join("/workspace/tmp", os.path.basename(dest_path))
    os.makedirs(os.path.dirname(tmp_local), exist_ok=True)
    e = None  # FIX: init error holder

    for attempt in range(retries):
        try:
            # Strip wget if present
            if download_url.startswith("wget "):
                download_url = download_url.replace("wget ", "").strip()

            with requests.get(download_url, stream=True, timeout=(10, 600)) as response:
                response.raise_for_status()
                with open(tmp_local, "wb") as f:
                    for chunk in response.iter_content(chunk_size=256 * 1024):  # 256 KB chunks
                        if chunk:
                            f.write(chunk)

            # Move to Onedata only after full download
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.move(tmp_local, dest_path)
            return "Downloaded", os.path.getsize(dest_path)

        except (RequestException, IncompleteRead, IOError) as err:
            e = err  # remember last error
            print(f"⚠️ HTTP download failed (attempt {attempt+1}/{retries}): {err}")
            if os.path.exists(tmp_local):
                os.remove(tmp_local)
            time.sleep(backoff * (attempt + 1))

    return f"Error downloading file from {download_url}: {e}", 0


# NEW: helper function for safe FTP downloads
def safe_download_ftp(download_url, dest_path, retries=3, backoff=10):
    tmp_local = os.path.join("/workspace/tmp", os.path.basename(dest_path))
    os.makedirs(os.path.dirname(tmp_local), exist_ok=True)
    e = None

    for attempt in range(retries):
        try:
            ftp_host = download_url.split("://")[1].split("/")[0]
            ftp = ftplib.FTP(ftp_host)
            ftp.login()
            ftp.cwd("/".join(download_url.split("/")[3:-1]))
            with open(tmp_local, "wb") as file:
                ftp.retrbinary("RETR " + download_url.split("/")[-1], file.write)
            ftp.quit()

            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.move(tmp_local, dest_path)
            return "Downloaded", os.path.getsize(dest_path)

        except Exception as err:
            e = err
            print(f"⚠️ FTP download failed (attempt {attempt+1}/{retries}): {err}")
            if os.path.exists(tmp_local):
                os.remove(tmp_local)
            time.sleep(backoff * (attempt + 1))

    return f"Error downloading file from {download_url}: {e}", 0


def process_yaml(yaml_path, output_dir, summary_file, failed_file):
    with open(yaml_path, "r") as file:
        data = yaml.safe_load(file)

    destination_name = sanitize_name(data["destination"]["name"])
    topics = data["items"]

    for topic in topics:
        topic_name = sanitize_name(topic["name"])
        topic_path = os.path.join(output_dir, destination_name, topic_name)

        process_urls(topic_path, topic.get("items", []), summary_file, failed_file)

        tutorials = topic["items"]
        for tutorial in tutorials:
            tutorial_name = sanitize_name(tutorial["name"])
            tutorial_path = os.path.join(topic_path, tutorial_name)

            process_urls(tutorial_path, tutorial.get("items", []), summary_file, failed_file)

            dois = tutorial.get("items", [])
            for doi in dois:
                doi_name = sanitize_name(doi.get("name", ""))
                doi_path = os.path.join(tutorial_path, doi_name)

                if not os.path.exists(doi_path):
                    os.makedirs(doi_path)

                process_urls(doi_path, doi.get("items", []), summary_file, failed_file)


def process_urls(output_path, items, summary_file, failed_file):
    for entry in items:
        url = entry.get("url", "")
        if url:
            download_url = unquote(url)
            filename = os.path.join(output_path, os.path.basename(urlparse(download_url).path))

            os.makedirs(output_path, exist_ok=True)

            # FIX: skip only if file exists and size > 0
            if os.path.exists(filename): #and os.path.getsize(filename) > 0:
                status = "Download skipped (File already exists)"
                file_size = os.path.getsize(filename)
            else:
                if download_url.startswith("ftp://"):
                    status, file_size = safe_download_ftp(download_url, filename)
                else:
                    status, file_size = safe_download_http(download_url, filename)

                # FIX: if failed, log to failed-downloads.txt
                if status.startswith("Error"):
                    with open(failed_file, "a") as ff:
                        ff.write(f"{download_url}\t{status}\n")

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


def main():
    start_time = time.time()

    parser = argparse.ArgumentParser(
        description="process data-library.yaml files and create output directory structure."
    )
    parser.add_argument("--input", dest="project_dir", required=True,
                        help="path to the training-material directory")
    parser.add_argument("--output", dest="output_dir", required=True,
                        help="path to the output directory where to download data and summary file")

    args = parser.parse_args()

    local_tmp_dir = os.path.join("/workspace", "tmp")
    os.makedirs(local_tmp_dir, exist_ok=True)
    local_summary_file = os.path.join(local_tmp_dir, "download-summary.tsv")
    failed_file = os.path.join(local_tmp_dir, "failed-downloads.txt")

    write_summary_header(local_summary_file)

    # -------------------------------
    # ORIGINAL LOOP (COMMENTED OUT):
    # for root, dirs, files in os.walk(args.project_dir):
    #     for file in files:
    #         if file == "data-library.yaml":
    #             yaml_path = os.path.join(root, file)
    #             process_yaml(yaml_path, args.output_dir, local_summary_file, failed_file)
    # -------------------------------

    # TESTING ONLY: microbiome/metaplasmidome_query
    yaml_path = os.path.join(
        args.project_dir,
        "topics/microbiome/tutorials/metaplasmidome_query/data-library.yaml"
    )
    process_yaml(yaml_path, args.output_dir, local_summary_file, failed_file)

    calculate_overall_size(local_summary_file)

    summary_file = os.path.join(args.output_dir, "download-summary.tsv")
    try:
        shutil.copy(local_summary_file, summary_file)
        print(f"✅ Summary file created locally at: {local_summary_file}")
        print(f"✅ Summary file copied to Onedata at: {summary_file}")
    except Exception as e:
        print(f"⚠️ Failed to copy summary to Onedata: {e}")

    try:
        shutil.copy(failed_file, os.path.join(args.output_dir, "failed-downloads.txt"))
        print(f"⚠️ Failed download log saved to: {failed_file}")
    except Exception as e:
        print(f"⚠️ Failed to copy failed-downloads.txt to Onedata: {e}")

    end_time = time.time()
    print(f"Script run time: {end_time - start_time} seconds")


if __name__ == "__main__":
    main()
