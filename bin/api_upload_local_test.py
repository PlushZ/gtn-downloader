#!/usr/bin/env python3
import os
import requests

# --- CONFIG ---
PROVIDER = "plg-cyfronet-01.datahub.egi.eu"
SPACE_ID = "6de387b1f76aec7e8b925fd1ab92f032chacd1"
TOKEN = os.environ.get("ONEPROVIDER_REST_ACCESS_TOKEN")
ROOT_ID = "0000000000584FBD677569642373706163655F3664653338376231663736616563376538623932356664316162393266303332636861636431233664653338376231663736616563376538623932356664316162393266303332636861636431"
# ----------------


def get_child_id(parent_id, name):
    """Find a child by name in a directory and return its fileId."""
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children"
    headers = {"X-Auth-Token": TOKEN}
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        for entry in r.json().get("children", []):
            if entry.get("name") == name:
                return entry.get("fileId")
    return None


def create_directory(parent_id, name):
    """Create directory under parent (if not exists). Returns its fileId."""
    url = f"https://{PROVIDER}/api/v3/oneprovider/data/{parent_id}/children?name={name}&type=DIR"
    headers = {"X-Auth-Token": TOKEN}

    print(f"📁 Creating directory: {name}")
    r = requests.post(url, headers=headers)

    if r.status_code == 201:
        file_id = r.json()["fileId"]
        print(f"✅ Created directory '{name}' with fileId={file_id}")
        return file_id
    elif r.status_code == 400 and "eexist" in r.text:
        print(f"ℹ️ Directory '{name}' already exists, fetching fileId...")
        return get_child_id(parent_id, name)
    else:
        print(f"❌ Failed to create directory '{name}': {r.status_code} - {r.text}")
    return None


def upload_file(parent_id, local_path, dest_name):
    """Upload file via REST API into Onedata directory."""
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
        print(f"ℹ️ File '{dest_name}' already exists, skipping upload.")
    else:
        print(f"❌ Upload failed: {r.status_code} - {r.text}")


def main():
    if not TOKEN:
        print("❌ Missing ONEPROVIDER_REST_ACCESS_TOKEN env variable")
        return

    # Step 1. Create or get test folder
    test_folder_id = create_directory(ROOT_ID, "test-folder2")
    if not test_folder_id:
        print("❌ Cannot proceed without folder ID.")
        return

    # Step 2. Create a small file
    test_file = "/tmp/test_onedata_upload3.txt"
    with open(test_file, "w") as f:
        f.write("Hello again from Onedata API test v3!\n")

    # Step 3. Upload the file
    upload_file(test_folder_id, test_file, "test_onedata_upload3.txt")


if __name__ == "__main__":
    main()
