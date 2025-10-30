#!/usr/bin/env python3
import os
import ftplib
import time

# --- CONFIG ---
TMP_DIR = "/tmp/gtn-ftp-test"
FTP_URL = "ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR170/002/SRR17054502/SRR17054502_1.fastq.gz"
CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB
# ----------------


def download_ftp(url, dest_path):
    """Stream FTP download with progress display."""
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    parts = url.replace("ftp://", "").split("/", 1)
    host = parts[0]
    path = parts[1]
    dir_path = os.path.dirname(path)
    filename = os.path.basename(path)

    print(f"🌐 Connecting to FTP: {host}")
    ftp = ftplib.FTP(host)
    ftp.login()
    ftp.cwd(dir_path)

    print(f"⬇️ Starting FTP download: {filename}")
    downloaded = 0
    last_mb = 0
    start = time.time()

    with open(dest_path, "wb") as f:
        def callback(chunk):
            nonlocal downloaded, last_mb
            f.write(chunk)
            downloaded += len(chunk)
            if downloaded - last_mb >= 100 * 1024 * 1024:
                print(f"📥 {downloaded / (1024**2):.1f} MB downloaded...")
                last_mb = downloaded

        ftp.retrbinary(f"RETR {filename}", callback, blocksize=CHUNK_SIZE)

    ftp.quit()
    print(f"✅ Download complete: {dest_path} ({downloaded / (1024**2):.1f} MB, {time.time() - start:.1f}s)")
    return dest_path


def main():
    os.makedirs(TMP_DIR, exist_ok=True)
    filename = os.path.basename(FTP_URL)
    local_path = os.path.join(TMP_DIR, filename)

    print(f"Testing FTP download: {FTP_URL}")
    try:
        download_ftp(FTP_URL, local_path)
        print(f"✅ File saved at: {local_path}")
    except Exception as e:
        print(f"❌ FTP download failed: {e}")


if __name__ == "__main__":
    main()
