import os
import requests

def main():
    output_dir = "/mnt/onedata/test-output"
    os.makedirs(output_dir, exist_ok=True)

    summary_file = os.path.join(output_dir, "test-summary.tsv")
    with open(summary_file, "w") as f:
        f.write("Path\tURL\tStatus\tFile Size\n")

    # Pick a small file from training-material repo
    test_url = "https://raw.githubusercontent.com/galaxyproject/training-material/main/CONTRIBUTING.md"
    filename = os.path.join(output_dir, os.path.basename(test_url))

    print(f"Downloading {test_url} -> {filename}")
    response = requests.get(test_url, stream=True)
    response.raise_for_status()
    with open(filename, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    file_size = os.path.getsize(filename)

    with open(summary_file, "a") as f:
        f.write(f"{output_dir}\t{test_url}\tDownloaded\t{file_size}\n")

    print("Done. Test summary and file written.")

if __name__ == "__main__":
    main()
