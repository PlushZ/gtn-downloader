# GTN Downloader

The GitHub Action automates the process of downloading data from the Galaxy Training Network (GTN) repository and transferring it to a cloud storage Onedata using Oneclient.

## How to access the data

The GTN data provided by the OneData store is accessible via:

* [OneData share](https://datahub.egi.eu/share/2697e33bd34f1870b0961414b8c77753chf583)
* via the OneData client and an read-only accessToken (see below)
* or via the OneData file-source plugin for example via the [European Galaxy server](https://usegalaxy.eu/)

To access the GTN via OneData or include it in your Galaxy server use the following public read-only accessToken and configuration.
```yaml
- type: onedata
  id: gtn_public_onedata
  label: GTN training data
  doc: Training data from the Galaxy Training Network (powered by Onedata)
  # The access Token is public and can be shared
  accessToken: "MDAxY2xvY2F00aW9uIGRhdGFodWIuZWdpLmV1CjAwNmJpZGVudGlmaWVyIDIvbm1kL3Vzci00yNmI4ZTZiMDlkNDdjNGFkN2E3NTU00YzgzOGE3MjgyY2NoNTNhNS9hY3QvMGJiZmY1NWU4NDRiMWJjZGEwNmFlODViM2JmYmRhNjRjaDU00YjYKMDAxNmNpZCBkYXRhLnJlYWRvbmx5CjAwNDljaWQgZGF00YS5wYXRoID00gTHpaa1pUTTROMkl4WmpjMllXVmpOMlU00WWpreU5XWmtNV00ZpT1RKbU1ETXlZMmhoWTJReAowMDJmc2lnbmF00dXJlIIQvnXp01Oey02LnaNwEkFJAyArzhHN8SlXSYFsBbSkqdqCg"
  onezoneDomain: "datahub.egi.eu"
```

## How this repo works

1. **Triggering**: The action is triggered on a schedule (first day of each month at 23:30) 

2. **Environment Setup**: It runs on a Ubuntu environment and sets up necessary environment variables like `ONEPROVIDER_REST_ACCESS_TOKEN`, `ONECLIENT_PROVIDER_HOST`, `ONEPROVIDER_ROOT_ID`, and `ONEPROVIDER_SPACE_ID`, which are required for accessing the remote storage via Onedata REST API. These variables are retrieved from repository secrets.

3. **Workflow Steps**:
   - **Checkout Repository**: It checks out the current repository to access its contents.
   - **Clone training-material repo**: It clones the GTN repository (`galaxyproject/training-material`) into the workspace for accessing the training materials.
   - **Set up Python**: Configures the Python environment.
   - **Install Dependencies**: Installs the Python dependencies listed in `requirements.txt`.
   - **Set Environment Variables**: Loads Onedata REST API credentials from GitHub Secrets.
   - **Run API upload**: Executes the Python script `bin/api_upload.py`, which parses data-library.yaml files from the GTN repository (`$GITHUB_WORKSPACE/training-material`), downloads the referenced datasets (HTTP/FTP), and uploads them to the Onedata space using the Onedata REST API over HTTPS.
