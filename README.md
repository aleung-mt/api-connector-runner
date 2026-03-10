# API Connector Runner

Minimal viable product for a configurable API connector framework.

## Current scope
- Cloud Run Job
- Bearer token auth only
- Full refresh only
- BigQuery raw JSON landing
- BigQuery run log table

## Local test

```powershell
uv venv
.venv\Scripts\Activate.ps1
uv pip install -r requirements.txt
uv run python -m app.main


---

## What to do locally now

You have two choices for testing the container locally:

### Option A, easiest
Just update the Dockerfile and stop there for now.

### Option B, better
Build the Docker image locally and run it once.

I recommend **Option B** if Docker Desktop is already installed on your machine.

---

## If Docker is installed, test locally

From the repo root, run:

```powershell
docker build -t api-connector-runner:local .

docker run --rm `
  -e CONNECTOR_NAME="hubspot_forms_submissions" `
  -e SOURCE_SYSTEM="hubspot" `
  -e API_BASE_URL="https://api.hubapi.com" `
  -e ENDPOINT_TEMPLATE="/form-integrations/v1/submissions/forms/{form_guid}" `
  -e FORM_GUID="" `
  -e PAGE_SIZE="50" `
  -e RESULTS_PATH="results" `
  -e NEXT_CURSOR_PATH="paging.next.after" `
  -e BQ_PROJECT_ID="" `
  -e BQ_DATASET="" `
  -e BQ_RAW_TABLE="" `
  -e BQ_RUN_LOG_TABLE="" `
  -e LOAD_MODE="full_refresh" `
  -e API_BEARER_TOKEN="your_real_hubspot_token" `
  -e GOOGLE_CLOUD_PROJECT="" `
  api-connector-runner:local
