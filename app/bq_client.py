import json
from datetime import datetime, timezone

from google.cloud import bigquery


def get_bq_client(project_id: str) -> bigquery.Client:
    return bigquery.Client(project=project_id)


def get_table_id(project_id: str, dataset: str, table: str) -> str:
    return f"{project_id}.{dataset}.{table}"


def build_run_log_row(
    run_id: str,
    connector_name: str,
    source_system: str,
    endpoint_name: str,
    load_mode: str,
    status: str,
    started_at: str,
    finished_at: str | None = None,
    pages_read: int | None = None,
    records_loaded: int | None = None,
    error_message: str | None = None,
) -> dict:
    return {
        "run_id": run_id,
        "connector_name": connector_name,
        "source_system": source_system,
        "endpoint_name": endpoint_name,
        "load_mode": load_mode,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": status,
        "pages_read": pages_read,
        "records_loaded": records_loaded,
        "error_message": error_message,
    }


def write_run_log_row(
    client: bigquery.Client,
    table_id: str,
    row: dict,
) -> None:
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        raise RuntimeError(f"Failed to insert run log row: {errors}")


def replace_raw_table_with_rows(
    client: bigquery.Client,
    table_id: str,
    rows: list[dict],
) -> None:
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_json(rows, table_id, job_config=job_config)
    load_job.result()


def transform_raw_record(
    connector_name: str,
    source_system: str,
    endpoint_name: str,
    run_id: str,
    record: dict,
) -> dict:
    record_id = record.get("conversionId")
    if record_id is not None:
        record_id = str(record_id)

    return {
        "connector_name": connector_name,
        "source_system": source_system,
        "endpoint_name": endpoint_name,
        "run_id": run_id,
        "record_id": record_id,
        "raw_record_json": record,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
