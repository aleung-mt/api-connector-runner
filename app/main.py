import uuid
from datetime import datetime, timezone

from app.config import load_config
from app.logger import setup_logger
from app.api_client import build_endpoint, fetch_all_pages
from app.bq_client import (
    build_run_log_row,
    get_bq_client,
    get_table_id,
    replace_raw_table_with_rows,
    transform_raw_record,
    write_run_log_row,
)


def main():
    logger = setup_logger()
    config = load_config()

    logger.info("API connector runner started")
    logger.info(
        "Loaded config: connector_name=%s, source_system=%s, endpoint_template=%s, "
        "page_size=%s, results_path=%s, next_cursor_path=%s, bq_dataset=%s, "
        "bq_raw_table=%s, bq_run_log_table=%s, load_mode=%s",
        config.connector_name,
        config.source_system,
        config.endpoint_template,
        config.page_size,
        config.results_path,
        config.next_cursor_path,
        config.bq_dataset,
        config.bq_raw_table,
        config.bq_run_log_table,
        config.load_mode,
    )

    logger.info("Config validation passed")

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    endpoint = build_endpoint(config.endpoint_template, config.form_guid)
    endpoint_name = endpoint

    logger.info("Generated run_id: %s", run_id)
    logger.info("Built endpoint: %s", endpoint)

    client = get_bq_client(config.bq_project_id)
    raw_table_id = get_table_id(
        config.bq_project_id,
        config.bq_dataset,
        config.bq_raw_table,
    )
    run_log_table_id = get_table_id(
        config.bq_project_id,
        config.bq_dataset,
        config.bq_run_log_table,
    )

    logger.info("Writing run log start row")
    start_row = build_run_log_row(
        run_id=run_id,
        connector_name=config.connector_name,
        source_system=config.source_system,
        endpoint_name=endpoint_name,
        load_mode=config.load_mode,
        status="running",
        started_at=started_at,
    )
    write_run_log_row(client, run_log_table_id, start_row)

    try:
        all_results = fetch_all_pages(
            api_base_url=config.api_base_url,
            endpoint=endpoint,
            bearer_token=config.api_bearer_token,
            page_size=config.page_size,
            results_path=config.results_path,
            next_cursor_path=config.next_cursor_path,
            logger=logger,
        )

        raw_rows = [
            transform_raw_record(
                connector_name=config.connector_name,
                source_system=config.source_system,
                endpoint_name=endpoint_name,
                run_id=run_id,
                record=record,
            )
            for record in all_results
        ]

        logger.info("Replacing raw table with %s rows", len(raw_rows))
        replace_raw_table_with_rows(client, raw_table_id, raw_rows)

        finished_at = datetime.now(timezone.utc).isoformat()

        success_row = build_run_log_row(
            run_id=run_id,
            connector_name=config.connector_name,
            source_system=config.source_system,
            endpoint_name=endpoint_name,
            load_mode=config.load_mode,
            status="success",
            started_at=started_at,
            finished_at=finished_at,
            pages_read=((len(all_results) - 1) // config.page_size) + 1 if all_results else 0,
            records_loaded=len(raw_rows),
            error_message=None,
        )
        write_run_log_row(client, run_log_table_id, success_row)

        logger.info("Load completed successfully")
        logger.info("Rows written to raw table: %s", len(raw_rows))

    except Exception as exc:
        finished_at = datetime.now(timezone.utc).isoformat()

        error_row = build_run_log_row(
            run_id=run_id,
            connector_name=config.connector_name,
            source_system=config.source_system,
            endpoint_name=endpoint_name,
            load_mode=config.load_mode,
            status="failed",
            started_at=started_at,
            finished_at=finished_at,
            error_message=str(exc),
        )
        write_run_log_row(client, run_log_table_id, error_row)
        logger.exception("Run failed")
        raise


if __name__ == "__main__":
    main()
