from app.config import load_config
from app.logger import setup_logger
from app.api_client import build_endpoint, fetch_first_page, get_nested_value


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

    endpoint = build_endpoint(config.endpoint_template, config.form_guid)
    logger.info("Built endpoint: %s", endpoint)

    payload = fetch_first_page(
        api_base_url=config.api_base_url,
        endpoint=endpoint,
        bearer_token=config.api_bearer_token,
        page_size=config.page_size,
    )

    results = get_nested_value(payload, config.results_path)
    next_cursor = get_nested_value(payload, config.next_cursor_path)

    if results is None:
        raise ValueError(f"RESULTS_PATH '{config.results_path}' not found in API response")

    if not isinstance(results, list):
        raise ValueError(f"RESULTS_PATH '{config.results_path}' did not resolve to a list")

    logger.info("Fetched first page successfully")
    logger.info("Records returned on first page: %s", len(results))
    logger.info("Next cursor present: %s", "yes" if next_cursor else "no")


if __name__ == "__main__":
    main()
