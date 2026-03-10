from app.config import load_config
from app.logger import setup_logger


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


if __name__ == "__main__":
    main()
