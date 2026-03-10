import os
from dataclasses import dataclass


def get_env(name: str, required: bool = True, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return value


@dataclass
class ConnectorConfig:
    connector_name: str
    source_system: str
    api_base_url: str
    endpoint_template: str
    form_guid: str
    page_size: int
    results_path: str
    next_cursor_path: str
    bq_project_id: str
    bq_dataset: str
    bq_raw_table: str
    bq_run_log_table: str
    load_mode: str
    api_bearer_token: str


def load_config() -> ConnectorConfig:
    config = ConnectorConfig(
        connector_name=get_env("CONNECTOR_NAME"),
        source_system=get_env("SOURCE_SYSTEM"),
        api_base_url=get_env("API_BASE_URL"),
        endpoint_template=get_env("ENDPOINT_TEMPLATE"),
        form_guid=get_env("FORM_GUID"),
        page_size=int(get_env("PAGE_SIZE")),
        results_path=get_env("RESULTS_PATH"),
        next_cursor_path=get_env("NEXT_CURSOR_PATH"),
        bq_project_id=get_env("BQ_PROJECT_ID"),
        bq_dataset=get_env("BQ_DATASET"),
        bq_raw_table=get_env("BQ_RAW_TABLE"),
        bq_run_log_table=get_env("BQ_RUN_LOG_TABLE"),
        load_mode=get_env("LOAD_MODE"),
        api_bearer_token=get_env("API_BEARER_TOKEN"),
    )

    validate_config(config)
    return config


def validate_config(config: ConnectorConfig) -> None:
    if config.load_mode != "full_refresh":
        raise ValueError("Only LOAD_MODE=full_refresh is supported in MVP")

    if config.page_size <= 0:
        raise ValueError("PAGE_SIZE must be greater than 0")

    if "{form_guid}" not in config.endpoint_template:
        raise ValueError("ENDPOINT_TEMPLATE must include '{form_guid}'")

    if not config.api_base_url.startswith("http"):
        raise ValueError("API_BASE_URL must start with http or https")
