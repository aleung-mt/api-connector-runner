import requests


def get_nested_value(data: dict, path: str):
    """
    Read a nested value from a dict using dot notation.
    Example: "paging.next.after"
    """
    current = data
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def build_endpoint(endpoint_template: str, form_guid: str) -> str:
    return endpoint_template.format(form_guid=form_guid)


def fetch_page(
    api_base_url: str,
    endpoint: str,
    bearer_token: str,
    page_size: int,
    after: str | None = None,
) -> dict:
    url = f"{api_base_url}{endpoint}"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    params = {
        "limit": page_size,
    }

    if after:
        params["after"] = after

    response = requests.get(url, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def fetch_all_pages(
    api_base_url: str,
    endpoint: str,
    bearer_token: str,
    page_size: int,
    results_path: str,
    next_cursor_path: str,
    logger,
) -> list[dict]:
    all_results: list[dict] = []
    after = None
    page_number = 0

    while True:
        page_number += 1

        payload = fetch_page(
            api_base_url=api_base_url,
            endpoint=endpoint,
            bearer_token=bearer_token,
            page_size=page_size,
            after=after,
        )

        results = get_nested_value(payload, results_path)
        next_cursor = get_nested_value(payload, next_cursor_path)

        if results is None:
            raise ValueError(f"RESULTS_PATH '{results_path}' not found in API response")

        if not isinstance(results, list):
            raise ValueError(f"RESULTS_PATH '{results_path}' did not resolve to a list")

        all_results.extend(results)

        logger.info(
            "Fetched page %s with %s records. Running total=%s",
            page_number,
            len(results),
            len(all_results),
        )

        if not next_cursor:
            logger.info("No next cursor found after page %s. Pagination complete.", page_number)
            break

        after = str(next_cursor)

    return all_results
