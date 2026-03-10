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


def fetch_first_page(
    api_base_url: str,
    endpoint: str,
    bearer_token: str,
    page_size: int,
) -> dict:
    url = f"{api_base_url}{endpoint}"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    params = {
        "limit": page_size,
    }

    response = requests.get(url, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    return response.json()
