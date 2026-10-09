import secrets


def is_valid_api_key(supplied: str | None, expected: str) -> bool:
    return supplied is not None and secrets.compare_digest(supplied.encode(), expected.encode())
