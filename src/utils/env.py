import os

def get_required_env(var_name: str, default=None) -> str:
    value = os.environ.get(var_name, default)
    if value is None:
        raise ValueError(f"Required environment variable '{var_name}' is not set.")
    return value
