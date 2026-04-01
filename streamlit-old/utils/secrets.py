"""Secret/config loading helpers."""

import importlib
import os


def load_gemini_api_key() -> str:
    """Load Gemini API key from env first, then local secret modules."""
    env_key = os.getenv("GEMINI_API_KEY")
    if env_key:
        return env_key

    module_candidates = (
        "secret_config",
        "utils.secret_config",
    )
    for module_name in module_candidates:
        try:
            module = importlib.import_module(module_name)
            secret_key = getattr(module, "GEMINI_API_KEY", "")
            if secret_key:
                return secret_key
        except ModuleNotFoundError:
            continue

    return ""
