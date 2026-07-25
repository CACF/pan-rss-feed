from functools import lru_cache
from fake_useragent import UserAgent


@lru_cache(maxsize=1)
def _get_user_agent_rotator():
    try:
        return UserAgent()
    except Exception:
        return None


def get_random_headers(base: dict | None = None) -> dict:
    """Return headers with a randomized User-Agent merged over base.

    Fallbacks to a static UA if fake_useragent fails.
    """
    headers = dict(base or {})
    rotator = _get_user_agent_rotator()
    ua_value = None
    try:
        if rotator is not None:
            ua_value = rotator.random
    except Exception:
        ua_value = None
    if not ua_value:
        ua_value = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
    headers.setdefault("User-Agent", ua_value)
    headers.setdefault("Accept-Language", "en-US,en;q=0.9")
    return headers
