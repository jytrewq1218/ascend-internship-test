import time
from typing import Any, Optional


def now_us() -> int:
    return int(time.time() * 1_000_000)


def ms_to_us(ms: Any) -> Optional[int]:
    try:
        if ms is None:
            return None
        return int(float(ms)) * 1_000
    except Exception:
        return None
