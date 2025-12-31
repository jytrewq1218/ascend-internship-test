import math
from typing import Any, Optional


def to_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        if math.isfinite(x):
            return int(x)
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        try:
            f = float(s)
            return int(f) if math.isfinite(f) else None
        except Exception:
            return None


def to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        f = float(x)
        return f if math.isfinite(f) else None
    s = str(x).strip()
    if not s:
        return None
    try:
        f = float(s)
        return f if math.isfinite(f) else None
    except Exception:
        return None


def to_bool(x: Any) -> Optional[bool]:
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    if not s:
        return None
    if s == "true":
        return True
    if s == "false":
        return False
    return None
