from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def json_default(value: Any) -> str | float:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def dumps(payload: Any) -> bytes:
    return json.dumps(payload, default=json_default, separators=(",", ":")).encode("utf-8")


def loads(payload: bytes | str) -> Any:
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    return json.loads(payload)

