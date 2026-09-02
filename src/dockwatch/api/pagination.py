"""Opaque cursor pagination helpers.

Cursors are base64url-encoded values of whatever column a listing is
keyset-paginated on, so a client never needs to know or depend on the
underlying sort key's type or format.
"""

from __future__ import annotations

import base64
import binascii

from fastapi import HTTPException

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


def encode_cursor(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")


def decode_cursor(cursor: str) -> str:
    try:
        return base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail="invalid cursor") from exc
