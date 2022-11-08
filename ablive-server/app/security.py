import secrets
from typing import Optional

from fastapi import Header, status
from fastapi.exceptions import HTTPException

from .config import settings


async def verify_api_key(
    x_api_key: Optional[str] = Header(),
) -> None:
    auth_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='invalid api key',
    )
    if not x_api_key:
        raise auth_exception

    if not secrets.compare_digest(x_api_key, settings.API_KEY):
        raise auth_exception
