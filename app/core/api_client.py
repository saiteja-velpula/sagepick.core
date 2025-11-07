import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    attempts: int = 3
    backoff_ms: int = 250
    retry_on_status: set[int] = None

    def __post_init__(self):
        if self.retry_on_status is None:
            self.retry_on_status = {408, 425, 429, 500, 502, 503, 504}


class ApiClient:
    def __init__(
        self,
        base_url: str,
        headers: dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
        timeout: float = 10.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.default_headers = {
            "Content-Type": "application/json",
            "User-Agent": "SAGEPICK-Core/1.0",
            **(headers or {}),
        }
        self.retry_config = retry_config or RetryConfig()
        self.timeout = timeout

        # Create async client with connection pooling
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        if hasattr(self, "_client"):
            await self._client.aclose()

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        request_headers = {**self.default_headers, **(headers or {})}

        max_attempts = max(1, self.retry_config.attempts)
        attempt = 1
        last_exception = None

        while attempt <= max_attempts:
            try:
                response = await self._client.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_data,
                    headers=request_headers,
                )

                # Check if response is successful
                if response.is_success:
                    if not response.content:
                        return {}
                    try:
                        return response.json()
                    except ValueError as exc:
                        logger.error(
                            "Failed to decode JSON response from %s: %s", url, exc
                        )
                        raise

                # Check if we should retry based on status code
                if (
                    attempt < max_attempts
                    and response.status_code in self.retry_config.retry_on_status
                ):
                    await self._delay(attempt - 1)
                    attempt += 1
                    continue

                # Raise HTTP error for non-retryable status codes
                response.raise_for_status()

            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
                last_exception = e
                logger.warning("Network error on attempt %s: %s", attempt, e)

                if attempt < max_attempts:
                    await self._delay(attempt - 1)
                    attempt += 1
                    continue
                else:
                    raise

            except httpx.HTTPStatusError as e:
                last_exception = e
                logger.error(f"HTTP error {e.response.status_code}: {e.response.text}")
                raise

            except Exception as e:
                last_exception = e
                logger.error(f"Unexpected error: {e}")
                raise

        # If we get here, all retries failed
        if last_exception:
            raise last_exception
        raise Exception("Request failed after all retry attempts")

    async def _delay(self, attempt: int):
        delay_ms = self.retry_config.backoff_ms * (2**attempt)
        delay_seconds = delay_ms / 1000
        logger.debug(f"Retrying after {delay_seconds}s delay...")
        await asyncio.sleep(delay_seconds)

    async def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        return await self._request("GET", endpoint, params=params, headers=headers)

    async def post(
        self,
        endpoint: str,
        json_data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "POST", endpoint, params=params, json_data=json_data, headers=headers
        )

    async def put(
        self,
        endpoint: str,
        json_data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "PUT", endpoint, params=params, json_data=json_data, headers=headers
        )

    async def delete(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        return await self._request("DELETE", endpoint, params=params, headers=headers)
