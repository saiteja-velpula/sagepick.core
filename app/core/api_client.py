import asyncio
from typing import Dict, Any, Optional, Set
import httpx
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    attempts: int = 3
    backoff_ms: int = 250
    retry_on_status: Set[int] = None
    
    def __post_init__(self):
        if self.retry_on_status is None:
            self.retry_on_status = {408, 425, 429, 500, 502, 503, 504}


class ApiClient:
    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        retry_config: Optional[RetryConfig] = None,
        timeout: float = 10.0
    ):
        self.base_url = base_url.rstrip('/')
        self.default_headers = {
            "Content-Type": "application/json",
            "User-Agent": "SAGEPICK-Core/1.0",
            **(headers or {})
        }
        self.retry_config = retry_config or RetryConfig()
        self.timeout = timeout
        
        # Create async client with connection pooling
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def close(self):
        if hasattr(self, '_client'):
            await self._client.aclose()
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        request_headers = {**self.default_headers, **(headers or {})}
        
        attempt = 0
        last_exception = None
        
        while attempt <= self.retry_config.attempts:
            try:
                response = await self._client.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_data,
                    headers=request_headers
                )
                
                # Check if response is successful
                if response.is_success:
                    return response.json()
                
                # Check if we should retry based on status code
                if (attempt < self.retry_config.attempts and 
                    response.status_code in self.retry_config.retry_on_status):
                    await self._delay(attempt)
                    attempt += 1
                    continue
                
                # Raise HTTP error for non-retryable status codes
                response.raise_for_status()
                
            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
                last_exception = e
                logger.warning(f"Network error on attempt {attempt + 1}: {e}")
                
                if attempt < self.retry_config.attempts:
                    await self._delay(attempt)
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
        delay_ms = self.retry_config.backoff_ms * (2 ** attempt)
        delay_seconds = delay_ms / 1000
        logger.debug(f"Retrying after {delay_seconds}s delay...")
        await asyncio.sleep(delay_seconds)
    
    async def get(
        self, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        return await self._request("GET", endpoint, params=params, headers=headers)
    
    async def post(
        self,
        endpoint: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        return await self._request("POST", endpoint, params=params, json_data=json_data, headers=headers)
    
    async def put(
        self,
        endpoint: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        return await self._request("PUT", endpoint, params=params, json_data=json_data, headers=headers)
    
    async def delete(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        return await self._request("DELETE", endpoint, params=params, headers=headers)