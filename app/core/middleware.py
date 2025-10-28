import time
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from .logging import (
    extract_correlation_id_from_request,
    set_correlation_id,
    get_structured_logger
)

logger = get_structured_logger(__name__)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Middleware to handle correlation IDs and request logging."""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Extract or generate correlation ID
        correlation_id = extract_correlation_id_from_request(request)
        
        # Set correlation ID in context
        set_correlation_id(correlation_id)
        
        # Log request start
        start_time = time.time()
        logger.info(
            "Request started",
            method=request.method,
            url=str(request.url),
            client_ip=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
        )
        
        try:
            # Process request
            response = await call_next(request)
            
            # Add correlation ID to response headers
            response.headers["x-correlation-id"] = correlation_id
            
            # Log request completion
            duration = time.time() - start_time
            logger.info(
                "Request completed",
                method=request.method,
                url=str(request.url),
                status_code=response.status_code,
                duration_ms=round(duration * 1000, 2),
            )
            
            return response
            
        except Exception as e:
            # Log request error
            duration = time.time() - start_time
            logger.error(
                "Request failed",
                method=request.method,
                url=str(request.url),
                duration_ms=round(duration * 1000, 2),
                error=str(e),
                exc_info=True,
            )
            raise