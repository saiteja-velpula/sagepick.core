import uuid
import logging
import json
from typing import Optional, Dict, Any
from contextvars import ContextVar
from fastapi import Request
from datetime import datetime

# Context variable to store correlation ID
correlation_id_var: ContextVar[Optional[str]] = ContextVar('correlation_id', default=None)


class CorrelationIdFormatter(logging.Formatter):
    """Custom formatter that adds correlation ID to log records."""
    
    def format(self, record):
        correlation_id = correlation_id_var.get()
        if correlation_id:
            record.correlation_id = correlation_id
        else:
            record.correlation_id = "no-correlation-id"
        
        return super().format(record)


class JSONFormatter(logging.Formatter):
    """Formatter that outputs logs in JSON format."""
    
    def format(self, record):
        correlation_id = correlation_id_var.get()
        
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": correlation_id or "no-correlation-id",
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add any extra fields
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                          'filename', 'module', 'exc_info', 'exc_text', 'stack_info',
                          'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
                          'thread', 'threadName', 'processName', 'process', 'getMessage']:
                log_entry[key] = value
        
        return json.dumps(log_entry, default=str)


def get_correlation_id() -> Optional[str]:
    """Get the current correlation ID."""
    return correlation_id_var.get()


def set_correlation_id(correlation_id: str) -> None:
    """Set the correlation ID for the current context."""
    correlation_id_var.set(correlation_id)


def generate_correlation_id() -> str:
    """Generate a new correlation ID."""
    return str(uuid.uuid4())


def extract_correlation_id_from_request(request: Request) -> str:
    """Extract or generate correlation ID from request headers."""
    # Check for existing correlation ID in headers
    correlation_id = request.headers.get("x-correlation-id")
    
    if not correlation_id:
        # Generate new correlation ID
        correlation_id = generate_correlation_id()
    
    return correlation_id


class StructuredLogger:
    """Structured logger with correlation ID support."""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
    
    def _log(self, level: int, message: str, **kwargs):
        """Internal logging method with structured data."""
        extra = {k: v for k, v in kwargs.items() if k != 'exc_info'}
        self.logger.log(level, message, extra=extra, exc_info=kwargs.get('exc_info'))
    
    def debug(self, message: str, **kwargs):
        self._log(logging.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs):
        self._log(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self._log(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self._log(logging.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        self._log(logging.CRITICAL, message, **kwargs)


def setup_logging(use_json: bool = False, correlation_id_in_format: bool = True):
    """Setup structured logging configuration."""
    # Remove all existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create new handler
    handler = logging.StreamHandler()
    
    if use_json:
        formatter = JSONFormatter()
    elif correlation_id_in_format:
        formatter = CorrelationIdFormatter(
            '%(asctime)s - %(correlation_id)s - %(name)s - %(levelname)s - %(message)s'
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)


def get_structured_logger(name: str) -> StructuredLogger:
    """Get a structured logger instance."""
    return StructuredLogger(name)