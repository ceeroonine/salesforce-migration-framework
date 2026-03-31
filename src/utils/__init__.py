"""Shared utilities for the migration framework."""

import logging
import json
from datetime import datetime
from typing import Any, Dict


def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Setup a configured logger.
    
    Args:
        name: Logger name
        level: Logging level
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level))
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime and other types."""
    
    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)
