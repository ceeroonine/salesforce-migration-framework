"""Data transformation engine for Salesforce migrations."""

from typing import Any, Dict, List, Optional, Callable
from pydantic import BaseModel, Field
from datetime import datetime, date
import re
from abc import ABC, abstractmethod


class TransformationRule(BaseModel):
    """Defines a single transformation rule."""
    name: str = Field(description="Rule identifier")
    rule_type: str = Field(description="Type: built-in or custom")
    config: Dict[str, Any] = Field(default_factory=dict, description="Rule-specific config")


class BaseTransformer(ABC):
    """Base class for custom transformers."""

    @abstractmethod
    def transform(self, value: Any, config: Dict[str, Any] = None) -> Any:
        """Apply transformation to value.
        
        Args:
            value: Value to transform
            config: Transformation configuration
            
        Returns:
            Transformed value
        """
        pass


class BuiltInTransformers:
    """Built-in transformation functions for common Salesforce data types."""

    @staticmethod
    def format_phone(value: Any, config: Dict[str, Any] = None) -> Optional[str]:
        """Format phone number.
        
        Args:
            value: Phone number string
            config: Format config (e.g., {'format': '+1-XXXXX'})
            
        Returns:
            Formatted phone number
        """
        if not value:
            return None
        
        # Remove non-digits
        digits = re.sub(r'\D', '', str(value))
        
        # Format as US phone if 10 digits
        if len(digits) == 10:
            return f"+1-{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"+1-{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
        
        return digits

    @staticmethod
    def normalize_text(value: Any, config: Dict[str, Any] = None) -> Optional[str]:
        """Normalize text (trim, uppercase/lowercase).
        
        Args:
            value: Text value
            config: Normalization config
            
        Returns:
            Normalized text
        """
        if not value:
            return None
        
        text = str(value).strip()
        
        if config:
            if config.get('uppercase'):
                text = text.upper()
            elif config.get('lowercase'):
                text = text.lower()
            elif config.get('titlecase'):
                text = text.title()
        
        return text

    @staticmethod
    def datetime_to_iso(value: Any, config: Dict[str, Any] = None) -> Optional[str]:
        """Convert datetime to ISO 8601 format.
        
        Args:
            value: DateTime value
            config: Conversion config
            
        Returns:
            ISO 8601 formatted datetime
        """
        if not value:
            return None
        
        if isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, date):
            return value.isoformat()
        
        try:
            dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            return dt.isoformat()
        except (ValueError, AttributeError):
            return str(value)

    @staticmethod
    def parse_date(value: Any, config: Dict[str, Any] = None) -> Optional[str]:
        """Parse date string to YYYY-MM-DD format.
        
        Args:
            value: Date value
            config: Parse config
            
        Returns:
            Date in YYYY-MM-DD format
        """
        if not value:
            return None
        
        if isinstance(value, date):
            return value.isoformat()
        
        try:
            dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            return dt.date().isoformat()
        except (ValueError, AttributeError):
            return str(value)[:10]

    @staticmethod
    def string_to_number(value: Any, config: Dict[str, Any] = None) -> Optional[float]:
        """Convert string to number.
        
        Args:
            value: String value
            config: Conversion config
            
        Returns:
            Numeric value
        """
        if value is None or value == '':
            return None
        
        try:
            # Remove common currency symbols
            clean = str(value).replace('$', '').replace(',', '').strip()
            return float(clean) if clean else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def boolean_from_text(value: Any, config: Dict[str, Any] = None) -> Optional[bool]:
        """Convert text to boolean.
        
        Args:
            value: Text value
            config: Parse config (true_values, false_values)
            
        Returns:
            Boolean value
        """
        if value is None:
            return None
        
        if isinstance(value, bool):
            return value
        
        config = config or {}
        true_values = config.get('true_values', ['yes', 'true', '1', 'y', 't'])
        false_values = config.get('false_values', ['no', 'false', '0', 'n', 'f'])
        
        text = str(value).lower().strip()
        
        if text in true_values:
            return True
        elif text in false_values:
            return False
        
        return None

    @staticmethod
    def concat_fields(value: Any, config: Dict[str, Any] = None) -> Optional[str]:
        """Concatenate multiple field values.
        
        Args:
            value: Dict with field values
            config: Config with 'fields', 'separator', 'skip_empty'
            
        Returns:
            Concatenated string
        """
        if not isinstance(value, dict) or not config:
            return str(value) if value else None
        
        fields = config.get('fields', [])
        separator = config.get('separator', ' ')
        skip_empty = config.get('skip_empty', True)
        
        parts = []
        for field in fields:
            val = value.get(field)
            if val is not None:
                if skip_empty and (val == '' or val == []):
                    continue
                parts.append(str(val))
        
        return separator.join(parts) if parts else None


class DataTransformer:
    """Applies transformations to field values during migration."""

    def __init__(self, custom_transformers: Dict[str, BaseTransformer] = None):
        """Initialize transformer.
        
        Args:
            custom_transformers: Dict of custom transformer instances
        """
        self.custom_transformers = custom_transformers or {}
        self.builtin_transformers = {
            'format_phone': BuiltInTransformers.format_phone,
            'normalize_text': BuiltInTransformers.normalize_text,
            'datetime_to_iso': BuiltInTransformers.datetime_to_iso,
            'parse_date': BuiltInTransformers.parse_date,
            'string_to_number': BuiltInTransformers.string_to_number,
            'boolean_from_text': BuiltInTransformers.boolean_from_text,
            'concat_fields': BuiltInTransformers.concat_fields,
        }

    def apply_transformation(
        self,
        transformer_name: str,
        value: Any,
        config: Dict[str, Any] = None
    ) -> Any:
        """Apply a named transformation.
        
        Args:
            transformer_name: Name of transformer
            value: Value to transform
            config: Transformation configuration
            
        Returns:
            Transformed value
            
        Raises:
            ValueError: If transformer not found
        """
        if transformer_name in self.builtin_transformers:
            return self.builtin_transformers[transformer_name](value, config)
        
        if transformer_name in self.custom_transformers:
            return self.custom_transformers[transformer_name].transform(value, config)
        
        raise ValueError(f"Transformer '{transformer_name}' not found")

    def transform_record(
        self,
        record: Dict[str, Any],
        field_mappings: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Apply transformations to all fields in a record.
        
        Args:
            record: Record with field values
            field_mappings: List of field mapping configs with transformers
            
        Returns:
            Record with values transformed
        """
        transformed = {}
        
        for mapping in field_mappings:
            source_field = mapping.get('source_field')
            target_field = mapping.get('target_field')
            transformer_name = mapping.get('transformer')
            
            if not source_field or not target_field:
                continue
            
            value = record.get(source_field)
            
            if transformer_name and value is not None:
                try:
                    config = mapping.get('transformer_config')
                    value = self.apply_transformation(transformer_name, value, config)
                except Exception as e:
                    raise ValueError(
                        f"Transformation failed for {source_field} "
                        f"with transformer '{transformer_name}': {str(e)}"
                    )
            
            transformed[target_field] = value
        
        return transformed
