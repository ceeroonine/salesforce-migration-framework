"""Data validation engine for Salesforce migrations."""

from typing import Any, Dict, List, Optional, Callable
from pydantic import BaseModel, Field
from abc import ABC, abstractmethod
import re
from datetime import datetime, date


class ValidationRule(BaseModel):
    """Defines a single validation rule."""
    name: str = Field(description="Validator identifier")
    rule_type: str = Field(description="Type of validation")
    field: str = Field(description="Field being validated")
    config: Dict[str, Any] = Field(default_factory=dict, description="Validation config")
    is_error: bool = Field(default=True, description="Fail migration if violated")


class ValidationResult(BaseModel):
    """Result of a validation check."""
    valid: bool = Field(description="Validation passed")
    field: str = Field(description="Field validated")
    message: str = Field(description="Validation message")
    is_error: bool = Field(default=True, description="Error or warning")
    rule_name: Optional[str] = Field(None, description="Rule that failed")


class BaseValidator(ABC):
    """Base class for custom validators."""

    @abstractmethod
    def validate(self, value: Any, config: Dict[str, Any] = None) -> tuple[bool, str]:
        """Validate a value.
        
        Args:
            value: Value to validate
            config: Validation configuration
            
        Returns:
            Tuple of (is_valid, message)
        """
        pass


class BuiltInValidators:
    """Built-in validators for common validation patterns."""

    @staticmethod
    def not_null(value: Any, config: Dict[str, Any] = None) -> tuple[bool, str]:
        """Validate that value is not null/empty."""
        if value is None or value == '' or value == []:
            return False, "Value cannot be empty"
        return True, "Valid"

    @staticmethod
    def min_length(value: Any, config: Dict[str, Any] = None) -> tuple[bool, str]:
        """Validate minimum length."""
        config = config or {}
        min_len = config.get('min_length', 1)
        
        if not value:
            return False, f"Value must have minimum length of {min_len}"
        
        if len(str(value)) < min_len:
            return False, f"Value '{value}' is shorter than {min_len} characters"
        
        return True, "Valid"

    @staticmethod
    def max_length(value: Any, config: Dict[str, Any] = None) -> tuple[bool, str]:
        """Validate maximum length."""
        config = config or {}
        max_len = config.get('max_length', 255)
        
        if len(str(value)) > max_len:
            return False, f"Value exceeds maximum length of {max_len}"
        
        return True, "Valid"

    @staticmethod
    def email_format(value: Any, config: Dict[str, Any] = None) -> tuple[bool, str]:
        """Validate email format."""
        if not value:
            return False, "Email cannot be empty"
        
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(pattern, str(value)):
            return False, f"'{value}' is not a valid email format"
        
        return True, "Valid"

    @staticmethod
    def phone_format(value: Any, config: Dict[str, Any] = None) -> tuple[bool, str]:
        """Validate phone format (digits only)."""
        if not value:
            return False, "Phone cannot be empty"
        
        digits = re.sub(r'\D', '', str(value))
        
        if len(digits) < 10:
            return False, f"Phone number must have at least 10 digits"
        
        if len(digits) > 15:
            return False, f"Phone number cannot exceed 15 digits"
        
        return True, "Valid"

    @staticmethod
    def url_format(value: Any, config: Dict[str, Any] = None) -> tuple[bool, str]:
        """Validate URL format."""
        if not value:
            return False, "URL cannot be empty"
        
        pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        if not re.match(pattern, str(value), re.IGNORECASE):
            return False, f"'{value}' is not a valid URL"
        
        return True, "Valid"

    @staticmethod
    def numeric_range(value: Any, config: Dict[str, Any] = None) -> tuple[bool, str]:
        """Validate numeric range."""
        config = config or {}
        min_val = config.get('min')
        max_val = config.get('max')
        
        try:
            num = float(value)
        except (TypeError, ValueError):
            return False, f"'{value}' is not a number"
        
        if min_val is not None and num < min_val:
            return False, f"Value {num} is below minimum {min_val}"
        
        if max_val is not None and num > max_val:
            return False, f"Value {num} exceeds maximum {max_val}"
        
        return True, "Valid"

    @staticmethod
    def in_list(value: Any, config: Dict[str, Any] = None) -> tuple[bool, str]:
        """Validate value is in allowed list."""
        config = config or {}
        allowed = config.get('allowed_values', [])
        
        if value not in allowed:
            return False, f"'{value}' is not in allowed values: {allowed}"
        
        return True, "Valid"

    @staticmethod
    def regex_match(value: Any, config: Dict[str, Any] = None) -> tuple[bool, str]:
        """Validate value matches regex pattern."""
        config = config or {}
        pattern = config.get('pattern')
        
        if not pattern:
            return False, "No regex pattern provided"
        
        try:
            if not re.match(pattern, str(value)):
                return False, f"'{value}' does not match pattern {pattern}"
        except re.error as e:
            return False, f"Invalid regex pattern: {e}"
        
        return True, "Valid"

    @staticmethod
    def date_format(value: Any, config: Dict[str, Any] = None) -> tuple[bool, str]:
        """Validate date format."""
        if not value:
            return False, "Date cannot be empty"
        
        try:
            if isinstance(value, (date, datetime)):
                return True, "Valid"
            
            datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            return True, "Valid"
        except (ValueError, AttributeError):
            return False, f"'{value}' is not a valid date format"

    @staticmethod
    def unique_value(value: Any, config: Dict[str, Any] = None) -> tuple[bool, str]:
        """Validate value is unique (requires context)."""
        # Note: This is a placeholder. Actual uniqueness check requires
        # comparison against existing records, typically done at batch level
        if not value or value == '':
            return False, "Value cannot be empty for uniqueness check"
        
        return True, "Valid (uniqueness check requires batch context)"


class DataValidator:
    """Validates data against defined rules."""

    def __init__(self, custom_validators: Dict[str, BaseValidator] = None):
        """Initialize validator.
        
        Args:
            custom_validators: Dict of custom validator instances
        """
        self.custom_validators = custom_validators or {}
        self.builtin_validators = {
            'not_null': BuiltInValidators.not_null,
            'min_length': BuiltInValidators.min_length,
            'max_length': BuiltInValidators.max_length,
            'email_format': BuiltInValidators.email_format,
            'phone_format': BuiltInValidators.phone_format,
            'url_format': BuiltInValidators.url_format,
            'numeric_range': BuiltInValidators.numeric_range,
            'in_list': BuiltInValidators.in_list,
            'regex_match': BuiltInValidators.regex_match,
            'date_format': BuiltInValidators.date_format,
            'unique_value': BuiltInValidators.unique_value,
        }

    def validate_value(
        self,
        rule_type: str,
        value: Any,
        config: Dict[str, Any] = None
    ) -> tuple[bool, str]:
        """Validate a single value against a rule.
        
        Args:
            rule_type: Type of validation rule
            value: Value to validate
            config: Validation configuration
            
        Returns:
            Tuple of (is_valid, message)
            
        Raises:
            ValueError: If validator not found
        """
        if rule_type in self.builtin_validators:
            return self.builtin_validators[rule_type](value, config)
        
        if rule_type in self.custom_validators:
            return self.custom_validators[rule_type].validate(value, config)
        
        raise ValueError(f"Validator '{rule_type}' not found")

    def validate_record(
        self,
        record: Dict[str, Any],
        validation_rules: List[ValidationRule]
    ) -> tuple[bool, List[ValidationResult]]:
        """Validate all fields in a record.
        
        Args:
            record: Record data to validate
            validation_rules: List of validation rules
            
        Returns:
            Tuple of (record_valid, list of validation results)
        """
        results = []
        record_valid = True
        
        for rule in validation_rules:
            value = record.get(rule.field)
            
            try:
                is_valid, message = self.validate_value(
                    rule.rule_type,
                    value,
                    rule.config
                )
                
                result = ValidationResult(
                    valid=is_valid,
                    field=rule.field,
                    message=message,
                    is_error=rule.is_error,
                    rule_name=rule.name
                )
                results.append(result)
                
                if not is_valid and rule.is_error:
                    record_valid = False
                    
            except Exception as e:
                result = ValidationResult(
                    valid=False,
                    field=rule.field,
                    message=f"Validation error: {str(e)}",
                    is_error=True,
                    rule_name=rule.name
                )
                results.append(result)
                record_valid = False
        
        return record_valid, results

    def get_validation_summary(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """Generate summary statistics from validation results.
        
        Args:
            results: List of validation results
            
        Returns:
            Summary dict with counts and details
        """
        errors = [r for r in results if not r.valid and r.is_error]
        warnings = [r for r in results if not r.valid and not r.is_error]
        
        return {
            'total_checks': len(results),
            'passed': len(results) - len(errors) - len(warnings),
            'errors': len(errors),
            'warnings': len(warnings),
            'error_details': [r.model_dump() for r in errors],
            'warning_details': [r.model_dump() for r in warnings],
        }
