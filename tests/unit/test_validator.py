"""Tests for validator functionality."""

import pytest
from src.core.validator import DataValidator, BuiltInValidators, ValidationRule


class TestBuiltInValidators:
    """Test built-in validator functions."""

    def test_not_null_valid(self):
        """Test not_null validator with valid value."""
        valid, message = BuiltInValidators.not_null("value")
        assert valid is True

    def test_not_null_invalid(self):
        """Test not_null validator with null value."""
        valid, message = BuiltInValidators.not_null(None)
        assert valid is False
        
        valid, message = BuiltInValidators.not_null("")
        assert valid is False

    def test_min_length_valid(self):
        """Test min_length validator with valid value."""
        valid, message = BuiltInValidators.min_length(
            "hello",
            {"min_length": 3}
        )
        assert valid is True

    def test_min_length_invalid(self):
        """Test min_length validator with short value."""
        valid, message = BuiltInValidators.min_length(
            "hi",
            {"min_length": 3}
        )
        assert valid is False

    def test_max_length_valid(self):
        """Test max_length validator with valid value."""
        valid, message = BuiltInValidators.max_length(
            "hello",
            {"max_length": 10}
        )
        assert valid is True

    def test_max_length_invalid(self):
        """Test max_length validator with long value."""
        valid, message = BuiltInValidators.max_length(
            "this is a very long string",
            {"max_length": 10}
        )
        assert valid is False

    def test_email_format_valid(self):
        """Test email format validator with valid email."""
        valid, message = BuiltInValidators.email_format("user@example.com")
        assert valid is True

    def test_email_format_invalid(self):
        """Test email format validator with invalid email."""
        valid, message = BuiltInValidators.email_format("not-an-email")
        assert valid is False

    def test_phone_format_valid(self):
        """Test phone format validator with valid phone."""
        valid, message = BuiltInValidators.phone_format("5551234567")
        assert valid is True

    def test_phone_format_invalid(self):
        """Test phone format validator with invalid phone."""
        valid, message = BuiltInValidators.phone_format("123")  # Too short
        assert valid is False

    def test_url_format_valid(self):
        """Test URL format validator with valid URL."""
        valid, message = BuiltInValidators.url_format("https://example.com")
        assert valid is True

    def test_url_format_invalid(self):
        """Test URL format validator with invalid URL."""
        valid, message = BuiltInValidators.url_format("not a url")
        assert valid is False

    def test_numeric_range_valid(self):
        """Test numeric range validator with valid value."""
        valid, message = BuiltInValidators.numeric_range(
            50,
            {"min": 0, "max": 100}
        )
        assert valid is True

    def test_numeric_range_below_min(self):
        """Test numeric range validator with value below minimum."""
        valid, message = BuiltInValidators.numeric_range(
            -10,
            {"min": 0, "max": 100}
        )
        assert valid is False

    def test_in_list_valid(self):
        """Test in_list validator with valid value."""
        valid, message = BuiltInValidators.in_list(
            "Active",
            {"allowed_values": ["Active", "Inactive"]}
        )
        assert valid is True

    def test_in_list_invalid(self):
        """Test in_list validator with invalid value."""
        valid, message = BuiltInValidators.in_list(
            "Unknown",
            {"allowed_values": ["Active", "Inactive"]}
        )
        assert valid is False

    def test_regex_match_valid(self):
        """Test regex match validator with valid value."""
        valid, message = BuiltInValidators.regex_match(
            "ABC123",
            {"pattern": r"^[A-Z]{3}[0-9]{3}$"}
        )
        assert valid is True

    def test_regex_match_invalid(self):
        """Test regex match validator with invalid value."""
        valid, message = BuiltInValidators.regex_match(
            "abc123",
            {"pattern": r"^[A-Z]{3}[0-9]{3}$"}
        )
        assert valid is False

    def test_date_format_valid(self):
        """Test date format validator with valid date."""
        valid, message = BuiltInValidators.date_format("2024-01-15")
        assert valid is True

    def test_date_format_invalid(self):
        """Test date format validator with invalid date."""
        valid, message = BuiltInValidators.date_format("not-a-date")
        assert valid is False


class TestDataValidator:
    """Test DataValidator class."""

    def test_validator_initialization(self):
        """Test validator initialization."""
        validator = DataValidator()
        assert len(validator.builtin_validators) > 0

    def test_validate_value_builtin(self):
        """Test validating with built-in validator."""
        validator = DataValidator()
        valid, message = validator.validate_value("email_format", "user@example.com")
        assert valid is True

    def test_validate_value_not_found(self):
        """Test error when validator not found."""
        validator = DataValidator()
        with pytest.raises(ValueError, match="Validator.*not found"):
            validator.validate_value("nonexistent", "value")

    def test_validate_record(self, validation_rules, sample_record):
        """Test validating entire record."""
        validator = DataValidator()
        # Need to adjust sample_record for validation
        record = {
            "Name": "Acme Corp",
            "Phone__c": "5551234567"
        }
        
        record_valid, results = validator.validate_record(record, validation_rules)
        assert record_valid is True  # Name is not null and long enough
        assert len(results) > 0

    def test_validate_record_with_error(self):
        """Test record validation that fails."""
        validator = DataValidator()
        rules = [
            ValidationRule(
                name="name_required",
                rule_type="not_null",
                field="Name",
                is_error=True
            )
        ]
        
        record = {"Name": None}
        record_valid, results = validator.validate_record(record, rules)
        assert record_valid is False
        assert any(not r.valid for r in results)

    def test_get_validation_summary(self):
        """Test validation summary generation."""
        validator = DataValidator()
        rules = [
            ValidationRule(
                name="name_required",
                rule_type="not_null",
                field="Name",
                is_error=True
            ),
            ValidationRule(
                name="phone_format",
                rule_type="phone_format",
                field="Phone",
                is_error=False
            )
        ]
        
        record = {"Name": "Acme", "Phone": "123"}  # Phone too short
        _, results = validator.validate_record(record, rules)
        summary = validator.get_validation_summary(results)
        
        assert summary["total_checks"] == 2
        assert summary["errors"] >= 0
