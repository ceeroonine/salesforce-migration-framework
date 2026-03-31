"""Tests for transformer functionality."""

import pytest
from datetime import datetime, date
from src.core.transformer import DataTransformer, BuiltInTransformers


class TestBuiltInTransformers:
    """Test built-in transformer functions."""

    def test_format_phone_us_10_digits(self):
        """Test formatting 10-digit US phone number."""
        result = BuiltInTransformers.format_phone("5551234567")
        assert result == "+1-555-123-4567"

    def test_format_phone_us_11_digits(self):
        """Test formatting 11-digit US phone number (with leading 1)."""
        result = BuiltInTransformers.format_phone("15551234567")
        assert result == "+1-555-123-4567"

    def test_format_phone_with_special_chars(self):
        """Test formatting phone with special characters."""
        result = BuiltInTransformers.format_phone("(555) 123-4567")
        assert result == "+1-555-123-4567"

    def test_format_phone_null(self):
        """Test formatting null phone returns None."""
        assert BuiltInTransformers.format_phone(None) is None
        assert BuiltInTransformers.format_phone("") is None

    def test_normalize_text_uppercase(self):
        """Test text normalization to uppercase."""
        result = BuiltInTransformers.normalize_text(
            "hello world",
            {"uppercase": True}
        )
        assert result == "HELLO WORLD"

    def test_normalize_text_lowercase(self):
        """Test text normalization to lowercase."""
        result = BuiltInTransformers.normalize_text(
            "HELLO WORLD",
            {"lowercase": True}
        )
        assert result == "hello world"

    def test_normalize_text_trim(self):
        """Test text normalization with trim."""
        result = BuiltInTransformers.normalize_text("  hello world  ")
        assert result == "hello world"

    def test_datetime_to_iso(self):
        """Test datetime to ISO conversion."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = BuiltInTransformers.datetime_to_iso(dt)
        assert result == "2024-01-15T10:30:00"

    def test_datetime_to_iso_from_string(self):
        """Test ISO conversion from ISO string."""
        result = BuiltInTransformers.datetime_to_iso("2024-01-15T10:30:00Z")
        assert "2024-01-15" in result

    def test_parse_date(self):
        """Test date parsing."""
        d = date(2024, 1, 15)
        result = BuiltInTransformers.parse_date(d)
        assert result == "2024-01-15"

    def test_string_to_number(self):
        """Test string to number conversion."""
        result = BuiltInTransformers.string_to_number("1,234.56")
        assert result == 1234.56

    def test_string_to_number_with_currency(self):
        """Test string to number with currency symbol."""
        result = BuiltInTransformers.string_to_number("$1,234.56")
        assert result == 1234.56

    def test_string_to_number_null(self):
        """Test string to number with null value."""
        assert BuiltInTransformers.string_to_number(None) is None
        assert BuiltInTransformers.string_to_number("") is None

    def test_boolean_from_text_true(self):
        """Test text to boolean conversion (true)."""
        assert BuiltInTransformers.boolean_from_text("yes") is True
        assert BuiltInTransformers.boolean_from_text("true") is True
        assert BuiltInTransformers.boolean_from_text("1") is True

    def test_boolean_from_text_false(self):
        """Test text to boolean conversion (false)."""
        assert BuiltInTransformers.boolean_from_text("no") is False
        assert BuiltInTransformers.boolean_from_text("false") is False
        assert BuiltInTransformers.boolean_from_text("0") is False

    def test_concat_fields(self):
        """Test field concatenation."""
        value = {"first_name": "John", "last_name": "Doe"}
        config = {"fields": ["first_name", "last_name"], "separator": " "}
        result = BuiltInTransformers.concat_fields(value, config)
        assert result == "John Doe"


class TestDataTransformer:
    """Test DataTransformer class."""

    def test_transformer_initialization(self):
        """Test transformer initialization."""
        transformer = DataTransformer()
        assert len(transformer.builtin_transformers) > 0

    def test_apply_builtin_transformation(self):
        """Test applying built-in transformation."""
        transformer = DataTransformer()
        result = transformer.apply_transformation(
            "normalize_text",
            "  HELLO  ",
            {"lowercase": True}
        )
        assert result == "hello"

    def test_apply_transformation_not_found(self):
        """Test error when transformer not found."""
        transformer = DataTransformer()
        with pytest.raises(ValueError, match="Transformer.*not found"):
            transformer.apply_transformation("nonexistent", "value")

    def test_transform_record(self):
        """Test transforming entire record."""
        transformer = DataTransformer()
        record = {
            "source_name": "  acme corp  ",
            "source_phone": "5551234567"
        }
        
        mappings = [
            {
                "source_field": "source_name",
                "target_field": "Name",
                "transformer": "normalize_text",
                "transformer_config": {"uppercase": True}
            },
            {
                "source_field": "source_phone",
                "target_field": "Phone",
                "transformer": "format_phone"
            }
        ]
        
        result = transformer.transform_record(record, mappings)
        assert result["Name"] == "ACME CORP"
        assert result["Phone"] == "+1-555-123-4567"
