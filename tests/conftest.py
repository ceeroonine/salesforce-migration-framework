"""Test fixtures for migration framework tests."""

import pytest
from src.core.mapper import ObjectMapping, FieldMapping
from src.core.validator import ValidationRule


@pytest.fixture
def sample_field_mapping():
    """Create a sample field mapping."""
    return FieldMapping(
        source_field="Name",
        target_field="Name",
        field_type="text",
        required=True
    )


@pytest.fixture
def sample_object_mapping():
    """Create a sample object mapping."""
    return ObjectMapping(
        source_object="Account",
        target_object="Account",
        field_mappings=[
            FieldMapping(
                source_field="Name",
                target_field="Name",
                field_type="text",
                required=True
            ),
            FieldMapping(
                source_field="Phone",
                target_field="Phone__c",
                field_type="phone",
                transformer="format_phone"
            ),
            FieldMapping(
                source_field="CreatedDate",
                target_field="CreatedAt__c",
                field_type="datetime",
                transformer="datetime_to_iso"
            ),
        ],
        external_id_field="ExternalID__c"
    )


@pytest.fixture
def sample_record():
    """Create a sample Salesforce record."""
    return {
        "Id": "001D000000IRFmaIAH",
        "Name": "Acme Corp",
        "Phone": "(555) 123-4567",
        "CreatedDate": "2024-01-15T10:30:00Z",
    }


@pytest.fixture
def validation_rules():
    """Create sample validation rules."""
    return [
        ValidationRule(
            name="account_name_required",
            rule_type="not_null",
            field="Name",
            is_error=True
        ),
        ValidationRule(
            name="account_name_length",
            rule_type="min_length",
            field="Name",
            config={"min_length": 2},
            is_error=True
        ),
        ValidationRule(
            name="phone_format",
            rule_type="phone_format",
            field="Phone__c",
            is_error=False  # Warning
        ),
    ]
