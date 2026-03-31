"""Tests for the ObjectMapper class."""

import pytest
from src.core.mapper import ObjectMapper, ObjectMapping, FieldMapping


class TestObjectMapper:
    """Test ObjectMapper functionality."""

    def test_mapper_initialization(self, sample_object_mapping):
        """Test mapper can be initialized with object mapping."""
        mapper = ObjectMapper(sample_object_mapping)
        assert mapper.mapping == sample_object_mapping
        assert len(mapper._field_map_index) == 3

    def test_get_field_mapping(self, sample_object_mapping):
        """Test retrieving individual field mapping."""
        mapper = ObjectMapper(sample_object_mapping)
        mapping = mapper.get_field_mapping("Name")
        
        assert mapping is not None
        assert mapping.source_field == "Name"
        assert mapping.target_field == "Name"
        assert mapping.required is True

    def test_get_field_mapping_not_found(self, sample_object_mapping):
        """Test retrieving non-existent field mapping."""
        mapper = ObjectMapper(sample_object_mapping)
        mapping = mapper.get_field_mapping("NonExistent")
        assert mapping is None

    def test_get_target_fields(self, sample_object_mapping):
        """Test getting all target fields."""
        mapper = ObjectMapper(sample_object_mapping)
        target_fields = mapper.get_target_fields()
        
        assert len(target_fields) == 3
        assert "Name" in target_fields
        assert "Phone__c" in target_fields
        assert "CreatedAt__c" in target_fields

    def test_get_required_fields(self, sample_object_mapping):
        """Test getting required target fields."""
        mapper = ObjectMapper(sample_object_mapping)
        required = mapper.get_required_fields()
        
        assert len(required) == 1
        assert required[0] == "Name"

    def test_map_record(self, sample_object_mapping, sample_record):
        """Test mapping a source record to target structure."""
        mapper = ObjectMapper(sample_object_mapping)
        target = mapper.map_record(sample_record)
        
        assert target["Name"] == "Acme Corp"
        assert target["Phone__c"] == "(555) 123-4567"
        assert target["CreatedAt__c"] == "2024-01-15T10:30:00Z"

    def test_map_record_with_default_value(self):
        """Test mapping with default value for null source field."""
        mapping = ObjectMapping(
            source_object="Account",
            target_object="Account",
            field_mappings=[
                FieldMapping(
                    source_field="Status",
                    target_field="Status__c",
                    default_value="Active"
                )
            ]
        )
        
        mapper = ObjectMapper(mapping)
        record = {"Status": None}
        target = mapper.map_record(record)
        
        assert target["Status__c"] == "Active"

    def test_map_record_missing_optional_field(self, sample_object_mapping):
        """Test mapping when optional field is missing from source."""
        mapper = ObjectMapper(sample_object_mapping)
        record = {"Name": "Acme Corp"}  # Missing Phone and CreatedDate
        target = mapper.map_record(record)
        
        assert "Name" in target
        assert "Phone__c" not in target  # Optional field not mapped

    def test_validate_mapping_duplicate_source_fields(self):
        """Test validation detects duplicate source fields."""
        mapping = ObjectMapping(
            source_object="Account",
            target_object="Account",
            field_mappings=[
                FieldMapping(source_field="Name", target_field="Name"),
                FieldMapping(source_field="Name", target_field="Name2"),
            ]
        )
        
        mapper = ObjectMapper(mapping)
        issues = mapper.validate_mapping()
        
        assert "Duplicate source fields detected" in issues

    def test_validate_mapping_duplicate_target_fields(self):
        """Test validation detects duplicate target fields."""
        mapping = ObjectMapping(
            source_object="Account",
            target_object="Account",
            field_mappings=[
                FieldMapping(source_field="Name", target_field="Name"),
                FieldMapping(source_field="Name2", target_field="Name"),
            ]
        )
        
        mapper = ObjectMapper(mapping)
        issues = mapper.validate_mapping()
        
        assert "Duplicate target fields detected" in issues

    def test_get_mapping_config(self, sample_object_mapping):
        """Test getting mapping configuration as dict."""
        mapper = ObjectMapper(sample_object_mapping)
        config = mapper.get_mapping_config()
        
        assert config["source_object"] == "Account"
        assert config["target_object"] == "Account"
        assert len(config["field_mappings"]) == 3
        assert config["external_id_field"] == "ExternalID__c"
