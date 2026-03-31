"""Field mapping and object mapper for Salesforce migrations."""

from typing import Any, Dict, List, Optional, Callable
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
import json


class FieldMapping(BaseModel):
    """Defines how a source field maps to a target field."""
    source_field: str = Field(description="Source field API name")
    target_field: str = Field(description="Target field API name")
    field_type: str = Field(default="text", description="Salesforce field type")
    required: bool = Field(default=False, description="Is target field required")
    default_value: Optional[Any] = Field(None, description="Default if source is null")
    transformer: Optional[str] = Field(None, description="Named transformer to apply")
    conditional_rules: Optional[List[Dict[str, Any]]] = Field(
        default=None, description="List of conditional transformation rules"
    )
    lookup_config: Optional[Dict[str, Any]] = Field(None, description="Lookup field configuration")
    custom_logic: Optional[str] = Field(None, description="Python expression for custom transformation")

    @field_validator('source_field', 'target_field')
    @classmethod
    def validate_field_names(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError('Field names must be non-empty strings')
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "source_field": "Phone",
                "target_field": "Phone__c",
                "field_type": "phone",
                "transformer": "format_phone",
            }
        }


class ObjectMapping(BaseModel):
    """Defines the complete mapping between source and target objects."""
    source_object: str = Field(description="Source Salesforce object name")
    target_object: str = Field(description="Target Salesforce object name")
    field_mappings: List[FieldMapping] = Field(description="List of field mappings")
    external_id_field: Optional[str] = Field(
        None, description="Field to use as external ID for upserts"
    )
    filter_condition: Optional[str] = Field(
        None, description="SOQL WHERE clause to filter source records"
    )
    batch_size: int = Field(default=200, description="Batch size for processing")
    validate_before_load: bool = Field(
        default=True, description="Run validation before loading to target"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "source_object": "Account",
                "target_object": "Account",
                "field_mappings": [
                    {
                        "source_field": "Name",
                        "target_field": "Name",
                        "field_type": "text",
                        "required": True,
                    }
                ],
                "external_id_field": "ExternalID__c",
            }
        }


class ObjectMapper:
    """Handles field-level mapping between Salesforce objects."""

    def __init__(self, mapping: ObjectMapping):
        """Initialize mapper with field mappings.
        
        Args:
            mapping: ObjectMapping configuration
        """
        self.mapping = mapping
        self._field_map_index = self._build_field_index()

    def _build_field_index(self) -> Dict[str, FieldMapping]:
        """Build index for quick field lookup by source field name."""
        return {fm.source_field: fm for fm in self.mapping.field_mappings}

    def get_field_mapping(self, source_field: str) -> Optional[FieldMapping]:
        """Get mapping for a source field.
        
        Args:
            source_field: Source field name
            
        Returns:
            FieldMapping if found, None otherwise
        """
        return self._field_map_index.get(source_field)

    def get_target_fields(self) -> List[str]:
        """Get list of all target field names.
        
        Returns:
            List of target field API names
        """
        return [fm.target_field for fm in self.mapping.field_mappings]

    def get_required_fields(self) -> List[str]:
        """Get list of required target fields.
        
        Returns:
            List of required target field names
        """
        return [fm.target_field for fm in self.mapping.field_mappings if fm.required]

    def map_record(self, source_record: Dict[str, Any]) -> Dict[str, Any]:
        """Create target record structure from source record.
        
        Maps field names from source to target using field mappings.
        Does not apply transformations or validation.
        
        Args:
            source_record: Source record data
            
        Returns:
            Target record with field names mapped
        """
        target_record = {}
        
        for source_field, value in source_record.items():
            mapping = self.get_field_mapping(source_field)
            if mapping:
                # Use default value if source is None
                if value is None and mapping.default_value is not None:
                    value = mapping.default_value
                target_record[mapping.target_field] = value
        
        # Add defaults for unmapped required fields if not present
        for mapping in self.mapping.field_mappings:
            if mapping.target_field not in target_record and mapping.default_value is not None:
                target_record[mapping.target_field] = mapping.default_value
        
        return target_record

    def get_mapping_config(self) -> Dict[str, Any]:
        """Get mapping configuration as dictionary.
        
        Returns:
            Mapping configuration
        """
        return {
            "source_object": self.mapping.source_object,
            "target_object": self.mapping.target_object,
            "field_mappings": [
                fm.model_dump() for fm in self.mapping.field_mappings
            ],
            "external_id_field": self.mapping.external_id_field,
            "filter_condition": self.mapping.filter_condition,
        }

    def validate_mapping(self) -> List[str]:
        """Validate mapping configuration.
        
        Returns:
            List of validation issues (empty if valid)
        """
        issues = []
        
        # Check for duplicate source fields
        source_fields = [fm.source_field for fm in self.mapping.field_mappings]
        if len(source_fields) != len(set(source_fields)):
            issues.append("Duplicate source fields detected")
        
        # Check for duplicate target fields
        target_fields = [fm.target_field for fm in self.mapping.field_mappings]
        if len(target_fields) != len(set(target_fields)):
            issues.append("Duplicate target fields detected")
        
        # Check required fields have defaults or source mapping
        required_targets = [fm.target_field for fm in self.mapping.field_mappings if fm.required]
        if required_targets and not all(
            any(fm.source_field for fm in self.mapping.field_mappings 
                if fm.target_field == rt) or any(fm.default_value for fm in self.mapping.field_mappings 
                if fm.target_field == rt)
            for rt in required_targets
        ):
            issues.append("Some required fields lack source mapping or default value")
        
        return issues
