"""Data types for Salesforce objects."""

from enum import Enum
from typing import Any, Dict, Optional
from datetime import datetime, date
from pydantic import BaseModel, Field


class SalesforceFieldType(str, Enum):
    """Salesforce field types."""
    TEXT = "text"
    NUMBER = "number"
    CURRENCY = "currency"
    PERCENT = "percent"
    DATE = "date"
    DATETIME = "datetime"
    TIME = "time"
    BOOLEAN = "boolean"
    LOOKUP = "lookup"
    REFERENCE = "reference"
    PICKLIST = "picklist"
    MULTIPICKLIST = "multipicklist"
    TEXTAREA = "textarea"
    RICHTEXT = "richtext"
    EMAIL = "email"
    URL = "url"
    PHONE = "phone"
    FORMULA = "formula"
    ROLLUP = "rollup"
    GEOLOCATION = "geolocation"
    JSON = "json"


class SalesforceObject(BaseModel):
    """Represents a Salesforce object."""
    api_name: str = Field(description="Salesforce API name (e.g., Account)")
    label: Optional[str] = Field(None, description="Human-readable label")
    fields: Dict[str, "SalesforceField"] = Field(default_factory=dict)

    class Config:
        extra = "allow"


class SalesforceField(BaseModel):
    """Represents a Salesforce field."""
    api_name: str = Field(description="Field API name")
    label: Optional[str] = Field(None, description="Field label")
    field_type: SalesforceFieldType = Field(description="Field type")
    required: bool = Field(default=False, description="Is field required")
    unique: bool = Field(default=False, description="Is field unique")
    precision: Optional[int] = Field(None, description="Number precision")
    scale: Optional[int] = Field(None, description="Decimal places")
    length: Optional[int] = Field(None, description="Text length")
    default_value: Optional[Any] = Field(None, description="Default value")
    picklist_values: Optional[list] = Field(None, description="Valid picklist values")

    class Config:
        extra = "allow"


class MigrationRecord(BaseModel):
    """Represents a record being migrated."""
    source_id: str = Field(description="Source Salesforce ID")
    source_data: Dict[str, Any] = Field(description="Raw source data")
    target_data: Optional[Dict[str, Any]] = Field(None, description="Transformed target data")
    target_id: Optional[str] = Field(None, description="Target Salesforce ID after creation")
    status: str = Field(default="pending", description="Migration status")
    errors: list = Field(default_factory=list, description="Validation/transformation errors")
    warnings: list = Field(default_factory=list, description="Non-critical issues")
