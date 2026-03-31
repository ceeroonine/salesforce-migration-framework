"""SOQL query builder for Salesforce queries."""

from typing import List, Optional, Dict, Any


class SOQLBuilder:
    """Builder for constructing SOQL (Salesforce Object Query Language) queries."""

    def __init__(self, sobject: str):
        """Initialize SOQL builder.
        
        Args:
            sobject: Salesforce object name (e.g., 'Account')
        """
        self.sobject = sobject
        self.fields: List[str] = []
        self.where_conditions: List[str] = []
        self.order_by: Optional[str] = None
        self.limit_count: Optional[int] = None
        self.offset_count: Optional[int] = None

    def select(self, *fields: str) -> "SOQLBuilder":
        """Add fields to SELECT clause.
        
        Args:
            *fields: Field names to select
            
        Returns:
            Self for chaining
        """
        self.fields.extend(fields)
        return self

    def where(self, condition: str) -> "SOQLBuilder":
        """Add a WHERE condition.
        
        Args:
            condition: WHERE clause condition
            
        Returns:
            Self for chaining
        """
        self.where_conditions.append(condition)
        return self

    def where_equals(self, field: str, value: Any) -> "SOQLBuilder":
        """Add equality condition.
        
        Args:
            field: Field name
            value: Value to match
            
        Returns:
            Self for chaining
        """
        # Escape single quotes in string values
        if isinstance(value, str):
            escaped_value = value.replace("'", "\\'")
            condition = f"{field} = '{escaped_value}'"
        else:
            condition = f"{field} = {value}"
        
        return self.where(condition)

    def where_in(self, field: str, values: List[Any]) -> "SOQLBuilder":
        """Add IN condition.
        
        Args:
            field: Field name
            values: List of values
            
        Returns:
            Self for chaining
        """
        # Format values for SOQL IN clause
        formatted_values = []
        for val in values:
            if isinstance(val, str):
                escaped_val = val.replace("'", "\\'")
                formatted_values.append(f"'{escaped_val}'")
            else:
                formatted_values.append(str(val))
        
        values_str = ",".join(formatted_values)
        condition = f"{field} IN ({values_str})"
        return self.where(condition)

    def where_like(self, field: str, pattern: str) -> "SOQLBuilder":
        """Add LIKE condition for pattern matching.
        
        Args:
            field: Field name
            pattern: Pattern (use % for wildcards)
            
        Returns:
            Self for chaining
        """
        escaped_pattern = pattern.replace("'", "\\'")
        condition = f"{field} LIKE '{escaped_pattern}'"
        return self.where(condition)

    def where_not_null(self, field: str) -> "SOQLBuilder":
        """Add NOT NULL condition.
        
        Args:
            field: Field name
            
        Returns:
            Self for chaining
        """
        return self.where(f"{field} != null")

    def where_null(self, field: str) -> "SOQLBuilder":
        """Add NULL condition.
        
        Args:
            field: Field name
            
        Returns:
            Self for chaining
        """
        return self.where(f"{field} = null")

    def order_by(self, field: str, direction: str = "ASC") -> "SOQLBuilder":
        """Add ORDER BY clause.
        
        Args:
            field: Field to order by
            direction: ASC or DESC
            
        Returns:
            Self for chaining
        """
        self.order_by = f"{field} {direction.upper()}"
        return self

    def limit(self, count: int) -> "SOQLBuilder":
        """Add LIMIT clause.
        
        Args:
            count: Maximum number of records
            
        Returns:
            Self for chaining
        """
        self.limit_count = count
        return self

    def offset(self, count: int) -> "SOQLBuilder":
        """Add OFFSET clause for pagination.
        
        Args:
            count: Number of records to skip
            
        Returns:
            Self for chaining
        """
        self.offset_count = count
        return self

    def build(self) -> str:
        """Build the SOQL query string.
        
        Returns:
            SOQL query string
        """
        # Start with SELECT
        if not self.fields:
            query = f"SELECT Id FROM {self.sobject}"
        else:
            fields_str = ", ".join(self.fields)
            query = f"SELECT {fields_str} FROM {self.sobject}"
        
        # Add WHERE conditions
        if self.where_conditions:
            where_str = " AND ".join(self.where_conditions)
            query += f" WHERE {where_str}"
        
        # Add ORDER BY
        if self.order_by:
            query += f" ORDER BY {self.order_by}"
        
        # Add OFFSET (must come before LIMIT in SOQL)
        if self.offset_count is not None:
            query += f" OFFSET {self.offset_count}"
        
        # Add LIMIT
        if self.limit_count is not None:
            query += f" LIMIT {self.limit_count}"
        
        return query

    def __str__(self) -> str:
        """Return SOQL query string."""
        return self.build()
