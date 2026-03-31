"""Tests for SOQL query builder."""

import pytest
from src.salesforce.query_builder import SOQLBuilder


class TestSOQLBuilder:
    """Test SOQL query builder functionality."""

    def test_simple_select(self):
        """Test basic SELECT query."""
        query = SOQLBuilder("Account").select("Id", "Name").build()
        assert "SELECT Id, Name FROM Account" in query

    def test_where_equals(self):
        """Test WHERE with equals condition."""
        query = (
            SOQLBuilder("Account")
            .select("Id", "Name")
            .where_equals("Status", "Active")
            .build()
        )
        assert "WHERE Status = 'Active'" in query

    def test_where_equals_numeric(self):
        """Test WHERE with numeric value."""
        query = (
            SOQLBuilder("Account")
            .select("Id")
            .where_equals("NumberOfEmployees", 100)
            .build()
        )
        assert "WHERE NumberOfEmployees = 100" in query

    def test_where_in(self):
        """Test WHERE IN condition."""
        query = (
            SOQLBuilder("Account")
            .select("Id")
            .where_in("Status", ["Active", "Pending", "Inactive"])
            .build()
        )
        assert "WHERE Status IN ('Active','Pending','Inactive')" in query

    def test_where_like(self):
        """Test WHERE LIKE condition."""
        query = (
            SOQLBuilder("Account")
            .select("Id")
            .where_like("Name", "%Inc%")
            .build()
        )
        assert "WHERE Name LIKE '%Inc%'" in query

    def test_where_not_null(self):
        """Test WHERE field is NOT NULL."""
        query = (
            SOQLBuilder("Account")
            .select("Id")
            .where_not_null("Phone")
            .build()
        )
        assert "WHERE Phone != null" in query

    def test_where_null(self):
        """Test WHERE field is NULL."""
        query = (
            SOQLBuilder("Account")
            .select("Id")
            .where_null("Email")
            .build()
        )
        assert "WHERE Email = null" in query

    def test_multiple_where_conditions(self):
        """Test multiple WHERE conditions (AND logic)."""
        query = (
            SOQLBuilder("Account")
            .select("Id", "Name")
            .where_equals("Status", "Active")
            .where_not_null("Phone")
            .build()
        )
        assert "WHERE Status = 'Active' AND Phone != null" in query

    def test_order_by(self):
        """Test ORDER BY clause."""
        query = (
            SOQLBuilder("Account")
            .select("Id", "Name")
            .order_by("Name", "ASC")
            .build()
        )
        assert "ORDER BY Name ASC" in query

    def test_order_by_desc(self):
        """Test ORDER BY with DESC."""
        query = (
            SOQLBuilder("Account")
            .select("Id")
            .order_by("CreatedDate", "DESC")
            .build()
        )
        assert "ORDER BY CreatedDate DESC" in query

    def test_limit(self):
        """Test LIMIT clause."""
        query = (
            SOQLBuilder("Account")
            .select("Id")
            .limit(100)
            .build()
        )
        assert "LIMIT 100" in query

    def test_offset(self):
        """Test OFFSET clause."""
        query = (
            SOQLBuilder("Account")
            .select("Id")
            .limit(50)
            .offset(100)
            .build()
        )
        assert "OFFSET 100" in query
        assert "LIMIT 50" in query

    def test_complex_query(self):
        """Test complex query with multiple clauses."""
        query = (
            SOQLBuilder("Account")
            .select("Id", "Name", "Phone")
            .where_equals("Status", "Active")
            .where_in("Type", ["Prospect", "Customer"])
            .order_by("CreatedDate", "DESC")
            .limit(500)
            .offset(0)
            .build()
        )
        assert "SELECT Id, Name, Phone FROM Account" in query
        assert "Status = 'Active'" in query
        assert "Type IN ('Prospect','Customer')" in query
        assert "ORDER BY CreatedDate DESC" in query
        assert "LIMIT 500" in query

    def test_escape_single_quotes(self):
        """Test escaping of single quotes in values."""
        query = (
            SOQLBuilder("Account")
            .select("Id")
            .where_equals("Name", "O'Brien Corp")
            .build()
        )
        assert "O\\'Brien Corp" in query

    def test_str_representation(self):
        """Test string representation returns query."""
        builder = SOQLBuilder("Account").select("Id", "Name")
        assert "SELECT Id, Name FROM Account" in str(builder)
