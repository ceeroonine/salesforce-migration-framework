"""Tests for Salesforce client."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from src.salesforce.client import SalesforceClient


class TestSalesforceClient:
    """Test SalesforceClient functionality."""

    @patch('src.salesforce.client.Salesforce')
    def test_client_initialization(self, mock_sf_class):
        """Test client initialization with valid credentials."""
        mock_instance = MagicMock()
        mock_instance.org_id = "00D50000000IZ3Z"
        mock_sf_class.return_value = mock_instance

        client = SalesforceClient(
            username="test@example.com",
            password="password123",
            security_token="token123",
            sandbox=True,
        )

        assert client.is_connected()
        assert client.org_id == "00D50000000IZ3Z"

    @patch('src.salesforce.client.Salesforce')
    def test_client_execute_soql(self, mock_sf_class):
        """Test executing SOQL query."""
        mock_instance = MagicMock()
        mock_instance.org_id = "00D50000000IZ3Z"
        mock_instance.query.return_value = {
            "totalSize": 1,
            "records": [{"Id": "001", "Name": "Acme Corp"}],
        }
        mock_sf_class.return_value = mock_instance

        client = SalesforceClient(
            username="test@example.com",
            password="password123",
        )

        result = client.execute_soql("SELECT Id, Name FROM Account")
        assert result["totalSize"] == 1
        assert result["records"][0]["Name"] == "Acme Corp"

    @patch('src.salesforce.client.Salesforce')
    def test_client_query_all(self, mock_sf_class):
        """Test query_all with pagination."""
        mock_instance = MagicMock()
        mock_instance.org_id = "00D50000000IZ3Z"
        mock_instance.query_all.return_value = [
            {"Id": "001", "Name": "Acme Corp"},
            {"Id": "002", "Name": "Globex Corp"},
        ]
        mock_sf_class.return_value = mock_instance

        client = SalesforceClient(
            username="test@example.com",
            password="password123",
        )

        records = client.query_all("SELECT Id, Name FROM Account")
        assert len(records) == 2
        assert records[0]["Name"] == "Acme Corp"

    @patch('src.salesforce.client.Salesforce')
    def test_client_create_record(self, mock_sf_class):
        """Test creating a record."""
        mock_instance = MagicMock()
        mock_instance.org_id = "00D50000000IZ3Z"
        mock_account = MagicMock()
        mock_account.create.return_value = "001D000000IRFmaIAH"
        mock_instance.Account = mock_account
        mock_sf_class.return_value = mock_instance

        client = SalesforceClient(
            username="test@example.com",
            password="password123",
        )

        record_id = client.create_record("Account", {"Name": "Acme Corp"})
        assert record_id == "001D000000IRFmaIAH"

    @patch('src.salesforce.client.Salesforce')
    def test_client_update_record(self, mock_sf_class):
        """Test updating a record."""
        mock_instance = MagicMock()
        mock_instance.org_id = "00D50000000IZ3Z"
        mock_account = MagicMock()
        mock_instance.Account = mock_account
        mock_sf_class.return_value = mock_instance

        client = SalesforceClient(
            username="test@example.com",
            password="password123",
        )

        result = client.update_record(
            "Account",
            "001D000000IRFmaIAH",
            {"Name": "New Name"},
        )
        assert result is True

    @patch('src.salesforce.client.Salesforce')
    def test_client_upsert_record(self, mock_sf_class):
        """Test upserting a record."""
        mock_instance = MagicMock()
        mock_instance.org_id = "00D50000000IZ3Z"
        mock_account = MagicMock()
        mock_account.upsert.return_value = {
            "id": "001D000000IRFmaIAH",
            "created": True,
        }
        mock_instance.Account = mock_account
        mock_sf_class.return_value = mock_instance

        client = SalesforceClient(
            username="test@example.com",
            password="password123",
        )

        result = client.upsert_record(
            "Account",
            "ExternalID__c",
            "EXT001",
            {"Name": "Acme Corp"},
        )
        assert result["created"] is True

    @patch('src.salesforce.client.Salesforce')
    def test_client_delete_record(self, mock_sf_class):
        """Test deleting a record."""
        mock_instance = MagicMock()
        mock_instance.org_id = "00D50000000IZ3Z"
        mock_account = MagicMock()
        mock_instance.Account = mock_account
        mock_sf_class.return_value = mock_instance

        client = SalesforceClient(
            username="test@example.com",
            password="password123",
        )

        result = client.delete_record("Account", "001D000000IRFmaIAH")
        assert result is True

    @patch('src.salesforce.client.Salesforce')
    def test_client_batch_upsert(self, mock_sf_class):
        """Test batch upsert operation."""
        mock_instance = MagicMock()
        mock_instance.org_id = "00D50000000IZ3Z"
        mock_account = MagicMock()
        mock_account.upsert.side_effect = [
            {"id": "001", "created": True},
            {"id": "002", "created": False},
        ]
        mock_instance.Account = mock_account
        mock_sf_class.return_value = mock_instance

        client = SalesforceClient(
            username="test@example.com",
            password="password123",
        )

        records = [
            {"ExternalID__c": "EXT001", "Name": "Acme Corp"},
            {"ExternalID__c": "EXT002", "Name": "Globex Corp"},
        ]

        result = client.batch_upsert("Account", "ExternalID__c", records)
        assert result["total"] == 2
        assert result["successful"] == 2
        assert result["failed"] == 0
