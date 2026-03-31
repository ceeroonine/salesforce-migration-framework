"""Tests for data loading."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from src.salesforce.loader import DataLoader


class TestDataLoader:
    """Test DataLoader functionality."""

    @patch('src.salesforce.loader.SalesforceClient')
    def test_create_records_success(self, mock_client):
        """Test creating records successfully."""
        mock_client.create_record.side_effect = [
            "001D000000IRFmaIAH",
            "001D000000IRFmaBBB",
        ]

        loader = DataLoader(mock_client)
        records = [{"Name": "Acme"}, {"Name": "Globex"}]

        result = loader.create_records("Account", records)

        assert result["total"] == 2
        assert result["successful"] == 2
        assert result["failed"] == 0

    @patch('src.salesforce.loader.SalesforceClient')
    def test_create_records_with_errors(self, mock_client):
        """Test creating records with some failures."""
        mock_client.create_record.side_effect = [
            "001D000000IRFmaIAH",
            Exception("Invalid field"),
        ]

        loader = DataLoader(mock_client)
        records = [{"Name": "Acme"}, {"Name": "Globex"}]

        result = loader.create_records("Account", records)

        assert result["total"] == 2
        assert result["successful"] == 1
        assert result["failed"] == 1

    @patch('src.salesforce.loader.SalesforceClient')
    def test_upsert_records(self, mock_client):
        """Test upserting records."""
        mock_client.batch_upsert.return_value = {
            "sobject": "Account",
            "total": 2,
            "successful": 2,
            "failed": 0,
            "results": [{"index": 0}, {"index": 1}],
            "errors": [],
        }

        loader = DataLoader(mock_client)
        records = [
            {"ExternalID__c": "EXT001", "Name": "Acme"},
            {"ExternalID__c": "EXT002", "Name": "Globex"},
        ]

        result = loader.upsert_records("Account", "ExternalID__c", records)

        assert result["total"] == 2
        assert result["successful"] == 2
        assert result["failed"] == 0

    @patch('src.salesforce.loader.SalesforceClient')
    def test_update_records(self, mock_client):
        """Test updating records."""
        mock_client.update_record.return_value = True

        loader = DataLoader(mock_client)
        records = [{"Id": "001", "Name": "Updated Acme"}]

        result = loader.update_records("Account", records)

        assert result["total"] == 1
        assert result["successful"] == 1
        assert result["failed"] == 0

    @patch('src.salesforce.loader.SalesforceClient')
    def test_delete_records(self, mock_client):
        """Test deleting records."""
        mock_client.delete_record.return_value = True

        loader = DataLoader(mock_client)
        record_ids = ["001", "002"]

        result = loader.delete_records("Account", record_ids)

        assert result["total"] == 2
        assert result["successful"] == 2
        assert result["failed"] == 0
