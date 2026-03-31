"""Tests for data extraction."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from src.salesforce.extractor import DataExtractor


class TestDataExtractor:
    """Test DataExtractor functionality."""

    @patch('src.salesforce.extractor.SalesforceClient')
    def test_extract_records_basic(self, mock_client):
        """Test basic record extraction."""
        # Setup mock client
        mock_instance = MagicMock()
        mock_instance.execute_soql.side_effect = [
            {  # First batch
                "totalSize": 2,
                "records": [
                    {"attributes": {"type": "Account"}, "Id": "001", "Name": "Acme"},
                    {"attributes": {"type": "Account"}, "Id": "002", "Name": "Globex"},
                ],
            },
            {  # Second batch (empty, signals end)
                "totalSize": 0,
                "records": [],
            },
        ]
        mock_client.execute_soql = mock_instance.execute_soql

        extractor = DataExtractor(mock_client)
        records = list(
            extractor.extract_records(
                "Account",
                fields=["Id", "Name"],
                batch_size=2,
            )
        )

        assert len(records) == 2
        assert records[0]["Name"] == "Acme"
        assert records[1]["Name"] == "Globex"

    @patch('src.salesforce.extractor.SalesforceClient')
    def test_extract_all(self, mock_client):
        """Test extracting all records into memory."""
        mock_instance = MagicMock()
        mock_instance.execute_soql.side_effect = [
            {"totalSize": 1, "records": [{"Id": "001", "Name": "Acme"}]},
            {"totalSize": 0, "records": []},
        ]
        mock_client.execute_soql = mock_instance.execute_soql

        extractor = DataExtractor(mock_client)
        records = extractor.extract_all("Account", fields=["Id", "Name"])

        assert len(records) == 1
        assert records[0]["Name"] == "Acme"
