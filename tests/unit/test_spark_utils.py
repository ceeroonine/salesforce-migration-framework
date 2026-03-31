"""Tests for Spark integration utilities."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from src.fabric.spark_utils import SparkDataFrameConverter


class TestSparkDataFrameConverter:
    """Test Spark DataFrame converter."""

    def test_converter_without_spark(self):
        """Test converter initialization without Spark."""
        converter = SparkDataFrameConverter()
        assert not converter.is_available()

    def test_converter_with_spark(self):
        """Test converter initialization with Spark."""
        mock_spark = MagicMock()
        converter = SparkDataFrameConverter(spark=mock_spark)
        assert converter.is_available()

    def test_records_to_dataframe_without_spark(self):
        """Test error when converting without Spark."""
        converter = SparkDataFrameConverter()
        records = [{"Id": "001", "Name": "Acme"}]
        
        with pytest.raises(RuntimeError, match="Spark not available"):
            converter.records_to_dataframe(records)

    def test_records_to_dataframe_with_spark(self):
        """Test converting records to DataFrame."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        mock_df.columns = ["Id", "Name"]
        mock_spark.createDataFrame.return_value = mock_df
        
        converter = SparkDataFrameConverter(spark=mock_spark)
        records = [{"Id": "001", "Name": "Acme"}]
        
        result = converter.records_to_dataframe(records)
        assert result is not None

    def test_migration_statistics(self):
        """Test calculating migration statistics."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.columns = ["Id", "Name", "Phone"]
        mock_df.schema = "schema"
        
        converter = SparkDataFrameConverter(spark=mock_spark)
        stats = converter.migration_statistics(mock_df)
        
        assert stats["total_records"] == 100
        assert stats["columns"] == 3

    def test_validation_results_to_dataframe(self):
        """Test converting validation results to DataFrame."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 5
        mock_spark.createDataFrame.return_value = mock_df
        
        converter = SparkDataFrameConverter(spark=mock_spark)
        validation_results = [
            {"field": "Name", "valid": False, "message": "Error"},
        ]
        
        result = converter.validation_results_to_dataframe(validation_results)
        assert result is not None
