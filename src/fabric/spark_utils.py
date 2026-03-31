"""Spark DataFrame utilities for Fabric integration."""

from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class SparkDataFrameConverter:
    """Converts migration data to Spark DataFrames for Fabric."""

    def __init__(self, spark=None):
        """Initialize converter.
        
        Args:
            spark: SparkSession instance (for Fabric notebooks)
        """
        self.spark = spark
        self._available = spark is not None

    def is_available(self) -> bool:
        """Check if Spark is available.
        
        Returns:
            True if Spark session is available
        """
        return self._available

    def records_to_dataframe(self, records: List[Dict[str, Any]], schema=None):
        """Convert list of records to Spark DataFrame.
        
        Args:
            records: List of record dictionaries
            schema: Optional PySpark schema
            
        Returns:
            Spark DataFrame
            
        Raises:
            RuntimeError: If Spark not available
        """
        if not self._available:
            raise RuntimeError(
                "Spark not available. Run this in a Fabric notebook."
            )
        
        try:
            if schema:
                df = self.spark.createDataFrame(records, schema=schema)
            else:
                df = self.spark.createDataFrame(records)
            
            logger.info(
                f"Created DataFrame with {df.count()} rows, "
                f"{len(df.columns)} columns"
            )
            return df
        except Exception as e:
            logger.error(f"Error creating DataFrame: {str(e)}")
            raise

    def validation_results_to_dataframe(self, validation_results: List[Dict]):
        """Convert validation results to DataFrame.
        
        Args:
            validation_results: List of validation result dicts
            
        Returns:
            Spark DataFrame with columns: field, valid, message, is_error, rule_name
        """
        if not self._available:
            raise RuntimeError("Spark not available")
        
        try:
            df = self.spark.createDataFrame(validation_results)
            logger.info(f"Created validation results DataFrame: {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error creating validation DataFrame: {str(e)}")
            raise

    def migration_statistics(self, records_df) -> Dict[str, Any]:
        """Calculate statistics from migration records.
        
        Args:
            records_df: Spark DataFrame with migration data
            
        Returns:
            Dictionary with statistics
        """
        if not self._available:
            raise RuntimeError("Spark not available")
        
        try:
            total_records = records_df.count()
            
            # Calculate column-level statistics
            stats = {
                "total_records": total_records,
                "columns": len(records_df.columns),
                "schema": str(records_df.schema),
            }
            
            logger.info(f"Calculated statistics: {total_records} records")
            return stats
        except Exception as e:
            logger.error(f"Error calculating statistics: {str(e)}")
            raise

    def save_to_delta(self, df, path: str, mode: str = "overwrite"):
        """Save DataFrame to Delta Lake.
        
        Args:
            df: Spark DataFrame
            path: Delta table path
            mode: Write mode (overwrite, append, ignore, error)
            
        Returns:
            True if successful
        """
        if not self._available:
            raise RuntimeError("Spark not available")
        
        try:
            df.write.format("delta").mode(mode).save(path)
            logger.info(f"Saved DataFrame to Delta: {path}")
            return True
        except Exception as e:
            logger.error(f"Error saving to Delta: {str(e)}")
            raise

    def read_delta(self, path: str):
        """Read DataFrame from Delta Lake.
        
        Args:
            path: Delta table path
            
        Returns:
            Spark DataFrame
        """
        if not self._available:
            raise RuntimeError("Spark not available")
        
        try:
            df = self.spark.read.format("delta").load(path)
            logger.info(f"Read DataFrame from Delta: {path}")
            return df
        except Exception as e:
            logger.error(f"Error reading from Delta: {str(e)}")
            raise
