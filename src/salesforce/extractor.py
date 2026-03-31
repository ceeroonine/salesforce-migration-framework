"""Data extraction module for reading from Salesforce."""

from typing import List, Dict, Any, Optional, Iterator
import logging
from src.salesforce.client import SalesforceClient
from src.salesforce.query_builder import SOQLBuilder

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extracts data from Salesforce orgs."""

    def __init__(self, client: SalesforceClient):
        """Initialize extractor.
        
        Args:
            client: SalesforceClient instance
        """
        self.client = client

    def extract_records(
        self,
        sobject: str,
        fields: List[str] = None,
        where_clause: str = None,
        batch_size: int = 500,
    ) -> Iterator[Dict[str, Any]]:
        """Extract records from Salesforce with automatic batching.
        
        Args:
            sobject: Salesforce object type
            fields: Fields to extract (None for Id only)
            where_clause: WHERE condition for filtering
            batch_size: Records per batch
            
        Yields:
            Record dictionaries
        """
        logger.info(f"Starting extraction of {sobject} records")
        
        # Build SOQL query
        query_builder = SOQLBuilder(sobject)
        
        if fields:
            query_builder.select(*fields)
        else:
            query_builder.select("Id")
        
        if where_clause:
            query_builder.where(where_clause)
        
        query_builder.order_by("Id", "ASC")
        query_builder.limit(batch_size)
        
        offset = 0
        total_extracted = 0
        
        while True:
            # Get batch
            batch_query = SOQLBuilder(sobject)
            if fields:
                batch_query.select(*fields)
            else:
                batch_query.select("Id")
            if where_clause:
                batch_query.where(where_clause)
            batch_query.order_by("Id", "ASC")
            batch_query.limit(batch_size)
            batch_query.offset(offset)
            
            query_str = batch_query.build()
            logger.debug(f"Executing: {query_str}")
            
            try:
                result = self.client.execute_soql(query_str)
                records = result.get("records", [])
                
                if not records:
                    logger.info(
                        f"Extraction complete: {total_extracted} total records"
                    )
                    break
                
                # Yield each record
                for record in records:
                    # Remove Salesforce metadata
                    clean_record = {k: v for k, v in record.items() if k != "attributes"}
                    yield clean_record
                    total_extracted += 1
                
                logger.debug(f"Extracted batch: {len(records)} records")
                
                # Check if we got fewer records than batch size (end of results)
                if len(records) < batch_size:
                    logger.info(
                        f"Extraction complete: {total_extracted} total records"
                    )
                    break
                
                offset += batch_size
                
            except Exception as e:
                logger.error(f"Error during extraction: {str(e)}")
                raise

    def extract_all(
        self,
        sobject: str,
        fields: List[str] = None,
        where_clause: str = None,
    ) -> List[Dict[str, Any]]:
        """Extract all records into memory (use with caution for large datasets).
        
        Args:
            sobject: Salesforce object type
            fields: Fields to extract
            where_clause: WHERE condition
            
        Returns:
            List of all records
        """
        records = list(
            self.extract_records(
                sobject=sobject,
                fields=fields,
                where_clause=where_clause,
            )
        )
        logger.info(f"Extracted {len(records)} records total")
        return records

    def extract_incremental(
        self,
        sobject: str,
        fields: List[str] = None,
        since_timestamp: str = None,
    ) -> Iterator[Dict[str, Any]]:
        """Extract records modified since a specific timestamp (delta sync).
        
        Args:
            sobject: Salesforce object type
            fields: Fields to extract
            since_timestamp: ISO timestamp (e.g., '2024-01-01T00:00:00Z')
            
        Yields:
            Modified record dictionaries
        """
        where_clause = None
        if since_timestamp:
            where_clause = f"LastModifiedDate >= {since_timestamp}"
        
        yield from self.extract_records(
            sobject=sobject,
            fields=fields,
            where_clause=where_clause,
        )
