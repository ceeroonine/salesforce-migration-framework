"""Data loading module for writing to Salesforce."""

from typing import List, Dict, Any, Optional
import logging
from src.salesforce.client import SalesforceClient

logger = logging.getLogger(__name__)


class DataLoader:
    """Loads data into Salesforce orgs."""

    def __init__(
        self,
        client: SalesforceClient,
        batch_size: int = 200,
    ):
        """Initialize loader.
        
        Args:
            client: SalesforceClient instance
            batch_size: Records per API call
        """
        self.client = client
        self.batch_size = batch_size

    def create_records(
        self,
        sobject: str,
        records: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Create multiple records in batches.
        
        Args:
            sobject: Salesforce object type
            records: List of record dictionaries
            
        Returns:
            Load result with success/error counts
        """
        logger.info(
            f"Starting bulk create for {len(records)} {sobject} records"
        )
        
        results = []
        errors = []
        
        for idx, record in enumerate(records):
            try:
                record_id = self.client.create_record(sobject, record)
                results.append({
                    "index": idx,
                    "id": record_id,
                    "status": "created",
                })
            except Exception as e:
                error_msg = str(e)
                errors.append({
                    "index": idx,
                    "error": error_msg,
                    "record": record,
                })
                logger.warning(
                    f"Error creating record {idx}: {error_msg}"
                )
        
        logger.info(
            f"Bulk create complete: {len(results)} created, "
            f"{len(errors)} failed"
        )
        
        return {
            "sobject": sobject,
            "total": len(records),
            "successful": len(results),
            "failed": len(errors),
            "results": results,
            "errors": errors,
        }

    def upsert_records(
        self,
        sobject: str,
        external_id_field: str,
        records: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Upsert records (create or update) using external ID.
        
        Args:
            sobject: Salesforce object type
            external_id_field: External ID field name
            records: List of record dictionaries
            
        Returns:
            Load result with success/error counts
        """
        logger.info(
            f"Starting bulk upsert for {len(records)} {sobject} records "
            f"using {external_id_field}"
        )
        
        return self.client.batch_upsert(
            sobject=sobject,
            external_id_field=external_id_field,
            records=records,
        )

    def update_records(
        self,
        sobject: str,
        records: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Update multiple records.
        
        Args:
            sobject: Salesforce object type
            records: List of records (must include Id field)
            
        Returns:
            Load result with success/error counts
        """
        logger.info(
            f"Starting bulk update for {len(records)} {sobject} records"
        )
        
        results = []
        errors = []
        
        for idx, record in enumerate(records):
            record_id = record.get("Id")
            if not record_id:
                errors.append({
                    "index": idx,
                    "error": "Missing Id field for update",
                    "record": record,
                })
                continue
            
            try:
                # Remove Id from data dict (not allowed in update)
                update_data = {k: v for k, v in record.items() if k != "Id"}
                self.client.update_record(sobject, record_id, update_data)
                results.append({
                    "index": idx,
                    "id": record_id,
                    "status": "updated",
                })
            except Exception as e:
                error_msg = str(e)
                errors.append({
                    "index": idx,
                    "id": record_id,
                    "error": error_msg,
                    "record": record,
                })
                logger.warning(
                    f"Error updating record {idx} ({record_id}): {error_msg}"
                )
        
        logger.info(
            f"Bulk update complete: {len(results)} updated, "
            f"{len(errors)} failed"
        )
        
        return {
            "sobject": sobject,
            "total": len(records),
            "successful": len(results),
            "failed": len(errors),
            "results": results,
            "errors": errors,
        }

    def delete_records(
        self,
        sobject: str,
        record_ids: List[str],
    ) -> Dict[str, Any]:
        """Delete multiple records.
        
        Args:
            sobject: Salesforce object type
            record_ids: List of record IDs
            
        Returns:
            Delete result with success/error counts
        """
        logger.info(
            f"Starting bulk delete for {len(record_ids)} {sobject} records"
        )
        
        results = []
        errors = []
        
        for idx, record_id in enumerate(record_ids):
            try:
                self.client.delete_record(sobject, record_id)
                results.append({
                    "index": idx,
                    "id": record_id,
                    "status": "deleted",
                })
            except Exception as e:
                error_msg = str(e)
                errors.append({
                    "index": idx,
                    "id": record_id,
                    "error": error_msg,
                })
                logger.warning(
                    f"Error deleting record {idx} ({record_id}): {error_msg}"
                )
        
        logger.info(
            f"Bulk delete complete: {len(results)} deleted, "
            f"{len(errors)} failed"
        )
        
        return {
            "sobject": sobject,
            "total": len(record_ids),
            "successful": len(results),
            "failed": len(errors),
            "results": results,
            "errors": errors,
        }
