"""Salesforce API client for authentication and connectivity."""

from typing import Any, Dict, Optional, List
from datetime import datetime
import logging
from simple_salesforce import Salesforce, SalesforceAuthenticationFailed
from simple_salesforce.exceptions import SalesforceMalformedRequest

logger = logging.getLogger(__name__)


class SalesforceClient:
    """Client for connecting to Salesforce orgs via REST API."""

    def __init__(
        self,
        username: str,
        password: str,
        security_token: str = "",
        sandbox: bool = False,
        instance_url: str = None,
    ):
        """Initialize Salesforce client.
        
        Args:
            username: Salesforce username
            password: Salesforce password
            security_token: Security token (if required)
            sandbox: Whether connecting to sandbox
            instance_url: Custom instance URL (overrides sandbox setting)
            
        Raises:
            SalesforceAuthenticationFailed: If authentication fails
        """
        self.username = username
        self.sandbox = sandbox
        self.instance_url = instance_url
        self._client = None
        self._authenticated_at = None

        try:
            self._client = Salesforce(
                username=username,
                password=password,
                security_token=security_token,
                sandbox=sandbox,
                instance=instance_url,
            )
            self._authenticated_at = datetime.now()
            logger.info(
                f"Successfully authenticated to Salesforce"
                f" {'sandbox' if sandbox else 'production'}"
            )
        except SalesforceAuthenticationFailed as e:
            logger.error(f"Salesforce authentication failed: {str(e)}")
            raise

    @property
    def org_id(self) -> str:
        """Get organization ID."""
        return self._client.org_id if self._client else None

    @property
    def authenticated_at(self) -> Optional[datetime]:
        """Get authentication timestamp."""
        return self._authenticated_at

    def is_connected(self) -> bool:
        """Check if client is connected.
        
        Returns:
            True if authenticated and connected
        """
        return self._client is not None and hasattr(self._client, 'org_id')

    def execute_soql(self, query: str) -> Dict[str, Any]:
        """Execute a SOQL query.
        
        Args:
            query: SOQL query string
            
        Returns:
            Query results dictionary with records
            
        Raises:
            SalesforceMalformedRequest: If query is invalid
        """
        if not self.is_connected():
            raise RuntimeError("Not connected to Salesforce")

        try:
            logger.debug(f"Executing SOQL: {query}")
            result = self._client.query(query)
            logger.info(
                f"Query returned {result['totalSize']} records"
            )
            return result
        except SalesforceMalformedRequest as e:
            logger.error(f"SOQL query error: {str(e)}")
            raise

    def query_all(self, query: str) -> List[Dict[str, Any]]:
        """Execute SOQL query with automatic result paging.
        
        Args:
            query: SOQL query string
            
        Returns:
            List of all record dictionaries
        """
        if not self.is_connected():
            raise RuntimeError("Not connected to Salesforce")

        try:
            logger.debug(f"Executing SOQL (all pages): {query}")
            result = self._client.query_all(query)
            records = list(result)
            logger.info(f"Query returned {len(records)} total records")
            return records
        except Exception as e:
            logger.error(f"SOQL query error: {str(e)}")
            raise

    def get_record(
        self,
        sobject: str,
        record_id: str,
        fields: List[str] = None,
    ) -> Dict[str, Any]:
        """Get a single record by ID.
        
        Args:
            sobject: Salesforce object type (e.g., 'Account')
            record_id: Record ID
            fields: Specific fields to retrieve (None for all)
            
        Returns:
            Record dictionary
        """
        if not self.is_connected():
            raise RuntimeError("Not connected to Salesforce")

        try:
            obj = getattr(self._client, sobject)
            record = obj.get(record_id)
            logger.debug(f"Retrieved {sobject} record: {record_id}")
            return record
        except Exception as e:
            logger.error(
                f"Error retrieving {sobject} {record_id}: {str(e)}"
            )
            raise

    def create_record(
        self,
        sobject: str,
        data: Dict[str, Any],
    ) -> str:
        """Create a new record.
        
        Args:
            sobject: Salesforce object type (e.g., 'Account')
            data: Record data dictionary
            
        Returns:
            New record ID
        """
        if not self.is_connected():
            raise RuntimeError("Not connected to Salesforce")

        try:
            obj = getattr(self._client, sobject)
            record_id = obj.create(data)
            logger.info(f"Created {sobject} record: {record_id}")
            return record_id
        except Exception as e:
            logger.error(f"Error creating {sobject}: {str(e)}")
            raise

    def update_record(
        self,
        sobject: str,
        record_id: str,
        data: Dict[str, Any],
    ) -> bool:
        """Update an existing record.
        
        Args:
            sobject: Salesforce object type
            record_id: Record ID to update
            data: Updated field values
            
        Returns:
            True if successful
        """
        if not self.is_connected():
            raise RuntimeError("Not connected to Salesforce")

        try:
            obj = getattr(self._client, sobject)
            obj.update(record_id, data)
            logger.info(f"Updated {sobject} record: {record_id}")
            return True
        except Exception as e:
            logger.error(
                f"Error updating {sobject} {record_id}: {str(e)}"
            )
            raise

    def upsert_record(
        self,
        sobject: str,
        external_id_field: str,
        external_id_value: str,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Upsert (create or update) a record using external ID.
        
        Args:
            sobject: Salesforce object type
            external_id_field: External ID field name
            external_id_value: External ID value
            data: Record data
            
        Returns:
            Result dictionary with id and created flag
        """
        if not self.is_connected():
            raise RuntimeError("Not connected to Salesforce")

        try:
            obj = getattr(self._client, sobject)
            result = obj.upsert(
                external_id_value,
                data,
                external_id_field,
            )
            logger.info(
                f"Upserted {sobject} with "
                f"{external_id_field}={external_id_value}: {result}"
            )
            return result
        except Exception as e:
            logger.error(
                f"Error upserting {sobject}: {str(e)}"
            )
            raise

    def delete_record(self, sobject: str, record_id: str) -> bool:
        """Delete a record.
        
        Args:
            sobject: Salesforce object type
            record_id: Record ID to delete
            
        Returns:
            True if successful
        """
        if not self.is_connected():
            raise RuntimeError("Not connected to Salesforce")

        try:
            obj = getattr(self._client, sobject)
            obj.delete(record_id)
            logger.info(f"Deleted {sobject} record: {record_id}")
            return True
        except Exception as e:
            logger.error(
                f"Error deleting {sobject} {record_id}: {str(e)}"
            )
            raise

    def batch_upsert(
        self,
        sobject: str,
        external_id_field: str,
        records: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Batch upsert multiple records.
        
        Args:
            sobject: Salesforce object type
            external_id_field: External ID field name
            records: List of record dictionaries
            
        Returns:
            Batch job status
        """
        if not self.is_connected():
            raise RuntimeError("Not connected to Salesforce")

        try:
            import json
            from .query_builder import SOQLBuilder

            logger.info(
                f"Starting batch upsert for {len(records)} "
                f"{sobject} records"
            )
            
            obj = getattr(self._client, sobject)
            results = []
            errors = []

            for idx, record in enumerate(records):
                try:
                    ext_id_value = record.get(external_id_field)
                    if not ext_id_value:
                        errors.append({
                            "index": idx,
                            "error": "Missing external ID field",
                            "record": record,
                        })
                        continue

                    result = obj.upsert(
                        ext_id_value,
                        record,
                        external_id_field,
                    )
                    results.append({
                        "index": idx,
                        "result": result,
                    })
                except Exception as e:
                    errors.append({
                        "index": idx,
                        "error": str(e),
                        "record": record,
                    })

            logger.info(
                f"Batch upsert complete: "
                f"{len(results)} succeeded, {len(errors)} failed"
            )
            
            return {
                "sobject": sobject,
                "total": len(records),
                "successful": len(results),
                "failed": len(errors),
                "results": results,
                "errors": errors,
            }
        except Exception as e:
            logger.error(f"Batch upsert error: {str(e)}")
            raise

    def disconnect(self):
        """Disconnect from Salesforce."""
        self._client = None
        logger.info("Disconnected from Salesforce")
