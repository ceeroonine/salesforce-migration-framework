"""Migration reporting for Microsoft Fabric."""

from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from src.core.validator import DataValidator, ValidationResult

logger = logging.getLogger(__name__)


class MigrationReporter:
    """Generates migration reports for analysis and reporting."""

    def __init__(self):
        """Initialize reporter."""
        self.migration_start: Optional[datetime] = None
        self.migration_end: Optional[datetime] = None
        self.total_records_processed = 0
        self.successful_records = 0
        self.failed_records = 0
        self.validation_issues: List[Dict[str, Any]] = []

    def start_migration(self):
        """Mark start of migration."""
        self.migration_start = datetime.now()
        logger.info("Migration started")

    def end_migration(self):
        """Mark end of migration."""
        self.migration_end = datetime.now()
        logger.info("Migration ended")

    def record_processed(
        self,
        success: bool,
        record_id: Optional[str] = None,
        error: Optional[str] = None,
    ):
        """Record processing result.
        
        Args:
            success: Whether record was successfully processed
            record_id: Record identifier
            error: Error message if failed
        """
        self.total_records_processed += 1
        if success:
            self.successful_records += 1
        else:
            self.failed_records += 1
            if error:
                self.validation_issues.append({
                    "record_id": record_id,
                    "error": error,
                })

    def add_validation_results(
        self,
        validation_results: List[ValidationResult],
    ):
        """Add validation results to report.
        
        Args:
            validation_results: List of validation result objects
        """
        for result in validation_results:
            if not result.valid:
                self.validation_issues.append(result.model_dump())

    def get_migration_summary(self) -> Dict[str, Any]:
        """Get migration summary statistics.
        
        Returns:
            Dictionary with summary statistics
        """
        duration = None
        if self.migration_start and self.migration_end:
            duration = (
                self.migration_end - self.migration_start
            ).total_seconds()
        
        success_rate = (
            (self.successful_records / self.total_records_processed * 100)
            if self.total_records_processed > 0
            else 0
        )
        
        return {
            "start_time": self.migration_start.isoformat() if self.migration_start else None,
            "end_time": self.migration_end.isoformat() if self.migration_end else None,
            "duration_seconds": duration,
            "total_records_processed": self.total_records_processed,
            "successful_records": self.successful_records,
            "failed_records": self.failed_records,
            "success_rate_percent": round(success_rate, 2),
            "validation_issues_count": len(self.validation_issues),
        }

    def get_validation_summary(self) -> Dict[str, Any]:
        """Get validation issues summary.
        
        Returns:
            Dictionary with validation statistics
        """
        critical_issues = [
            v for v in self.validation_issues 
            if v.get("is_error", True)
        ]
        warnings = [
            v for v in self.validation_issues 
            if not v.get("is_error", True)
        ]
        
        return {
            "total_issues": len(self.validation_issues),
            "critical_errors": len(critical_issues),
            "warnings": len(warnings),
            "issues_by_field": self._group_issues_by_field(),
        }

    def _group_issues_by_field(self) -> Dict[str, int]:
        """Group validation issues by field name.
        
        Returns:
            Dictionary with field names and issue counts
        """
        grouped = {}
        for issue in self.validation_issues:
            field = issue.get("field", "unknown")
            grouped[field] = grouped.get(field, 0) + 1
        return grouped

    def generate_html_report(self) -> str:
        """Generate HTML report.
        
        Returns:
            HTML string
        """
        summary = self.get_migration_summary()
        validation = self.get_validation_summary()
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Migration Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #333; }}
                .summary {{ background: #f0f0f0; padding: 15px; border-radius: 5px; }}
                .success {{ color: green; }}
                .error {{ color: red; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
            </style>
        </head>
        <body>
            <h1>Migration Report</h1>
            
            <div class="summary">
                <h2>Migration Summary</h2>
                <p><strong>Start Time:</strong> {summary['start_time']}</p>
                <p><strong>End Time:</strong> {summary['end_time']}</p>
                <p><strong>Duration:</strong> {summary['duration_seconds']} seconds</p>
                <p><strong>Total Records:</strong> {summary['total_records_processed']}</p>
                <p class="success"><strong>Successful:</strong> {summary['successful_records']}</p>
                <p class="error"><strong>Failed:</strong> {summary['failed_records']}</p>
                <p><strong>Success Rate:</strong> {summary['success_rate_percent']}%</p>
            </div>
            
            <div class="summary">
                <h2>Validation Summary</h2>
                <p><strong>Total Issues:</strong> {validation['total_issues']}</p>
                <p class="error"><strong>Critical Errors:</strong> {validation['critical_errors']}</p>
                <p><strong>Warnings:</strong> {validation['warnings']}</p>
            </div>
        </body>
        </html>
        """
        return html
