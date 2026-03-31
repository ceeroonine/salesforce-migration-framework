"""Tests for migration reporter."""

import pytest
from src.fabric.reporter import MigrationReporter


class TestMigrationReporter:
    """Test migration reporter functionality."""

    def test_reporter_initialization(self):
        """Test reporter initialization."""
        reporter = MigrationReporter()
        assert reporter.total_records_processed == 0
        assert reporter.successful_records == 0
        assert reporter.failed_records == 0

    def test_start_and_end_migration(self):
        """Test migration start and end."""
        reporter = MigrationReporter()
        reporter.start_migration()
        assert reporter.migration_start is not None
        
        reporter.end_migration()
        assert reporter.migration_end is not None

    def test_record_processed_success(self):
        """Test recording successful record processing."""
        reporter = MigrationReporter()
        reporter.record_processed(success=True, record_id="001")
        
        assert reporter.total_records_processed == 1
        assert reporter.successful_records == 1
        assert reporter.failed_records == 0

    def test_record_processed_failure(self):
        """Test recording failed record processing."""
        reporter = MigrationReporter()
        reporter.record_processed(
            success=False,
            record_id="001",
            error="Invalid data"
        )
        
        assert reporter.total_records_processed == 1
        assert reporter.successful_records == 0
        assert reporter.failed_records == 1
        assert len(reporter.validation_issues) == 1

    def test_get_migration_summary(self):
        """Test getting migration summary."""
        reporter = MigrationReporter()
        reporter.start_migration()
        reporter.record_processed(success=True)
        reporter.record_processed(success=True)
        reporter.record_processed(success=False)
        reporter.end_migration()
        
        summary = reporter.get_migration_summary()
        
        assert summary["total_records_processed"] == 3
        assert summary["successful_records"] == 2
        assert summary["failed_records"] == 1
        assert summary["success_rate_percent"] == pytest.approx(66.67, abs=0.1)

    def test_get_validation_summary(self):
        """Test getting validation summary."""
        reporter = MigrationReporter()
        reporter.validation_issues = [
            {"field": "Name", "is_error": True},
            {"field": "Phone", "is_error": True},
            {"field": "Email", "is_error": False},
        ]
        
        summary = reporter.get_validation_summary()
        
        assert summary["total_issues"] == 3
        assert summary["critical_errors"] == 2
        assert summary["warnings"] == 1

    def test_generate_html_report(self):
        """Test HTML report generation."""
        reporter = MigrationReporter()
        reporter.start_migration()
        reporter.record_processed(success=True)
        reporter.record_processed(success=False)
        reporter.end_migration()
        
        html = reporter.generate_html_report()
        
        assert "<!DOCTYPE html>" in html
        assert "Migration Report" in html
        assert "2" in html  # Total records
        assert "50" in html  # Success rate
