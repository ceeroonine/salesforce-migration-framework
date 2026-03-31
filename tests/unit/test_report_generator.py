"""Tests for report generation."""

import pytest
import json
import os
from src.fabric.report_generator import ReportGenerator


class TestReportGenerator:
    """Test report generation functionality."""

    def test_generator_initialization(self):
        """Test report generator initialization."""
        generator = ReportGenerator("Test Migration")
        assert generator.migration_name == "Test Migration"
        assert generator.generated_at is not None

    def test_generate_reconciliation_report_matching(self):
        """Test reconciliation report with matching records."""
        generator = ReportGenerator("Test Migration")
        
        source = [
            {"Id": "001", "Name": "Acme"},
            {"Id": "002", "Name": "Globex"},
        ]
        target = [
            {"Id": "001", "Name": "Acme"},
            {"Id": "002", "Name": "Globex"},
        ]
        
        report = generator.generate_reconciliation_report(source, target)
        
        assert report["source_record_count"] == 2
        assert report["target_record_count"] == 2
        assert report["reconciliation_status"] == "PASSED"
        assert len(report["missing_in_target"]) == 0

    def test_generate_reconciliation_report_missing(self):
        """Test reconciliation report with missing records."""
        generator = ReportGenerator("Test Migration")
        
        source = [
            {"Id": "001", "Name": "Acme"},
            {"Id": "002", "Name": "Globex"},
        ]
        target = [{"Id": "001", "Name": "Acme"}]
        
        report = generator.generate_reconciliation_report(source, target)
        
        assert report["reconciliation_status"] == "FAILED"
        assert "002" in report["missing_in_target"]

    def test_generate_reconciliation_report_mismatched(self):
        """Test reconciliation report with mismatched data."""
        generator = ReportGenerator("Test Migration")
        
        source = [{"Id": "001", "Name": "Acme", "Phone": "555-1234"}]
        target = [{"Id": "001", "Name": "Acme Inc", "Phone": "555-1234"}]
        
        report = generator.generate_reconciliation_report(source, target)
        
        assert report["reconciliation_status"] == "FAILED"
        assert len(report["mismatched_records"]) == 1
        assert "Name" in report["mismatched_records"][0]["differences"]

    def test_generate_data_quality_report(self):
        """Test data quality report generation."""
        generator = ReportGenerator("Test Migration")
        
        records = [
            {"Id": "001", "Name": "Acme", "Phone": None},
            {"Id": "002", "Name": "Globex", "Phone": "555-5678"},
        ]
        
        report = generator.generate_data_quality_report(records, {})
        
        assert report["total_records"] == 2
        assert report["field_statistics"]["Phone"]["null_count"] == 1
        assert report["field_statistics"]["Name"]["null_count"] == 0

    def test_generate_field_mapping_report(self):
        """Test field mapping report generation."""
        generator = ReportGenerator("Test Migration")
        
        source_fields = ["Name", "Phone", "Email", "Website"]
        target_fields = ["Name", "Phone", "Email", "URL"]
        mapping = {"Name": "Name", "Phone": "Phone", "Email": "Email"}
        
        report = generator.generate_field_mapping_report(
            source_fields,
            target_fields,
            mapping
        )
        
        assert report["total_source_fields"] == 4
        assert report["total_target_fields"] == 4
        assert report["mapped_fields"] == 3
        assert "Website" in report["unmapped_source_fields"]
        assert "URL" in report["unmapped_target_fields"]

    def test_export_report_json(self, tmp_path):
        """Test exporting report to JSON."""
        generator = ReportGenerator("Test Migration")
        report = {"status": "success", "records": 100}
        filepath = tmp_path / "report.json"
        
        generator.export_report_json(report, str(filepath))
        
        assert filepath.exists()
        with open(filepath) as f:
            data = json.load(f)
        assert data["status"] == "success"

    def test_export_report_html(self, tmp_path):
        """Test exporting report to HTML."""
        generator = ReportGenerator("Test Migration")
        report = {"status": "success", "records": 100}
        filepath = tmp_path / "report.html"
        
        generator.export_report_html(report, str(filepath))
        
        assert filepath.exists()
        with open(filepath) as f:
            html = f.read()
        assert "Test Migration" in html
        assert "<!DOCTYPE html>" in html
