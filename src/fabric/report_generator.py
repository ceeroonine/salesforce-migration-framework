"""Report generation for migration analysis."""

from typing import List, Dict, Any, Optional
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generates detailed migration reports."""

    def __init__(self, migration_name: str):
        """Initialize report generator.
        
        Args:
            migration_name: Name of migration for reporting
        """
        self.migration_name = migration_name
        self.generated_at = datetime.now()

    def generate_reconciliation_report(
        self,
        source_records: List[Dict[str, Any]],
        target_records: List[Dict[str, Any]],
        source_id_field: str = "Id",
        target_id_field: str = "Id",
    ) -> Dict[str, Any]:
        """Generate reconciliation report comparing source and target.
        
        Args:
            source_records: Records from source system
            target_records: Records from target system
            source_id_field: ID field in source records
            target_id_field: ID field in target records
            
        Returns:
            Reconciliation report dictionary
        """
        logger.info("Generating reconciliation report")
        
        source_ids = {r.get(source_id_field): r for r in source_records}
        target_ids = {r.get(target_id_field): r for r in target_records}
        
        # Find missing and extra records
        missing_in_target = set(source_ids.keys()) - set(target_ids.keys())
        extra_in_target = set(target_ids.keys()) - set(source_ids.keys())
        
        # Find records with mismatched data
        mismatched = self._find_mismatched_records(
            source_ids,
            target_ids,
            list(set(source_ids.keys()) & set(target_ids.keys())),
        )
        
        report = {
            "migration_name": self.migration_name,
            "generated_at": self.generated_at.isoformat(),
            "source_record_count": len(source_records),
            "target_record_count": len(target_records),
            "reconciliation_status": "PASSED" if not missing_in_target and not extra_in_target and not mismatched else "FAILED",
            "missing_in_target": list(missing_in_target),
            "extra_in_target": list(extra_in_target),
            "mismatched_records": mismatched,
        }
        
        return report

    def _find_mismatched_records(
        self,
        source_map: Dict[str, Dict],
        target_map: Dict[str, Dict],
        common_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """Find records with mismatched data.
        
        Args:
            source_map: Source records indexed by ID
            target_map: Target records indexed by ID
            common_ids: IDs present in both systems
            
        Returns:
            List of mismatched record details
        """
        mismatched = []
        
        for record_id in common_ids:
            source_rec = source_map[record_id]
            target_rec = target_map[record_id]
            
            # Find field-level differences
            differences = {}
            for key in source_rec:
                if key not in target_rec:
                    differences[key] = {"source": source_rec[key], "target": None}
                elif source_rec[key] != target_rec[key]:
                    differences[key] = {
                        "source": source_rec[key],
                        "target": target_rec[key],
                    }
            
            if differences:
                mismatched.append({
                    "record_id": record_id,
                    "differences": differences,
                })
        
        return mismatched

    def generate_data_quality_report(
        self,
        records: List[Dict[str, Any]],
        validation_rules: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Generate data quality report.
        
        Args:
            records: Records to analyze
            validation_rules: Validation rules applied
            
        Returns:
            Data quality report
        """
        logger.info("Generating data quality report")
        
        quality_metrics = {
            "total_records": len(records),
            "null_counts": {},
            "data_types": {},
            "field_statistics": {},
        }
        
        if not records:
            return quality_metrics
        
        # Analyze each field
        sample_record = records[0]
        for field in sample_record.keys():
            values = [r.get(field) for r in records]
            null_count = sum(1 for v in values if v is None or v == "")
            
            quality_metrics["null_counts"][field] = null_count
            quality_metrics["field_statistics"][field] = {
                "total": len(records),
                "non_null": len(records) - null_count,
                "null_count": null_count,
                "null_percent": round(null_count / len(records) * 100, 2),
            }
        
        return quality_metrics

    def generate_field_mapping_report(
        self,
        source_fields: List[str],
        target_fields: List[str],
        mapping_config: Dict[str, str],
    ) -> Dict[str, Any]:
        """Generate field mapping report.
        
        Args:
            source_fields: Available source fields
            target_fields: Available target fields
            mapping_config: Field mapping configuration
            
        Returns:
            Field mapping report
        """
        logger.info("Generating field mapping report")
        
        unmapped_source = set(source_fields) - set(mapping_config.keys())
        unmapped_target = set(target_fields) - set(mapping_config.values())
        
        report = {
            "total_source_fields": len(source_fields),
            "total_target_fields": len(target_fields),
            "mapped_fields": len(mapping_config),
            "unmapped_source_fields": list(unmapped_source),
            "unmapped_target_fields": list(unmapped_target),
            "mapping_coverage_percent": round(
                len(mapping_config) / max(len(source_fields), 1) * 100, 2
            ),
        }
        
        return report

    def export_report_json(self, report: Dict[str, Any], filepath: str):
        """Export report to JSON file.
        
        Args:
            report: Report dictionary
            filepath: Output file path
        """
        try:
            with open(filepath, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Report exported to {filepath}")
        except Exception as e:
            logger.error(f"Error exporting report: {str(e)}")
            raise

    def export_report_html(self, report: Dict[str, Any], filepath: str):
        """Export report to HTML file.
        
        Args:
            report: Report dictionary
            filepath: Output file path
        """
        html = self._dict_to_html_table(report)
        try:
            with open(filepath, 'w') as f:
                f.write(f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>{self.migration_name} Report</title>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; }}
                        h1 {{ color: #333; }}
                        table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
                        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                        th {{ background-color: #4CAF50; color: white; }}
                        .summary {{ background: #f9f9f9; }}
                    </style>
                </head>
                <body>
                    <h1>{self.migration_name} - Report</h1>
                    <p>Generated: {self.generated_at.isoformat()}</p>
                    {html}
                </body>
                </html>
                """)
            logger.info(f"HTML report exported to {filepath}")
        except Exception as e:
            logger.error(f"Error exporting HTML report: {str(e)}")
            raise

    def _dict_to_html_table(self, data: Dict[str, Any], indent: int = 0) -> str:
        """Recursively convert dict to HTML table.
        
        Args:
            data: Dictionary to convert
            indent: HTML indentation level
            
        Returns:
            HTML string
        """
        html = "<table>\n"
        
        for key, value in data.items():
            if isinstance(value, dict):
                html += f"<tr><th>{key}</th><td>"
                html += self._dict_to_html_table(value, indent + 1)
                html += "</td></tr>\n"
            elif isinstance(value, list):
                html += f"<tr><td>{key}</td><td>{', '.join(map(str, value[:10]))}"
                if len(value) > 10:
                    html += f" ... ({len(value) - 10} more)"
                html += "</td></tr>\n"
            else:
                html += f"<tr><td>{key}</td><td>{value}</td></tr>\n"
        
        html += "</table>\n"
        return html
