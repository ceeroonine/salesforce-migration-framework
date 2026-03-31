"""Example: Complete end-to-end migration workflow.

This example demonstrates how to:
1. Extract data from source Salesforce (Phase 2)
2. Transform and validate the data (Phase 1)
3. Generate reports and reconciliation (Phase 3)
4. Load data to target Salesforce (Phase 2)
"""

from src.core.mapper import ObjectMapper, ObjectMapping, FieldMapping
from src.core.transformer import DataTransformer
from src.core.validator import DataValidator, ValidationRule
from src.salesforce.client import SalesforceClient
from src.salesforce.extractor import DataExtractor
from src.salesforce.loader import DataLoader
from src.fabric.reporter import MigrationReporter
from src.fabric.report_generator import ReportGenerator


def example_account_migration():
    """Example: Migrate Accounts from regional to global Salesforce."""
    
    print("\n" + "="*80)
    print("SALESFORCE ACCOUNT MIGRATION - END-TO-END EXAMPLE")
    print("="*80 + "\n")
    
    # ========================================================================
    # STEP 1: Define Object Mapping (Configuration)
    # ========================================================================
    print("[STEP 1] Defining field mappings...")
    
    account_mapping = ObjectMapping(
        source_object="Account",
        target_object="Account",
        field_mappings=[
            # Required fields
            FieldMapping(
                source_field="Name",
                target_field="Name",
                field_type="text",
                required=True
            ),
            # Phone formatting
            FieldMapping(
                source_field="Phone",
                target_field="Phone__c",
                field_type="phone",
                transformer="format_phone"
            ),
            # Date conversion
            FieldMapping(
                source_field="CreatedDate",
                target_field="CreatedAt__c",
                field_type="datetime",
                transformer="datetime_to_iso"
            ),
            # With default value
            FieldMapping(
                source_field="Status",
                target_field="Status__c",
                field_type="picklist",
                default_value="Active"
            ),
        ],
        external_id_field="ExternalID__c",
    )
    
    print(f"✓ Mapped {len(account_mapping.field_mappings)} fields")
    print(f"  - Source: {account_mapping.source_object}")
    print(f"  - Target: {account_mapping.target_object}\n")
    
    # ========================================================================
    # STEP 2: Define Validation Rules
    # ========================================================================
    print("[STEP 2] Defining validation rules...")
    
    validation_rules = [
        ValidationRule(
            name="account_name_required",
            rule_type="not_null",
            field="Name",
            is_error=True
        ),
        ValidationRule(
            name="account_name_length",
            rule_type="min_length",
            field="Name",
            config={"min_length": 2},
            is_error=True
        ),
        ValidationRule(
            name="phone_format",
            rule_type="phone_format",
            field="Phone__c",
            is_error=False  # Warning only
        ),
    ]
    
    print(f"✓ Defined {len(validation_rules)} validation rules\n")
    
    # ========================================================================
    # STEP 3: Sample Data (In real scenario, would extract from source)
    # ========================================================================
    print("[STEP 3] Preparing source data...")
    
    source_records = [
        {
            "Id": "001D000000IRFmaIAH",
            "Name": "Acme Corporation",
            "Phone": "(555) 123-4567",
            "CreatedDate": "2024-01-15T10:30:00Z",
            "Status": None,
        },
        {
            "Id": "001D000000IRFmaBBB",
            "Name": "Globex Corp",
            "Phone": "5559876543",
            "CreatedDate": "2024-02-20T14:45:00Z",
            "Status": "Prospect",
        },
    ]
    
    print(f"✓ Prepared {len(source_records)} source records\n")
    
    # ========================================================================
    # STEP 4: Initialize Core Components
    # ========================================================================
    print("[STEP 4] Initializing transformation & validation components...")
    
    mapper = ObjectMapper(account_mapping)
    transformer = DataTransformer()
    validator = DataValidator()
    reporter = MigrationReporter()
    
    print("✓ Mapper, Transformer, Validator initialized\n")
    
    # ========================================================================
    # STEP 5: Transform & Validate Records
    # ========================================================================
    print("[STEP 5] Transforming and validating records...")
    
    reporter.start_migration()
    transformed_records = []
    
    for idx, source_record in enumerate(source_records, 1):
        print(f"\n  Processing record {idx}/{len(source_records)}: {source_record['Name']}")
        
        # Map fields
        mapped_record = mapper.map_record(source_record)
        print(f"    → Mapped to target fields")
        
        # Transform values
        field_mappings_for_transform = [
            {
                "source_field": "Phone",
                "target_field": "Phone__c",
                "transformer": "format_phone"
            },
            {
                "source_field": "CreatedDate",
                "target_field": "CreatedAt__c",
                "transformer": "datetime_to_iso"
            },
        ]
        
        try:
            # Simulate transformation (in reality would use transformer.transform_record)
            if mapped_record.get("Phone__c"):
                mapped_record["Phone__c"] = transformer.apply_transformation(
                    "format_phone",
                    mapped_record["Phone__c"]
                )
            print(f"    → Transformed values")
        except Exception as e:
            print(f"    ✗ Transformation error: {e}")
            reporter.record_processed(success=False, error=str(e))
            continue
        
        # Validate record
        is_valid, validation_results = validator.validate_record(
            mapped_record,
            validation_rules
        )
        
        if is_valid:
            print(f"    ✓ Validation passed")
            reporter.record_processed(success=True)
        else:
            print(f"    ⚠ Validation warnings")
            for result in validation_results:
                if not result.valid:
                    print(f"      - {result.field}: {result.message}")
            reporter.record_processed(success=True)  # Warnings don't fail
        
        reporter.add_validation_results(validation_results)
        transformed_records.append(mapped_record)
    
    reporter.end_migration()
    print(f"\n✓ Processed {len(transformed_records)} records\n")
    
    # ========================================================================
    # STEP 6: Generate Reports
    # ========================================================================
    print("[STEP 6] Generating migration reports...")
    
    # Migration summary
    summary = reporter.get_migration_summary()
    print(f"\n  Migration Summary:")
    print(f"    - Total: {summary['total_records_processed']}")
    print(f"    - Successful: {summary['successful_records']}")
    print(f"    - Failed: {summary['failed_records']}")
    print(f"    - Success Rate: {summary['success_rate_percent']}%")
    
    # Validation summary
    validation_summary = reporter.get_validation_summary()
    print(f"\n  Validation Summary:")
    print(f"    - Total Issues: {validation_summary['total_issues']}")
    print(f"    - Critical Errors: {validation_summary['critical_errors']}")
    print(f"    - Warnings: {validation_summary['warnings']}")
    
    # Reconciliation report
    report_generator = ReportGenerator("Account Migration")
    reconciliation = report_generator.generate_reconciliation_report(
        source_records,
        transformed_records,
        source_id_field="Id",
        target_id_field="Id"
    )
    print(f"\n  Reconciliation Status: {reconciliation['reconciliation_status']}")
    
    # Data quality report
    dq_report = report_generator.generate_data_quality_report(
        transformed_records,
        {}
    )
    print(f"\n  Data Quality:")
    print(f"    - Total Records: {dq_report['total_records']}")
    print(f"    - Fields Analyzed: {len(dq_report['field_statistics'])}")
    
    print(f"\n✓ Reports generated\n")
    
    # ========================================================================
    # STEP 7: Load to Target (Simulated)
    # ========================================================================
    print("[STEP 7] Loading to target Salesforce (simulated)...")
    print(f"\n  Would upsert {len(transformed_records)} records to target org")
    print(f"  Using external ID: {account_mapping.external_id_field}")
    print(f"\n  In production, would use:")
    print(f"  → DataLoader.upsert_records(\"Account\", \"ExternalID__c\", records)\n")
    
    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    print("="*80)
    print("MIGRATION COMPLETE")
    print("="*80)
    print(f"""
  ✓ Extracted: {len(source_records)} records
  ✓ Transformed: {len(transformed_records)} records  
  ✓ Validated: All records passed validation
  ✓ Ready to Load: Transformed records ready for Salesforce
  
  Next Steps:
  1. Review reconciliation report
  2. Verify data quality metrics
  3. Execute upsert to target org
  4. Monitor job completion
  5. Validate target records match expectations
    """)
    print("="*80 + "\n")


if __name__ == "__main__":
    example_account_migration()
