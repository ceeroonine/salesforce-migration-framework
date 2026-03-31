# Microsoft Fabric Integration - Configuration Examples

This directory contains configuration and example files for the Salesforce Migration Framework.

## Files

### sample_config.yaml
Complete YAML configuration for migrating Account and Contact objects with:
- Field mappings (source to target)
- Transformation rules (phone formatting, date conversion, etc.)
- Validation rules (format checks, required fields, etc.)
- Batch processing settings
- Error handling configuration

### end_to_end_migration.py
Full end-to-end example showing:
1. **Phase 1 (Core)**: Object mapping, transformation, validation
2. **Phase 2 (Salesforce)**: Data extraction and loading
3. **Phase 3 (Fabric)**: Report generation and reconciliation

## Quick Start

### Run the Example

```bash
python examples/end_to_end_migration.py
```

This demonstrates the complete migration workflow:
- Define mappings and validation rules
- Transform sample data
- Validate against rules
- Generate reports
- Show output ready for Salesforce

## Configuration Usage

Load YAML configuration:

```python
import yaml

with open('examples/sample_config.yaml') as f:
    config = yaml.safe_load(f)

# Create mapping from config
from src.core.mapper import ObjectMapping, FieldMapping

mapping = ObjectMapping(
    source_object=config['objects'][0]['source_object'],
    target_object=config['objects'][0]['target_object'],
    field_mappings=[
        FieldMapping(**field) 
        for field in config['objects'][0]['field_mappings']
    ]
)
```

## Real-World Migration Pattern

```python
# 1. Connect to Salesforce
source_client = SalesforceClient(
    username="regional@company.com",
    password="password",
    sandbox=True
)

# 2. Extract data
extractor = DataExtractor(source_client)
records = extractor.extract_all("Account", fields=["Id", "Name", "Phone"])

# 3. Transform & validate
mapper = ObjectMapper(mapping)
transformer = DataTransformer()
validator = DataValidator()

transformed_records = []
for record in records:
    mapped = mapper.map_record(record)
    is_valid, results = validator.validate_record(mapped, validation_rules)
    if is_valid:
        transformed_records.append(mapped)

# 4. Generate reports
reporter = MigrationReporter()
report_gen = ReportGenerator("Account Migration")
recon_report = report_gen.generate_reconciliation_report(records, transformed_records)

# 5. Load to target
target_client = SalesforceClient(
    username="global@company.com",
    password="password2",
    sandbox=False
)
loader = DataLoader(target_client)
result = loader.upsert_records("Account", "ExternalID__c", transformed_records)
```

## Microsoft Fabric Notebook Integration

In a Fabric notebook (PySpark):

```python
from src.fabric.spark_utils import SparkDataFrameConverter
from src.fabric.reporter import MigrationReporter

# Convert to Spark DataFrame for distributed processing
converter = SparkDataFrameConverter(spark)  # spark is provided in notebook
df = converter.records_to_dataframe(transformed_records)

# Save to Delta Lake for downstream reporting
converter.save_to_delta(df, "/migration/account_staging")

# Generate reports
reporter = MigrationReporter()
html_report = reporter.generate_html_report()
displayHTML(html_report)  # Display in Fabric notebook
```
