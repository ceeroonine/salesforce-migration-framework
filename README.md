# Salesforce Migration Framework

A modular, reusable Python framework for migrating core Salesforce objects from regional to global platforms with comprehensive validation and Microsoft Fabric reporting.

## Features

- **Field-Level Mapping**: Define object and field transformations via YAML configuration
- **Pluggable Validation**: Built-in validators for data quality checks with custom rule support
- **Data Transformation Pipeline**: Composable transformation steps with Salesforce data type support
- **Salesforce Integration**: Read from/write to Salesforce orgs with incremental sync capability
- **Microsoft Fabric Reporting**: Generate reconciliation and data quality reports
- **CLI Tool**: Simple command-line interface for running migrations
- **Comprehensive Logging**: Full audit trail and error tracking

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest

# View coverage
pytest --cov=src tests/
```

## Architecture

The framework is organized into three layers:

```
CLI Layer (click commands)
     ↓
Core Layer (mapping, transformation, validation)
     ↓
Integration Layer (Salesforce, Fabric, Storage)
```

## Documentation

- [Architecture & Design](docs/ARCHITECTURE.md) - System design and data flow
- [Configuration Guide](docs/CONFIG.md) - How to define mapping and validation rules
- [API Reference](docs/API.md) - Core module documentation
- [Migration Guide](docs/MIGRATION_GUIDE.md) - Step-by-step migration walkthrough

## Project Structure

```
src/
├── core/              # Core transformation and validation logic
├── salesforce/        # Salesforce API integration
├── fabric/            # Microsoft Fabric reporting
├── cli/               # Command-line interface
└── utils/             # Shared utilities

tests/
├── unit/              # Unit tests for each module
├── integration/       # Integration tests
└── fixtures/          # Test data and configurations
```

## Configuration Example

See `examples/sample_config.yaml` for a complete migration configuration.

```yaml
migration:
  name: "Regional to Global Account Migration"
  description: "Migrate core account records"
  
objects:
  - source_object: Account
    target_object: Account
    fields:
      - source: Name
        target: Name
      - source: Phone
        target: Phone__c
        validation:
          type: phone_format
      - source: CreatedDate
        target: CreatedAt__c
        transformer: datetime_to_iso
```

## Development

### Running Tests

```bash
pytest                          # Run all tests
pytest tests/unit/             # Run unit tests only
pytest --cov=src --cov-report=html  # Generate coverage report
```

### Adding Custom Validators

```python
from src.core.validator import CustomValidator

class MyValidator(CustomValidator):
    def validate(self, value):
        # Your validation logic
        return value.startswith("PRE_")
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit changes with clear messages
4. Push to the branch and create a Pull Request

## License

MIT

## Support

For issues, questions, or contributions, please open a GitHub issue.
