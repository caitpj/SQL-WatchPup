# Data Quality Framework

A flexible and extensible data quality testing framework that supports multiple databases (Snowflake, DuckDB, PostgreSQL, and Trino) and allows for custom test definitions.

## Features

- 🔍 Built-in test types:
  - `no_nulls`: Check for NULL values
  - `unique`: Verify column uniqueness
  - `accepted_values`: Validate against a list of allowed values
  - `max_len`: Check string length
- 🎯 Custom test support with Jinja templating
- 📝 YAML-based configuration
- 🔌 Support for multiple databases:
  - Snowflake
  - DuckDB
  - PostgreSQL
  - Trino
- 🎨 Colorful console output with visual test results
- 📊 Detailed test summary statistics

## Installation

1. Clone the repository:
```bash
git clone https://github.com/caitpj/SQL-WatchPup.git
cd data-quality-framework
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

### Main Configuration (config.yml)
```yaml
db_config_path: "config/db_details.yml"
table_configs_path: "config/tables/"
custom_tests_path: "custom_tests/"
```

### Database Configuration (db_details.yml)

Snowflake:
```yaml
type: "snowflake"
user: "SNOWFLAKE_USER"
password: "SNOWFLAKE_PASSWORD"
account: "SNOWFLAKE_ACCOUNT"    # Example: xy12345.us-east-1
warehouse: "SNOWFLAKE_WAREHOUSE"
database: "SNOWFLAKE_DATABASE"
schema: "SNOWFLAKE_SCHEMA"      # Optional, defaults to PUBLIC
role: "SNOWFLAKE_ROLE" 
```

DuckDB:
```yaml
type: "duckdb"
database_file: "data_quality_test.duckdb"
```

PostgreSQL:
```yaml
type: "postgresql"
host: "localhost"
port: "5432"
database: "your_database"
user: "your_username"
password: "your_password"
```

Trino:
```yaml
type: "trino"
host: "localhost"
port: "8080"
user: "your_username"
catalog: "your_catalog"
schema: "your_schema"  # optional
```

### Table Configuration (config/tables/users.yml)
```yaml
sandbox.users:
  columns:
    - name: user_id
      tests:
        - unique
        - no_nulls
        - max_len: 20
    
    - name: status
      tests:
        - no_nulls
        - accepted_values: ['active', 'inactive', 'pending']
  ...
```

### Custom Tests

Create SQL files in the `custom_tests` directory:

```sql
-- custom_tests/no_future_dates.sql
select *
from {{schema}}.{{table_name}}
where {{column}} > current_date() + 1
```

## Usage

1. Set up your configuration files as shown above.

2. Run the framework:
```bash
python run_dq_checks.py
```

## Sample Output

```
TEST SUMMARY
================================================================================

Overall Results:
Total tests run: 25
Tests passed:    20 (80.0%)
Tests failed:    5

Test Results Distribution
│●●●●●●●●●●●●●●●●●○○○○○○○○│ Failed: 5/25 (20.0%)

Results by Table:
sandbox.users:
├─ Total Tests: 15
├─ Passed: 12 (80.0%)
└─ Failed: 3
   Column Details:
   user_id:
   ├─ [████████████████████] 5/5 (100.0%)
```

## Project Structure
```
data_quality/
├── config/
│   ├── db_details.yml
│   └── tables/
│       ├── users.yml
│       └── orders.yml
├── custom_tests/
│   ├── no_future_dates.sql
│   └── custom_positive_amount.sql
├── run_dq_checks.py
├── main_config.yml
├── requirements.txt
├── db_details.yml
└── README.md
```

## Extending the Framework

### Adding New Database Support

1. Create a new connector class that implements the `DatabaseConnector` interface:
```python
class NewDBConnector(DatabaseConnector):
    def connect(self, config):
        # Implementation
    
    def execute_query(self, query):
        # Implementation
    
    def close(self):
        # Implementation
```

2. Add the connector to the `CONNECTORS` dictionary in `DataQualityFramework`.

### Adding New Test Types

Add new test logic to the `_run_default_test` method in `DataQualityFramework`.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT](https://choosealicense.com/licenses/mit/)
