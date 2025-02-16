import yaml
import os
import sys
import jinja2
import pandas as pd
import traceback
import logging
import argparse
from termcolor import colored
from typing import Dict, List, Tuple, Any
from abc import ABC, abstractmethod

# Set up logging
logging.basicConfig(
    level=logging.WARNING,  # Change from DEBUG to WARNING
    format='%(message)s'    # Simplified format
)
logger = logging.getLogger(__name__)

class DatabaseConnector(ABC):
    """Abstract base class for database connectors"""
    
    @abstractmethod
    def connect(self, config: Dict[str, Any]) -> Any:
        """Establish database connection"""
        pass
        
    @abstractmethod
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query and return results as DataFrame"""
        pass
        
    @abstractmethod
    def close(self):
        """Close database connection"""
        pass
        
    @property
    @abstractmethod
    def default_schema(self) -> str:
        """Return default schema name for this database type"""
        pass

class DuckDBConnector(DatabaseConnector):
    def __init__(self):
        self.conn = None
        try:
            import duckdb
            self.duckdb = duckdb
        except ImportError:
            raise ImportError("duckdb package is required for DuckDB connection. Install it with: pip install duckdb")
        
    def connect(self, config: Dict[str, Any]):
        logger.info("Connecting to DuckDB...")
        self.conn = self.duckdb.connect(config['database_file'])
        logger.info("DuckDB connection successful")
        return self
        
    def execute_query(self, query: str) -> pd.DataFrame:
        return self.conn.execute(query).fetchdf()
        
    def close(self):
        if self.conn:
            self.conn.close()
            
    @property
    def default_schema(self) -> str:
        return 'main'

class PostgresConnector(DatabaseConnector):
    def __init__(self):
        self.engine = None
        try:
            from sqlalchemy import create_engine
            self.create_engine = create_engine
        except ImportError:
            raise ImportError("sqlalchemy package is required for PostgreSQL connection. Install it with: pip install sqlalchemy psycopg2-binary")
        
    def connect(self, config: Dict[str, Any]):
        logger.info("Connecting to PostgreSQL...")
        conn_string = "postgresql://{user}:{password}@{host}:{port}/{database}".format(**config)
        self.engine = self.create_engine(conn_string)
        logger.info("PostgreSQL connection successful")
        return self
        
    def execute_query(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query, self.engine)
        
    def close(self):
        if self.engine:
            self.engine.dispose()
            
    @property
    def default_schema(self) -> str:
        return 'public'

class TrinoConnector(DatabaseConnector):
    def __init__(self):
        self.conn = None
        try:
            import trino
            self.trino = trino
        except ImportError:
            raise ImportError("trino package is required for Trino connection. Install it with: pip install trino")
        
    def connect(self, config: Dict[str, Any]):
        logger.info("Connecting to Trino...")
        self.conn = self.trino.dbapi.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            catalog=config['catalog'],
            schema=config.get('schema', 'default')
        )
        logger.info("Trino connection successful")
        return self
        
    def execute_query(self, query: str) -> pd.DataFrame:
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
        
    def close(self):
        if self.conn:
            self.conn.close()
            
    @property
    def default_schema(self) -> str:
        return 'default'
    
class SnowflakeConnector(DatabaseConnector):
    def __init__(self):
        self.conn = None
        try:
            from snowflake import connector
            self.snowflake = connector
        except ImportError:
            raise ImportError("snowflake-connector-python package is required for Snowflake connection. Install it with: pip install snowflake-connector-python")

    def connect(self, config: Dict[str, Any]):
        # Get values from environment variables if they exist
        connection_params = {
            'user': os.environ.get('SNOWFLAKE_USER'),
            'password': os.environ.get('SNOWFLAKE_PASSWORD'),
            'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE'),
            'database': os.environ.get('SNOWFLAKE_DATABASE'),
            'schema': os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            'role': os.environ.get('SNOWFLAKE_ROLE')
        }
        
        # Check for missing required parameters
        required_params = ['user', 'password', 'account', 'warehouse', 'database']
        missing_params = [param for param in required_params if not connection_params.get(param)]
        
        if missing_params:
            logger.error(f"Missing required environment variables: {', '.join(missing_params)}")
            raise ValueError(f"Missing required environment variables: {', '.join(missing_params)}")
        
        # Remove None values
        connection_params = {k: v for k, v in connection_params.items() if v is not None}
        
        try:
            self.conn = self.snowflake.connect(**connection_params)
            
            # Test the connection with a simple query
            test_cursor = self.conn.cursor()
            try:
                test_cursor.execute("SELECT current_version()")
                test_cursor.fetchone()
            finally:
                test_cursor.close()
                
        except self.snowflake.errors.DatabaseError as e:
            if "Incorrect username or password was specified" in str(e):
                logger.error("❌ Authentication failed: Please check your SNOWFLAKE_USER and SNOWFLAKE_PASSWORD environment variables")
            elif "Failed to connect to DB" in str(e):
                logger.error("❌ Connection failed: Please check your SNOWFLAKE_ACCOUNT value")
            else:
                logger.error(f"❌ Database error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {str(e)}")
            raise
            
        return self
        
    def execute_query(self, query: str) -> pd.DataFrame:
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        finally:
            cursor.close()
        
    def close(self):
        if self.conn:
            self.conn.close()
            
    @property
    def default_schema(self) -> str:
        return 'PUBLIC'

def resolve_path(base_path: str, relative_path: str) -> str:
    """Resolve a path relative to the base path."""
    if os.path.isabs(relative_path):
        return relative_path
    return os.path.normpath(os.path.join(os.path.dirname(base_path), relative_path))

class DataQualityFramework:
    CONNECTORS = {
        'duckdb': 'DuckDBConnector',
        'postgresql': 'PostgresConnector',
        'postgres': 'PostgresConnector',
        'trino': 'TrinoConnector',
        'snowflake': 'SnowflakeConnector'
    }

    def __init__(self, yaml_files: List[str] = None):
        """Initialize framework with optional specific YAML files to test
        
        Args:
            yaml_files: Optional list of specific YAML files to test
        """
        self.script_path = os.path.abspath(__file__)
        self.yaml_files = yaml_files
        
        parent_dir = os.path.dirname(os.path.dirname(self.script_path))
        main_config_path = os.path.join(parent_dir, 'master_config.yml')
        
        logger.info(f"Initializing DataQualityFramework")
        if yaml_files:
            logger.info(f"Will only test the following YAML files: {', '.join(yaml_files)}")
        
        self.main_config = self._load_yaml(main_config_path)
        
        # Resolve db_config_path relative to script location
        db_config_path = resolve_path(self.script_path, self.main_config['db_config_path'])
        self.db_config = self._load_yaml(db_config_path)
        self.db = self._initialize_db_connector()
        self.custom_tests = self._load_custom_tests()
        self.table_configs = self._load_table_configs()
        
    @staticmethod
    def _load_yaml(path: str) -> dict:
        """Load and parse YAML file."""
        logger.debug(f"Loading YAML file: {path}")
        try:
            with open(path, 'r') as file:
                data = yaml.safe_load(file)
                logger.debug(f"Successfully loaded YAML from {path}")
                return data
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {path}")
            raise FileNotFoundError(f"Configuration file not found: {path}")
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {path}: {str(e)}")
            raise ValueError(f"Error parsing YAML file {path}: {str(e)}")
        
    def _initialize_db_connector(self) -> DatabaseConnector:
        """Initialize appropriate database connector based on config"""
        try:
            db_type = self.db_config['type'].lower()
            logger.info(f"Initializing {db_type} connector")
            
            if db_type not in self.CONNECTORS:
                logger.error(f"Unsupported database type: {db_type}")
                raise ValueError(f"Unsupported database type: {db_type}")
            
            connector_name = self.CONNECTORS[db_type]
            logger.debug(f"Using connector class: {connector_name}")
            
            connector_class = globals()[connector_name]
            
            try:
                logger.info("Attempting database connection...")
                connector = connector_class().connect(self.db_config)
                logger.info("Database connection successful")
                return connector
            except ImportError as e:
                logger.error(f"Failed to import required package for {db_type}")
                raise ImportError(f"Failed to initialize {db_type} connector. {str(e)}")
            except Exception as e:
                logger.error(f"Failed to connect to {db_type} database")
                logger.error(f"Connection parameters (excluding sensitive data): {
                    {k: v for k, v in self.db_config.items() if k not in ['password', 'user']}
                }")
                raise Exception(f"Failed to connect to database: {str(e)}")
                
        except KeyError as e:
            logger.error("Missing required configuration")
            logger.error(f"Available config keys: {list(self.db_config.keys())}")
            raise KeyError(f"Missing required configuration: {str(e)}")
        except Exception as e:
            logger.error("Unexpected error during database initialization")
            raise
        
    def _load_custom_tests(self) -> dict:
        """Load custom test SQL queries from custom_tests directory."""
        custom_tests = {}
        custom_tests_dir = resolve_path(self.script_path, 
                                      self.main_config.get('custom_tests_path', 'custom_tests'))
        
        logger.info(f"Loading custom tests from: {custom_tests_dir}")
        
        if not os.path.exists(custom_tests_dir):
            logger.warning(f"Custom tests directory '{custom_tests_dir}' not found")
            return custom_tests
            
        for filename in os.listdir(custom_tests_dir):
            if filename.endswith('.sql'):
                test_name = filename[:-4]  # Remove .sql extension
                file_path = os.path.join(custom_tests_dir, filename)
                logger.debug(f"Loading custom test: {test_name} from {file_path}")
                try:
                    with open(file_path, 'r') as file:
                        custom_tests[test_name] = file.read()
                except Exception as e:
                    logger.error(f"Error loading custom test {filename}: {str(e)}")
                    
        logger.info(f"Loaded {len(custom_tests)} custom tests")
        return custom_tests
        
    def _parse_table_identifier(self, table_identifier: str) -> Tuple[str, str]:
        """Parse schema and table name from table identifier."""
        parts = table_identifier.split('.')
        if len(parts) == 2:
            return parts[0], parts[1]
        return self.db.default_schema, parts[0]
        
    def _load_table_configs(self) -> Dict[str, dict]:
        """Load YAML files from the specified config directory.
        If yaml_files is specified, only load those specific files from the config directory."""
        table_configs = {}
        config_dir = resolve_path(self.script_path, self.main_config['table_configs_path'])
        
        logger.info(f"Loading table configs from: {config_dir}")
        
        if not os.path.exists(config_dir):
            raise FileNotFoundError(f"Config directory not found: {config_dir}")

        # Get all YAML files in the directory
        available_files = {
            os.path.splitext(f)[0]: f  # Store filename without extension as key
            for f in os.listdir(config_dir)
            if f.endswith(('.yml', '.yaml'))
        }
        
        if self.yaml_files:
            # Process only specified files
            for yaml_file in self.yaml_files:
                # Remove extension if present
                base_name = os.path.splitext(yaml_file)[0]
                
                if base_name in available_files:
                    file_path = os.path.join(config_dir, available_files[base_name])
                    logger.info(f"Loading specified config file: {file_path}")
                    try:
                        config = self._load_yaml(file_path)
                        if config:
                            table_configs.update(config)
                    except Exception as e:
                        logger.error(f"Failed to load config file {yaml_file}: {str(e)}")
                else:
                    available_files_list = ", ".join(sorted(available_files.keys()))
                    raise FileNotFoundError(
                        f"\nYAML file '{yaml_file}' not found in config directory: {config_dir}"
                        f"\n{colored('Available YAML files:', 'yellow')} {available_files_list}"
                    )
        else:
            # Process all YAML files in directory
            for filename in available_files.values():
                file_path = os.path.join(config_dir, filename)
                logger.debug(f"Loading table config from: {file_path}")
                try:
                    config = self._load_yaml(file_path)
                    if config:
                        table_configs.update(config)
                except Exception as e:
                    logger.error(f"Failed to load config file {filename}: {str(e)}")
        
        if not table_configs:
            if self.yaml_files:
                raise ValueError("No valid configurations found in specified YAML files")
            else:
                raise ValueError("No valid configurations found in config directory")
        
        logger.info(f"Loaded configurations for {len(table_configs)} tables from {len(table_configs)} files")
        return table_configs
        
    def _run_custom_test(self, schema: str, table: str, column: str, test: str) -> bool:
        """Run a custom test using Jinja templating."""
        if test not in self.custom_tests:
            logger.warning(f"Warning: Custom test '{test}' not found")
            return False
            
        try:
            template = jinja2.Template(self.custom_tests[test])
            query = template.render(schema=schema, table_name=table, column=column)
            logger.debug(f"Executing custom test query:\n{query}")
            
            result = self.db.execute_query(query)
            
            # For custom tests, we consider the test passed if the result is empty
            # This means no violations were found
            return len(result) == 0
            
        except Exception as e:
            logger.error(f"Error running custom test '{test}' on {schema}.{table}.{column}: {str(e)}")
            logger.debug(f"Result data (if available): {result if 'result' in locals() else 'No result'}")
            return False

    def _run_default_test(self, schema: str, table: str, column: str, test: str, params: any = None) -> bool:
        """Run a default test on a specific column."""
        qualified_table = f"{schema}.{table}"
        logger.debug(f"Running default test '{test}' on {qualified_table}.{column}")
        
        try:
            if test == 'no_nulls':
                query = f"SELECT COUNT(*) as null_count FROM {qualified_table} WHERE {column} IS NULL"
                result = self.db.execute_query(query)
                return int(result.iloc[0].iloc[0]) == 0
                
            elif test == 'unique':
                query = f"""
                    SELECT COUNT(*) as duplicate_count 
                    FROM (
                        SELECT {column}
                        FROM {qualified_table}
                        GROUP BY {column}
                        HAVING COUNT(*) > 1
                    ) t
                """
                result = self.db.execute_query(query)
                logger.debug(f"Unique test result: {result}")
                return int(result.iloc[0].iloc[0]) == 0
                
            elif test == 'accepted_values':
                if not params:
                    return False
                values_str = ", ".join([f"'{val}'" for val in params])
                query = f"""
                    SELECT COUNT(*) as invalid_count
                    FROM {qualified_table}
                    WHERE {column} NOT IN ({values_str})
                    AND {column} IS NOT NULL
                """
                result = self.db.execute_query(query)
                return int(result.iloc[0].iloc[0]) == 0
                
            elif test == 'max_len':
                if not params:
                    return False
                query = f"""
                    SELECT COUNT(*) as oversize_count
                    FROM {qualified_table}
                    WHERE LENGTH(CAST({column} AS VARCHAR)) > {params}
                """
                result = self.db.execute_query(query)
                return int(result.iloc[0].iloc[0]) == 0
                
            logger.warning(f"Unknown default test: {test}")
            return False
                
        except Exception as e:
            logger.error(f"Error running test '{test}' on {qualified_table}.{column}: {str(e)}")
            logger.debug("Query result:", result if 'result' in locals() else 'No result')
            return False
        
    def _print_distribution(self, passed: int, failed: int, width: int = 40) -> str:
        """Create a visually appealing test distribution display."""
        total = passed + failed
        fail_rate = (failed / total * 100) if total > 0 else 0

        chars = "○●"
            
        empty_char = chars[0]
        fill_char = chars[-1]
        
        # Create the bars with percentage-based widths
        pass_width = int(width * (passed / total)) if total > 0 else 0
        fail_width = width - pass_width
        
        # Build the visualization
        lines = []
        lines.append(colored("Test Results Distribution", "cyan", attrs=['bold']))
        
        # Add pass/fail bars with labels and counts
        pass_bar = colored(fill_char * pass_width, 'green') + colored(empty_char * fail_width, 'red')
        pass_label = f"Failed: {failed}/{total}"
        lines.append(f"{pass_bar} {colored(pass_label, attrs=['bold'])} ({fail_rate:.1f}%)")
        
        return "\n".join(lines)

    def _print_bar(self, value: int, max_value: int, width: int = 50, fill: str = "█") -> str:
        """Create a text-based bar visualization with colored empty blocks."""
        bar_width = int(width * (value / max_value)) if max_value > 0 else 0
        filled = colored(fill * bar_width, 'green')
        empty = colored("░" * (width - bar_width), 'red')
        return filled + empty

    def _print_summary_statistics(self, table_results: Dict[str, Dict[str, List[bool]]]):
        """Print detailed test summary with visualizations."""
        total_tests = 0
        total_passed = 0
        table_stats = {}
        
        # Collect statistics
        for table, columns in table_results.items():
            table_total = 0
            table_passed = 0
            column_stats = {}
            
            for column, results in columns.items():
                column_total = len(results)
                column_passed = sum(results)
                table_total += column_total
                table_passed += column_passed
                column_stats[column] = {
                    'total': column_total,
                    'passed': column_passed,
                    'failed': column_total - column_passed
                }
            
            table_stats[table] = {
                'total': table_total,
                'passed': table_passed,
                'failed': table_total - table_passed,
                'columns': column_stats
            }
            total_tests += table_total
            total_passed += table_passed
        
        # Print summary
        print("\n" + "="*80)
        print(colored("TEST SUMMARY", "cyan", attrs=['bold']))
        print("="*80)
        
        # Overall statistics
        total_failed = total_tests - total_passed
        pass_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0
        
        print(f"\n{colored('Overall Results:', 'cyan', attrs=['bold'])}")
        print(f"Total tests run: {colored(str(total_tests), 'yellow', attrs=['bold'])}")
        print(f"Tests passed:    {colored(str(total_passed), 'green', attrs=['bold'])} ", end="")
        print(f"({colored(f'{pass_rate:.1f}%', 'green', attrs=['bold'])})")
        print(f"Tests failed:    {colored(str(total_failed), 'red', attrs=['bold'])}")
        
        # Visual representation with new distribution display
        print("\n" + self._print_distribution(total_passed, total_failed, 50))
        
        # Per-table statistics
        print("\n" + colored("Results by Table:", "cyan", attrs=['bold']))
        for table, stats in table_stats.items():
            table_pass_rate = (stats['passed'] / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"\n{colored(table, 'yellow', attrs=['bold'])}:")
            print(f"├─ Total Tests: {stats['total']}")
            print(f"├─ Passed: {colored(str(stats['passed']), 'green')} ({table_pass_rate:.1f}%)")
            print(f"└─ Failed: {colored(str(stats['failed']), 'red')}")
            
            # Per-column statistics
            print(colored("   Column Details:", "cyan"))
            for column, col_stats in stats['columns'].items():
                col_pass_rate = (col_stats['passed'] / col_stats['total'] * 100) if col_stats['total'] > 0 else 0
                print(f"   {column}:")
                print(f"   ├─ [{self._print_bar(col_stats['passed'], col_stats['total'], 20, '█')}] ", end="")
                print(f"{col_stats['passed']}/{col_stats['total']} ({col_pass_rate:.1f}%)")
        
        print("\n")
        logger.info("Summary statistics printed successfully")

    def run_tests(self):
        """Run all tests specified in the configuration."""
        table_results = {}
        
        for table_identifier, config in self.table_configs.items():
            schema, table = self._parse_table_identifier(table_identifier)
            qualified_table = f"{schema}.{table}"
            print(f"\nRunning tests for {colored(qualified_table, 'cyan', attrs=['bold'])}")
            
            table_results[qualified_table] = {}
            
            for column_config in config.get('columns', []):
                column_name = column_config['name']
                print(f"\nColumn: {colored(column_name, 'cyan', attrs=['bold'])}")
                
                column_results = []
                failures = []
                
                for test in column_config.get('tests', []):
                    if isinstance(test, dict):
                        test_name = list(test.keys())[0]
                        test_params = test[test_name]
                        is_custom = test_name not in ['no_nulls', 'unique', 'accepted_values', 'max_len']
                        
                        if is_custom:
                            result = self._run_custom_test(schema, table, column_name, test_name)
                        else:
                            result = self._run_default_test(schema, table, column_name, test_name, test_params)
                    else:
                        is_custom = test not in ['no_nulls', 'unique', 'accepted_values', 'max_len']
                        
                        if is_custom:
                            result = self._run_custom_test(schema, table, column_name, test)
                        else:
                            result = self._run_default_test(schema, table, column_name, test)
                    
                    column_results.append(result)
                    
                    # Display test status for all tests
                    test_display = test if isinstance(test, str) else f"{list(test.keys())[0]}: {list(test.values())[0]}"
                    status = colored('PASS', 'green') if result else colored('FAIL', 'red')
                    print(f"Test '{test_display}': {status}")
                    if not result:
                        failures.append(test_display)
                
                table_results[qualified_table][column_name] = column_results
        
        self._print_summary_statistics(table_results)
        self.db.close()

def main():
    parser = argparse.ArgumentParser(description='Run data quality checks')
    parser.add_argument('--yaml-files', nargs='+', help='Specific YAML files to test from the table_configs_path directory')
    args = parser.parse_args()
    
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config = os.path.join(parent_dir, 'master_config.yml')
    
    try:
        logger.info("Loading configuration...")
        framework = DataQualityFramework(yaml_files=args.yaml_files)
        logger.info("Configuration loaded successfully")
        
        if args.yaml_files:
            logger.info(f"Running tests only for specified YAML files: {', '.join(args.yaml_files)}")
        
        logger.info(f"Database type: {framework.db_config.get('type')}")
        logger.info(f"Found {len(framework.table_configs)} tables to test")
        
        logger.info("Starting tests...")
        framework.run_tests()
        
    except FileNotFoundError as e:
        logger.error(colored("Error:", 'red') + f" {str(e)}")
        sys.exit(1)
    except ImportError as e:
        logger.error(colored("Package Error:", 'red') + f" {str(e)}")
        sys.exit(1)
    except yaml.YAMLError as e:
        logger.error(colored("YAML Error:", 'red') + f" {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(colored("Unexpected Error:", 'red'))
        logger.error(str(e))
        logger.error("\nFull traceback:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()