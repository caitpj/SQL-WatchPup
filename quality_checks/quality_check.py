import yaml
import os
import sys
import jinja2
import pandas as pd
from datetime import datetime
from termcolor import colored
from typing import Dict, List, Tuple, Any
from pathlib import Path
from abc import ABC, abstractmethod
from importlib import import_module

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
        self.conn = self.duckdb.connect(config['database_file'])
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
        conn_string = "postgresql://{user}:{password}@{host}:{port}/{database}".format(**config)
        self.engine = self.create_engine(conn_string)
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
        self.conn = self.trino.dbapi.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            catalog=config['catalog'],
            schema=config.get('schema', 'default')
        )
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

class DataQualityFramework:
    CONNECTORS = {
        'duckdb': 'DuckDBConnector',
        'postgresql': 'PostgresConnector',
        'postgres': 'PostgresConnector',
        'trino': 'TrinoConnector'
    }
    
    def __init__(self, main_config_path: str):
        """Initialize framework with main config path"""
        self.main_config = self._load_yaml(main_config_path)
        self.db_config = self._load_yaml(self.main_config['db_config_path'])
        self.db = self._initialize_db_connector()
        self.custom_tests = self._load_custom_tests()
        self.table_configs = self._load_table_configs()
        
    @staticmethod
    def _load_yaml(path: str) -> dict:
        """Load and parse YAML file."""
        try:
            with open(path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file {path}: {str(e)}")
        
    def _initialize_db_connector(self) -> DatabaseConnector:
        """Initialize appropriate database connector based on config"""
        db_type = self.db_config['type'].lower()
        
        if db_type not in self.CONNECTORS:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        connector_name = self.CONNECTORS[db_type]
        connector_class = globals()[connector_name]
        
        try:
            return connector_class().connect(self.db_config)
        except ImportError as e:
            raise ImportError(f"Failed to initialize {db_type} connector. {str(e)}")
        
    def _load_custom_tests(self) -> dict:
        """Load custom test SQL queries from custom_tests directory."""
        custom_tests = {}
        custom_tests_dir = self.main_config.get('custom_tests_path', 'custom_tests')
        
        if not os.path.exists(custom_tests_dir):
            print(f"Warning: Custom tests directory '{custom_tests_dir}' not found")
            return custom_tests
            
        for filename in os.listdir(custom_tests_dir):
            if filename.endswith('.sql'):
                test_name = filename[:-4]  # Remove .sql extension
                with open(os.path.join(custom_tests_dir, filename), 'r') as file:
                    custom_tests[test_name] = file.read()
                    
        return custom_tests
        
    def _parse_table_identifier(self, table_identifier: str) -> Tuple[str, str]:
        """Parse schema and table name from table identifier."""
        parts = table_identifier.split('.')
        if len(parts) == 2:
            return parts[0], parts[1]
        return self.db.default_schema, parts[0]
        
    def _load_table_configs(self) -> Dict[str, dict]:
        """Load all YAML files from the specified config directory."""
        table_configs = {}
        config_dir = self.main_config['table_configs_path']
        
        if not os.path.exists(config_dir):
            raise FileNotFoundError(f"Config directory '{config_dir}' not found")
            
        for filename in os.listdir(config_dir):
            if filename.endswith(('.yml', '.yaml')):
                file_path = os.path.join(config_dir, filename)
                try:
                    config = self._load_yaml(file_path)
                    if config:
                        table_configs.update(config)
                except Exception as e:
                    print(f"Warning: Failed to load config file {filename}: {str(e)}")
                    
        return table_configs
        
    def _run_default_test(self, schema: str, table: str, column: str, test: str, params: any = None) -> bool:
        """Run a default test on a specific column."""
        qualified_table = f"{schema}.{table}"
        
        try:
            if test == 'no_nulls':
                query = f"SELECT COUNT(*) as count FROM {qualified_table} WHERE {column} IS NULL LIMIT 1"
                result = self.db.execute_query(query)
                return result.iloc[0]['count'] == 0
                
            elif test == 'unique':
                query = f"""
                    SELECT COUNT(*) as count FROM (
                        SELECT {column} FROM {qualified_table}
                        GROUP BY {column} HAVING COUNT(*) > 1
                        LIMIT 1
                    ) t
                """
                result = self.db.execute_query(query)
                return result.iloc[0]['count'] == 0
                
            elif test == 'accepted_values':
                if not params:
                    return False
                values_str = ", ".join([f"'{val}'" for val in params])
                query = f"""
                    SELECT COUNT(*) as count
                    FROM {qualified_table}
                    WHERE {column} NOT IN ({values_str})
                    AND {column} IS NOT NULL
                    LIMIT 1
                """
                result = self.db.execute_query(query)
                return result.iloc[0]['count'] == 0
                
            elif test == 'max_len':
                if not params:
                    return False
                query = f"""
                    SELECT COUNT(*) as count
                    FROM {qualified_table}
                    WHERE LENGTH(CAST({column} AS VARCHAR)) > {params}
                    LIMIT 1
                """
                result = self.db.execute_query(query)
                return result.iloc[0]['count'] == 0
                
            return False
            
        except Exception as e:
            print(f"Error running test '{test}' on {qualified_table}.{column}: {str(e)}")
            return False
        
    def _run_custom_test(self, schema: str, table: str, column: str, test: str) -> bool:
        """Run a custom test using Jinja templating."""
        if test not in self.custom_tests:
            print(f"Warning: Custom test '{test}' not found")
            return False
            
        try:
            template = jinja2.Template(self.custom_tests[test])
            query = template.render(schema=schema, table_name=table, column=column)
            
            result = self.db.execute_query(query)
            return len(result) == 0
            
        except Exception as e:
            print(f"Error running custom test '{test}' on {schema}.{table}.{column}: {str(e)}")
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

    def _print_summary_statistics(self, table_results: Dict[str, Dict[str, List[bool]]]):
        """Print detailed test summary with visualizations."""
        total_tests = 0
        total_passed = 0
        table_stats = {}
    
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
                    
                    status = colored("PASS", "green") if result else colored("FAIL", "red")
                    test_display = test if isinstance(test, str) else f"{list(test.keys())[0]}: {list(test.values())[0]}"
                    print(f"Test '{test_display}': {status}")
                
                table_results[qualified_table][column_name] = column_results
        
        self._print_summary_statistics(table_results)
        self.db.close()

def main():
    if len(sys.argv) > 2:
        print("Usage: python quality_check.py <optional main_config_path>")
        sys.exit(1)
    elif len(sys.argv) == 2:
        config = sys.argv[1]
    else:
        config = 'config.yml'
        
    try:
        framework = DataQualityFramework(config)
        framework.run_tests()
    except ImportError as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()