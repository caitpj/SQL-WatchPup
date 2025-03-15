import pytest
import os
import tempfile
import yaml
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data_lineage"))
sys.path.insert(0, project_root)

from generate_lineage import SQLLineageMapper

class TestSQLLineageMapper:
    
    @pytest.fixture
    def config_file(self):
        """Create a temporary config file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as f:
            yaml.dump({
                'sql_folder_path': './test_sql_folder',
                'lineage_output': './test_output',
                'file-schema': 'test_schema'
            }, f)
            return f.name
    
    @pytest.fixture
    def lineage_mapper(self, config_file):
        """Create an instance of SQLLineageMapper for testing."""
        mapper = SQLLineageMapper(config_file, debug=True)
        yield mapper
        os.unlink(config_file)
    
    def test_extract_parent_tables_basic(self, lineage_mapper):
        """Test basic SQL parent table extraction."""
        sql = """
        SELECT * 
        FROM schema1.table1
        JOIN schema2.table2 ON table1.id = table2.id
        """
        
        expected_tables = {'schema1.table1', 'schema2.table2'}
        result = lineage_mapper.extract_parent_tables(sql)
        
        assert result == expected_tables

    def test_extract_parent_tables_complex(self, lineage_mapper):
        """Test complex SQL parent table extraction."""
        sql = """
        WITH cte1 AS (
            SELECT * FROM x.table1
        ),
        cte2 AS (
            SELECT * FROM schema2.table2
            cross JOIN (
                values ('a', 'b')
            ) c(cols)
            cross join lateral (
                select * from file_schema.model_1_1
            ) a
        )
        SELECT 
            cte1.*, 
            cte2.*,
            (SELECT COUNT(*) FROM schema3.lookup_table WHERE id = cte1.id)
        FROM cte1
        LEFT JOIN cte2 ON cte1.id = cte2.id
        """
        
        result = lineage_mapper.extract_parent_tables(sql)
        
        expected_tables = {'x.table1', 'schema2.table2', 'schema3.lookup_table', 'file_schema.model_1_1'}
        result = lineage_mapper.extract_parent_tables(sql)
        
        assert result == expected_tables