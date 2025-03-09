import os
import sys
import pytest
import tempfile
import shutil
import subprocess
import json
import re
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Add the correct path to import the script (two directories up)
SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(SCRIPT_DIR)
import generate_lineage

# Configuration for the tests
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURE_DIR = os.path.join(TEST_DIR, "fixtures")
# SCRIPT_DIR is already defined above where we added the path for importing generate_lineage

class TestSQLLineageMapper:
    @pytest.fixture(scope="class")
    def setup_test_environment(self):
        """Create a temporary directory with test fixtures"""
        # Create a temporary directory
        temp_dir = tempfile.mkdtemp()
        
        # Create subdirectories
        sql_dir = os.path.join(temp_dir, "sql")
        output_dir = os.path.join(temp_dir, "output")
        ui_dir = os.path.join(temp_dir, "ui")
        
        os.makedirs(sql_dir)
        os.makedirs(output_dir)
        os.makedirs(ui_dir)
        
        # Copy UI files to the temporary directory
        for file in ["template.html", "visualization.js", "styles.css"]:
            src = os.path.join(SCRIPT_DIR, file)
            if os.path.exists(src):
                shutil.copy(src, os.path.join(ui_dir, file))
            else:
                raise FileNotFoundError(f"UI file not found: {src}")
        
        # Create sample SQL files
        self._create_sample_sql_files(sql_dir)
        
        # Create a test config file
        config_path = os.path.join(temp_dir, "test_config.yml")
        with open(config_path, "w") as f:
            f.write(f"""
sql_folder_path: {sql_dir}
lineage_output: {output_dir}
lineage_ui: {ui_dir}
file-schema: analytics
""")
        
        yield {
            "temp_dir": temp_dir,
            "sql_dir": sql_dir,
            "output_dir": output_dir,
            "ui_dir": ui_dir,
            "config_path": config_path
        }
        
        # Cleanup after the tests
        shutil.rmtree(temp_dir)
    
    def _create_sample_sql_files(self, sql_dir):
        """Create sample SQL files for testing"""
        # First SQL file: source_table.sql
        with open(os.path.join(sql_dir, "source_table.sql"), "w") as f:
            f.write("""
-- This is a source table
CREATE TABLE analytics.source_table AS
SELECT * FROM raw_data.input_table;
""")
        
        # Second SQL file: intermediate_table.sql
        with open(os.path.join(sql_dir, "intermediate_table.sql"), "w") as f:
            f.write("""
-- This is an intermediate table
CREATE TABLE analytics.intermediate_table AS
SELECT 
    id,
    SUM(value) as total_value
FROM analytics.source_table
GROUP BY id;
""")
        
        # Third SQL file: final_table.sql
        with open(os.path.join(sql_dir, "final_table.sql"), "w") as f:
            f.write("""
-- This is a final table
CREATE TABLE analytics.final_table AS
SELECT 
    i.id,
    i.total_value,
    s.created_at
FROM analytics.intermediate_table i
JOIN analytics.source_table s ON i.id = s.id;
""")

    @pytest.fixture
    def mapper_instance(self, setup_test_environment):
        """Create an instance of the SQLLineageMapper class"""
        return generate_lineage.SQLLineageMapper(
            setup_test_environment["config_path"],
            debug=True
        )

    def test_extract_parent_tables(self, mapper_instance):
        """Test that parent tables are correctly extracted from SQL"""
        sql = """
        SELECT * FROM schema1.table1
        JOIN schema2.table2 ON schema1.table1.id = schema2.table2.id
        """
        
        parent_tables = mapper_instance.extract_parent_tables(sql)
        
        assert "schema1.table1" in parent_tables
        assert "schema2.table2" in parent_tables
        assert len(parent_tables) == 2

    def test_process_sql_files(self, mapper_instance):
        """Test that SQL files are correctly processed and relationships are built"""
        mapper_instance.process_sql_files()
        
        relationships = mapper_instance.relationships
        
        # Check if all expected relationships are found
        assert ("raw_data.input_table", "analytics.source_table") in relationships
        assert ("analytics.source_table", "analytics.intermediate_table") in relationships
        assert ("analytics.intermediate_table", "analytics.final_table") in relationships
        assert ("analytics.source_table", "analytics.final_table") in relationships
    
    def test_generate_mermaid(self, mapper_instance):
        """Test that Mermaid markup is correctly generated"""
        # First process the SQL files to build relationships
        mapper_instance.process_sql_files()
        
        # Generate Mermaid markup
        mermaid_content = mapper_instance.generate_mermaid()
        
        # Check if the Mermaid markup contains the expected content
        assert "flowchart TD" in mermaid_content
        assert "raw_data.input_table-->analytics.source_table" in mermaid_content.replace(" ", "")
        assert "analytics.source_table-->analytics.intermediate_table" in mermaid_content.replace(" ", "")
        assert "analytics.intermediate_table-->analytics.final_table" in mermaid_content.replace(" ", "")
        assert "analytics.source_table-->analytics.final_table" in mermaid_content.replace(" ", "")

    def test_save_mermaid(self, mapper_instance, setup_test_environment):
        """Test that Mermaid markup is correctly saved to a file"""
        # First process the SQL files and generate Mermaid markup
        mapper_instance.process_sql_files()
        mermaid_content = mapper_instance.generate_mermaid()
        
        # Save Mermaid markup to a file
        output_file = mapper_instance.save_mermaid(mermaid_content)
        
        # Check if the file exists
        assert os.path.exists(output_file)
        
        # Check if the file contains the expected content
        with open(output_file, "r") as f:
            content = f.read()
            assert "```mermaid" in content
            assert mermaid_content in content
            assert "```" in content

    def test_build_lineage(self, mapper_instance, setup_test_environment):
        """Test the entire lineage building process"""
        # Build lineage
        success = mapper_instance.build_lineage()
        
        # Check if the process was successful
        assert success
        
        # Check if the output files exist
        md_file = os.path.join(setup_test_environment["output_dir"], "lineage.md")
        assert os.path.exists(md_file)
        
        html_file = os.path.join(setup_test_environment["output_dir"], "sql_lineage_interactive.html")
        assert os.path.exists(html_file)
        
        # Verify the content of the HTML file
        with open(html_file, "r") as f:
            content = f.read()
            soup = BeautifulSoup(content, "html.parser")
            
            # Check if the HTML file contains the expected structure
            assert soup.title.text == "Interactive SQL Lineage Diagram"
            assert soup.find(id="graph-container") is not None
            assert soup.find(id="search-container") is not None
            assert soup.find(id="reset-view") is not None
            
            # Check if the graph data is correctly embedded
            script_tag = soup.find(id="graph-data")
            assert script_tag is not None
            
            # Parse the graph data as JSON
            graph_data = json.loads(script_tag.text)
            
            # Check if the graph data contains the expected nodes and links
            node_ids = [node["id"] for node in graph_data["nodes"]]
            assert "raw_data.input_table" in node_ids
            assert "analytics.source_table" in node_ids
            assert "analytics.intermediate_table" in node_ids
            assert "analytics.final_table" in node_ids
            
            # Verify links
            found_links = 0
            for link in graph_data["links"]:
                source = link["source"] if isinstance(link["source"], str) else link["source"]["id"]
                target = link["target"] if isinstance(link["target"], str) else link["target"]["id"]
                
                if source == "raw_data.input_table" and target == "analytics.source_table":
                    found_links += 1
                elif source == "analytics.source_table" and target == "analytics.intermediate_table":
                    found_links += 1
                elif source == "analytics.intermediate_table" and target == "analytics.final_table":
                    found_links += 1
                elif source == "analytics.source_table" and target == "analytics.final_table":
                    found_links += 1
            
            assert found_links >= 4


class TestCommandLineInterface:
    """Test the command-line interface of the script"""
    
    @pytest.fixture(scope="class")
    def setup_cli_test(self):
        """Create a temporary directory with test fixtures for CLI testing"""
        # Create a temporary directory
        temp_dir = tempfile.mkdtemp()
        
        # Create subdirectories
        sql_dir = os.path.join(temp_dir, "sql")
        output_dir = os.path.join(temp_dir, "output")
        ui_dir = os.path.join(temp_dir, "ui")
        
        os.makedirs(sql_dir)
        os.makedirs(output_dir)
        os.makedirs(ui_dir)
        
        # Copy UI files to the temporary directory
        for file in ["template.html", "visualization.js", "styles.css"]:
            src = os.path.join(SCRIPT_DIR, file)
            if os.path.exists(src):
                shutil.copy(src, os.path.join(ui_dir, file))
        
        # Create sample SQL files (same as above)
        with open(os.path.join(sql_dir, "source_table.sql"), "w") as f:
            f.write("""
-- This is a source table
CREATE TABLE analytics.source_table AS
SELECT * FROM raw_data.input_table;
""")
        
        with open(os.path.join(sql_dir, "intermediate_table.sql"), "w") as f:
            f.write("""
-- This is an intermediate table
CREATE TABLE analytics.intermediate_table AS
SELECT 
    id,
    SUM(value) as total_value
FROM analytics.source_table
GROUP BY id;
""")
        
        with open(os.path.join(sql_dir, "final_table.sql"), "w") as f:
            f.write("""
-- This is a final table
CREATE TABLE analytics.final_table AS
SELECT 
    i.id,
    i.total_value,
    s.created_at
FROM analytics.intermediate_table i
JOIN analytics.source_table s ON i.id = s.id;
""")
        
        # Create a test config file
        config_path = os.path.join(temp_dir, "test_config.yml")
        with open(config_path, "w") as f:
            f.write(f"""
sql_folder_path: {sql_dir}
lineage_output: {output_dir}
lineage_ui: {ui_dir}
file-schema: analytics
""")
        
        yield {
            "temp_dir": temp_dir,
            "sql_dir": sql_dir,
            "output_dir": output_dir,
            "ui_dir": ui_dir,
            "config_path": config_path,
            "script_path": os.path.join(SCRIPT_DIR, "generate_lineage.py")
        }
        
        # Cleanup after the tests
        shutil.rmtree(temp_dir)
    
    def test_cli_help(self, setup_cli_test):
        """Test that the CLI help message is displayed correctly"""
        result = subprocess.run(
            [sys.executable, setup_cli_test["script_path"], "--help"],
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0
        assert "Analyze SQL files and generate lineage diagrams" in result.stdout
    
    def test_cli_with_config(self, setup_cli_test):
        """Test running the script with a config file"""
        result = subprocess.run(
            [
                sys.executable, 
                setup_cli_test["script_path"], 
                "--config", setup_cli_test["config_path"],
                "--no-browser"  # Don't open browser during tests
            ],
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0
        assert "Created Mermaid diagram" in result.stdout
        assert "Created interactive diagram" in result.stdout
        
        # Check if the output files were created
        md_file = os.path.join(setup_cli_test["output_dir"], "lineage.md")
        html_file = os.path.join(setup_cli_test["output_dir"], "sql_lineage_interactive.html")
        
        assert os.path.exists(md_file)
        assert os.path.exists(html_file)
    
    def test_cli_with_debug(self, setup_cli_test):
        """Test running the script with debug mode enabled"""
        result = subprocess.run(
            [
                sys.executable, 
                setup_cli_test["script_path"], 
                "--config", setup_cli_test["config_path"],
                "--debug",
                "--no-browser"  # Don't open browser during tests
            ],
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0
        assert "Debug mode enabled" in result.stdout


class TestBrowserIntegration:
    """Test the generated visualization in a browser environment"""
    
    @pytest.fixture(scope="class")
    def setup_browser_test(self):
        """Create a temporary directory with test fixtures and run the script"""
        # Create a temporary directory
        temp_dir = tempfile.mkdtemp()
        
        # Create subdirectories
        sql_dir = os.path.join(temp_dir, "sql")
        output_dir = os.path.join(temp_dir, "output")
        ui_dir = os.path.join(temp_dir, "ui")
        
        os.makedirs(sql_dir)
        os.makedirs(output_dir)
        os.makedirs(ui_dir)
        
        # Copy UI files to the temporary directory
        for file in ["template.html", "visualization.js", "styles.css"]:
            src = os.path.join(SCRIPT_DIR, file)
            if os.path.exists(src):
                shutil.copy(src, os.path.join(ui_dir, file))
        
        # Create sample SQL files
        with open(os.path.join(sql_dir, "source_table.sql"), "w") as f:
            f.write("""
-- This is a source table
CREATE TABLE analytics.source_table AS
SELECT * FROM raw_data.input_table;
""")
        
        with open(os.path.join(sql_dir, "intermediate_table.sql"), "w") as f:
            f.write("""
-- This is an intermediate table
CREATE TABLE analytics.intermediate_table AS
SELECT 
    id,
    SUM(value) as total_value
FROM analytics.source_table
GROUP BY id;
""")
        
        with open(os.path.join(sql_dir, "final_table.sql"), "w") as f:
            f.write("""
-- This is a final table
CREATE TABLE analytics.final_table AS
SELECT 
    i.id,
    i.total_value,
    s.created_at
FROM analytics.intermediate_table i
JOIN analytics.source_table s ON i.id = s.id;
""")
        
        # Create a test config file
        config_path = os.path.join(temp_dir, "test_config.yml")
        with open(config_path, "w") as f:
            f.write(f"""
sql_folder_path: {sql_dir}
lineage_output: {output_dir}
lineage_ui: {ui_dir}
file-schema: analytics
""")
        
        # Run the script to generate the visualization
        subprocess.run(
            [
                sys.executable, 
                os.path.join(SCRIPT_DIR, "generate_lineage.py"), 
                "--config", config_path,
                "--no-browser"  # Don't open browser during tests
            ],
            capture_output=True
        )
        
        # Get the path to the generated HTML file
        html_file = os.path.join(output_dir, "sql_lineage_interactive.html")
        
        yield {
            "temp_dir": temp_dir,
            "html_file": html_file,
            "html_url": f"file://{os.path.abspath(html_file)}"
        }
        
        # Cleanup after the tests
        shutil.rmtree(temp_dir)
    
    @pytest.mark.skipif(
        "GITHUB_ACTIONS" in os.environ, 
        reason="Browser tests don't run well in CI environments"
    )
    def test_browser_rendering(self, setup_browser_test):
        """Test that the visualization renders correctly in a browser"""
        # Use headless Chrome for testing
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            
            # Load the HTML file
            driver.get(setup_browser_test["html_url"])
            
            # Wait for the graph to be rendered
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#graph-container svg"))
            )
            
            # Check if the nodes are rendered
            nodes = driver.find_elements(By.CSS_SELECTOR, ".node")
            assert len(nodes) >= 4  # We have at least 4 nodes
            
            # Check if the edges are rendered
            edges = driver.find_elements(By.CSS_SELECTOR, ".edge")
            assert len(edges) >= 4  # We have at least 4 edges
            
            # Test search functionality
            search_input = driver.find_element(By.CSS_SELECTOR, "#search-input")
            search_input.send_keys("source_table")
            
            # Wait for search results
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".dropdown-item"))
                )
                
                # Check if search results are displayed
                dropdown_items = driver.find_elements(By.CSS_SELECTOR, ".dropdown-item")
                assert len(dropdown_items) > 0
                
                # Click on the first search result
                dropdown_items[0].click()
                
                # Check if the selected node is highlighted
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".node.highlighted"))
                )
                
                highlighted_nodes = driver.find_elements(By.CSS_SELECTOR, ".node.highlighted")
                assert len(highlighted_nodes) == 1
                
            except TimeoutException:
                # If search dropdown doesn't appear, it might be because exact match was found
                # Let's check if any node is highlighted
                highlighted_nodes = driver.find_elements(By.CSS_SELECTOR, ".node.highlighted")
                # Either a node is highlighted, or there's other UI issue
                if len(highlighted_nodes) == 0:
                    pytest.fail("Search functionality failed - no nodes highlighted")
            
            # Test reset view button
            reset_button = driver.find_element(By.CSS_SELECTOR, "#reset-view")
            reset_button.click()
            
            # Wait for reset to complete (all nodes should be visible)
            WebDriverWait(driver, 5).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, ".node:not(.hidden)")) >= 4
            )
            
            # Check if all nodes are visible
            visible_nodes = driver.find_elements(By.CSS_SELECTOR, ".node:not(.hidden)")
            assert len(visible_nodes) >= 4
            
        finally:
            if 'driver' in locals():
                driver.quit()


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])