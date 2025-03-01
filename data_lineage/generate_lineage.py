import os
import sys
import re
import argparse
import yaml
import webbrowser
from pathlib import Path
import logging
from sqllineage.runner import LineageRunner

# Define colors for console output
GREEN = "\033[92m"
BLUE = "\033[94m"
RED = "\033[91m"
YELLOW = "\033[93m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
BOLD = "\033[1m"
UNDERLINE = "\033[4m"
RESET = "\033[0m"

class SQLLineageMapper:
    def __init__(self, config_path: str, debug=False):
        self.config = self.load_config(config_path)
        self.root_folder = self.config['sql_folder_path']
        self.output_folder = self.config.get('lineage_output', 'output')
        self.file_schema = self.config.get('file-schema', None)
        self.relationships = set()
        self.debug = debug
        self.setup_logging()
        
        # SQL functions that should not be considered tables
        self.sql_functions = {'table', 'unnest', 'lateral', 'flatten', 'json_table'}
        
        # SQL keywords that typically appear after table references
        self.sql_terminators = {'where', 'group by', 'order by', 'having', 'limit', 'on', 'join'}
        
        # File paths for output
        self.md_file_path = Path(self.output_folder) / "lineage.md"
        
    def load_config(self, config_path: str) -> dict:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        if 'sql_folder_path' not in config:
            raise ValueError("Configuration must include 'sql_folder_path'")
        return config
    
    def setup_logging(self):
        level = logging.DEBUG if self.debug else logging.WARNING
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def extract_parent_tables(self, sql_content: str, file_path=None):
        """Extract source tables from SQL using sqllineage."""
        try:
            # sqllineage doesn't handle some SQL comments well, remove them first
            sql_clean = self.remove_comments(sql_content)
            
            # Parse SQL using sqllineage
            lineage = LineageRunner(sql_clean)
            
            # Extract source tables
            parent_tables = set()
            for table in lineage.source_tables:
                table_str = str(table).lower()
                
                # Skip any function-like tables
                is_function = False
                for func in self.sql_functions:
                    if table_str.endswith(f".{func}") or table_str == func:
                        is_function = True
                        break
                
                if is_function:
                    if self.debug:
                        self.logger.debug(f"Skipping SQL function: {table_str}")
                    continue
                
                parent_tables.add(table_str)
            
            # Log debugging information
            if file_path and self.debug:
                if not parent_tables:
                    self.logger.debug(f"No source tables found in {file_path}")
                else:
                    self.logger.debug(f"Found tables in {file_path}: {parent_tables}")
                
            return parent_tables
            
        except Exception as e:
            if self.debug:
                self.logger.debug(f"Error extracting tables: {str(e)}")
                if file_path:
                    self.logger.debug(f"SQL in {file_path}: {sql_content[:200]}...")
            
            # If sqllineage fails, try a more basic approach as fallback
            return self.extract_tables_with_regex(sql_content, file_path)
    
    def remove_comments(self, sql_content: str) -> str:
        """Remove SQL comments to help with parsing."""
        # Remove multi-line comments
        sql_without_multi_comments = re.sub(r'/\*.*?\*/', ' ', sql_content, flags=re.DOTALL)
        # Remove single-line comments
        sql_without_comments = re.sub(r'--.*$', '', sql_without_multi_comments, flags=re.MULTILINE)
        return sql_without_comments
    
    def extract_tables_with_regex(self, sql_content: str, file_path=None):
        """Fallback method using regex to extract tables."""
        # Remove comments for cleaner parsing
        sql_clean = self.remove_comments(sql_content)
        
        # Look for CTE definitions
        cte_pattern = r'(?:with|,)\s+([a-zA-Z0-9_]+)\s+as\s*\('
        cte_matches = re.finditer(cte_pattern, sql_clean, re.IGNORECASE)
        cte_names = {match.group(1).lower() for match in cte_matches}
        
        # Build the pattern for terminators
        terminators = '|'.join(self.sql_terminators)
        from_join_pattern = fr'(?:from|join)\s+(.*?)(?:{terminators}|\)|$)'
        from_join_matches = re.finditer(from_join_pattern, sql_clean, re.IGNORECASE | re.DOTALL)
        
        parent_tables = set()
        
        for match in from_join_matches:
            # Get the text after FROM/JOIN
            table_reference = match.group(1).strip()
            
            # Skip function calls by checking for parentheses
            if '(' in table_reference and ')' in table_reference:
                continue
                
            # Extract schema-qualified table names (supporting curly braces)
            # Pattern for schema.table or {schema}.{table} format
            table_pattern = r'(\{?[a-zA-Z0-9_]+\}?)\.(\{?[a-zA-Z0-9_]+\}?)'
            table_match = re.search(table_pattern, table_reference)
            
            if table_match:
                schema = table_match.group(1).lower()
                table = table_match.group(2).lower()
                
                # Clean the table name (remove any braces for function checking)
                schema_clean = schema.strip('{}')
                table_clean = table.strip('{}')
                
                # Skip if a SQL function
                if table_clean in self.sql_functions:
                    continue
                
                # Skip if a CTE
                if table_clean in cte_names:
                    continue
                
                # Add the table with proper formatting
                if '{' in table_match.group(1):
                    schema = f"{{{schema_clean}}}"
                else:
                    schema = schema_clean
                    
                if '{' in table_match.group(2):
                    table = f"{{{table_clean}}}"
                else:
                    table = table_clean
                    
                parent_tables.add(f"{schema}.{table}")
            else:
                # No schema qualification, just table name
                # If there are aliases or other clauses, just get the first part
                table = table_reference.split(' ')[0].lower()
                
                # Clean and check the table name
                table_clean = table.strip('{}')
                
                # Skip if empty, a CTE, or a SQL function
                if (not table_clean or 
                    table_clean in cte_names or 
                    table_clean in self.sql_functions):
                    continue
                
                # Add the table with proper formatting
                if '{' in table:
                    table = f"{{{table_clean}}}"
                else:
                    table = table_clean
                    
                parent_tables.add(table)
        
        if self.debug:
            self.logger.debug(f"Regex fallback found tables: {parent_tables}")
            
        return parent_tables
    
    def process_sql_files(self):
        """Process all SQL files and build relationships."""
        sql_files = list(Path(self.root_folder).rglob('*.sql'))
        file_count = len(sql_files)
        self.logger.info(f"Found {file_count} SQL files to process")
        print(f"🔎 Found {BOLD}{file_count}{RESET} SQL files to analyze")
        
        if self.debug:
            print(f"{MAGENTA}🐛 Debug mode enabled - detailed logging activated{RESET}")
        
        # Add a progress tracker
        processed = 0
        
        for file_path in sql_files:
            try:
                # Show progress periodically
                processed += 1
                if processed % 10 == 0 or processed == file_count:
                    print(f"{CYAN}⏳ Processing: {processed}/{file_count} files ({int(processed/file_count*100)}%){RESET}", end='\r')
                
                # Read file content
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if self.debug:
                    self.logger.debug(f"Processing file: {file_path}")
                
                # Get target table name from filename
                table_name = file_path.stem.lower()
                target_table = f"{self.file_schema}.{table_name}" if self.file_schema else table_name
                
                # Get source tables
                source_tables = self.extract_parent_tables(content, file_path)
                
                # Add relationships
                for source in source_tables:
                    self.relationships.add((source, target_table))
                    
            except Exception as e:
                self.logger.error(f"Error processing {file_path}: {str(e)}")
                if self.debug:
                    print(f"{RED}⚠️ Error processing {file_path}: {str(e)}{RESET}")
    
    def generate_mermaid(self):
        """Generate Mermaid flowchart markup."""
        if not self.relationships:
            raise ValueError("No SQL relationships found. Check if your SQL files contain FROM or JOIN statements.")
            
        lines = ["flowchart TD"]
        for source, target in self.relationships:
            # Escape table names with curly braces for Mermaid
            source_escaped = f'"{source}"' if '{' in source or '}' in source else source
            target_escaped = f'"{target}"' if '{' in target or '}' in target else target
            lines.append(f"    {source_escaped}-->{target_escaped}")
        
        return "\n".join(lines)
    
    def save_mermaid(self, mermaid_content: str, filename=None):
        """Save Mermaid content to a markdown file."""
        if filename is None:
            output_file = self.md_file_path
        else:
            output_file = Path(self.output_folder) / filename
            
        output_dir = output_file.parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            f.write(f"```mermaid\n{mermaid_content}\n```")
            
        return output_file
    
    def build_lineage(self):
        """Build complete lineage map and save it."""
        print(f"🔄 Processing SQL files...")
        self.process_sql_files()
        
        if not self.relationships:
            print(f"\n{RED}{BOLD}FAILED:{RESET} No SQL relationships found. Check if your SQL files contain FROM or JOIN statements.")
            return False
        
        relationship_count = len(self.relationships)
        print(f"📊 Found {BOLD}{relationship_count}{RESET} table relationships")
        
        print(f"🔄 Generating Mermaid diagram...{RESET}")
        mermaid_content = self.generate_mermaid()
        md_file = self.save_mermaid(mermaid_content)
        
        print(f"✅ {GREEN}Created Mermaid diagram at {BOLD}{UNDERLINE}{os.path.relpath(str(md_file))}{RESET}")
        return True

# Helper functions for parsing Mermaid and generating HTML
def parse_mermaid_flowchart(mermaid_content):
    """Parse mermaid flowchart to extract nodes and links with improved regex for schema-qualified names with curly braces."""
    lines = mermaid_content.split('\n')
    
    # Skip flowchart/graph definition line
    relationship_lines = []
    start_index = 0
    
    if lines and (lines[0].strip().startswith("flowchart") or 
                 lines[0].strip().startswith("graph")):
        start_index = 1
    
    relationship_lines = [line.strip() for line in lines[start_index:] if line.strip()]
    
    # Extract relationships with a comprehensive regex
    nodes = set()
    links = []
    
    # Updated regex that handles node IDs with dots and curly braces (schema.table format)
    relationship_pattern = r'(?:"([^"]+)"|([a-zA-Z0-9_\-.\{\}]+))\s*-->\s*(?:"([^"]+)"|([a-zA-Z0-9_\-.\{\}]+))'
    
    for line in relationship_lines:
        match = re.search(relationship_pattern, line)
        if match:
            # Extract source and target, handling both quoted and unquoted formats
            source = match.group(1) if match.group(1) else match.group(2)
            target = match.group(3) if match.group(3) else match.group(4)
            
            nodes.add(source)
            nodes.add(target)
            links.append({"source": source, "target": target})
    
    # Convert nodes to list of dictionaries
    node_list = [{"id": node, "name": node} for node in nodes]
    
    return {
        "nodes": node_list,
        "links": links
    }

def generate_html(graph_data, output_path, html_template_path, js_file_path, css_file_path, no_browser=False):
    """Generate an HTML file using external template, JS, and CSS files."""
    # Check if the required files exist
    if not os.path.exists(html_template_path):
        raise FileNotFoundError(f"HTML template file not found at: {html_template_path}")
    
    if not os.path.exists(js_file_path):
        raise FileNotFoundError(f"JavaScript file not found at: {js_file_path}")
    
    if not os.path.exists(css_file_path):
        raise FileNotFoundError(f"CSS file not found at: {css_file_path}")
    
    # Read the files
    def read_file_content(file_path):
        with open(file_path, 'r') as file:
            return file.read()
    
    html_template = read_file_content(html_template_path)
    js_content = read_file_content(js_file_path)
    css_content = read_file_content(css_file_path)
    
    # Convert graph data to JSON string
    import json
    graph_json = json.dumps(graph_data)
    
    # Replace placeholders in the template
    html_content = html_template.replace("/* CSS_PLACEHOLDER */", css_content)
    html_content = html_content.replace("/* JS_PLACEHOLDER */", js_content)
    html_content = html_content.replace("/* GRAPH_DATA_PLACEHOLDER */", graph_json)
    
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Write the HTML file
    with open(output_path, 'w') as f:
        f.write(html_content)
    
    # Get the relative path for display
    rel_path = os.path.relpath(output_path)
    
    # Print success message
    print(f"✅ {GREEN}Created interactive diagram at {BOLD}{UNDERLINE}{rel_path}{RESET}")
    
    # Handle browser opening based on no_browser flag
    if no_browser:
        print(f"🌐 Interactive visualization is ready. Use a web browser to open")
    else:
        # Try to open the browser automatically
        absolute_path = os.path.abspath(output_path)
        file_url = f"file://{absolute_path}"
        
        try:
            print(f"🔗 Attempting to open browser...")
            if webbrowser.open(file_url):
                print(f"{GREEN}🚀 Browser launched!{RESET}")
            else:
                print(f"{YELLOW}⚠️  Could not open browser automatically. Please open the file manually.{RESET}")
        except Exception as e:
            print(f"{YELLOW}⚠️  Error launching browser: {str(e)}{RESET}")
            print(f"{BLUE}🌐 Please open the file manually: {UNDERLINE}file://{absolute_path}{RESET}")

def extract_mermaid_from_markdown(file_path):
    """Extract mermaid content from a markdown file."""
    mermaid_pattern = r'```mermaid\n(.*?)```'
    
    with open(file_path, 'r') as f:
        content = f.read()
        match = re.search(mermaid_pattern, content, re.DOTALL)
        if match:
            return match.group(1).strip()
        else:
            raise ValueError("No mermaid content found in the provided file")

def main():
    parser = argparse.ArgumentParser(description='Analyze SQL files and generate lineage diagrams')
    parser.add_argument('--config', '-c', help='Path to the YAML configuration file', default='config.yml')
    parser.add_argument('--no-browser', '-n', action='store_true', help='Do not open browser automatically')
    parser.add_argument('--debug', '-d', action='store_true', help='Enable debug mode with verbose logging')
    
    args = parser.parse_args()
    
    try:
        # Print a nice banner
        print(f"\n{BOLD}{CYAN}=================================================={RESET}")
        print(f"{BOLD}{CYAN}                SQL Lineage Mapper                  {RESET}")
        print(f"{BOLD}{CYAN}=================================================={RESET}\n")
        
        # Get the script's directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Use config file in the script directory if relative path is provided
        config_path = os.path.join(script_dir, args.config) if not os.path.isabs(args.config) else args.config
        print(f"🔄 Loading configuration from: {config_path}")
            
        # Initialize the mapper with debug mode if requested
        mapper = SQLLineageMapper(config_path, debug=args.debug)
        
        # Log file-schema configuration if present
        if mapper.file_schema:
            print(f"📂 Using schema template: '{BOLD}{mapper.file_schema}{RESET}'")
        
        print(f"🔍 Searching for SQL files in: {BOLD}{mapper.root_folder}{RESET}")
        
        # Build lineage map
        if not mapper.build_lineage():
            sys.exit(1)
            
        # Generate interactive visualization if UI files exist
        output_dir = mapper.output_folder
        ui_dir = mapper.config.get('lineage_ui', 'ui')
        if not os.path.isabs(ui_dir):
            ui_dir = os.path.join(script_dir, ui_dir)
            
        # Paths for UI components
        html_template_path = os.path.join(ui_dir, 'template.html')
        js_file_path = os.path.join(ui_dir, 'visualization.js')
        css_file_path = os.path.join(ui_dir, 'styles.css')
        html_output_path = os.path.join(output_dir, "sql_lineage_interactive.html")
        
        if all(os.path.exists(f) for f in [html_template_path, js_file_path, css_file_path]):
            print(f"🔄 Generating interactive visualization...")
            
            # Extract mermaid content from the generated markdown file
            mermaid_content = extract_mermaid_from_markdown(str(mapper.md_file_path))
            
            # Parse mermaid to graph data
            graph_data = parse_mermaid_flowchart(mermaid_content)
            
            # Generate HTML with D3.js visualization, passing the no_browser flag
            generate_html(graph_data, html_output_path, html_template_path, js_file_path, css_file_path, args.no_browser)
        
        print(f"\n{BOLD}{CYAN}=================================================={RESET}")
        print(f"{BOLD}{CYAN}                    End                             {RESET}")
        print(f"{BOLD}{CYAN}=================================================={RESET}\n")
        
    except FileNotFoundError as e:
        print(f"\n{RED}{BOLD}FAILED:{RESET} {str(e)}")
        print(f"{YELLOW}Please check if the specified files exist.{RESET}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{RED}{BOLD}FAILED:{RESET} {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()