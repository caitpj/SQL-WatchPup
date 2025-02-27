#!/usr/bin/env python3
import re
import sys
import os
import argparse
import json
import yaml
import webbrowser
from pathlib import Path
import logging

class SQLLineageMapper:
    def __init__(self, config_path: str):
        self.config = self.load_config(config_path)
        self.root_folder = self.config['sql_folder_path']
        self.output_folder = self.config.get('lineage_output', 'output')
        self.relationships = set()  # Store table relationships
        self.setup_logging()
        
    def load_config(self, config_path: str) -> dict:
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            if 'sql_folder_path' not in config:
                raise ValueError("Configuration must include 'sql_folder_path'")
            return config
        except Exception as e:
            raise Exception(f"Error loading configuration: {str(e)}")
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.WARNING,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def find_sql_files(self):
        return list(Path(self.root_folder).rglob('*.sql'))
    
    def extract_ctes(self, sql_content: str) -> set:
        """Extract CTE names from SQL content."""
        # Match 'WITH name AS (' or ', name AS ('
        cte_pattern = r'(?:with|,)\s+([a-zA-Z0-9_]+)\s+as\s*\('
        ctes = set()
        
        # Find all CTEs in the SQL, ignoring case
        matches = re.finditer(cte_pattern, sql_content, re.IGNORECASE)
        for match in matches:
            # Check if the match isn't in a comment
            line_start = sql_content.rfind('\n', 0, match.start()) + 1
            if line_start == 0:
                line_start = 0
            line_to_match = sql_content[line_start:match.start()]
            if not line_to_match.strip().startswith('--'):
                ctes.add(match.group(1).lower())
                
        return ctes
    
    def extract_parent_tables(self, sql_content: str):
        """Extract parent table names from SQL content using regex, excluding CTEs."""
        parent_tables = set()
        
        # First, get all CTEs to exclude them
        ctes = self.extract_ctes(sql_content)
        
        # Split into lines but preserve multi-line statements
        sql_content = ' '.join(
            line for line in sql_content.split('\n')
            if not line.strip().startswith('--')
        )
        
        # Find all 'from schema.table' or 'from table' patterns
        from_pattern = r'from\s+(([a-zA-Z0-9_]+)\.)?([a-zA-Z0-9_]+)'
        from_matches = re.finditer(from_pattern, sql_content, re.IGNORECASE)
        
        # Find all 'join schema.table' or 'join table' patterns
        join_pattern = r'join\s+(([a-zA-Z0-9_]+)\.)?([a-zA-Z0-9_]+)'
        join_matches = re.finditer(join_pattern, sql_content, re.IGNORECASE)
        
        # Process matches
        for match in list(from_matches) + list(join_matches):
            table_name = match.group(3).lower()
            # Only add if it's not a CTE
            if table_name not in ctes:
                parent_tables.add(table_name)
        
        return parent_tables
    
    def process_sql_file(self, file_path: Path):
        """Process a single SQL file and track relationships."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            table_name = file_path.stem.lower()
            parent_tables = self.extract_parent_tables(content)
            
            # Add relationships
            for parent in parent_tables:
                self.relationships.add((parent, table_name))
            
        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {str(e)}")
    
    def build_lineage_map(self):
        """Build the complete lineage map for all SQL files."""
        sql_files = self.find_sql_files()
        self.logger.info(f"Found {len(sql_files)} SQL files to process")
        
        for file_path in sql_files:
            self.process_sql_file(file_path)
    
    def generate_mermaid(self):
        """Generate simple Mermaid flowchart markup."""
        mermaid_lines = ["flowchart TD"]
        
        # Add relationships
        for source, target in self.relationships:
            mermaid_lines.append(f"    {source}-->{target}")
        
        return "\n".join(mermaid_lines)
    
    def save_mermaid(self, mermaid_content: str):
        """Save Mermaid content to a markdown file in the output folder."""
        output_dir = Path(self.output_folder)
        output_path_file = output_dir / "lineage.md"
        
        try:
            # Create output directory if it doesn't exist
            output_dir.mkdir(parents=True, exist_ok=True)
            
            markdown_content = f"""```mermaid
{mermaid_content}
```"""
            with open(output_path_file, 'w') as f:
                f.write(markdown_content)
            self.logger.info(f"Saved Mermaid diagram to {output_path_file}")
            return output_path_file
            
        except Exception as e:
            self.logger.error(f"Error saving Mermaid file: {str(e)}")
            raise

def extract_mermaid_from_markdown(file_path):
    """Extract mermaid content from a markdown file efficiently."""
    mermaid_pattern = r'```mermaid\n(.*?)```'
    
    # Read file in chunks to handle large files more efficiently
    chunk_size = 8192
    mermaid_content = ""
    with open(file_path, 'r') as f:
        buffer = ""
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            buffer += chunk
            
            # Try to find mermaid content in current buffer
            match = re.search(mermaid_pattern, buffer, re.DOTALL)
            if match:
                mermaid_content = match.group(1).strip()
                break
            
            # Keep the last potential part of a mermaid block for next chunk
            if '```mermaid' in buffer:
                last_start = buffer.rfind('```mermaid')
                buffer = buffer[last_start:]
            else:
                buffer = ""
    
    if not mermaid_content:
        raise ValueError("No mermaid content found in the provided file")
    
    return mermaid_content

def parse_mermaid_flowchart(mermaid_content):
    """Parse mermaid flowchart to extract nodes and links with improved regex."""
    lines = mermaid_content.split('\n')
    
    # Check if the first line contains flowchart/graph definition
    # and skip it only if it does
    relationship_lines = []
    start_index = 0
    
    if lines and (lines[0].strip().startswith("flowchart") or 
                 lines[0].strip().startswith("graph")):
        start_index = 1
    
    relationship_lines = [line.strip() for line in lines[start_index:] if line.strip()]
    
    # Extract relationships with a more comprehensive regex
    # This handles more node ID formats including quoted IDs
    nodes = set()
    links = []
    
    # Improved regex that handles more node ID formats
    relationship_pattern = r'(?:"([^"]+)"|([a-zA-Z0-9_\-:]+))\s*-->\s*(?:"([^"]+)"|([a-zA-Z0-9_\-:]+))'
    
    for line in relationship_lines:
        match = re.search(relationship_pattern, line)
        if match:
            # Extract source and target, handling both quoted and unquoted formats
            source = match.group(1) if match.group(1) else match.group(2)
            target = match.group(3) if match.group(3) else match.group(4)
            
            nodes.add(source)
            nodes.add(target)
            links.append({"source": source, "target": target})
    
    # Convert nodes to list of dictionaries with formatted names for better display
    node_list = [
        {
            "id": node, 
            "name": format_node_name(node)
        } 
        for node in nodes
    ]
    
    return {
        "nodes": node_list,
        "links": links
    }

def format_node_name(node_id):
    """Format node ID to a more readable name."""
    # Replace underscores with spaces and capitalize words
    return ' '.join(word.capitalize() for word in node_id.split('_'))

def read_file_content(file_path):
    """Read file content from a given file path."""
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except Exception as e:
        return None

def generate_html(graph_data, output_path, html_template_path, js_file_path, css_file_path):
    """Generate an HTML file using external template, JS, and CSS files."""
    # Check if the required files exist
    if not os.path.exists(html_template_path):
        raise FileNotFoundError(f"HTML template file not found at: {html_template_path}")
    
    if not os.path.exists(js_file_path):
        raise FileNotFoundError(f"JavaScript file not found at: {js_file_path}")
    
    if not os.path.exists(css_file_path):
        raise FileNotFoundError(f"CSS file not found at: {css_file_path}")
    
    # Read the files
    html_template = read_file_content(html_template_path)
    js_content = read_file_content(js_file_path)
    css_content = read_file_content(css_file_path)
    
    if html_template is None:
        raise IOError(f"Failed to read HTML template file: {html_template_path}")
    
    if js_content is None:
        raise IOError(f"Failed to read JavaScript file: {js_file_path}")
    
    if css_content is None:
        raise IOError(f"Failed to read CSS file: {css_file_path}")
    
    # Convert graph data to JSON string
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
    
    # Define colors
    GREEN = "\033[92m"
    BLUE = "\033[94m"
    RESET = "\033[0m"
    
    # Print success message
    print(f"{GREEN}SUCCESS:{RESET} Created interactive diagram at {rel_path}")
    print(f"{BLUE}Open this file in a web browser to view the interactive diagram{RESET}")
    
    # Try to open the browser automatically
    absolute_path = os.path.abspath(output_path)
    file_url = f"file://{absolute_path}"
    
    try:
        webbrowser.open(file_url)
    except Exception:
        pass

def main():
    parser = argparse.ArgumentParser(description='Analyze SQL files, generate lineage diagrams, and create interactive visualizations')
    parser.add_argument('--config', '-c', help='Path to the YAML configuration file', default='config.yml')
    parser.add_argument('--no-browser', action='store_true', help='Do not open browser automatically')
    
    args = parser.parse_args()
    
    # Disable info logging at the very start
    logging.getLogger().setLevel(logging.WARNING)
    
    try:
        # Get the script's directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Use config file in the script directory if relative path is provided
        if not os.path.isabs(args.config):
            config_path = os.path.join(script_dir, args.config)
        else:
            config_path = args.config
            
        # Load the config file
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            
        # Get paths from config
        sql_folder_path = config.get('sql_folder_path')
        output_dir = config.get('lineage_output', 'output')
        ui_dir = config.get('lineage_ui', 'ui')
        
        # Validate SQL folder path exists
        if not sql_folder_path:
            raise ValueError("sql_folder_path is missing in the config file")
            
        # If paths are relative, make them relative to script directory
        if not os.path.isabs(sql_folder_path):
            sql_folder_path = os.path.join(script_dir, sql_folder_path)
        if not os.path.isabs(output_dir):
            output_dir = os.path.join(script_dir, output_dir)
        if not os.path.isabs(ui_dir):
            ui_dir = os.path.join(script_dir, ui_dir)
            
        # Check if SQL folder exists
        if not os.path.exists(sql_folder_path):
            raise FileNotFoundError(f"SQL folder not found: {sql_folder_path}")
        
        # Find SQL files using os.walk
        sql_files_found = []
        for root, dirs, files in os.walk(sql_folder_path):
            for file in files:
                if file.lower().endswith('.sql'):
                    full_path = os.path.join(root, file)
                    sql_files_found.append(full_path)
        
        if not sql_files_found:
            print("No SQL files found in the specified directory. Please check the sql_folder_path in the config file.")
            # Create empty diagram
            empty_mermaid = "flowchart TD"
            md_file_path = os.path.join(output_dir, "lineage.md")
            os.makedirs(os.path.dirname(md_file_path), exist_ok=True)
            with open(md_file_path, 'w') as f:
                f.write(f"```mermaid\n{empty_mermaid}\n```")
            
            # Define colors
            GREEN = "\033[92m"
            RESET = "\033[0m"
            rel_path = os.path.relpath(md_file_path)
            print(f"{GREEN}SUCCESS:{RESET} Created empty Mermaid diagram at {rel_path}")
            return
            
        html_filename = "sql_lineage_interactive.html"
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize the mapper
        mapper = SQLLineageMapper(config_path)
        mapper.output_folder = output_dir  # Ensure the output folder is the absolute path
        
        # Process each SQL file we found
        for sql_file in sql_files_found:
            try:
                mapper.process_sql_file(Path(sql_file))
            except Exception as e:
                print(f"Error processing file {sql_file}: {e}")
        
        # Generate and save the Mermaid diagram
        mermaid_content = mapper.generate_mermaid()
        
        # Only proceed if we have actual relationships
        if len(mapper.relationships) == 0:
            print("No SQL relationships found. Check if your SQL files contain FROM or JOIN statements.")
            # Still save the empty diagram for reference
            md_file_path = mapper.save_mermaid(mermaid_content)
            return
            
        md_file_path = mapper.save_mermaid(mermaid_content)
        
        # Step 2: Convert Mermaid diagram to interactive visualization
        # Paths for template, JS and CSS files in ui directory specified in config
        html_template_path = os.path.join(ui_dir, 'template.html')
        js_file_path = os.path.join(ui_dir, 'visualization.js')
        css_file_path = os.path.join(ui_dir, 'styles.css')
        
        # Check UI files exist
        missing_ui_files = False
        for file_path, file_desc in [
            (html_template_path, "HTML template"),
            (js_file_path, "JavaScript"),
            (css_file_path, "CSS")
        ]:
            if not os.path.exists(file_path):
                print(f"WARNING: {file_desc} file not found at: {file_path}")
                missing_ui_files = True
        
        if missing_ui_files:
            print("Cannot create interactive HTML visualization due to missing UI files.")
            return
            
        # Extract mermaid content from the generated markdown file
        mermaid_content = extract_mermaid_from_markdown(md_file_path)
        
        # Parse mermaid to graph data
        graph_data = parse_mermaid_flowchart(mermaid_content)
        
        # Set whether to open the browser based on command-line arguments
        if args.no_browser:
            # Monkey patch webbrowser.open to do nothing
            original_open = webbrowser.open
            webbrowser.open = lambda x: False
            
        # Generate HTML with D3.js visualization
        html_output_path = os.path.join(output_dir, html_filename)
        generate_html(graph_data, html_output_path, html_template_path, js_file_path, css_file_path)
        
        # Restore original function if we patched it
        if args.no_browser:
            webbrowser.open = original_open
        
    except FileNotFoundError as e:
        print(f"Error: {str(e)}")
        print("Please create the required template, JS and CSS files before running this script.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()