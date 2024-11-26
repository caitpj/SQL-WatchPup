import re
import yaml
from pathlib import Path
import logging

class SQLLineageMapper:
    def __init__(self, config_path: str):
        self.config = self.load_config(config_path)
        self.root_folder = self.config['sql_folder_path']
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
            level=logging.INFO,
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
        """Save Mermaid content to a markdown file, creating directories if needed."""
        output_path_file = Path(self.config.get('lineage_output_path_file', 'lineage.md'))
        
        try:
            # Create parent directories if they don't exist
            output_path_file.parent.mkdir(parents=True, exist_ok=True)
            
            markdown_content = f"""```mermaid
{mermaid_content}
```"""
            with open(output_path_file, 'w') as f:
                f.write(markdown_content)
            self.logger.info(f"Saved Mermaid diagram to {output_path_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving Mermaid file: {str(e)}")

def main():
    config_path = "config.yml"
    
    try:
        mapper = SQLLineageMapper(config_path)
        mapper.build_lineage_map()
        mermaid_content = mapper.generate_mermaid()
        mapper.save_mermaid(mermaid_content)
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()