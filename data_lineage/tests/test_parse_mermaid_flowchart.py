import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from generate_lineage import parse_mermaid_flowchart

def test_parse_mermaid_flowchart_basic():
    """Test basic flowchart parsing with simple node names."""
    mermaid_content = """
    flowchart TD
    A --> B
    B --> C
    C --> D
    """
    
    result = parse_mermaid_flowchart(mermaid_content)
    
    # Check nodes
    assert len(result["nodes"]) == 4
    assert {"id": "A", "name": "A"} in result["nodes"]
    assert {"id": "B", "name": "B"} in result["nodes"]
    assert {"id": "C", "name": "C"} in result["nodes"]
    assert {"id": "D", "name": "D"} in result["nodes"]
    
    # Check links
    assert len(result["links"]) == 3
    assert {"source": "A", "target": "B"} in result["links"]
    assert {"source": "B", "target": "C"} in result["links"]
    assert {"source": "C", "target": "D"} in result["links"]

def test_parse_mermaid_flowchart_with_quoted_names():
    """Test flowchart parsing with quoted node names."""
    mermaid_content = """
    flowchart TD
    "Node A" --> "Node B"
    "Node B" --> "Node C with spaces"
    """
    
    result = parse_mermaid_flowchart(mermaid_content)
    
    # Check nodes
    assert len(result["nodes"]) == 3
    assert {"id": "Node A", "name": "Node A"} in result["nodes"]
    assert {"id": "Node B", "name": "Node B"} in result["nodes"]
    assert {"id": "Node C with spaces", "name": "Node C with spaces"} in result["nodes"]
    
    # Check links
    assert len(result["links"]) == 2
    assert {"source": "Node A", "target": "Node B"} in result["links"]
    assert {"source": "Node B", "target": "Node C with spaces"} in result["links"]

def test_parse_mermaid_flowchart_with_schema_qualified_names():
    """Test flowchart parsing with schema-qualified names (containing dots)."""
    mermaid_content = """
    flowchart TD
    schema.table1 --> schema.table2
    schema.table2 --> other_schema.table3
    """
    
    result = parse_mermaid_flowchart(mermaid_content)
    
    # Check nodes
    assert len(result["nodes"]) == 3
    assert {"id": "schema.table1", "name": "schema.table1"} in result["nodes"]
    assert {"id": "schema.table2", "name": "schema.table2"} in result["nodes"]
    assert {"id": "other_schema.table3", "name": "other_schema.table3"} in result["nodes"]
    
    # Check links
    assert len(result["links"]) == 2
    assert {"source": "schema.table1", "target": "schema.table2"} in result["links"]
    assert {"source": "schema.table2", "target": "other_schema.table3"} in result["links"]

def test_parse_mermaid_flowchart_mixed_formats():
    """Test flowchart parsing with mixed node name formats."""
    mermaid_content = """
    flowchart TD
    A --> "Node B"
    "Node B" --> schema.table1
    schema.table1 --> {schema}.table2
    """
    
    result = parse_mermaid_flowchart(mermaid_content)
    
    # Check nodes
    assert len(result["nodes"]) == 4
    assert {"id": "A", "name": "A"} in result["nodes"]
    assert {"id": "Node B", "name": "Node B"} in result["nodes"]
    assert {"id": "schema.table1", "name": "schema.table1"} in result["nodes"]
    assert {"id": "{schema}.table2", "name": "{schema}.table2"} in result["nodes"]
    
    # Check links
    assert len(result["links"]) == 3
    assert {"source": "A", "target": "Node B"} in result["links"]
    assert {"source": "Node B", "target": "schema.table1"} in result["links"]
    assert {"source": "schema.table1", "target": "{schema}.table2"} in result["links"]

def test_parse_mermaid_flowchart_no_relationships():
    """Test flowchart parsing with no relationships defined."""
    mermaid_content = """
    flowchart TD
    """
    
    result = parse_mermaid_flowchart(mermaid_content)
    
    # Check empty results
    assert len(result["nodes"]) == 0
    assert len(result["links"]) == 0

def test_parse_mermaid_flowchart_with_other_content():
    """Test flowchart parsing with non-relationship content mixed in."""
    mermaid_content = """
    flowchart TD
    A --> B
    %% This is a comment
    B --> C
    subgraph Group1
    C --> D
    end
    """
    
    result = parse_mermaid_flowchart(mermaid_content)
    
    # Check nodes
    assert len(result["nodes"]) == 4
    assert {"id": "A", "name": "A"} in result["nodes"]
    assert {"id": "B", "name": "B"} in result["nodes"]
    assert {"id": "C", "name": "C"} in result["nodes"]
    assert {"id": "D", "name": "D"} in result["nodes"]
    
    # Check links
    assert len(result["links"]) == 3
    assert {"source": "A", "target": "B"} in result["links"]
    assert {"source": "B", "target": "C"} in result["links"]
    assert {"source": "C", "target": "D"} in result["links"]