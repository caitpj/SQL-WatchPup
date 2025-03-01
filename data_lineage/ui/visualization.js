// Data lineage visualization using dagre-d3
// Parse the graph data
const graphDataElem = document.getElementById('graph-data');
const graphData = JSON.parse(graphDataElem.textContent);

// Create a nodeById map for quick access
const nodeById = {};
graphData.nodes.forEach(node => {
  nodeById[node.id] = node;
  // Initialize position properties
  node.x = 0;
  node.y = 0;
  node.fx = null;
  node.fy = null;
});

// Set up SVG dimensions
const width = window.innerWidth;
const height = window.innerHeight;

// Create SVG container
const svg = d3.select("#graph-container")
  .append("svg")
  .attr("width", width)
  .attr("height", height);

// Create root group for zoom/pan
const rootGroup = svg.append("g");

// Add zoom behavior
const zoom = d3.zoom()
  .scaleExtent([0.1, 4])
  .on("zoom", (event) => {
    rootGroup.attr("transform", event.transform);
  });

svg.call(zoom);

// Create separate groups for edges and nodes
const edgesGroup = rootGroup.append("g").attr("class", "edges");
const nodesGroup = rootGroup.append("g").attr("class", "nodes");

// Create the dagre-d3 renderer
const render = new dagreD3.render();

// Create a new directed graph for layout calculation
const dagreGraph = new dagreD3.graphlib.Graph()
  .setGraph({
    rankdir: "LR", // Left to right layout
    nodesep: 70,   // Spacing between nodes in same rank
    ranksep: 150,  // Spacing between ranks
    marginx: 40,
    marginy: 40,
    edgesep: 25    // Spacing between edges
  })
  .setDefaultEdgeLabel(() => ({}));

// Process nodes for dagre layout
graphData.nodes.forEach(node => {
  const name = node.name || node.id || "Node";
  const textWidth = name.length * 8;
  const nodeWidth = Math.max(textWidth + 20, 80);
  
  dagreGraph.setNode(node.id, {
    label: name,
    width: nodeWidth,
    height: 40,
    rx: 5,
    ry: 5
  });
});

// Process edges for dagre layout
graphData.links.forEach(link => {
  const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
  const targetId = typeof link.target === 'object' ? link.target.id : link.target;
  
  dagreGraph.setEdge(sourceId, targetId, {
    curve: d3.curveBasis,
    arrowhead: 'vee'
  });
});

// Run the dagre layout algorithm
dagre.layout(dagreGraph);

// Transfer positions from the dagre layout to our node objects
dagreGraph.nodes().forEach(nodeId => {
  const dagreNode = dagreGraph.node(nodeId);
  const node = nodeById[nodeId];
  if (node) {
    node.x = dagreNode.x;
    node.y = dagreNode.y;
    // Store original positions for reset
    node.originalX = dagreNode.x;
    node.originalY = dagreNode.y;
    // Store size
    node.width = dagreNode.width;
    node.height = dagreNode.height;
  }
});

// Create edge elements
const edges = edgesGroup.selectAll("g.edge")
  .data(graphData.links)
  .enter()
  .append("g")
  .attr("class", "edge")
  .attr("id", d => {
    const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
    const targetId = typeof d.target === 'object' ? d.target.id : d.target;
    return `edge-${sourceId}-${targetId}`;
  });

// Add the path for each edge
edges.append("path")
  .attr("class", "edge-path")
  .attr("id", d => {
    const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
    const targetId = typeof d.target === 'object' ? d.target.id : d.target;
    return `edge-${sourceId}-${targetId}`;
  })
  .attr("marker-end", "url(#arrowhead)")
  .attr("stroke", "#999")
  .attr("stroke-width", 1.5)
  .attr("fill", "none");

// Create node elements
const nodes = nodesGroup.selectAll(".node")
  .data(graphData.nodes)
  .enter()
  .append("g")
  .attr("class", "node")
  .attr("id", d => `node-${d.id}`);

// Add rectangle for each node
nodes.append("rect")
  .attr("width", d => d.width)
  .attr("height", d => d.height)
  .attr("rx", 5)
  .attr("ry", 5)
  .attr("x", d => -d.width / 2)
  .attr("y", d => -d.height / 2)
  .attr("fill", "#69b3a2")
  .attr("stroke", "#999");

// Add text label for each node
nodes.append("text")
  .attr("text-anchor", "middle")
  .attr("dominant-baseline", "central")
  .attr("fill", "white")
  .text(d => d.name || d.id);

// Define arrow marker
svg.append("defs").append("marker")
  .attr("id", "arrowhead")
  .attr("viewBox", "0 -5 10 10")
  .attr("refX", 10)
  .attr("refY", 0)
  .attr("markerWidth", 6)
  .attr("markerHeight", 6)
  .attr("orient", "auto")
  .append("path")
  .attr("d", "M0,-5L10,0L0,5")
  .attr("fill", "#999");

// Function to update node positions
function updateNodesAndEdges() {
  // Update node positions
  nodes.attr("transform", d => `translate(${d.x},${d.y})`);
  
  // Update edge paths
  edges.select("path").attr("d", d => {
    const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
    const targetId = typeof d.target === 'object' ? d.target.id : d.target;
    const sourceNode = nodeById[sourceId];
    const targetNode = nodeById[targetId];
    
    if (!sourceNode || !targetNode) return "";
    
    // Calculate edge path with a curve
    const dx = targetNode.x - sourceNode.x;
    const dy = targetNode.y - sourceNode.y;
    const dr = Math.sqrt(dx * dx + dy * dy);
    
    // Offset the start and end points by half node width/height
    const sourceX = sourceNode.x + Math.sign(dx) * (sourceNode.width / 2);
    const sourceY = sourceNode.y;
    const targetX = targetNode.x - Math.sign(dx) * (targetNode.width / 2);
    const targetY = targetNode.y;
    
    // Return a curved path
    return `M${sourceX},${sourceY}C${sourceX + dx/3},${sourceY} ${targetX - dx/3},${targetY} ${targetX},${targetY}`;
  });
}

// Drag event handlers
function dragStart(event, d) {
  d3.select(this).classed("dragging", true);
  
  // Don't change the highlight state when dragging starts
  // The highlight should only change when clicking
  
  event.sourceEvent.stopPropagation(); // Prevent pan during drag
}

function dragging(event, d) {
  // Update the node position
  d.x += event.dx;
  d.y += event.dy;
  
  // Update the display
  updateNodesAndEdges();
}

function dragEnd(event, d) {
  d3.select(this).classed("dragging", false);
  // Keep highlighting after drag ends
}

// Create a drag behavior
const drag = d3.drag()
  .on("start", dragStart)
  .on("drag", dragging)
  .on("end", dragEnd);

// Attach the drag behavior to the nodes
nodes.call(drag);

// Find all ancestors (parents, grandparents, etc.) of a node
function findAncestors(nodeId, visited = new Set()) {
  if (visited.has(nodeId)) return visited;
  visited.add(nodeId);
  
  // Find all edges where this node is the target
  graphData.links.forEach(link => {
    const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
    const targetId = typeof link.target === 'object' ? link.target.id : link.target;
    
    if (targetId === nodeId) {
      findAncestors(sourceId, visited);
    }
  });
  
  return visited;
}

// Find all descendants (children, grandchildren, etc.) of a node
function findDescendants(nodeId, visited = new Set()) {
  if (visited.has(nodeId)) return visited;
  visited.add(nodeId);
  
  // Find all edges where this node is the source
  graphData.links.forEach(link => {
    const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
    const targetId = typeof link.target === 'object' ? link.target.id : link.target;
    
    if (sourceId === nodeId) {
      findDescendants(targetId, visited);
    }
  });
  
  return visited;
}

// Find all family members (ancestors and descendants) of a node
function findNodeFamily(nodeId) {
  const ancestors = findAncestors(nodeId, new Set());
  const descendants = findDescendants(nodeId, new Set());
  
  // Combine ancestors and descendants to get the complete family
  const family = new Set([...ancestors, ...descendants]);
  return family;
}

// Function to show only the family of the selected node and rearrange them
function showAndRearrangeFamily(nodeId) {
  if (!nodeId) return;
  
  // Clear any existing highlights first
  clearHighlights();
  
  // Find all family members (ancestors and descendants)
  const family = findNodeFamily(nodeId);
  
  // Hide all nodes not in the family
  nodes.classed("hidden", d => !family.has(d.id));
  
  // Hide all edges not connecting family members
  edges.classed("hidden", d => {
    const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
    const targetId = typeof d.target === 'object' ? d.target.id : d.target;
    
    return !(family.has(sourceId) && family.has(targetId));
  });
  
  // Highlight the selected node
  d3.select(`#node-${CSS.escape(nodeId)}`).classed("highlighted", true);
  
  // Create a new dagre layout for just the family, passing the selected node ID
  rearrangeFamilyLayout(family, nodeId);
}

// Create a new layout for the family nodes
function rearrangeFamilyLayout(family, selectedNodeId) {
  // Remember the position of the selected node
  const selectedNode = nodeById[selectedNodeId];
  const selectedNodePos = { x: selectedNode.x, y: selectedNode.y };
  
  // Create a new dagre graph for layout calculation
  const familyGraph = new dagreD3.graphlib.Graph()
    .setGraph({
      rankdir: "LR", // Left to right layout
      nodesep: 70,    // Spacing between nodes in same rank
      ranksep: 120,   // Spacing between ranks
      marginx: 40,
      marginy: 40,
      edgesep: 25     // Spacing between edges
    })
    .setDefaultEdgeLabel(() => ({}));
    
  // Add only family nodes to the graph
  graphData.nodes.forEach(node => {
    if (family.has(node.id)) {
      const name = node.name || node.id || "Node";
      const textWidth = name.length * 8;
      const nodeWidth = Math.max(textWidth + 20, 80);
      
      familyGraph.setNode(node.id, {
        label: name,
        width: nodeWidth,
        height: 40,
        rx: 5,
        ry: 5
      });
    }
  });
  
  // Add edges between family nodes
  graphData.links.forEach(link => {
    const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
    const targetId = typeof link.target === 'object' ? link.target.id : link.target;
    
    if (family.has(sourceId) && family.has(targetId)) {
      familyGraph.setEdge(sourceId, targetId, {
        curve: d3.curveBasis,
        arrowhead: 'vee'
      });
    }
  });
  
  // Run the dagre layout algorithm on the family graph
  dagre.layout(familyGraph);
  
  // Create an animation for the transition
  const t = d3.transition()
    .duration(750);
  
  // Calculate the offset between dagre's position for the selected node and its actual position
  const selectedDagreNode = familyGraph.node(selectedNodeId);
  const offsetX = selectedNodePos.x - selectedDagreNode.x;
  const offsetY = selectedNodePos.y - selectedDagreNode.y;
  
  // Update positions from the new layout, applying the offset to keep selected node fixed
  familyGraph.nodes().forEach(nodeId => {
    const dagreNode = familyGraph.node(nodeId);
    const node = nodeById[nodeId];
    if (node) {
      // Apply the offset to all positions so the selected node stays in place
      node.targetX = dagreNode.x + offsetX;
      node.targetY = dagreNode.y + offsetY;
    }
  });
  
  // For the selected node, ensure it doesn't move
  if (selectedNode) {
    selectedNode.targetX = selectedNodePos.x;
    selectedNode.targetY = selectedNodePos.y;
  }
  
  // Animate nodes to their new positions, except the selected node
  nodes.filter(d => family.has(d.id) && d.id !== selectedNodeId)
    .transition(t)
    .attr("transform", d => `translate(${d.targetX},${d.targetY})`);
  
  // Animate edge paths at the same time as nodes move
  edges.filter(d => {
    const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
    const targetId = typeof d.target === 'object' ? d.target.id : d.target;
    return family.has(sourceId) && family.has(targetId);
  }).select("path")
    .transition(t)
    .attrTween("d", function(d) {
      const sourceId = typeof d.source === 'object' ? d.source.id : d.source;
      const targetId = typeof d.target === 'object' ? d.target.id : d.target;
      const sourceNode = nodeById[sourceId];
      const targetNode = nodeById[targetId];
      
      // Skip if nodes aren't available
      if (!sourceNode || !targetNode) return () => "";
      
      // For the source or target that is being animated, we need to 
      // interpolate its position during animation
      const sourceIsMoving = sourceId !== selectedNodeId && family.has(sourceId);
      const targetIsMoving = targetId !== selectedNodeId && family.has(targetId);
      
      // Initial positions
      const startSourceX = sourceNode.x;
      const startSourceY = sourceNode.y;
      const startTargetX = targetNode.x;
      const startTargetY = targetNode.y;
      
      // Final positions
      const endSourceX = sourceIsMoving ? sourceNode.targetX : sourceNode.x;
      const endSourceY = sourceIsMoving ? sourceNode.targetY : sourceNode.y;
      const endTargetX = targetIsMoving ? targetNode.targetX : targetNode.x;
      const endTargetY = targetIsMoving ? targetNode.targetY : targetNode.y;
      
      return function(t) {
        // Interpolate positions
        const currentSourceX = sourceIsMoving ? startSourceX + (endSourceX - startSourceX) * t : startSourceX;
        const currentSourceY = sourceIsMoving ? startSourceY + (endSourceY - startSourceY) * t : startSourceY;
        const currentTargetX = targetIsMoving ? startTargetX + (endTargetX - startTargetX) * t : startTargetX;
        const currentTargetY = targetIsMoving ? startTargetY + (endTargetY - startTargetY) * t : startTargetY;
        
        // Calculate path parameters
        const dx = currentTargetX - currentSourceX;
        const dy = currentTargetY - currentSourceY;
        
        // Offset start and end points by half node width
        const sourceX = currentSourceX + Math.sign(dx) * (sourceNode.width / 2);
        const sourceY = currentSourceY;
        const targetX = currentTargetX - Math.sign(dx) * (targetNode.width / 2);
        const targetY = currentTargetY;
        
        // Return curved path
        return `M${sourceX},${sourceY}C${sourceX + dx/3},${sourceY} ${targetX - dx/3},${targetY} ${targetX},${targetY}`;
      };
    });
  
  // Also update actual positions (after animation)
  setTimeout(() => {
    familyGraph.nodes().forEach(nodeId => {
      const node = nodeById[nodeId];
      if (node && nodeId !== selectedNodeId) {
        node.x = node.targetX;
        node.y = node.targetY;
      }
    });
    updateNodesAndEdges();
    
    // Removed the fitFamilyView call to prevent automatic zooming
  }, 750);
}

// Function to fit the view to show the entire family
function fitFamilyView(family) {
  // Calculate bounds of the family
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  
  graphData.nodes.forEach(node => {
    if (family.has(node.id)) {
      const halfWidth = node.width / 2;
      const halfHeight = node.height / 2;
      
      minX = Math.min(minX, node.x - halfWidth);
      minY = Math.min(minY, node.y - halfHeight);
      maxX = Math.max(maxX, node.x + halfWidth);
      maxY = Math.max(maxY, node.y + halfHeight);
    }
  });
  
  // Calculate dimensions
  const familyWidth = maxX - minX;
  const familyHeight = maxY - minY;
  
  if (familyWidth <= 0 || familyHeight <= 0) return;
  
  // Calculate scale to fit with padding
  const padding = 60;
  const scaleX = (width - padding*2) / familyWidth;
  const scaleY = (height - padding*2) / familyHeight;
  const scale = Math.min(scaleX, scaleY, 2);
  
  // Calculate center of the family
  const centerX = (minX + maxX) / 2;
  const centerY = (minY + maxY) / 2;
  
  // Calculate translation to center the family
  const translateX = width/2 - centerX * scale;
  const translateY = height/2 - centerY * scale;
  
  // Apply the transform with animation
  svg.transition()
    .duration(500)
    .call(zoom.transform, d3.zoomIdentity
      .translate(translateX, translateY)
      .scale(scale));
}

// Show all nodes and edges
function showAllNodes() {
  nodes.classed("hidden", false);
  edges.classed("hidden", false);
  clearHighlights();
}

// Highlight only the node and its directly connected arrows
function highlightNode(nodeId) {
  if (!nodeId) return;
  
  // Highlight the selected node
  d3.select(`#node-${CSS.escape(nodeId)}`).classed("highlighted", true);
  
  // Find and highlight connected edges only (not the connected nodes)
  graphData.links.forEach(link => {
    const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
    const targetId = typeof link.target === 'object' ? link.target.id : link.target;
    
    // If this link connects to our node, highlight it
    if (sourceId === nodeId || targetId === nodeId) {
      d3.select(`#edge-${CSS.escape(sourceId)}-${CSS.escape(targetId)} path`)
        .classed("highlighted", true);
    }
  });
}

// Clear highlights
function clearHighlights() {
  nodes.classed("highlighted", false);
  edges.selectAll("path").classed("highlighted", false);
}

// Center view on a specific node
function centerOnNode(node) {
  const nodeX = node.x;
  const nodeY = node.y;
  
  svg.transition()
    .duration(500)
    .call(zoom.transform, d3.zoomIdentity
      .translate(width/2 - nodeX, height/2 - nodeY)
      .scale(1.0));
}

// Create a dropdown container for search results
const searchInput = document.getElementById("search-input");
const searchDropdown = document.createElement("div");
searchDropdown.id = "search-dropdown";
searchDropdown.className = "search-dropdown";

// Add the dropdown next to the search input
if (searchInput) {
  searchInput.parentNode.appendChild(searchDropdown);
}

// Update dropdown position based on search input
function updateDropdownPosition() {
  if (!searchInput) return;
  
  const inputRect = searchInput.getBoundingClientRect();
  searchDropdown.style.top = inputRect.height + "px";
}

// Search functionality with dropdown
if (searchInput) {
  let searchTimeout;
  
  searchInput.addEventListener("input", function(e) {
    clearTimeout(searchTimeout);
    const searchTerm = e.target.value.toLowerCase();
    
    // Clear dropdown when search is cleared
    if (searchTerm === "") {
      searchDropdown.style.display = "none";
      searchDropdown.innerHTML = "";
      return;
    }
    
    // Only show dropdown after 3 characters are entered
    if (searchTerm.length < 3) {
      searchDropdown.style.display = "none";
      searchDropdown.innerHTML = "";
      return;
    }
    
    searchTimeout = setTimeout(() => {
      // Find matching nodes
      const matchingNodes = graphData.nodes.filter(node => {
        const nameStr = (node.name || "").toLowerCase();
        const idStr = (node.id || "").toLowerCase();
        return nameStr.includes(searchTerm) || idStr.includes(searchTerm);
      });
      
      // Update the dropdown
      searchDropdown.innerHTML = "";
      
      if (matchingNodes.length > 0) {
        searchDropdown.style.display = "block";
        updateDropdownPosition();
        
        matchingNodes.forEach((node, index) => {
          const item = document.createElement("div");
          item.className = "dropdown-item";
          item.textContent = node.name || node.id;
          
          // Handle click on dropdown item
          item.addEventListener("click", function() {
            searchInput.value = node.name || node.id; // Set the search input to the selected node
            searchDropdown.style.display = "none"; // Hide dropdown
            
            // Show and rearrange the family
            showAndRearrangeFamily(node.id);
          });
          
          searchDropdown.appendChild(item);
        });
      } else {
        searchDropdown.style.display = "none";
      }
    }, 300);
  });
  
  // Handle enter key
  searchInput.addEventListener("keydown", function(e) {
    if (e.key === "Enter") {
      const firstItem = searchDropdown.querySelector(".dropdown-item");
      if (firstItem) {
        firstItem.click(); // Simulate click on the first item
      }
    }
  });
  
  // Hide dropdown when clicking outside
  document.addEventListener("click", function(e) {
    if (e.target !== searchInput && !searchDropdown.contains(e.target)) {
      searchDropdown.style.display = "none";
    }
  });

  // Update dropdown position on window resize
  window.addEventListener("resize", updateDropdownPosition);
}

// Update node click handler
nodes.on("click", function(event, d) {
  event.stopPropagation();
  showAndRearrangeFamily(d.id);
});

// Reset view functionality
const resetViewBtn = document.getElementById("reset-view");
if (resetViewBtn) {
  resetViewBtn.addEventListener("click", function() {
    // Show all nodes
    nodes.classed("hidden", false);
    edges.classed("hidden", false);
    
    // Clear highlights
    nodes.classed("highlighted", false);
    edges.classed("highlighted", false);
    
    // Reset search input
    if (searchInput) {
      searchInput.value = "";
      searchDropdown.style.display = "none";
      searchDropdown.innerHTML = "";
    }
    
    // Reset node positions to original layout with animation
    const t = d3.transition().duration(750);
    
    nodes.transition(t)
      .attr("transform", d => `translate(${d.originalX},${d.originalY})`);
    
    // Update actual positions after animation
    setTimeout(() => {
      graphData.nodes.forEach(node => {
        node.x = node.originalX;
        node.y = node.originalY;
      });
      
      updateNodesAndEdges();
      
      // Reset zoom/pan
      svg.transition()
        .duration(500)
        .call(zoom.transform, getOptimalZoomTransform());
    }, 750);
  });
}

// Show All button functionality - simplified to act like page refresh
const showAllBtn = document.getElementById("show-all");
if (showAllBtn) {
  showAllBtn.addEventListener("click", function() {
    // Simply reload the page to get a completely fresh state
    window.location.reload();
  });
}

// REMOVED: Background click handler to show all nodes
// This functionality is now only available via the Show All button

// Resize handler
window.addEventListener('resize', function() {
  const newWidth = window.innerWidth;
  const newHeight = window.innerHeight;
  
  svg.attr("width", newWidth)
     .attr("height", newHeight);

  // Apply optimal zoom
  svg.call(zoom.transform, getOptimalZoomTransform());
});

// Calculate optimal zoom transform to fit graph
function getOptimalZoomTransform() {
  // Find the bounds of the graph
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  
  graphData.nodes.forEach(node => {
    const halfWidth = node.width / 2;
    const halfHeight = node.height / 2;
    
    minX = Math.min(minX, node.x - halfWidth);
    minY = Math.min(minY, node.y - halfHeight);
    maxX = Math.max(maxX, node.x + halfWidth);
    maxY = Math.max(maxY, node.y + halfHeight);
  });
  
  // Calculate graph dimensions
  const graphWidth = maxX - minX;
  const graphHeight = maxY - minY;
  
  if (graphWidth <= 0 || graphHeight <= 0) return d3.zoomIdentity;
  
  // Calculate scale to fit the graph with padding
  const padding = 40;
  const scaleX = (width - padding*2) / graphWidth;
  const scaleY = (height - padding*2) / graphHeight;
  const scale = Math.min(scaleX, scaleY, 2); // Cap scale at 2x
  
  // Calculate center of the graph
  const centerX = (minX + maxX) / 2;
  const centerY = (minY + maxY) / 2;
  
  // Calculate translation to center the graph
  const translateX = width/2 - centerX * scale;
  const translateY = height/2 - centerY * scale;
  
  return d3.zoomIdentity
    .translate(translateX, translateY)
    .scale(scale);
}

// Make sure all nodes are visible initially
showAllNodes();

// Initial setup
updateNodesAndEdges();
svg.call(zoom.transform, getOptimalZoomTransform());

console.log("Graph rendered with", graphData.nodes.length, "nodes");