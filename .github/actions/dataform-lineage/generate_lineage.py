#!/usr/bin/env python3
"""
Dataform Lineage Diagram Generator

Generates lineage diagrams from Dataform compilation results in multiple formats:
- Mermaid (markdown)
- Graphviz DOT
- vis.js HTML (interactive)
"""

import json
import os
import sys
from pathlib import Path


def make_node_id(schema: str, name: str) -> str:
    """Create a valid node ID (alphanumeric with underscores)."""
    return f"{schema}__{name}".replace(".", "_").replace("-", "_")


def parse_compilation_result(compilation_data: dict) -> tuple[set, list]:
    """
    Parse Dataform compilation result and extract nodes and edges.

    Returns:
        tuple: (nodes set of (id, label, schema), edges list of (from_id, to_id))
    """
    nodes = set()
    edges = []

    actions = compilation_data.get("compilationResultActions", [])
    print(f"Processing {len(actions)} actions from compilation result")

    for action in actions:
        target = action.get("target", {})
        schema = target.get("schema", "unknown")
        name = target.get("name", "unknown")
        node_id = make_node_id(schema, name)
        label = f"{schema}.{name}"
        nodes.add((node_id, label, schema))

        # Get dependencies from relation or operations
        dep_targets = []
        if "relation" in action:
            dep_targets = action["relation"].get("dependencyTargets", [])
        elif "operations" in action:
            dep_targets = action["operations"].get("dependencyTargets", [])

        for dep in dep_targets:
            dep_schema = dep.get("schema", "unknown")
            dep_name = dep.get("name", "unknown")
            dep_id = make_node_id(dep_schema, dep_name)
            dep_label = f"{dep_schema}.{dep_name}"
            nodes.add((dep_id, dep_label, dep_schema))
            edges.append((dep_id, node_id))

    return nodes, edges


def generate_mermaid(nodes: set, edges: list) -> str:
    """Generate Mermaid diagram markdown."""
    lines = ["# Dataform Lineage Diagram", "", "```mermaid", "graph LR"]

    if edges:
        edge_lines = []
        for from_id, to_id in edges:
            from_label = next((label for nid, label, _ in nodes if nid == from_id), from_id)
            to_label = next((label for nid, label, _ in nodes if nid == to_id), to_id)
            edge_lines.append(f'    {from_id}["{from_label}"] --> {to_id}["{to_label}"]')
        lines.extend(sorted(set(edge_lines)))
    else:
        lines.append("    NoData[No dependencies found]")

    lines.append("```")
    return "\n".join(lines)


def generate_dot(nodes: set, edges: list) -> str:
    """Generate Graphviz DOT diagram."""
    lines = [
        "digraph dataform_lineage {",
        "    rankdir=LR;",
        "    node [shape=box];",
        ""
    ]

    # Add node definitions
    for node_id, label, _ in sorted(nodes):
        lines.append(f'    {node_id} [label="{label}"];')

    lines.append("")

    # Add edges
    for from_id, to_id in sorted(set(edges)):
        lines.append(f"    {from_id} -> {to_id};")

    lines.append("}")
    return "\n".join(lines)


def generate_visjs_html(nodes: set, edges: list) -> str:
    """Generate interactive vis.js HTML visualization."""

    # Build nodes JSON
    nodes_json = []
    for node_id, label, schema in sorted(nodes):
        nodes_json.append({
            "id": node_id,
            "label": label,
            "group": schema,
            "font": {"size": 12}
        })

    # Build edges JSON
    edges_json = []
    for i, (from_id, to_id) in enumerate(sorted(set(edges))):
        edges_json.append({
            "id": i,
            "from": from_id,
            "to": to_id,
            "arrows": "to"
        })

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dataform Lineage</title>
    <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: #f5f5f5;
        }}
        .header {{
            background: #1a1a2e;
            color: white;
            padding: 1rem 2rem;
        }}
        .header h1 {{
            font-size: 1.5rem;
            font-weight: 500;
        }}
        #graph {{
            width: 100%;
            height: calc(100vh - 60px);
            background: white;
        }}
        .controls {{
            position: absolute;
            top: 80px;
            right: 20px;
            background: white;
            padding: 1rem;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            z-index: 1000;
        }}
        .controls button {{
            display: block;
            width: 100%;
            padding: 0.5rem 1rem;
            margin-bottom: 0.5rem;
            border: 1px solid #ddd;
            background: white;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.875rem;
        }}
        .controls button:hover {{
            background: #f0f0f0;
        }}
        .controls button:last-child {{
            margin-bottom: 0;
        }}
        .search-box {{
            position: absolute;
            top: 80px;
            left: 20px;
            background: white;
            padding: 1rem;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            z-index: 1000;
        }}
        .search-box input {{
            padding: 0.5rem;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 0.875rem;
            width: 200px;
        }}
        .search-box input:focus {{
            outline: none;
            border-color: #1a1a2e;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Dataform Lineage</h1>
    </div>
    <div class="search-box">
        <input type="text" id="search" placeholder="Search nodes..." onkeyup="searchNodes(this.value)">
    </div>
    <div class="controls">
        <button onclick="network.fit()">Fit to View</button>
        <button onclick="setHierarchical()">Hierarchical</button>
        <button onclick="setFreeform()">Freeform</button>
    </div>
    <div id="graph"></div>

    <script>
        const nodes = new vis.DataSet({json.dumps(nodes_json, indent=2)});

        const edges = new vis.DataSet({json.dumps(edges_json, indent=2)});

        const container = document.getElementById("graph");
        const data = {{ nodes: nodes, edges: edges }};

        const options = {{
            layout: {{
                hierarchical: {{
                    enabled: true,
                    direction: "LR",
                    sortMethod: "directed",
                    levelSeparation: 250,
                    nodeSpacing: 40,
                    shakeTowards: "roots"
                }}
            }},
            physics: {{
                enabled: false
            }},
            edges: {{
                smooth: {{
                    type: "cubicBezier",
                    forceDirection: "horizontal"
                }},
                arrows: {{
                    to: {{ scaleFactor: 0.5 }}
                }},
                color: {{ color: "#848484", hover: "#1a1a2e" }}
            }},
            nodes: {{
                shape: "box",
                margin: 10
            }},
            interaction: {{
                hover: true,
                tooltipDelay: 200,
                hideEdgesOnDrag: true
            }}
        }};

        const network = new vis.Network(container, data, options);

        function setHierarchical() {{
            network.setOptions({{
                layout: {{
                    hierarchical: {{
                        enabled: true,
                        direction: "LR",
                        sortMethod: "directed",
                        levelSeparation: 250,
                        nodeSpacing: 40,
                        shakeTowards: "roots"
                    }}
                }},
                physics: {{ enabled: false }}
            }});
            network.fit();
        }}

        function setFreeform() {{
            network.setOptions({{
                layout: {{
                    hierarchical: {{ enabled: false }}
                }},
                physics: {{
                    enabled: true,
                    solver: "forceAtlas2Based",
                    forceAtlas2Based: {{
                        gravitationalConstant: -50,
                        centralGravity: 0.01,
                        springLength: 150,
                        springConstant: 0.08
                    }},
                    stabilization: {{ iterations: 100 }}
                }}
            }});
        }}

        // Fit on load
        network.once("stabilizationIterationsDone", function() {{
            network.fit();
        }});

        // Highlight connected nodes on hover
        network.on("hoverNode", function(params) {{
            const nodeId = params.node;
            const connectedNodes = network.getConnectedNodes(nodeId);
            const connectedEdges = network.getConnectedEdges(nodeId);

            // Dim all nodes except hovered and connected
            nodes.forEach(node => {{
                if (node.id === nodeId || connectedNodes.includes(node.id)) {{
                    nodes.update({{ id: node.id, opacity: 1 }});
                }} else {{
                    nodes.update({{ id: node.id, opacity: 0.2 }});
                }}
            }});
        }});

        network.on("blurNode", function(params) {{
            // Reset all nodes
            nodes.forEach(node => {{
                nodes.update({{ id: node.id, opacity: 1 }});
            }});
        }});

        // Search functionality
        function searchNodes(query) {{
            if (!query) {{
                // Reset all nodes to default
                nodes.forEach(node => {{
                    nodes.update({{ id: node.id, opacity: 1 }});
                }});
                return;
            }}

            query = query.toLowerCase();
            const matchingIds = [];

            nodes.forEach(node => {{
                if (node.label.toLowerCase().includes(query)) {{
                    matchingIds.push(node.id);
                    nodes.update({{ id: node.id, opacity: 1 }});
                }} else {{
                    nodes.update({{ id: node.id, opacity: 0.2 }});
                }}
            }});

            // Focus on first match
            if (matchingIds.length > 0) {{
                network.focus(matchingIds[0], {{
                    scale: 1,
                    animation: {{ duration: 300 }}
                }});
            }}
        }}
    </script>
</body>
</html>'''

    return html


def main():
    # Read configuration from environment
    compilation_json = os.environ.get("COMPILATION_DETAILS")
    if not compilation_json:
        print("Error: COMPILATION_DETAILS environment variable not set")
        sys.exit(1)

    output_formats = os.environ.get("OUTPUT_FORMATS", "mermaid,visjs").split(",")
    output_dir = Path(os.environ.get("OUTPUT_DIR", "docs"))

    # Parse compilation result
    try:
        compilation_data = json.loads(compilation_json)
    except json.JSONDecodeError as e:
        print(f"Error parsing compilation JSON: {e}")
        print(f"Raw data preview: {compilation_json[:500]}")
        sys.exit(1)

    # Extract nodes and edges
    nodes, edges = parse_compilation_result(compilation_data)
    print(f"Found {len(nodes)} nodes and {len(edges)} edges")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    generated_files = []

    # Generate requested formats
    for fmt in output_formats:
        fmt = fmt.strip().lower()

        if fmt == "mermaid":
            content = generate_mermaid(nodes, edges)
            output_file = output_dir / "lineage.md"
            output_file.write_text(content)
            generated_files.append(str(output_file))
            print(f"Generated Mermaid diagram: {output_file}")

        elif fmt == "dot":
            content = generate_dot(nodes, edges)
            output_file = output_dir / "lineage.dot"
            output_file.write_text(content)
            generated_files.append(str(output_file))
            print(f"Generated DOT diagram: {output_file}")

        elif fmt == "visjs":
            content = generate_visjs_html(nodes, edges)
            output_file = output_dir / "lineage.html"
            output_file.write_text(content)
            generated_files.append(str(output_file))
            print(f"Generated vis.js HTML: {output_file}")

        else:
            print(f"Warning: Unknown format '{fmt}', skipping")

    # Output for GitHub Actions
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"files_generated={','.join(generated_files)}\n")

    print(f"Successfully generated {len(generated_files)} files")


if __name__ == "__main__":
    main()
