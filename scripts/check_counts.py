
import json
from pathlib import Path

data_dir = Path(r"c:\Users\vinicios.buzzi\buzzi\geovalida\data\03_processed")
step5_path = data_dir / "snapshot_step5_post_unitary.json"
step6_path = data_dir / "snapshot_step6_sede_consolidation.json"

def count_municipalities(path, name):
    print(f"\n--- Analyzing {name} ---")
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Snapshot structure has 'nodes' at root
        graph_nodes = data.get('nodes', None)
        
        if graph_nodes is None:
            # Fallback to old structure or check 'graph'
            graph_data = data.get('graph', {})
            graph_nodes = graph_data.get('nodes', {})

        nodes_list = []
        if isinstance(graph_nodes, dict):
             for k, v in graph_nodes.items():
                 # Ensure 'id' is present
                 if 'id' not in v:
                     v['id'] = k
                 nodes_list.append(v)
        elif isinstance(graph_nodes, list):
             nodes_list = graph_nodes
        
        muns = [n for n in nodes_list if n.get('type') == 'municipality']
        print(f"Total Municipalities: {len(muns)}")
        
        # Specific check for 4316808
        target_id = "4316808"
        found_node = None
        for n in muns:
            if str(n.get('id')) == target_id:
                found_node = n
                break
                
        print(f"Contains {target_id}: {found_node is not None}")
        if found_node:
            print(f"  Node Data: {found_node}")
        
    except Exception as e:
        print(f"Error: {e}")

count_municipalities(step5_path, "Step 5")
count_municipalities(step6_path, "Step 6")
