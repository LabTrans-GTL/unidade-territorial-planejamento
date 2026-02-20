
import json
from pathlib import Path

data_dir = Path(r"c:\Users\vinicios.buzzi\buzzi\geovalida\data\03_processed")
step5_path = data_dir / "snapshot_step5_post_unitary.json"
step6_path = data_dir / "snapshot_step6_sede_consolidation.json"
target_mun = "4316808"

def check_snapshot(path, name):
    print(f"\n--- Checking {name} ---")
    if not path.exists():
        print(f"File not found: {path}")
        return

    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        utp_seeds = data.get('utp_seeds', {})
        graph_nodes = data.get('graph', {}).get('nodes', [])
        
        # Check if target is a seed
        found_as_seed = False
        seed_utp_id = None
        for utp_id, mun_id in utp_seeds.items():
            if str(mun_id) == target_mun:
                found_as_seed = True
                seed_utp_id = utp_id
                break
        
        if found_as_seed:
            print(f"YES: {target_mun} IS a sede for UTP {seed_utp_id}")
        else:
            print(f"NO: {target_mun} is NOT a sede.")
            
        # Check node data for 'sede_utp' property
        # The graph structure in snapshot might be different (list of nodes with id and data)
        # Scan nodes
        found_node = False
        is_sede_utp_prop = False
        utp_id_prop = None
        
        # graph_nodes is typically a list of dictionaries if loaded from JSON export
        # structure: [{'id': ..., 'data': {...}}, ...] OR [{'id': ..., '...': ...}] depending on networkx export format
        # The key might be 'nodes' list
        
        # Let's handle list of dicts with 'id' key
        if isinstance(graph_nodes, list):
             for node in graph_nodes:
                node_id = str(node.get('id'))
                if node_id == target_mun:
                    found_node = True
                    is_sede_utp_prop = node.get('sede_utp', False)
                    utp_id_prop = node.get('utp_id')
                    break
        else:
             print("Graph nodes format not recognized (not a list).")

        if found_node:
            print(f"Node found: utp_id={utp_id_prop}, sede_utp property={is_sede_utp_prop}")
        else:
            print("Node NOT found in graph nodes.")

    except Exception as e:
        print(f"Error reading {name}: {e}")

check_snapshot(step5_path, "Step 5 (Post Unitary)")
check_snapshot(step6_path, "Step 6 (Sede Consolidation)")
