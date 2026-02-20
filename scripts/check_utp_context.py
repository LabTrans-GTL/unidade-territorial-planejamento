
import json
from pathlib import Path

data_dir = Path(r"c:\Users\vinicios.buzzi\buzzi\geovalida\data\03_processed")
step6_path = data_dir / "snapshot_step6_sede_consolidation.json"
target_mun = "4316808"

def find_node_context(path):
    print(f"Loading {path}...")
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        graph_nodes = data.get('graph', {}).get('nodes', [])
        utp_seeds = data.get('utp_seeds', {})
        
        # Check UTP of target
        target_utp = None
        target_node_data = None
        
        if isinstance(graph_nodes, list):
             for node in graph_nodes:
                if str(node.get('id')) == target_mun:
                    target_node_data = node
                    target_utp = node.get('utp_id')
                    break
        
        print(f"Target {target_mun} found in graph: {target_node_data is not None}")
        if target_node_data:
            print(f"  Current UTP: {target_utp}")
            print(f"  Node Data: {target_node_data}")
            
            # Check if this UTP has a seed
            seed_id = utp_seeds.get(str(target_utp))
            print(f"  Seed for UTP {target_utp}: {seed_id}")
            
            if str(seed_id) == target_mun:
                print("  -> Target IS the seed for its UTP.")
            else:
                print("  -> Target is NOT the seed.")
        
        # Check if it exists in utp_seeds map at all (maybe mapped to another UTP key?)
        # utp_seeds is {utp_id: mun_id}
        
    except Exception as e:
        print(f"Error: {e}")

find_node_context(step6_path)
