import json
from pathlib import Path

def analyze_snapshot(snapshot_path):
    with open(snapshot_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Hierarchy is usually a list of nodes and edges or similar
    # Based on previous knowledge, it's often a dict with 'nodes' and 'adjacency'
    # but let's look at the structure first.
    
    # Wait, snapshot format:
    # {
    #   "timestamp": ...,
    #   "nodes": { "MUN_ID": {"type": "municipality", "utp": "UTP_ID", ...}, "UTP_ID": {"type": "utp", ...} },
    #   "utp_seeds": { "UTP_ID": "MUN_ID" }
    # }
    
    nodes = data.get('nodes', {})
    utp_to_muns = {}
    
    for node_id, node_data in nodes.items():
        if node_data.get('type') == 'municipality':
            utp_id = node_data.get('utp_id')
            if utp_id:
                if utp_id not in utp_to_muns:
                    utp_to_muns[utp_id] = []
                utp_to_muns[utp_id].append(node_id)
    
    # Create a reverse mapping for hierarchy if possible
    # In some snapshots, the hierarchy is stored as 'hierarchy' (nx graph dict)
    # but based on view_file, they are just nodes.
    # Wait, how does manager.py load it?
    # self.graph.hierarchy.add_edge(rm_node, utp_node)
    
    # Let's check if there is an 'edges' or 'adjacency' or if UTP nodes have a 'parent' attr.
    # Actually, let's look at the UTP node data itself.
    
    unitary_utps = []
    for utp_id, muns in utp_to_muns.items():
        if len(muns) == 1:
            mun_id = muns[0]
            mun_data = nodes.get(mun_id, {})
            rm = mun_data.get('regiao_metropolitana')
            
            # Try to find parent of UTP_utp_id
            utp_node_id = f"UTP_{utp_id}"
            
            # If the JSON doesn't have edges, maybe we can find which RM node 
            # contains this UTP if we had a full dump.
            # But wait, look at manager.py load_from_initialization_json:
            # it builds it from list of municipalities.
            
            unitary_utps.append({
                'utp': utp_id,
                'mun': mun_id,
                'name': mun_data.get('name'),
                'rm': rm
            })
            
    # Let's also search for which RM nodes exist and if they have children.
    # Actually, let's just print all RM nodes.
    rms = [n for n, d in nodes.items() if d.get('type') == 'rm']
    print(f"RM nodes: {rms}")
            
    print(f"Total UTPs: {len(utp_to_muns)}")
    print(f"Unitary UTPs: {len(unitary_utps)}")
    for u in unitary_utps:
        print(f"  {u['utp']}: {u['name']} ({u['mun']}) - RM: {u['rm']}")

if __name__ == "__main__":
    snapshot = Path(r'c:\Users\vinicios.buzzi\buzzi\geovalida\data\03_processed\snapshot_step8_final.json')
    if snapshot.exists():
        analyze_snapshot(snapshot)
    else:
        print("Snapshot not found")
