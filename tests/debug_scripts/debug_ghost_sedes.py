
import json
from pathlib import Path
from collections import defaultdict

snapshot_path = Path("data/03_processed/snapshot_step6_sede_consolidation.json")

def check_ghost_sedes():
    if not snapshot_path.exists():
        print(f"Snapshot not found: {snapshot_path}")
        return

    with open(snapshot_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    nodes = data.get('nodes', {})
    
    # Group sedes by UTP
    utp_sedes = defaultdict(list)
    
    for node_id, attrs in nodes.items():
        if attrs.get('type') == 'municipality':
            utp_id = attrs.get('utp_id')
            is_sede = attrs.get('sede_utp', False)
            
            if is_sede:
                utp_sedes[utp_id].append({
                    'id': node_id,
                    'name': attrs.get('name')
                })
    
    # Check for duplicates
    found_issues = False
    for utp_id, sedes in utp_sedes.items():
        if len(sedes) > 1:
            found_issues = True
            print(f"[!] UTP {utp_id} has {len(sedes)} sedes:")
            for s in sedes:
                print(f"   - {s['id']}: {s['name']}")
    
    if not found_issues:
        print("[OK] No ghost sedes found (1 sede per UTP).")
    else:
        print("[X] Ghost sedes found!")

if __name__ == "__main__":
    check_ghost_sedes()
