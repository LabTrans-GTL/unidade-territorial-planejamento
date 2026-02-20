
import sys
from pathlib import Path
import logging

# Add project root to path
project_root = Path("c:/Users/vinicios.buzzi/buzzi/geovalida")
sys.path.insert(0, str(project_root))

from src.core.manager import GeoValidaManager

def check_rm():
    manager = GeoValidaManager()
    if not manager.step_0_initialize_data():
        print("Failed to load data")
        return

    # Valença (BA) - 2932903
    # Cairu (BA) - 2905404 (Sede of UTP 350)
    # Floresta (PE) - 2606002
    
    muns = {
        2932903: "Valença",
        2905404: "Cairu",
        2606002: "Floresta",
        2601201: "Arcoverde" # Sede of UTP 59, adjacent to Floresta
    }
    
    print("\n--- RM Status Check ---")
    for mun_id, name in muns.items():
        if manager.graph.hierarchy.has_node(mun_id):
            rm = manager.graph.hierarchy.nodes[mun_id].get('regiao_metropolitana')
            print(f"{name} ({mun_id}): RM = '{rm}'")
        else:
            print(f"{name} ({mun_id}): Not in graph")

    # Also check UTP RM mapping if possible
    # UTP 350 (Cairu)
    # UTP 59 (Arcoverde)
    print("\n--- UTP RM Check ---")
    utps = ["350", "59"]
    for utp_id in utps:
        rm = manager.validator.get_rm_of_utp(utp_id)
        print(f"UTP {utp_id}: RM = '{rm}'")

if __name__ == "__main__":
    check_rm()
