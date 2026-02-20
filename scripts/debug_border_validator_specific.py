import sys
import os
import logging
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.manager import GeoValidaManager
from src.pipeline.border_validator_v2 import BorderValidatorV2

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("GeoValida.BorderValidatorV2")
logger.setLevel(logging.DEBUG)

def debug_specific_municipalities():
    print("Initializing Manager...")
    manager = GeoValidaManager()
    
    # Load data
    print("Loading Data...")
    if not manager.step_0_initialize_data():
        print("Failed to load data")
        return

    # Analyze flows
    print("Analyzing Flows...")
    manager.step_2_analyze_flows()
    flow_df = manager.analyzer.full_flow_df
    
    # Generate Map/GDF (needed for adjacency)
    print("Generating Map...")
    manager.map_generator.sync_with_graph(manager.graph)
    gdf = manager.map_generator.gdf_complete
    
    # Initialize Validator
    validator = BorderValidatorV2(manager.graph, manager.validator)
    validator._build_adjacency_graph(gdf)
    
    # Municipalities to check
    targets = [
        {'id': 2932903, 'name': 'Valença (BA)'}, 
        {'id': 2606002, 'name': 'Floresta (PE)'}
    ]
    
    for target in targets:
        mun_id = target['id']
        name = target['name']
        print(f"\n--- Debugging {name} ({mun_id}) ---")
        
        # Get current UTP
        current_utp = manager.graph.get_municipality_utp(mun_id)
        if current_utp == "NAO_ENCONTRADO" or current_utp == "SEM_UTP":
            print(f"Municipality not in any UTP!")
            continue
            
        print(f"Current UTP: {current_utp}")
        
        # Check flows to sedes
        sede_flows = validator._get_flows_to_sedes(mun_id, flow_df)
        print(f"Flows to sedes (within 2h):")
        for sede_id, flow, time in sede_flows:
            sede_utp = manager.graph.get_municipality_utp(sede_id)
            print(f"  -> Sede {sede_id} (UTP {sede_utp}): {flow:.2f} trips, {time:.2f}h")
            
        # Check Adjacency
        print("Adjacency Check:")
        if mun_id in validator.adjacency_graph:
            neighbors = list(validator.adjacency_graph[mun_id])
            neighbor_utps = set()
            for n in neighbors:
                n_utp = manager.graph.get_municipality_utp(n)
                neighbor_utps.add(n_utp)
            print(f"  Adjacent UTPs: {neighbor_utps}")
        else:
            print("  No adjacency info found (isolated in adjacency graph?)")

        # Run _find_better_utp logic
        print("Running _find_better_utp logic:")
        result = validator._find_better_utp(mun_id, current_utp, flow_df)
        if result:
            print(f"  RESULT: Recommendation to move to UTP {result[0]} (Flow: {result[1]})")
        else:
            print(f"  RESULT: No move recommended.")

if __name__ == "__main__":
    debug_specific_municipalities()
