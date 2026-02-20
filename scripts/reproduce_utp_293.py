
import sys
import os
import pandas as pd
import geopandas as gpd
from pathlib import Path
import logging

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.manager import GeoValidaManager
from src.pipeline.sede_consolidator import SedeConsolidator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ReproduceUTP293")

def trace_utp_293():
    logger.info("Initializing Manager...")
    manager = GeoValidaManager()
    
    # Load data
    logger.info("Loading Data...")
    if not manager.step_0_initialize_data():
        logger.error("Failed to load data.")
        return

    # Ensure necessary data is loaded
    manager.sede_analyzer.load_initialization_data()
    manager.sede_analyzer.load_impedance_data()
    
    # Sync UTPs
    if manager.sede_analyzer.df_municipios is not None:
         for idx, row in manager.sede_analyzer.df_municipios.iterrows():
             cd_mun = row['cd_mun']
             current_utp = manager.graph.get_municipality_utp(cd_mun)
             if current_utp != "NAO_ENCONTRADO" and current_utp != "SEM_UTP":
                 manager.sede_analyzer.df_municipios.at[idx, 'utp_id'] = current_utp

    # Target UTP/Municipality
    TARGET_ID = "2932900" 
    
    # Check initial state
    utp_initial = manager.graph.get_municipality_utp(int(TARGET_ID))
    logger.info(f"Initial State for Valença ({TARGET_ID}): UTP={utp_initial}")
    
    # Check if it is a sede
    is_sede_initial = False
    if manager.graph.hierarchy.has_node(int(TARGET_ID)):
        is_sede_initial = manager.graph.hierarchy.nodes[int(TARGET_ID)].get('sede_utp', False)
    logger.info(f"Initial Sede Status for {TARGET_ID}: {is_sede_initial}")
    
    # Initialize Consolidator
    consolidator = SedeConsolidator(manager.graph, manager.validator, manager.sede_analyzer)
    
    logger.info("Calculating metrics...")
    df_metrics = manager.sede_analyzer.calculate_socioeconomic_metrics()
    
    # Filter candidates to see if Valença is involved
    candidates = consolidator._filter_candidates(df_metrics)
    
    valenca_candidate = None
    for cand in candidates:
        if str(cand['sede_origem']) == TARGET_ID:
            logger.info(f"Valença FOUND as ORIGIN candidate: {cand}")
            valenca_candidate = cand
        if str(cand['sede_destino']) == TARGET_ID:
            logger.info(f"Valença FOUND as DESTINATION candidate: {cand}")

    # Run consolidation simulation
    if valenca_candidate or True: # Force run to see what happens
        logger.info("Simulating consolidation for Valença...")
        
        # Load GDF for adjacency
        if manager.map_generator.gdf_complete is None:
             logger.info("Loading shapefiles...")
             manager.map_generator.load_shapefiles()
        
        gdf = manager.map_generator.gdf_complete
        if gdf is None:
             logger.error("Failed to load GDF!")
             return

        # Check reciprocal
        consolidator.run_sede_consolidation(manager.flow_analyzer.flow_df if hasattr(manager, 'flow_analyzer') else None, gdf, None)
        
        # Check final state
        utp_final = manager.graph.get_municipality_utp(int(TARGET_ID))
        logger.info(f"Final State for Valença ({TARGET_ID}): UTP={utp_final}")
        
        # Check if it is a sede
        is_sede_final = False
        if manager.graph.hierarchy.has_node(int(TARGET_ID)):
            is_sede_final = manager.graph.hierarchy.nodes[int(TARGET_ID)].get('sede_utp', False)
        logger.info(f"Final Sede Status for {TARGET_ID}: {is_sede_final}")

        # Check the destination UTP's sede
        utp_dest_str = str(utp_final)
        logger.info(f"Checking Sede for Destination UTP {utp_dest_str}...")
        if utp_dest_str in manager.graph.utp_seeds:
            seed_id = manager.graph.utp_seeds[utp_dest_str]
            logger.info(f"UTP {utp_dest_str} Sede ID: {seed_id}")
            if manager.graph.hierarchy.has_node(seed_id):
                 logger.info(f"Sede Node {seed_id} attributes: {manager.graph.hierarchy.nodes[seed_id]}")
        else:
            logger.error(f"UTP {utp_dest_str} has NO SEDE in utp_seeds!")

if __name__ == "__main__":
    trace_utp_293()
