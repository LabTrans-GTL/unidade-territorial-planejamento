
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DebugSede")

def debug_candidates():
    logger.info("Initializing Manager...")
    manager = GeoValidaManager()
    
    # Load data
    logger.info("Loading Data...")
    if not manager.step_0_initialize_data():
        logger.error("Failed to load data.")
        return

    # Ensure shapefiles are loaded for distance fallback
    if manager.map_generator.gdf_complete is None or manager.map_generator.gdf_complete.empty:
         logger.info("Loading shapefiles manually for debug...")
         manager.map_generator.load_shapefiles()

    # Ensure analyzer has necessary data
    logger.info("Setting up SedeAnalyzer...")
    manager.sede_analyzer.load_initialization_data()
    manager.sede_analyzer.load_impedance_data()
    
    # Sync UTPs
    if manager.sede_analyzer.df_municipios is not None:
         for idx, row in manager.sede_analyzer.df_municipios.iterrows():
             cd_mun = row['cd_mun']
             current_utp = manager.graph.get_municipality_utp(cd_mun)
             if current_utp != "NAO_ENCONTRADO" and current_utp != "SEM_UTP":
                 manager.sede_analyzer.df_municipios.at[idx, 'utp_id'] = current_utp

    # Initialize Consolidator
    consolidator = SedeConsolidator(manager.graph, manager.validator, manager.sede_analyzer)
    
    # Run candidate detection logic manually
    logger.info("Calculate Metrics...")
    
    # DEBUG: Check if sedes exist
    df_mun = manager.sede_analyzer.df_municipios
    if df_mun is not None:
         sedes_count = df_mun['sede_utp'].sum()
         logger.info(f"DEBUG: Total Sedes in DataFrame: {sedes_count}")
         if sedes_count == 0:
             logger.info("DEBUG: Preview of df_municipios columns:")
             logger.info(df_mun.columns.tolist())
             logger.info("DEBUG: Preview of sede_utp values:")
             logger.info(df_mun['sede_utp'].value_counts())
    
    df_metrics = manager.sede_analyzer.calculate_socioeconomic_metrics()
    
    candidates = []
    logger.info("Scanning for candidates...")
    
    logger.info("Scanning for candidates...")
    
    count_alerts = 0
    count_diff_utp = 0
    count_asym = 0
    
    for idx, row in df_metrics.iterrows():
        if row['tem_alerta_dependencia']:
            count_alerts += 1
            
            sede_origem = row['cd_mun_sede']
            utp_origem = row['utp_id']
            sede_destino = row['principal_destino_cd']
            
            # Check destination
            dest_row = df_metrics[df_metrics['cd_mun_sede'] == sede_destino]
            if dest_row.empty:
                continue

            dest_row = dest_row.iloc[0]
            utp_destino = manager.graph.get_municipality_utp(sede_destino)

            if utp_origem != utp_destino:
                count_diff_utp += 1
                
                score_origin = consolidator._get_sede_score(row)
                score_dest = consolidator._get_sede_score(dest_row)
                
                # Debug sample
                if count_diff_utp <= 10:
                    logger.info(f"Sample Check: {row['nm_sede']} -> {dest_row['nm_sede']}")
                    logger.info(f"   Scores: {score_origin} vs {score_dest}")
                    logger.info(f"   Origin Raw: Aero={row.get('tem_aeroporto')}, Tur={row.get('turismo')}")
                    logger.info(f"   Dest   Raw: Aero={dest_row.get('tem_aeroporto')}, Tur={dest_row.get('turismo')}")

                is_asymmetric = (score_origin == 0 and score_dest > 0) or (score_origin > 0 and score_dest == 0)
                
                if is_asymmetric:
                    count_asym += 1
                    logger.info(f"   MATCH FOUND! {row['nm_sede']} -> {dest_row['nm_sede']}")
                    candidates.append({
                        'nm_origem': row['nm_sede'],
                        'nm_destino': dest_row['nm_sede'],
                        'score_origem': score_origin,
                        'score_dest': score_dest
                    })

    logger.info(f"Stats: Alerts={count_alerts}, DiffUTP={count_diff_utp}, Asymmetric={count_asym}")

    logger.info(f"Total Candidates Found: {len(candidates)}")
    for c in candidates:
        logger.info(f"MATCH: {c['nm_origem']} ({c['score_origem']}) -> {c['nm_destino']} ({c['score_dest']})")

if __name__ == "__main__":
    debug_candidates()
