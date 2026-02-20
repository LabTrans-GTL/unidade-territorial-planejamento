
import sys
import os
import pandas as pd
import logging

# Add project root to path
sys.path.append(os.getcwd())
# Force UTF-8 Output
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

from src.core.manager import GeoValidaManager
from src.pipeline.sede_consolidator import SedeConsolidator

def debug_candidates():
    # Setup Manager
    manager = GeoValidaManager()
    
    # Configure Logging to stdout
    logging.basicConfig(level=logging.INFO, format='%(message)s', force=True)
    # Silence other loggers that use emojis or verbose output
    logging.getLogger("GeoValida").setLevel(logging.WARNING) 
    logger = logging.getLogger("DebugScript")
    logger.setLevel(logging.INFO)
    
    logger.info("Loading Data...")
    manager.step_0_initialize_data()
    
    # Ensure raw data loaded
    if manager.sede_analyzer.df_municipios is None:
        manager.sede_analyzer.load_initialization_data()
    
    # Ensure impedance
    manager.sede_analyzer.load_impedance_data()
    
    # Calculate Metrics
    logger.info("Calculating Socioeconomic Metrics...")
    df_metrics = manager.sede_analyzer.calculate_socioeconomic_metrics()
    
    logger.info(f"Total Sedes in Metrics: {len(df_metrics)}")
    
    # Pairs to check
    # Alto Alegre (1400050) -> Boa Vista (1400100)
    # Morros (2107100) -> São Luís (2111300)
    
    debug_pairs = [
        (1400050, 1400100),
        (2107100, 2111300)
    ]
    
    consolidator = SedeConsolidator(manager.graph, manager.validator, manager.sede_analyzer)
    
    for origin_id, target_id in debug_pairs:
        logger.info(f"\n--- DEBUGGING PAIR: {origin_id} -> {target_id} ---")
        
        # 1. Check Origin in Metrics
        origin_row = df_metrics[df_metrics['cd_mun_sede'] == origin_id]
        if origin_row.empty:
            logger.error(f"Origin {origin_id} NOT FOUND in metrics.")
            continue
        origin_row = origin_row.iloc[0]
        
        # 2. Check Alert
        logger.info(f"Origin Alert: {origin_row['tem_alerta_dependencia']}")
        if not origin_row['tem_alerta_dependencia']:
            logger.warning("Origin has NO dependency alert.")
            
        # 3. Check Target in Metrics
        target_row = df_metrics[df_metrics['cd_mun_sede'] == target_id]
        if target_row.empty:
            logger.error(f"Target {target_id} NOT FOUND in metrics.")
            # Check raw df
            raw_target = manager.sede_analyzer.df_municipios[manager.sede_analyzer.df_municipios['cd_mun'] == target_id]
            if not raw_target.empty:
                 logger.info(f"Target found in RAW DataFrame. sede_utp attribute: {raw_target.iloc[0].get('sede_utp')}")
            continue
        target_row = target_row.iloc[0]
        
        # 4. Check is_sede
        is_sede = manager.sede_analyzer.is_sede(target_id)
        logger.info(f"is_sede({target_id}) = {is_sede}")
        
        # 5. Scores
        score_origin = consolidator._get_sede_score(origin_row)
        score_target = consolidator._get_sede_score(target_row)
        
        logger.info(f"Origin Score: {score_origin} (Airport={origin_row['tem_aeroporto']}, Tourism='{origin_row['turismo']}')")
        logger.info(f"Target Score: {score_target} (Airport={target_row['tem_aeroporto']}, Tourism='{target_row['turismo']}')")
        
        # 6. Apply Logic
        if score_origin == 0:
            if score_target == 0:
                rank_orig = consolidator._get_regic_rank(origin_row['regic'])
                rank_dest = consolidator._get_regic_rank(target_row['regic'])
                logger.info(f"0/0 Logic: Rank Origin {rank_orig} vs Dest {rank_dest}")
                if rank_dest < rank_orig:
                    logger.info("  -> CANDIDATE (Rank)")
                else:
                    logger.info("  -> REJECTED (Rank)")
            elif score_target in [1, 2]:
                logger.info(f"0/{score_target} Logic: Target has Airport={target_row['tem_aeroporto']}")
                if target_row['tem_aeroporto']:
                    logger.info("  -> CANDIDATE (Airport)")
                else:
                    logger.info("  -> REJECTED (No Airport)")
            else:
                logger.info("  -> REJECTED (Target Score invalid)")
        else:
            logger.info("  -> REJECTED (Origin Score != 0)")

if __name__ == "__main__":
    debug_candidates()
