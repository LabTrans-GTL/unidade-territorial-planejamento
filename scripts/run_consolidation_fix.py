
import logging
import pandas as pd
import geopandas as gpd
from pathlib import Path
import sys

# Setup paths
project_root = Path(r"c:\Users\vinicios.buzzi\buzzi\geovalida")
sys.path.append(str(project_root))

from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator
from src.pipeline.sede_analyzer import SedeAnalyzer
from src.pipeline.sede_consolidator import SedeConsolidator

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("GeoValida.RunConsolidation")

def main():
    logger.info("Starting Sede Consolidation Fix Runner...")
    
    data_dir = project_root / "data" / "03_processed"
    
    # 1. Load Graph from Step 5 Snapshot
    step5_path = data_dir / "snapshot_step5_post_unitary.json"
    if not step5_path.exists():
        logger.error(f"Step 5 snapshot not found at {step5_path}")
        return
        
    graph = TerritorialGraph()
    graph.load_snapshot(step5_path)
    logger.info("Graph loaded from Step 5.")
    
    # 2. Load DataFrames (Flow + Metrics)
    # Load GDF from raw shapefiles
    gdf_path = project_root / "data" / "01_raw" / "shapefiles" / "BR_Municipios_2024.shp"
    if not gdf_path.exists():
         logger.error(f"GDF not found at {gdf_path}")
         return

    logger.info(f"Loading GDF from {gdf_path}...")
    gdf = gpd.read_file(gdf_path)
    # Ensure CD_MUN is int and rename if needed (shapefiles usually have CD_MUN)
    if 'CD_MUN' in gdf.columns:
        gdf['CD_MUN'] = gdf['CD_MUN'].astype(int)
    
    # 3. Initialize Validator and Analyzer
    validator = TerritorialValidator(graph)
    
    # Init SedeAnalyzer correctly (no graph arg in __init__)
    # It loads data from initialization.json
    analyzer = SedeAnalyzer(data_path=data_dir.parent) # data/03_processed -> parent -> data
    
    # Load base data (metrics)
    if not analyzer.load_initialization_data():
        logger.error("Failed to load initialization data for analyzer")
        return
        
    # 4. Run Consolidation
    consolidator = SedeConsolidator(graph, validator, analyzer)
    
    # Try loading flow data (Rodoviaria Coletiva as proxy for main flow?)
    # or just skip if analyzer handles metrics internally? 
    # The consolidator uses df_metrics from analyzer, which presumably is already calculated or cached?
    # run_sede_consolidation takes flow_df but uses it for _get_total_flow helper which is NOT CALLED in the visible main loop logic 
    # (only in helper, maybe unused?).
    # Let's check source code of sede_consolidator again...
    # _get_total_flow is a helper. Used? 
    # It is NOT used in run_sede_consolidation main loop.
    # So flow_df might be optional.
    flow_df = None
    
    changes = consolidator.run_sede_consolidation(flow_df, gdf, None)
    
    logger.info(f"Consolidation finished with {changes} changes.")

if __name__ == "__main__":
    main()
