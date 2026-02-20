
import sys
import logging
from pathlib import Path
import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("GeoValida.Optimization")

from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator
from src.pipeline.analyzer import ODAnalyzer
from src.pipeline.border_validator import BorderValidator
from src.interface.consolidation_manager import ConsolidationManager
from src.pipeline.mapper import UTPMapGenerator

def main():
    logger.info("🚀 Starting Border Optimization Script (Standalone)")

    # 1. Initialize Components
    graph = TerritorialGraph()
    validator = TerritorialValidator(graph)
    analyzer = ODAnalyzer()
    
    # We need a map generator for GDF (geometry)
    map_generator = UTPMapGenerator(graph)

    # 2. Load Snapshot (Step 6 - Post Sede Consolidation)
    snapshot_path = PROJECT_ROOT / "data" / "03_processed" / "snapshot_step6_sede_consolidation.json"
    
    if not snapshot_path.exists():
        logger.error(f"❌ Snapshot not found: {snapshot_path}")
        logger.info("Please run the full pipeline up to Step 6 first.")
        return

    try:
        graph.load_snapshot(snapshot_path)
        logger.info("✅ Snapshot loaded successfully")
        logger.info(f"   Nodes: {len(graph.hierarchy.nodes())}")
        logger.info(f"   Sedes: {len(graph.utp_seeds)}")
    except Exception as e:
        logger.error(f"❌ Failed to load snapshot: {e}")
        return

    # 3. Load Flow Data
    logger.info("📦 Loading Flow Data (OD Analyzer)...")
    flow_df = analyzer.run_full_analysis()
    
    if flow_df is None or flow_df.empty:
        logger.error("❌ Failed to load flow data")
        return

    # 4. Load Shapefiles (Needed for Adjacency)
    logger.info("🗺️ Loading Shapefiles...")
    try:
        map_generator.load_shapefiles()
        map_generator.sync_with_graph(graph)
        logger.info(f"   Loaded {len(map_generator.gdf_complete)} geometries")
    except Exception as e:
        logger.error(f"❌ Failed to load shapefiles: {e}")
        # Try to proceed if GDF is available in some form, but likely will fail
        return

    # 5. Initialize Border Validator
    # We don't have a full ConsolidationManager history here, so we create a dummy or fresh one
    # Use a dummy consolidation manager to avoid errors if it tries to log
    # But BorderValidator expects it.
    
    # Fix: Create a simple consolidation manager just for logging
    consolidation_manager = ConsolidationManager()
    
    border_validator = BorderValidator(
        graph=graph,
        validator=validator,
        consolidation_manager=consolidation_manager
    )

    # 6. Run Optimization
    # Use max_iterations=50 to allow finding the sweet spot
    logger.info("🔄 Running Border Validation with Optimization Analysis...")
    
    total_relocated, history = border_validator.run_border_validation(
        flow_df=flow_df,
        gdf=map_generator.gdf_complete,
        max_iterations=50
    )
    
    if history:
         # Find iteration with max satisfiability
         scores = [entry['satisfied_count'] for entry in history]
         max_score = max(scores)
         max_idx = scores.index(max_score)
         optimal_iter = history[max_idx]['iteration']
         optimal_pct = history[max_idx]['satisfaction_pct']
         
         print("\n" + "="*50)
         print(f"📊 OPTIMIZATION RESULTS FOUND")
         print("="*50)
         print(f"✅ Optimal number of iterations: {optimal_iter}")
         print(f"   Max Satisfiability Achieved: {max_score} municipalities ({optimal_pct:.2f}%)")
         print("="*50 + "\n")
    else:
         print("⚠️ No history returned from validation.")

    logger.info("🎉 Script Complete")
    logger.info(f"Check output in: {PROJECT_ROOT / 'data' / '03_processed' / 'satisfiability_chart.png'}")

if __name__ == "__main__":
    main()
