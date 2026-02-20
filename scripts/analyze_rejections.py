
import sys
import logging
from pathlib import Path
import pandas as pd
import collections

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

# Setup logging
logging.basicConfig(level=logging.WARN, format='%(message)s') # Minimal logging to stdout
logger = logging.getLogger("GeoValida.RejectionAnalysis")
logger.setLevel(logging.INFO)

from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator
from src.pipeline.analyzer import ODAnalyzer
from src.pipeline.border_validator import BorderValidator
from src.interface.consolidation_manager import ConsolidationManager
from src.pipeline.mapper import UTPMapGenerator

def analyze():
    print("Starting Rejection Analysis...")
    
    # 1. Initialize
    graph = TerritorialGraph()
    validator = TerritorialValidator(graph)
    map_generator = UTPMapGenerator(graph)
    
    # 2. Load Snapshot
    snapshot_path = PROJECT_ROOT / "data" / "03_processed" / "snapshot_step6_sede_consolidation.json"
    if not snapshot_path.exists():
        print("Snapshot not found")
        return
        
    try:
        graph.load_snapshot(snapshot_path)
    except Exception as e:
        print(f"Failed to load snapshot: {e}")
        return

    # 3. Load Flow
    analyzer = ODAnalyzer()
    flow_df = analyzer.run_full_analysis()
    
    # 4. Load Geometry
    try:
        map_generator.load_shapefiles()
        map_generator.sync_with_graph(graph)
    except Exception as e:
        print(f"Failed to load shapefiles: {e}")
        return
        
    # 5. Run Validator for 1 Iteration
    bv = BorderValidator(graph, validator, ConsolidationManager())
    
    print("\nRunning 1 Iteration of Border Validation...")
    bv.run_border_validation(flow_df, map_generator.gdf_complete, max_iterations=1)
    
    # 6. Analyze Rejections
    rejections = bv.rejections_log
    
    if not rejections:
        print("\nNo rejections found (or no relocations attempted).")
        return
        
    print(f"\nANALYSIS OF {len(rejections)} REJECTIONS")
    print("="*60)
    
    # Count reasons
    reason_counts = collections.Counter([r['reason'] for r in rejections])
    
    for reason, count in reason_counts.most_common():
        pct = (count / len(rejections)) * 100
        print(f"{reason}: {count} ({pct:.1f}%)")
        
    print("="*60)
    
    # Sample details for top reasons
    for reason, _ in reason_counts.most_common():
        print(f"\nDETAILS FOR: {reason}")
        samples = [r for r in rejections if r['reason'] == reason][:5]
        for s in samples:
            details = s.get('details', '')
            print(f"   - {s['mun_name']} ({s['mun_id']}): {s['utp_origem']} -> {s['proposed_utp']}")
            if details:
                print(f"     Details: {details}")

if __name__ == "__main__":
    analyze()
