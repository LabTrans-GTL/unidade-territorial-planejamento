# scripts/verify_impedance_strictness.py
import sys
from pathlib import Path
import logging
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator
from src.pipeline.border_validator_v2 import BorderValidatorV2
from src.pipeline.sede_analyzer import SedeAnalyzer

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("VerifyImpedance")

def verify_strictness():
    logger.info("="*60)
    logger.info("VERIFYING STRICT IMPEDANCE HANDLING")
    logger.info("="*60)

    # 1. Setup Components
    graph = TerritorialGraph()
    validator = TerritorialValidator(graph)
    
    # BorderValidator
    bv = BorderValidatorV2(graph, validator)
    if bv.impedance_df is None:
        logger.error("❌ Failed to load impedance for BorderValidator")
        return

    # SedeAnalyzer
    sa = SedeAnalyzer()
    if not sa.load_impedance_data():
        logger.error("❌ Failed to load impedance for SedeAnalyzer")
        return
        
    # 2. Test Case: Ibitiara (2913002) -> Vitória da Conquista (2933307)
    # We confirmed this pair is MISSING in impedancias_filtradas_2h.csv
    origin = 2913002
    dest = 2933307
    label = "Ibitiara -> Conquista"
    
    logger.info(f"\nTesting Pair: {label} ({origin} -> {dest})")
    
    # 3. Test SedeAnalyzer (should return None, NO fallback)
    logger.info("\n[Test 1] SedeAnalyzer.get_travel_time")
    sa_time = sa.get_travel_time(origin, dest)
    if sa_time is None:
        logger.info("✅ PASS: SedeAnalyzer returned None (correctly handled missing data)")
    else:
        logger.error(f"❌ FAIL: SedeAnalyzer returned {sa_time} (expected None)")

    # 4. Test BorderValidator._get_travel_time (should return None)
    logger.info("\n[Test 2] BorderValidator._get_travel_time")
    bv_time = bv._get_travel_time(origin, dest)
    if bv_time is None:
        logger.info("✅ PASS: BorderValidator returned None")
    else:
        logger.error(f"❌ FAIL: BorderValidator returned {bv_time} (expected None)")

    # 5. Test BorderValidator._has_flow_to_sede (should return False)
    # We need to mock a flow dataframe where this flow exists but impedance is missing
    logger.info("\n[Test 3] BorderValidator._has_flow_to_sede (Strict Checks)")
    
    mock_flows = pd.DataFrame({
        'mun_origem': [origin],
        'mun_destino': [dest],
        'viagens': [1000]
    })
    
    has_flow = bv._has_flow_to_sede(origin, dest, mock_flows, max_time=2.0)
    
    if not has_flow:
        logger.info("✅ PASS: _has_flow_to_sede returned False (Strict check worked)")
    else:
        logger.error("❌ FAIL: _has_flow_to_sede returned True (Strict check failed)")

    # 6. Test BorderValidator._get_flows_to_sedes (should exclude this pair)
    # We need to ensure the destination is treated as a seed in the graph for this test
    logger.info("\n[Test 4] BorderValidator._get_flows_to_sedes (Strict Filtering)")
    
    # Hack graph to make destination a Sede
    # UTP_TEST -> dest
    graph.utp_seeds['TEST'] = str(dest)
    graph.hierarchy.add_node(dest, type='municipio') 
    # Just need get_municipality_utp to work
    # We can mock the function strictly for this test or add nodes properly
    # Adding node to hierarchy with successor might be complex, let's mock the method
    original_get_utp = graph.get_municipality_utp
    graph.get_municipality_utp = lambda x: 'TEST' if x == dest else None
    
    try:
        results = bv._get_flows_to_sedes(origin, mock_flows, max_time=2.0)
        
        # Expect EMPTY results because impedance is missing -> treated as > 2h -> excluded
        if not results:
            logger.info("✅ PASS: _get_flows_to_sedes returned empty list (excluded due to missing time)")
        else:
            logger.error(f"❌ FAIL: _get_flows_to_sedes returned {results} (expected empty)")
            
    finally:
        # Restore
        graph.get_municipality_utp = original_get_utp

if __name__ == "__main__":
    verify_strictness()
