# scripts/test_impedance_lookup.py
import sys
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator
from src.pipeline.border_validator_v2 import BorderValidatorV2

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("TestImpedance")

def test_lookup():
    logger.info("="*60)
    logger.info("TESTING IMPEDANCE LOOKUP IN BORDER VALIDATOR V2")
    logger.info("="*60)

    # 1. Initialize Dummy Graph/Validator (not needed for lookup test, but required for init)
    logger.info("Initializing components...")
    graph = TerritorialGraph()
    validator = TerritorialValidator(graph)
    
    # 2. Initialize BorderValidatorV2
    # It should autoload impedance data
    bv = BorderValidatorV2(graph, validator)
    
    if bv.impedance_df is None:
        logger.error("❌ Failed to load impedance data!")
        return
        
    logger.info(f"✅ Impedance data loaded: {len(bv.impedance_df)} records")
    
    # 3. Test Cases (7-digit codes)
    # Using known pairs (e.g., from previous logs or common knowledge)
    # Example: 
    # Valença (2932900) -> Cairu (2905906)
    # 6-digit: 293290 -> 290590
    
    test_cases = [
        (2932903, 2905404, "Valença -> Cairu"), # Corrected IDs
        (2927408, 2905404, "Salvador -> Cairu"), 
        (4316808, 4316808, "Self (Santa Cruz do Sul)"),
    ]
    
    logger.info("\nRunning lookup tests...")
    
    for origin, dest, label in test_cases:
        logger.info(f"\n--- Testing: {label} ---")
        logger.info(f"Input (7-digit): {origin} -> {dest}")
        
        # Calculate expected 6-digit
        orig_6 = origin // 10
        dest_6 = dest // 10
        logger.info(f"Key (6-digit):   {orig_6} -> {dest_6}")
        
        # Perform lookup
        time = bv._get_travel_time(origin, dest)
        
        if time is not None:
             logger.info(f"✅ RESULT: {time:.4f} hours")
        else:
             logger.warning(f"⚠️ RESULT: None (Not found in matrix)")

if __name__ == "__main__":
    test_lookup()
