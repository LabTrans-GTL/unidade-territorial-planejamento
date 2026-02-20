
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
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RM_Check")

def check_rm_status():
    print("Initializing Manager...")
    manager = GeoValidaManager()
    
    # Load data
    print("Loading Data...")
    if not manager.step_0_initialize_data():
        print("Failed to load data")
        return

    # We need to load RM data specifically if it's not in step 0.
    # Looking at logs: "Carregando Composição de RMs de ...Composicao_RM_2024.xlsx...".
    # This usually happens in step 0 or step 1.
    # debug_border_validator_specific calls step_0_initialize_data. 
    # And the logs showed "Carregando Composição de RMs..." so it should be there.

    print("Checking RM Status...")
    validator = BorderValidatorV2(manager.graph, manager.validator)

    targets = [
        {'id': 2932903, 'name': 'Valença (BA)', 'target_utp_sede': 2905404}, 
        {'id': 2606002, 'name': 'Floresta (PE)', 'target_utp_sede': 2604106} 
        # Using 2604106 (UTP 78) as one of the targets for Floresta
    ]
    
    for target in targets:
        mun_id = target['id']
        name = target['name']
        print(f"\n--- Checking {name} ({mun_id}) ---")
        
        # Check Mun RM
        mun_rm = validator._get_mun_rm(mun_id)
        print(f"  Municipality RM: {mun_rm}")
        
        # Check Current UTP RM
        current_utp = manager.graph.get_municipality_utp(mun_id)
        current_utp_rm = manager.validator.get_rm_of_utp(current_utp)
        print(f"  Current UTP ({current_utp}) RM: {current_utp_rm}")
        
        # Check Target UTP RM
        target_sede = target['target_utp_sede']
        target_utp = manager.graph.get_municipality_utp(target_sede)
        target_utp_rm = manager.validator.get_rm_of_utp(target_utp)
        print(f"  Target UTP ({target_utp}) (Sede {target_sede}) RM: {target_utp_rm}")
        
        # Test Compatibility
        compat = validator._validate_rm_compatibility(mun_id, target_utp)
        print(f"  Compatibility (Mun -> Target): {compat}")

if __name__ == "__main__":
    check_rm_status()
