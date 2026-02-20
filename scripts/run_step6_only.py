#!/usr/bin/env python3
import json
import sys
from pathlib import Path

# Ensure project root is on sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.manager import GeoValidaManager

print('Initializing manager and running Steps 0,1,2,5,6 (minimal)...')
manager = GeoValidaManager()

# Step 0
ok0 = manager.step_0_initialize_data()
print('step_0_initialize_data ->', ok0)

# Step 1
try:
    manager.step_1_generate_initial_map()
    print('step_1_generate_initial_map -> done')
except Exception as e:
    print('step_1_generate_initial_map -> ERROR', e)

# Step 2
try:
    manager.step_2_analyze_flows()
    print('step_2_analyze_flows -> done')
except Exception as e:
    print('step_2_analyze_flows -> ERROR', e)

# Step 5 (functional consolidation)
try:
    ch5 = manager.step_5_consolidate_functional()
    print('step_5_consolidate_functional ->', ch5)
except Exception as e:
    print('step_5_consolidate_functional -> ERROR', e)

# Step 6 (sede consolidation)
try:
    ch6 = manager.step_6_consolidate_sedes()
    print('step_6_consolidate_sedes ->', ch6)
except Exception as e:
    import traceback
    print('step_6_consolidate_sedes -> ERROR', e)
    traceback.print_exc()

# Print relevant CSV rows
csv_path = Path(__file__).parent.parent / 'data' / '03_processed' / 'sede_consolidation_result.csv'
if csv_path.exists():
    import pandas as pd
    df = pd.read_csv(csv_path)
    print('\n=== sede_consolidation_result.csv rows for 2601607,2603009,2612208 ===')
    print(df[df['sede_origem'].isin([2601607,2603009,2612208]) | df['sede_destino'].isin([2601607,2603009,2612208])])
else:
    print('\nCSV not found at', csv_path)

# Print snapshot step6 if exists
snap_path = Path(__file__).parent.parent / 'data' / '03_processed' / 'snapshot_step6_sede_consolidation.json'
if snap_path.exists():
    print('\nSnapshot path:', snap_path)
    with open(snap_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    # Try to find entries for these mun ids in 'utps_mapping' or 'consolidations'
    utp_map = data.get('utps_mapping', {})
    print('\nUTP mapping samples (keys contain 260 or 261):')
    for k,v in list(utp_map.items())[:50]:
        pass
    # Print if specific mapping exists
    for key in ['2601607','2603009','2612208']:
        if key in utp_map:
            print(f'{key} ->', utp_map[key])
    # Also print consolidations entries related
    print('\nConsolidations entries containing these IDs:')
    for c in data.get('consolidations', []):
        if any(str(x) in [str(2601607), str(2603009), str(2612208)] for x in [c.get('details',{}).get('mun_id'), c.get('source_utp'), c.get('target_utp')]):
            print(c)
else:
    print('\nSnapshot not found at', snap_path)

print('\nDone')
