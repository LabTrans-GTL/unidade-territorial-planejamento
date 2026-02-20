"""
Script to test the RM normalization hypothesis and impedance parsing differences.
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.graph import TerritorialGraph
from src.utils.snapshot import SnapshotManager

print("=" * 80)
print("TESTING VERSION DIFFERENCES")
print("=" * 80)

# Test 1: RM Values Distribution
print("\n[TEST 1: RM VALUES IN DATA]")
print("=" * 80)

snapshot_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "snapshot_step7_seat_consolidation.json"
if snapshot_path.exists():
    snapshot = SnapshotManager.load(snapshot_path)
    graph = TerritorialGraph()
    graph.load_from_snapshot(snapshot)
    
    mun_rms = {}
    for node, data in graph.hierarchy.nodes(data=True):
        if data.get('type') == 'municipality':
            rm = data.get('regiao_metropolitana', 'NOT_SET')
            if rm is None:
                rm = 'NONE_VALUE'
            mun_rms[rm] = mun_rms.get(rm, 0) + 1
    
    print(f"\nDistribution of RM values across {sum(mun_rms.values())} municipalities:")
    for rm_value, count in sorted(mun_rms.items(), key=lambda x: -x[1])[:20]:
        print(f"  '{rm_value}': {count} municipalities")
    
    # Check specific normalization cases
    print(f"\n[NORMALIZATION TEST]")
    test_values = ['SEM_RM', 'RM_SEM_RM', 'sem_rm', 'rm_sem_rm', None, '']
    
    for test_val in test_values:
        # OLD VERSION logic
        old_normalized = ''
        if test_val and test_val.lower() == 'sem_rm':
            old_normalized = ''
        else:
            old_normalized = test_val if test_val else ''
        
        # NEW VERSION logic
        new_normalized = ''
        if test_val is None or str(test_val).upper().strip() in ['SEM_RM', 'RM_SEM_RM', 'NAN', 'NONE', '']:
            new_normalized = ''
        else:
            new_normalized = str(test_val).strip()
        
        match = "✅" if old_normalized == new_normalized else "❌"
        print(f"  {match} '{test_val}' → OLD: '{old_normalized}' | NEW: '{new_normalized}'")

else:
    print("Snapshot not found!")

# Test 2: Impedance Parsing
print("\n\n[TEST 2: IMPEDANCE PARSING]")
print("=" * 80)

impedance_path = Path(__file__).parent.parent.parent / "data" / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"

if impedance_path.exists():
    # OLD VERSION: without decimal=','
    try:
        df_old = pd.read_csv(impedance_path, sep=';', encoding='utf-8', nrows=10000)
        if len(df_old.columns) >= 4:
            df_old.columns = ['par_ibge', 'origem', 'destino', 'tempo']
            df_old['tempo'] = pd.to_numeric(df_old['tempo'], errors='coerce')
            valid_old = df_old['tempo'].notna().sum()
            print(f"\nOLD VERSION (without decimal=','):")
            print(f"  Total records: {len(df_old)}")
            print(f"  Valid tempo values: {valid_old} ({valid_old/len(df_old)*100:.1f}%)")
            print(f"  Sample tempo values: {df_old['tempo'].head(5).tolist()}")
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # NEW VERSION: with decimal=','
    try:
        df_new = pd.read_csv(impedance_path, sep=';', decimal=',', encoding='utf-8', nrows=10000)
        if len(df_new.columns) >= 4:
            df_new.columns = ['par_ibge', 'origem', 'destino', 'tempo']
            df_new['tempo'] = pd.to_numeric(df_new['tempo'], errors='coerce')
            valid_new = df_new['tempo'].notna().sum()
            print(f"\nNEW VERSION (with decimal=','):")
            print(f"  Total records: {len(df_new)}")
            print(f"  Valid tempo values: {valid_new} ({valid_new/len(df_new)*100:.1f}%)")
            print(f"  Sample tempo values: {df_new['tempo'].head(5).tolist()}")
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # Show raw sample
    print(f"\nRaw file sample (first 3 lines):")
    with open(impedance_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i < 3:
                print(f"  {line.strip()}")
            else:
                break
else:
    print("Impedance file not found!")

print("\n" + "=" * 80)
