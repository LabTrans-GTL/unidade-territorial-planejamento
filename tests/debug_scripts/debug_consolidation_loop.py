"""
Debug script to simulate the exact consolidation logic
"""
import json
import pandas as pd
import sys

# Fix encoding
sys.stdout.reconfigure(encoding='utf-8')

# Load cache
with open('data/sede_analysis_cache.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Create a simple sede lookup (simulating is_sede)
sede_ids = {int(s['cd_mun_sede']) for s in data['sede_analysis']}

print("Total sedes in cache:", len(sede_ids))
print()

# Simulate the consolidation loop
df_metrics = pd.DataFrame(data['sede_analysis'])

print(f"Total rows in metrics: {len(df_metrics)}")
print(f"Rows with tem_alerta_dependencia: {df_metrics['tem_alerta_dependencia'].sum()}")
print()

candidates = []

for _, row in df_metrics.iterrows():
    # Must be a dependency alert to be considered
    if not row['tem_alerta_dependencia']:
        continue
    
    print(f"\n>>> Processing alert for: {row['nm_sede']} ({row['cd_mun_sede']})")
    
    sede_origem = row['cd_mun_sede']
    utp_origem = row['utp_id']
    sede_destino = row['principal_destino_cd']
    
    print(f"    sede_destino value: {sede_destino}, type: {type(sede_destino)}")
    print(f"    pd.isna(sede_destino): {pd.isna(sede_destino)}")
    
    # Check validity of destination
    if pd.isna(sede_destino):
        print(f"    ❌ SKIPPED: sede_destino is NaN")
        continue
    
    # Check if is_sede
    sede_destino_int = int(sede_destino)
    print(f"    sede_destino_int: {sede_destino_int}")
    print(f"    Is sede?: {sede_destino_int in sede_ids}")
    
    if sede_destino_int not in sede_ids:
        print(f"    ❌ SKIPPED: Destination {sede_destino_int} is not a sede")
        continue
    
    print(f"    ✅ Destination is valid sede!")
    
    # Get metrics for Destination  
    dest_row = df_metrics[df_metrics['cd_mun_sede'] == sede_destino]
    print(f"    dest_row found: {not dest_row.empty}")
    
    if dest_row.empty:
        print(f"    ❌ SKIPPED: Could not find destination in metrics")
        continue
    
    print(f"    ✅ Passed all checks - would be processed for scoring!")

print(f"\n\nTotal candidates that passed initial validation: {len(candidates)}")
