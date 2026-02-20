"""
Debug script for Floresta (2605707) - specific case analysis.
Why is it not being moved to UTP 321?
"""

import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

MUN_ID = 2605707
TARGET_UTP = "321"

print("=" * 80)
print(f"DEBUGGING FLORESTA ({MUN_ID}) - Why not moving to UTP {TARGET_UTP}?")
print("=" * 80)

# Load snapshot
import json

snapshot_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "snapshot_step7_seat_consolidation.json"
with open(snapshot_path, 'r', encoding='utf-8') as f:
    snapshot = json.load(f)

# Get current UTP
current_utp = snapshot['municipalities'].get(str(MUN_ID))
print(f"\n[CURRENT STATE]")
print(f"Floresta current UTP: {current_utp}")
print(f"Should be in UTP: {TARGET_UTP}")
print(f"Needs to move: {'YES' if str(current_utp) != str(TARGET_UTP) else 'NO'}")

# Load geodata
geojson_path = Path(__file__).parent.parent.parent / "data" / "01_raw" / "municipios" / "municipios_br_2022.geojson"
gdf = gpd.read_file(geojson_path)

floresta_data = gdf[gdf['CD_MUN'] == MUN_ID]
if floresta_data.empty:
    print(f"\n[ERROR] Floresta not found in GeoJSON!")
else:
    print(f"\n[GEODATA]")
    print(f"Name: {floresta_data.iloc[0]['NM_MUN']}")
    print(f"State: {floresta_data.iloc[0].get('SIGLA_UF', 'N/A')}")

# Check border validation results
result_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "border_validation_result.csv"
if result_path.exists():
    df_results = pd.read_csv(result_path)
    floresta_results = df_results[df_results['mun_id'] == MUN_ID]
    
    print(f"\n[BORDER VALIDATION RESULTS]")
    if floresta_results.empty:
        print(f"Floresta NOT in border validation results!")
        print(f"This means:")
        print(f"  1. It's not a border municipality, OR")
        print(f"  2. Its principal flow is not to an adjacent municipality of different UTP, OR")
        print(f"  3. It's a Sede municipality (cannot move)")
    else:
        print(f"Found {len(floresta_results)} records:")
        for _, row in floresta_results.iterrows():
            print(f"  - Iteration {row['iteration']}: {row['action']} - {row['reason']}")
            if row.get('details'):
                print(f"    Details: {row['details']}")

# Check flows
flow_path = Path(__file__).parent.parent.parent / "data" / "01_raw" / "flows" / "dados_deslocamento_limpos.csv"
flow_df = pd.read_csv(flow_path)

floresta_flows = flow_df[flow_df['mun_origem'] == MUN_ID].copy()

print(f"\n[FLOW ANALYSIS]")
print(f"Total outgoing flows: {len(floresta_flows)}")

if len(floresta_flows) > 0:
    # Load impedance
    impedance_path = Path(__file__).parent.parent.parent / "data" / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"
    df_impedance = pd.read_csv(impedance_path, sep=';', decimal=',', encoding='utf-8')
    if len(df_impedance.columns) >= 4:
        df_impedance.columns = ['par_ibge', 'origem', 'destino', 'tempo']
        df_impedance['origem'] = df_impedance['origem'].astype(int)
        df_impedance['destino'] = df_impedance['destino'].astype(int)
        df_impedance['tempo'] = pd.to_numeric(df_impedance['tempo'], errors='coerce')
    
    # Merge flows with impedance
    floresta_flows = floresta_flows.merge(
        df_impedance[['origem', 'destino', 'tempo']],
        left_on=['mun_origem', 'mun_destino'],
        right_on=['origem', 'destino'],
        how='left'
    )
    
    # Filter by 2h
    flows_2h = floresta_flows[floresta_flows['tempo'] <= 2.0].copy()
    
    print(f"Flows within 2h: {len(flows_2h)}")
    
    if len(flows_2h) > 0:
        # Get top flows
        top_flows = flows_2h.nlargest(10, 'viagens')
        
        print(f"\nTop 10 flows (<=2h):")
        for idx, (_, row) in enumerate(top_flows.iterrows(), 1):
            dest_id = int(row['mun_destino'])
            dest_utp = snapshot['municipalities'].get(str(dest_id), 'UNKNOWN')
            
            # Check if destination is in UTP 321
            is_target_utp = "*** TARGET UTP ***" if str(dest_utp) == TARGET_UTP else ""
            
            print(f"  {idx}. Dest: {dest_id} [UTP: {dest_utp}] - {row['viagens']:.0f} trips, {row['tempo']:.2f}h {is_target_utp}")
        
        # Check principal flow
        principal = flows_2h.nlargest(1, 'viagens').iloc[0]
        principal_dest = int(principal['mun_destino'])
        principal_utp = snapshot['municipalities'].get(str(principal_dest), 'UNKNOWN')
        principal_viagens = principal['viagens']
        total_viagens = flows_2h['viagens'].sum()
        pct = (principal_viagens / total_viagens * 100) if total_viagens > 0 else 0
        
        print(f"\n[PRINCIPAL FLOW]")
        print(f"  Destination: {principal_dest} [UTP: {principal_utp}]")
        print(f"  Trips: {principal_viagens:.0f}")
        print(f"  Total trips: {total_viagens:.0f}")
        print(f"  Percentage: {pct:.1f}%")
        print(f"  Passes 5% threshold: {'YES' if pct >= 5 else 'NO'}")
        print(f"  Passes 3% threshold: {'YES' if pct >= 3 else 'NO'}")

# Check adjacency
print(f"\n[ADJACENCY CHECK]")
print(f"Building adjacency graph...")

from shapely.prepared import prep
import networkx as nx

# Build adjacency graph
adjacency_graph = nx.Graph()
adjacency_graph.add_nodes_from(gdf['CD_MUN'].tolist())

# Simple adjacency check for Floresta
if not floresta_data.empty:
    floresta_geom = floresta_data.iloc[0].geometry
    floresta_geom_prep = prep(floresta_geom)
    
    neighbors = []
    for idx, row in gdf.iterrows():
        if row['CD_MUN'] != MUN_ID:
            if floresta_geom_prep.intersects(row.geometry):
                neighbors.append(int(row['CD_MUN']))
    
    print(f"Floresta has {len(neighbors)} spatial neighbors")
    
    # Check neighbors' UTPs
    neighbor_utps = {}
    for n in neighbors:
        n_utp = snapshot['municipalities'].get(str(n), 'UNKNOWN')
        neighbor_utps[n_utp] = neighbor_utps.get(n_utp, 0) + 1
    
    print(f"\nNeighbor UTPs distribution:")
    for utp, count in sorted(neighbor_utps.items(), key=lambda x: -x[1]):
        marker = "*** TARGET ***" if str(utp) == TARGET_UTP else ""
        marker2 = "*** CURRENT ***" if str(utp) == str(current_utp) else ""
        print(f"  UTP {utp}: {count} neighbors {marker}{marker2}")
    
    # Check if principal flow destination is a neighbor
    if len(flows_2h) > 0:
        principal_dest_id = int(flows_2h.nlargest(1, 'viagens').iloc[0]['mun_destino'])
        is_neighbor = principal_dest_id in neighbors
        print(f"\nPrincipal flow destination ({principal_dest_id}) is a neighbor: {'YES' if is_neighbor else 'NO'}")
        
        if not is_neighbor:
            print(f"  ^^ THIS IS WHY IT'S BEING REJECTED! ^^")
            print(f"  The border_validator REQUIRES flow to be to an ADJACENT municipality!")

print("\n" + "=" * 80)
