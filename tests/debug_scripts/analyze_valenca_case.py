"""
Debug script to analyze the Valença, Presidente Tancredo Neves, and Taperoá case.
These municipalities should be checked for transitive relationships and potential
border validation issues.
"""

import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd
import networkx as nx

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.graph import TerritorialGraph
from src.utils.snapshot import SnapshotManager

# Municipalities to analyze
TARGET_MUNS = {
    2932903: "Valença",
    2925758: "Presidente Tancredo Neves",
    2931202: "Taperoá"
}

print("=" * 80)
print("ANALYZING VALENÇA, PRESIDENTE TANCREDO NEVES, AND TAPEROÁ")
print("=" * 80)

# Load snapshot
snapshot_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "snapshot_step7_seat_consolidation.json"
snapshot = SnapshotManager.load(snapshot_path)

# Load flow data
flow_path = Path(__file__).parent.parent.parent / "data" / "01_raw" / "flows" / "dados_deslocamento_limpos.csv"
flow_df = pd.read_csv(flow_path)

# Load impedance data
impedance_path = Path(__file__).parent.parent.parent / "data" / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"
df_impedance = pd.read_csv(impedance_path, sep=';', decimal=',', encoding='utf-8')
if len(df_impedance.columns) >= 4:
    df_impedance.columns = ['par_ibge', 'origem', 'destino', 'tempo']
    df_impedance['origem'] = df_impedance['origem'].astype(int)
    df_impedance['destino'] = df_impedance['destino'].astype(int)
    df_impedance['tempo'] = pd.to_numeric(df_impedance['tempo'], errors='coerce')
    df_impedance = df_impedance[df_impedance['tempo'].notna()]

# Load GeoDataFrame
geojson_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "municipios_br_2022.geojson"
gdf = gpd.read_file(geojson_path)

# Merge UTP assignments
utp_assignment = pd.DataFrame([
    {"CD_MUN": k, "UTP_ID": v}
    for k, v in snapshot['municipalities'].items()
])
utp_assignment['CD_MUN'] = utp_assignment['CD_MUN'].astype(int)
gdf['CD_MUN'] = gdf['CD_MUN'].astype(int)
gdf = gdf.merge(utp_assignment, on='CD_MUN', how='left')

print("\n1️⃣ BASIC INFORMATION")
print("=" * 80)
for mun_id, mun_name in TARGET_MUNS.items():
    mun_data = gdf[gdf['CD_MUN'] == mun_id]
    if not mun_data.empty:
        utp = mun_data.iloc[0]['UTP_ID']
        print(f"\n{mun_name} ({mun_id}):")
        print(f"  Current UTP: {utp}")
    else:
        print(f"\n{mun_name} ({mun_id}): NOT FOUND in GeoDataFrame")

print("\n\n2️⃣ ADJACENCY INFORMATION")
print("=" * 80)

# Build adjacency graph
from shapely.prepared import prep

adjacency_graph = nx.Graph()
adjacency_graph.add_nodes_from(gdf['CD_MUN'].tolist())

print(f"   Detecting adjacencies for {len(gdf)} municipalities...")
for idx_i, row_i in gdf.iterrows():
    geom_i_prep = prep(row_i.geometry)
    for idx_j, row_j in gdf.loc[idx_i + 1:].iterrows():
        if geom_i_prep.intersects(row_j.geometry) and not row_i.geometry.touches(row_j.geometry):
            adjacency_graph.add_edge(row_i['CD_MUN'], row_j['CD_MUN'])

for mun_id, mun_name in TARGET_MUNS.items():
    if mun_id in adjacency_graph:
        neighbors = list(adjacency_graph.neighbors(mun_id))
        print(f"\n{mun_name} ({mun_id}) has {len(neighbors)} neighbors:")
        
        # Check if target municipalities are neighbors of each other
        neighbor_in_targets = [n for n in neighbors if n in TARGET_MUNS]
        if neighbor_in_targets:
            print(f"  ⭐ NEIGHBORS FROM TARGET LIST:")
            for n in neighbor_in_targets:
                n_data = gdf[gdf['CD_MUN'] == n]
                if not n_data.empty:
                    n_name = n_data.iloc[0]['NM_MUN']
                    n_utp = n_data.iloc[0]['UTP_ID']
                    print(f"     - {n_name} ({n}) [UTP: {n_utp}]")
        
        # Show all neighbors with UTP
        print(f"  All neighbors (showing first 10):")
        for n in neighbors[:10]:
            n_data = gdf[gdf['CD_MUN'] == n]
            if not n_data.empty:
                n_name = n_data.iloc[0]['NM_MUN']
                n_utp = n_data.iloc[0]['UTP_ID']
                print(f"     - {n_name} ({n}) [UTP: {n_utp}]")
    else:
        print(f"\n{mun_name} ({mun_id}): NOT in adjacency graph")

print("\n\n3️⃣ FLOW ANALYSIS")
print("=" * 80)

for mun_id, mun_name in TARGET_MUNS.items():
    print(f"\n{mun_name} ({mun_id}):")
    
    # Get outgoing flows
    flows_out = flow_df[flow_df['mun_origem'] == mun_id].copy()
    
    if flows_out.empty:
        print("  No outgoing flows found")
        continue
    
    # Merge with impedance
    flows_out = flows_out.merge(
        df_impedance[['origem', 'destino', 'tempo']],
        left_on=['mun_origem', 'mun_destino'],
        right_on=['origem', 'destino'],
        how='left'
    )
    
    # Filter by 2h time
    flows_2h = flows_out[flows_out['tempo'] <= 2.0].copy()
    
    print(f"  Total flows: {len(flows_out)}")
    print(f"  Flows ≤2h: {len(flows_2h)}")
    
    if not flows_2h.empty:
        # Get top 10 flows
        top_flows = flows_2h.nlargest(10, 'viagens')
        
        print(f"\n  Top 10 flows (≤2h):")
        for _, row in top_flows.iterrows():
            dest_id = int(row['mun_destino'])
            dest_data = gdf[gdf['CD_MUN'] == dest_id]
            
            if not dest_data.empty:
                dest_name = dest_data.iloc[0]['NM_MUN']
                dest_utp = dest_data.iloc[0]['UTP_ID']
                
                # Check if destination is a neighbor
                is_neighbor = dest_id in adjacency_graph.neighbors(mun_id) if mun_id in adjacency_graph else False
                is_target = "⭐" if dest_id in TARGET_MUNS else ""
                neighbor_mark = "✅ ADJACENT" if is_neighbor else "❌ NOT ADJACENT"
                
                print(f"     {is_target} {dest_name} ({dest_id}) [UTP: {dest_utp}] - {row['viagens']:.0f} trips, {row['tempo']:.2f}h - {neighbor_mark}")
            else:
                print(f"     Unknown ({dest_id}) - {row['viagens']:.0f} trips, {row['tempo']:.2f}h")
    
    # Calculate flow percentages
    total_flow = flows_2h['viagens'].sum()
    print(f"\n  Total outgoing flow (≤2h): {total_flow:.0f}")
    
    if total_flow > 0 and not flows_2h.empty:
        max_flow_row = flows_2h.nlargest(1, 'viagens').iloc[0]
        max_flow = max_flow_row['viagens']
        max_flow_pct = (max_flow / total_flow) * 100
        dest_id = int(max_flow_row['mun_destino'])
        dest_data = gdf[gdf['CD_MUN'] == dest_id]
        dest_name = dest_data.iloc[0]['NM_MUN'] if not dest_data.empty else f"Mun_{dest_id}"
        
        print(f"  Principal flow: {dest_name} ({dest_id}) - {max_flow:.0f} trips ({max_flow_pct:.1f}%)")

print("\n\n4️⃣ BORDER MUNICIPALITY STATUS")
print("=" * 80)

for mun_id, mun_name in TARGET_MUNS.items():
    mun_data = gdf[gdf['CD_MUN'] == mun_id]
    if mun_data.empty:
        continue
    
    current_utp = mun_data.iloc[0]['UTP_ID']
    
    # Check if it's a border municipality
    # (has at least one neighbor in a different UTP)
    if mun_id in adjacency_graph:
        neighbors = list(adjacency_graph.neighbors(mun_id))
        neighbor_utps = []
        
        for n in neighbors:
            n_data = gdf[gdf['CD_MUN'] == n]
            if not n_data.empty:
                n_utp = n_data.iloc[0]['UTP_ID']
                neighbor_utps.append(n_utp)
        
        different_utp_neighbors = [u for u in neighbor_utps if u != current_utp]
        
        is_border = len(different_utp_neighbors) > 0
        
        print(f"\n{mun_name} ({mun_id}):")
        print(f"  Current UTP: {current_utp}")
        print(f"  Total neighbors: {len(neighbors)}")
        print(f"  Neighbors in different UTPs: {len(different_utp_neighbors)}")
        print(f"  IS BORDER MUNICIPALITY: {'YES ✅' if is_border else 'NO ❌'}")
        
        if is_border:
            unique_neighbor_utps = set(different_utp_neighbors)
            print(f"  Adjacent UTPs: {sorted(unique_neighbor_utps)}")

print("\n\n5️⃣ TRANSITIVE CHAIN ANALYSIS")
print("=" * 80)

# Build a flow chain
print("\nBuilding flow chain for target municipalities...")

for mun_id, mun_name in TARGET_MUNS.items():
    flows_out = flow_df[flow_df['mun_origem'] == mun_id].copy()
    
    if flows_out.empty:
        continue
    
    # Merge with impedance
    flows_out = flows_out.merge(
        df_impedance[['origem', 'destino', 'tempo']],
        left_on=['mun_origem', 'mun_destino'],
        right_on=['origem', 'destino'],
        how='left'
    )
    
    # Filter by 2h and adjacent only
    flows_2h = flows_out[flows_out['tempo'] <= 2.0].copy()
    
    if flows_2h.empty:
        continue
    
    # Get principal flow
    max_flow = flows_2h.nlargest(1, 'viagens')
    if max_flow.empty:
        continue
    
    dest_id = int(max_flow.iloc[0]['mun_destino'])
    
    # Check if adjacent
    is_adjacent = dest_id in adjacency_graph.neighbors(mun_id) if mun_id in adjacency_graph else False
    
    if not is_adjacent:
        continue
    
    # Get destination info
    dest_data = gdf[gdf['CD_MUN'] == dest_id]
    if dest_data.empty:
        continue
    
    dest_name = dest_data.iloc[0]['NM_MUN']
    dest_utp = dest_data.iloc[0]['UTP_ID']
    
    # Get current UTP
    mun_data = gdf[gdf['CD_MUN'] == mun_id]
    current_utp = mun_data.iloc[0]['UTP_ID']
    
    if dest_utp != current_utp:
        print(f"\n{mun_name} ({mun_id}) [UTP: {current_utp}]")
        print(f"  → wants to move to: {dest_name} ({dest_id}) [UTP: {dest_utp}]")
        
        # Check if destination also wants to move
        if dest_id in TARGET_MUNS:
            print(f"     ⚠️  Destination is also in target list - potential cycle!")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
