"""
WHY is Floresta (2605707) NOT being considered for border validation?
Current UTP: 675
Should be: 321
"""

import json
import pandas as pd
import geopandas as gpd
from pathlib import Path

MUN_ID = 2605707

print("=" * 80)
print(f"WHY FLORESTA ({MUN_ID}) NOT MOVING FROM UTP 675 TO UTP 321?")
print("=" * 80)

# Load snapshot
snap_path = Path("data/03_processed/snapshot_step8_border_validation.json")
with open(snap_path, 'r', encoding='utf-8') as f:
    snapshot = json.load(f)

nodes = snapshot.get('nodes', {})
floresta = nodes.get(str(MUN_ID), {})

print(f"\nFloresta current state:")
print(f"  Name: {floresta.get('name')}")
print(f"  UTP: {floresta.get('utp_id')}")
print(f"  Is Sede: {floresta.get('sede_utp')}")

# Check if it's a sede
utp_seeds = snapshot.get('utp_seeds', {})
is_sede = MUN_ID in utp_seeds.values() or str(MUN_ID) in utp_seeds.values()
print(f"  Is UTP Sede: {is_sede}")

if is_sede:
    print("\n*** REASON: Floresta is a SEDE municipality - cannot move! ***")
else:
    # Load flows
    flow_df = pd.read_csv("data/01_raw/flows/dados_deslocamento_limpos.csv")
    floresta_flows = flow_df[flow_df['mun_origem'] == MUN_ID].copy()
    
    print(f"\n[FLOWS]")
    print(f"Total outgoing flows: {len(floresta_flows)}")
    
    if len(floresta_flows) == 0:
        print("*** REASON: No outgoing flows! ***")
    else:
        # Load impedance
        imp_df = pd.read_csv("data/01_raw/impedance/impedancias_filtradas_2h.csv", 
                            sep=';', decimal=',', encoding='utf-8')
        imp_df.columns = ['par_ibge', 'origem', 'destino', 'tempo']
        imp_df['origem'] = imp_df['origem'].astype(int)
        imp_df['destino'] = imp_df['destino'].astype(int)
        imp_df['tempo'] = pd.to_numeric(imp_df['tempo'], errors='coerce')
        
        # Merge
        floresta_flows = floresta_flows.merge(
            imp_df[['origem', 'destino', 'tempo']],
            left_on=['mun_origem', 'mun_destino'],
            right_on=['origem', 'destino'],
            how='left'
        )
        
        flows_2h = floresta_flows[floresta_flows['tempo'] <= 2.0].copy()
        print(f"Flows within 2h: {len(flows_2h)}")
        
        if len(flows_2h) == 0:
            print("*** REASON: No flows within 2h! ***")
        else:
            # Get principal flow
            principal = flows_2h.nlargest(1, 'viagens').iloc[0]
            principal_dest = int(principal['mun_destino'])
            principal_trips = principal['viagens']
            total_trips = flows_2h['viagens'].sum()
            pct = (principal_trips / total_trips * 100) if total_trips > 0 else 0
            
            # Get destination UTP
            dest_node = nodes.get(str(principal_dest), {})
            dest_utp = dest_node.get('utp_id',  'UNKNOWN')
            
            print(f"\n[PRINCIPAL FLOW]")
            print(f"  To: {principal_dest}")
            print(f"  Dest UTP: {dest_utp}")
            print(f"  Trips: {principal_trips:.0f}")
            print(f"  % of total: {pct:.1f}%")
            print(f"  Passes 5% threshold: {'YES' if pct >= 5 else 'NO'}")
            
            # Check if dest UTP is different
            current_utp = floresta.get('utp_id')
            if str(dest_utp) == str(current_utp):
                print(f"\n*** REASON: Principal flow is to SAME UTP ({current_utp})! ***")
            elif pct < 5:
                print(f"\n*** REASON: Flow too weak ({pct:.1f}% < 5%)! ***")
            else:
                # Check adjacency
                print(f"\n[CHECKING ADJACENCY]")
                gdf = gpd.read_file("data/01_raw/municipios/municipios_br_2022.geojson")
                
                floresta_geom = gdf[gdf['CD_MUN'] == MUN_ID].iloc[0].geometry
                dest_geom = gdf[gdf['CD_MUN'] == principal_dest]
                
                if dest_geom.empty:
                    print(f"  Destination {principal_dest} not in geodata!")
                else:
                    dest_geom = dest_geom.iloc[0].geometry
                    is_adjacent = floresta_geom.intersects(dest_geom)
                    
                    print(f"  Floresta and {principal_dest} are adjacent: {is_adjacent}")
                    
                    if not is_adjacent:
                        print(f"\n*** REASON: Principal flow is NOT to an adjacent municipality! ***")
                        print(f"  This is the ADJACENCY-ONLY constraint!")
                    else:
                        print(f"\n  Floresta SHOULD be moved but wasn't!")
                        print(f"  Check other validation rules (RM, fragmentation, etc.)")

print("\n" + "=" * 80)
