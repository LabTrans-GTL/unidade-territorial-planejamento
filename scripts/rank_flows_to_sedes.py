
import sys
from pathlib import Path

# Add project root to path
# This file is in scripts/rank_flows_to_sedes.py
# Root is ../../
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

import pandas as pd
import logging
from src.core.manager import GeoValidaManager

def rank_flows():
    print("Initializing Manager...")
    manager = GeoValidaManager()
    logging.getLogger("GeoValida").setLevel(logging.WARNING)
    
    if not manager.step_0_initialize_data():
        print("Failed to init data.")
        return
        
    print("Loading flows...")
    manager.step_2_analyze_flows()
    flow_df = manager.analyzer.full_flow_df
    
    print("Loading impedance (2h)...")
    impedance_path = project_root / "data/01_raw/impedance/impedancias_filtradas_2h.csv"
    df_imp = pd.DataFrame()
    if impedance_path.exists():
        df_imp = pd.read_csv(impedance_path, sep=';', encoding='latin-1')
        
        # Check if columns are already correct or need mapping
        cols = [c.upper() for c in df_imp.columns]
        
        # Normalize to lower for consistent processing
        df_imp.columns = [c.lower() for c in df_imp.columns]
        
        # Map known columns
        # Expected: cod_ibge_origem_1, cod_ibge_destino_1 (6 digits)
        # Or: cod_ibge_origem, cod_ibge_destino
        
        # If codified as origen_1/destino_1 (6 digits) used in prev scripts
        if 'cod_ibge_origem_1' in df_imp.columns:
             df_imp = df_imp.rename(columns={
                'cod_ibge_origem_1': 'origem_6',
                'cod_ibge_destino_1': 'destino_6',
                'tempo': 'tempo'
             })
        elif 'cod_ibge_origem' in df_imp.columns:
             # Create 6 digits from 7 if needed
             # But usually filtered file has specific columns
             pass
             
        # Ensure we have what we need
        if 'origem_6' in df_imp.columns:
            df_imp['origem_6'] = pd.to_numeric(df_imp['origem_6'], errors='coerce').fillna(0).astype(int)
            df_imp['destino_6'] = pd.to_numeric(df_imp['destino_6'], errors='coerce').fillna(0).astype(int)
            # Fix comma decimal
            if df_imp['tempo'].dtype == object:
                df_imp['tempo'] = (
                    df_imp['tempo'].astype(str).str.replace(',', '.').astype(float)
                )
            
            # Create lookup
            # Use tuple (orig, dest) -> time
            # Note: The file might have duplicates, we take first or mean? Usually unique.
            imp_lookup = df_imp.set_index(['origem_6', 'destino_6'])['tempo'].to_dict()
        else:
            print(f"Impedance file columns mismatch: {df_imp.columns.tolist()}")
            return
    else:
        print(f"Impedance file not found at {impedance_path}")
        return

    # Identify Sedes
    sedes = set()
    sede_names = {}
    
    # Also load hierarchy names for ALL municipalities to avoid lookup inside loop
    mun_names = {}
    for n, d in manager.graph.hierarchy.nodes(data=True):
        try:
            mun_names[int(n)] = d.get('name', str(n))
        except (ValueError, TypeError):
            continue
        
    for utp, sede_id in manager.graph.utp_seeds.items():
        try:
            sid = int(sede_id)
            sedes.add(sid)
            sede_names[sid] = mun_names.get(sid, str(sid))
        except (ValueError, TypeError):
            continue
    
    print(f"Found {len(sedes)} sedes.")
    
    # Process each municipality
    print("Processing flows...")
    
    # Filter flows where destination is a Sede
    flows_to_sedes = flow_df[flow_df['mun_destino'].isin(sedes)].copy()
    
    results = []
    
    # Group by origin
    # We want ALL municipalities that have ANY flow to a sede
    # If a municipality has NO flow to ANY sede, it won't appear here (which is expected for flow ranking)
    
    for mun_id, group in flows_to_sedes.groupby('mun_origem'):
        try:
            mun_id = int(mun_id)
        except:
            continue
        
        orig_name = mun_names.get(mun_id, str(mun_id))
            
        # Get Current Sede/UTP
        current_utp = manager.graph.get_municipality_utp(mun_id)
        current_sede_id = None
        current_sede_name = "Unknown"
        
        if current_utp and str(current_utp) in manager.graph.utp_seeds:
            try:
                current_sede_id = int(manager.graph.utp_seeds[str(current_utp)])
                current_sede_name = mun_names.get(current_sede_id, str(current_sede_id))
            except:
                pass
        
        # Sort flows desc
        group = group.sort_values('viagens', ascending=False)
        
        rank = 1
        for _, row in group.iterrows():
            sede_id = int(row['mun_destino'])
            flow_val = float(row['viagens'])
            
            # Lookup time
            orig_6 = mun_id // 10
            dest_6 = sede_id // 10
            
            time_val = imp_lookup.get((orig_6, dest_6), None)
            
            target_sede_name = sede_names.get(sede_id, str(sede_id))
            
            results.append({
                'Origin_ID': mun_id,
                'Origin_Name': orig_name,
                'Current_UTP': current_utp,
                'Current_Sede': current_sede_name,
                'Rank': rank,
                'Target_Sede_ID': sede_id,
                'Target_Sede_Name': target_sede_name,
                'Flow': flow_val,
                'Time_Hours': time_val if time_val is not None else '' # Blank if missing
            })
            rank += 1
            
    # Save to CSV
    res_df = pd.DataFrame(results)
    output_path = "analysis_flows_to_sedes.csv"
    res_df.to_csv(output_path, index=False, sep=';', decimal=',')
    print(f"Sorted analysis saved to {output_path}")
    
    # Preview
    print("\n--- Top 10 High Flow Mismatches (Rank 1 to Different Sede) ---")
    mismatches = res_df[
        (res_df['Rank'] == 1) & 
        (res_df['Target_Sede_Name'] != res_df['Current_Sede'])
    ]
    
    if not mismatches.empty:
        print(mismatches[['Origin_Name', 'Current_Sede', 'Target_Sede_Name', 'Flow', 'Time_Hours']].head(10).to_string())
    else:
        print("No mismatches found in Rank 1.")

if __name__ == "__main__":
    rank_flows()
