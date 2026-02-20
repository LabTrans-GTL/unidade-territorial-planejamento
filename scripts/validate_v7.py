#!/usr/bin/env python3
"""
V7 Validation Script
====================
Validates V7 UTP configuration against two critical rules:
1. RM Integrity: All municipalities in a UTP must either all belong to the same RM or none belong to any RM.
2. Contiguity: All municipalities in a UTP must be geographically contiguous (no isolated islands).

Output: Excel file with detailed error report.
"""

import pandas as pd
import geopandas as gpd
import networkx as nx
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def load_data():
    """Load all necessary datasets."""
    print("Loading datasets...")
    
    # Determine project root (parent of scripts directory)
    project_root = Path(__file__).parent.parent
    
    # V7 Base
    v7_path = project_root / "data" / "01_raw" / "v7_base 2(br_municipios_2024).csv"
    v7_df = pd.read_csv(v7_path, sep=';', encoding='latin-1')
    print(f"[OK] Loaded V7: {len(v7_df)} municipalities")
    
    # RM Composition
    rm_path = project_root / "data" / "01_raw" / "Composicao_RM_2024.xlsx"
    rm_df = pd.read_excel(rm_path)
    print(f"[OK] Loaded RM: {len(rm_df)} records")
    
    # Shapefiles
    shp_path = project_root / "data" / "01_raw" / "shapefiles" / "BR_Municipios_2024.shp"
    gdf = gpd.read_file(shp_path)
    print(f"[OK] Loaded Geometries: {len(gdf)} municipalities")

    
    return v7_df, rm_df, gdf

def check_rm_integrity(v7_df, rm_df):
    """
    Check RM Integrity Rule:
    - All municipalities in a UTP must either:
      a) All belong to the same RM, OR
      b) None belong to any RM
    """
    print("\n" + "="*60)
    print("CHECKING RULE 1: RM INTEGRITY")
    print("="*60)
    
    # Merge V7 with RM data
    # V7 uses CD_MUN, RM uses COD_MUN
    v7_df['CD_MUN'] = v7_df['CD_MUN'].astype(str)
    rm_df['COD_MUN'] = rm_df['COD_MUN'].astype(str)
    
    merged = v7_df.merge(
        rm_df[['COD_MUN', 'NOME_RECMETROPOL']],
        left_on='CD_MUN',
        right_on='COD_MUN',
        how='left'
    )
    
    # Group by UTP and check RM consistency
    errors = []
    municipality_details = []
    
    for utp_id, group in merged.groupby('UTP'):
        # Get unique RMs (excluding NaN)
        rms = group['NOME_RECMETROPOL'].dropna().unique()
        num_with_rm = group['NOME_RECMETROPOL'].notna().sum()
        total_munis = len(group)
        
        has_error = False
        error_type = None
        error_desc = None
        
        # Error Case 1: Multiple different RMs in same UTP
        if len(rms) > 1:
            has_error = True
            error_type = 'Múltiplas RMs'
            error_desc = f'UTP contém {len(rms)} RMs diferentes: {", ".join(rms)}'
        
        # Error Case 2: Mix of RM and Non-RM municipalities
        elif len(rms) == 1 and num_with_rm < total_munis:
            has_error = True
            error_type = 'Mix RM/Não-RM'
            error_desc = f'{num_with_rm} municípios em RM ({rms[0]}), {total_munis - num_with_rm} fora de RM'
        
        if has_error:
            # Add summary error
            errors.append({
                'UTP': utp_id,
                'Error_Type': error_type,
                'Error_Description': error_desc,
                'Total_Municipalities': total_munis,
                'Municipalities_with_RM': num_with_rm,
                'Municipalities_without_RM': total_munis - num_with_rm,
                'RMs_Found': ", ".join(rms) if len(rms) > 0 else "Nenhuma",
                'Municipality_List': ", ".join(group['NM_MUN'].tolist()),
                'Municipality_Codes': ", ".join(group['CD_MUN'].tolist())
            })
            
            # Add detailed municipality breakdown
            for idx, row in group.iterrows():
                municipality_details.append({
                    'UTP': utp_id,
                    'Error_Type': error_type,
                    'Município_Código': row['CD_MUN'],
                    'Município_Nome': row['NM_MUN'],
                    'UF': row['SIGLA_UF'],
                    'Região_Metropolitana': row['NOME_RECMETROPOL'] if pd.notna(row['NOME_RECMETROPOL']) else 'Sem RM',
                    'Problema_Identificado': 'Pertence a RM diferente dos demais' if (len(rms) > 1 and pd.notna(row['NOME_RECMETROPOL'])) 
                                            else 'Não pertence a RM enquanto outros pertencem' if (len(rms) == 1 and pd.isna(row['NOME_RECMETROPOL']))
                                            else 'Pertence a RM enquanto outros não pertencem',
                    'População_2017': row.get('POP2017', 'N/A'),
                    'É_Sede_UTP': 'Sim' if row.get('SEDE', 0) == 1 else 'Não'
                })
    
    print(f"Found {len(errors)} RM integrity violations")
    print(f"Total municipalities affected: {len(municipality_details)}")
    
    return pd.DataFrame(errors), pd.DataFrame(municipality_details)

def check_contiguity(v7_df, gdf):
    """
    Check Contiguity Rule:
    - All municipalities in a UTP must be geographically contiguous.
    - Uses graph analysis: each UTP's municipalities must form a connected component.
    """
    print("\n" + "="*60)
    print("CHECKING RULE 2: CONTIGUITY")
    print("="*60)
    
    # Ensure consistent municipality codes
    v7_df['CD_MUN'] = v7_df['CD_MUN'].astype(str).str.strip()
    
    # Find the municipality code column in shapefile
    mun_col = None
    for col in ['CD_MUN', 'CD_GEOCMU', 'GEOCODIGO', 'COD_MUN']:
        if col in gdf.columns:
            mun_col = col
            break
    
    if mun_col is None:
        print(f"Warning: Available columns in shapefile: {list(gdf.columns)}")
        print("Using first column as municipality code")
        mun_col = gdf.columns[0]
    
    gdf[mun_col] = gdf[mun_col].astype(str).str.strip()
    
    print(f"Building adjacency graph from {len(gdf)} geometries...")
    
    # Build adjacency graph using SPATIAL INDEX for massive performance improvement
    adj_graph = nx.Graph()
    
    # Add all municipalities as nodes
    for idx, row in gdf.iterrows():
        adj_graph.add_node(row[mun_col])
    
    # Use spatial index to find potential neighbors efficiently
    print("Computing adjacencies using spatial index (this will be much faster)...")
    
    total_edges = 0
    for idx, muni_a in gdf.iterrows():
        # Get bounding box of current municipality
        possible_matches_idx = list(gdf.sindex.intersection(muni_a.geometry.bounds))
        
        # Check only the geometries that might actually touch (massive speedup!)
        for match_idx in possible_matches_idx:
            if idx < match_idx:  # Avoid duplicates and self-loops
                muni_b = gdf.iloc[match_idx]
                if muni_a.geometry.touches(muni_b.geometry):
                    adj_graph.add_edge(muni_a[mun_col], muni_b[mun_col])
                    total_edges += 1
        
        # Progress indicator every 500 municipalities
        if (idx + 1) % 500 == 0:
            print(f"  Processed {idx + 1}/{len(gdf)} municipalities... ({total_edges} edges found)")
    
    print(f"[OK] Graph built: {adj_graph.number_of_nodes()} nodes, {adj_graph.number_of_edges()} edges")


    
    # Check contiguity for each UTP
    errors = []
    municipality_details = []
    
    for utp_id, group in v7_df.groupby('UTP'):
        muni_codes = group['CD_MUN'].tolist()
        
        # Get nodes present in both the UTP and the graph
        valid_nodes = [m for m in muni_codes if m in adj_graph.nodes]
        
        if len(valid_nodes) == 0:
            errors.append({
                'UTP': utp_id,
                'Error_Type': 'Sem Geometria',
                'Error_Description': 'Nenhum município encontrado no shapefile',
                'Total_Municipalities': len(group),
                'Connected_Components': 0,
                'Largest_Component_Size': 0,
                'Municipality_List': ", ".join(group['NM_MUN'].tolist()),
                'Municipality_Codes': ", ".join(muni_codes)
            })
            continue
        
        if len(valid_nodes) < len(muni_codes):
            print(f"Warning: UTP {utp_id} has {len(muni_codes) - len(valid_nodes)} municipalities not in shapefile")
        
        # Create subgraph for this UTP
        subgraph = adj_graph.subgraph(valid_nodes)
        
        # Check if connected
        components = list(nx.connected_components(subgraph))
        
        if len(components) > 1:
            # Not contiguous - multiple disconnected components
            largest_comp = max(components, key=len)
            
            # Summary error
            errors.append({
                'UTP': utp_id,
                'Error_Type': 'Não Contígua',
                'Error_Description': f'UTP dividida em {len(components)} componentes desconectados',
                'Total_Municipalities': len(valid_nodes),
                'Connected_Components': len(components),
                'Largest_Component_Size': len(largest_comp),
                'Municipality_List': ", ".join(group['NM_MUN'].tolist()),
                'Municipality_Codes': ", ".join(muni_codes)
            })
            
            # Create a mapping of municipality to component number
            mun_to_component = {}
            for comp_idx, component_set in enumerate(sorted(components, key=len, reverse=True), 1):
                for mun_code in component_set:
                    mun_to_component[mun_code] = comp_idx
            
            # Add detailed municipality breakdown
            for idx, row in group.iterrows():
                mun_code = row['CD_MUN']
                component_num = mun_to_component.get(mun_code, 'N/A')
                is_largest = component_num == 1
                component_size = len(components[component_num - 1]) if component_num != 'N/A' else 0
                
                municipality_details.append({
                    'UTP': utp_id,
                    'Error_Type': 'Não Contígua',
                    'Município_Código': mun_code,
                    'Município_Nome': row['NM_MUN'],
                    'UF': row['SIGLA_UF'],
                    'Componente_Número': f'Componente {component_num}' if component_num != 'N/A' else 'Sem Geometria',
                    'Tamanho_Componente': component_size,
                    'É_Componente_Principal': 'Sim' if is_largest else 'Não',
                    'Problema_Identificado': 'Município isolado geograficamente' if component_size == 1 
                                            else f'Faz parte de componente secundário ({component_size} municípios)'
                                            if not is_largest 
                                            else 'Componente principal (maior grupo conectado)',
                    'População_2017': row.get('POP2017', 'N/A'),
                    'É_Sede_UTP': 'Sim' if row.get('SEDE', 0) == 1 else 'Não'
                })
    
    print(f"Found {len(errors)} contiguity violations")
    print(f"Total municipalities affected: {len(municipality_details)}")
    
    return pd.DataFrame(errors), pd.DataFrame(municipality_details)

def generate_report(rm_errors, rm_details, contiguity_errors, contiguity_details, output_path):
    """Generate Excel report with all errors."""
    print("\n" + "="*60)
    print("GENERATING REPORT")
    print("="*60)
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Summary sheet
        summary_data = {
            'Categoria': [
                'Total de UTPs com Erros de RM', 
                'Total de Municípios Afetados (RM)',
                'Total de UTPs com Erros de Contiguidade',
                'Total de Municípios Afetados (Contiguidade)',
                'Total Geral de UTPs com Erros',
                'Total Geral de Municípios Afetados'
            ],
            'Quantidade': [
                len(rm_errors), 
                len(rm_details),
                len(contiguity_errors),
                len(contiguity_details),
                len(rm_errors) + len(contiguity_errors),
                len(rm_details) + len(contiguity_details)
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Resumo', index=False)
        
        # RM errors - Summary sheet
        if not rm_errors.empty:
            rm_errors.to_excel(writer, sheet_name='Erros_RM_Resumo', index=False)
        else:
            pd.DataFrame({'Mensagem': ['Nenhum erro de RM encontrado']}).to_excel(
                writer, sheet_name='Erros_RM_Resumo', index=False
            )
        
        # RM errors - Detailed municipality sheet
        if not rm_details.empty:
            rm_details.to_excel(writer, sheet_name='Erros_RM_Detalhado', index=False)
        else:
            pd.DataFrame({'Mensagem': ['Nenhum erro de RM encontrado']}).to_excel(
                writer, sheet_name='Erros_RM_Detalhado', index=False
            )
        
        # Contiguity errors - Summary sheet
        if not contiguity_errors.empty:
            contiguity_errors.to_excel(writer, sheet_name='Erros_Contig_Resumo', index=False)
        else:
            pd.DataFrame({'Mensagem': ['Nenhum erro de contiguidade encontrado']}).to_excel(
                writer, sheet_name='Erros_Contig_Resumo', index=False
            )
        
        # Contiguity errors - Detailed municipality sheet
        if not contiguity_details.empty:
            contiguity_details.to_excel(writer, sheet_name='Erros_Contig_Detalhado', index=False)
        else:
            pd.DataFrame({'Mensagem': ['Nenhum erro de contiguidade encontrado']}).to_excel(
                writer, sheet_name='Erros_Contig_Detalhado', index=False
            )
    
    print(f"[OK] Report saved to: {output_path}")
    print(f"\nSummary:")
    print(f"  - RM Errors (UTPs): {len(rm_errors)}")
    print(f"  - RM Errors (Municipalities): {len(rm_details)}")
    print(f"  - Contiguity Errors (UTPs): {len(contiguity_errors)}")
    print(f"  - Contiguity Errors (Municipalities): {len(contiguity_details)}")
    print(f"  - Total UTPs with Errors: {len(rm_errors) + len(contiguity_errors)}")
    print(f"  - Total Municipalities Affected: {len(rm_details) + len(contiguity_details)}")


def main():
    """Main execution."""
    print("V7 VALIDATION SCRIPT")
    print("=" * 60)
    
    # Load data
    v7_df, rm_df, gdf = load_data()
    
    # Check RM integrity
    rm_errors, rm_details = check_rm_integrity(v7_df, rm_df)
    
    # Check contiguity
    contiguity_errors, contiguity_details = check_contiguity(v7_df, gdf)
    
    # Generate report
    project_root = Path(__file__).parent.parent
    output_path = project_root / "data" / "03_validation" / "v7_validation_report.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generate_report(rm_errors, rm_details, contiguity_errors, contiguity_details, output_path)
    
    print("\n" + "="*60)
    print("VALIDATION COMPLETE")
    print("="*60)


if __name__ == "__main__":
    main()
