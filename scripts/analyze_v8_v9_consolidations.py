#!/usr/bin/env python3
"""
V8-V9 Consolidation Analysis Script
====================================
Analyzes unitary UTPs in V8 (UTP_FINAL) and shows consolidation details from V9 pipeline.
Shows:
- Which unitary UTPs were consolidated
- Target UTP
- Consolidation reason (RM/Flow/REGIC)
- Flow values
- Detailed municipality information

Output: Excel file with detailed consolidation report.
"""

import pandas as pd
import json
from pathlib import Path
import sys
from collections import defaultdict

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def load_data():
    """Load all necessary datasets."""
    print("Loading datasets...")
    
    # Determine project root
    project_root = Path(__file__).parent.parent
    
    # V8 Base (UTP_FINAL.xlsx)
    v8_path = project_root / "data" / "01_raw" / "UTP_FINAL.xlsx"
    v8_df = pd.read_excel(v8_path)
    print(f"[OK] Loaded V8 (UTP_FINAL): {len(v8_df)} municipalities")
    
    # Consolidation Log
    log_path = project_root / "data" / "consolidation_log.json"
    with open(log_path, 'r', encoding='utf-8') as f:
        consolidation_log = json.load(f)
    print(f"[OK] Loaded consolidation log: {len(consolidation_log['consolidations'])} consolidations")
    
    # Consolidation Result (V9 mapping)
    result_path = project_root / "data" / "consolidation_result.json"
    with open(result_path, 'r', encoding='utf-8') as f:
        consolidation_result = json.load(f)
    print(f"[OK] Loaded consolidation result: {len(consolidation_result['utps_mapping'])} UTP mappings")
    
    return v8_df, consolidation_log, consolidation_result

def identify_unitary_utps(v8_df, utp_column='UTPs_PAN11'):
    """Identify unitary UTPs (single municipality) in V8."""
    print("\n" + "="*60)
    print("IDENTIFYING UNITARY UTPs IN V8")
    print("="*60)
    
    # Group by UTP and count municipalities
    utp_counts = v8_df.groupby(utp_column).size().reset_index(name='num_municipalities')
    
    # Filter unitary UTPs
    unitary_utps = utp_counts[utp_counts['num_municipalities'] == 1]
    
    print(f"Total UTPs in V8: {len(utp_counts)}")
    print(f"Unitary UTPs (1 municipality): {len(unitary_utps)}")
    print(f"Multi-municipality UTPs: {len(utp_counts) - len(unitary_utps)}")
    
    return unitary_utps[utp_column].tolist()

def analyze_consolidations(v8_df, consolidation_log, consolidation_result, unitary_utps, utp_column='UTPs_PAN11'):
    """Analyze consolidations for unitary UTPs."""
    print("\n" + "="*60)
    print("ANALYZING CONSOLIDATIONS")
    print("="*60)
    
    consolidations_data = []
    unitary_munis_data = []
    
    # Create mapping from UTP to municipalities
    # UTPs can be numeric or string (e.g., "NOVA6"), so we keep them as-is
    v8_df_copy = v8_df.copy()
    v8_df_copy[utp_column] = v8_df_copy[utp_column].astype(str)
    
    utp_to_munis = v8_df_copy.groupby(utp_column).agg({
        'CD_MUN': list,
        'NM_MUN': list,
        'SIGLA_UF': lambda x: x.iloc[0] if len(x) > 0 else 'N/A'
    }).to_dict('index')
    
    # Process each consolidation from log
    for cons in consolidation_log['consolidations']:
        source_utp = str(cons['source_utp'])
        target_utp = str(cons['target_utp'])
        
        # Check if source was unitary
        # Try to match as string first (handles both numeric and string IDs)
        was_unitary = False
        for utp_val in unitary_utps:
            if str(utp_val) == source_utp or (isinstance(utp_val, float) and abs(utp_val - float(source_utp if source_utp.replace('.', '', 1).isdigit() else -1)) < 0.01):
                was_unitary = True
                break
        
        # Get details
        details = cons.get('details', {})
        mun_id = details.get('mun_id', 'N/A')
        mun_nome = details.get('nm_mun', 'N/A')
        rm = details.get('rm', 'Sem RM')
        fluxo = details.get('viagens', 0)
        mun_destino = details.get('mun_destino', 'N/A')
        
        # Get target UTP info (using string key)
        target_munis = utp_to_munis.get(target_utp, {})
        target_muni_list = target_munis.get('NM_MUN', ['N/A'])
        target_uf = target_munis.get('SIGLA_UF', 'N/A')
        
        consolidations_data.append({
            'ID_Consolidação': cons['id'],
            'UTP_Origem': source_utp,
            'UTP_Destino': target_utp,
            'Era_Unitária': 'Sim' if was_unitary else 'Não',
            'Município_Código': mun_id,
            'Município_Nome': mun_nome,
            'UF_Origem': details.get('uf', 'N/A'),
            'Município_Destino': mun_destino,
            'UTP_Destino_Municípios': ', '.join(target_muni_list[:5]) + ('...' if len(target_muni_list) > 5 else ''),
            'UTP_Destino_Total_Munis': len(target_muni_list),
            'UF_Destino': target_uf,
            'Região_Metropolitana': rm,
            'Fluxo_Viagens': fluxo,
            'Motivo_Consolidação': cons['reason'],
            'Timestamp': cons['timestamp']
        })
        
        # Detailed unitary municipality data
        if was_unitary:
            unitary_munis_data.append({
                'UTP_Original_V8': source_utp,
                'UTP_Final_V9': target_utp,
                'Município_Código': mun_id,
                'Município_Nome': mun_nome,
                'UF': details.get('uf', 'N/A'),
                'Região_Metropolitana': rm,
                'Fluxo_para_Destino': fluxo,
                'Município_Destino_Código': mun_destino,
                'Motivo': cons['reason'],
                'Data_Consolidação': cons['timestamp']
            })
    
    print(f"Total consolidations: {len(consolidations_data)}")
    print(f"Unitary UTPs consolidated: {len(unitary_munis_data)}")
    
    return pd.DataFrame(consolidations_data), pd.DataFrame(unitary_munis_data)

def generate_summary(v8_df, unitary_utps, consolidations_df, utp_column='UTPs_PAN11'):
    """Generate summary statistics."""
    
    total_utps_v8 = v8_df[utp_column].nunique()
    total_unitary = len(unitary_utps)
    consolidated_unitary = consolidations_df[consolidations_df['Era_Unitária'] == 'Sim'].shape[0]
    remaining_unitary = total_unitary - consolidated_unitary
    
    summary = {
        'Métrica': [
            'Total de UTPs em V8',
            'UTPs Unitárias em V8',
            '% UTPs Unitárias',
            'UTPs Unitárias Consolidadas',
            'UTPs Unitárias Restantes',
            '% Consolidação de Unitárias',
            'Total de Consolidações',
            'Consolidações por RM + Fluxo',
            'Consolidações por Fluxo (Sem RM)',
            'Consolidações por REGIC'
        ],
        'Valor': [
            total_utps_v8,
            total_unitary,
            f"{(total_unitary / total_utps_v8 * 100):.2f}%",
            consolidated_unitary,
            remaining_unitary,
            f"{(consolidated_unitary / total_unitary * 100):.2f}%" if total_unitary > 0 else "0%",
            len(consolidations_df),
            len(consolidations_df[consolidations_df['Motivo_Consolidação'].str.contains('Com RM', na=False)]),
            len(consolidations_df[consolidations_df['Motivo_Consolidação'].str.contains('Sem RM', na=False)]),
            len(consolidations_df[consolidations_df['Motivo_Consolidação'].str.contains('REGIC', na=False)])
        ]
    }
    
    return pd.DataFrame(summary)

def generate_report(v8_df, unitary_utps, consolidations_df, unitary_munis_df, output_path):
    """Generate comprehensive Excel report."""
    print("\n" + "="*60)
    print("GENERATING REPORT")
    print("="*60)
    
    summary_df = generate_summary(v8_df, unitary_utps, consolidations_df)
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Summary sheet
        summary_df.to_excel(writer, sheet_name='Resumo', index=False)
        
        # All consolidations
        if not consolidations_df.empty:
            consolidations_df.to_excel(writer, sheet_name='Todas_Consolidações', index=False)
        else:
            pd.DataFrame({'Mensagem': ['Nenhuma consolidação encontrada']}).to_excel(
                writer, sheet_name='Todas_Consolidações', index=False
            )
        
        # Unitary UTPs consolidations (detailed)
        if not unitary_munis_df.empty:
            unitary_munis_df.to_excel(writer, sheet_name='Unitárias_Consolidadas', index=False)
        else:
            pd.DataFrame({'Mensagem': ['Nenhuma UTP unitária consolidada']}).to_excel(
                writer, sheet_name='Unitárias_Consolidadas', index=False
            )
        
        # Consolidations by reason
        if not consolidations_df.empty:
            by_reason = consolidations_df.groupby('Motivo_Consolidação').agg({
                'ID_Consolidação': 'count',
                'Fluxo_Viagens': 'mean'
            }).reset_index()
            by_reason.columns = ['Motivo', 'Quantidade', 'Fluxo_Médio']
            by_reason.to_excel(writer, sheet_name='Por_Motivo', index=False)
    
    print(f"[OK] Report saved to: {output_path}")
    print(f"\nReport Contents:")
    print(f"  - Summary statistics")
    print(f"  - All consolidations: {len(consolidations_df)} records")
    print(f"  - Unitary UTPs consolidated: {len(unitary_munis_df)} records")
    print(f"  - Consolidations by reason breakdown")

def main():
    """Main execution."""
    print("V8-V9 CONSOLIDATION ANALYSIS")
    print("=" * 60)
    
    # Load data
    v8_df, consolidation_log, consolidation_result = load_data()
    
    # Identify unitary UTPs in V8
    unitary_utps = identify_unitary_utps(v8_df, utp_column='UTPs_PAN11')
    
    # Analyze consolidations
    consolidations_df, unitary_munis_df = analyze_consolidations(
        v8_df, consolidation_log, consolidation_result, unitary_utps, utp_column='UTPs_PAN11'
    )
    
    # Generate report
    project_root = Path(__file__).parent.parent
    output_path = project_root / "data" / "03_validation" / "v8_v9_consolidation_analysis.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generate_report(v8_df, unitary_utps, consolidations_df, unitary_munis_df, output_path)
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
    print(f"\nKey Findings:")
    print(f"  - Unitary UTPs in V8: {len(unitary_utps)}")
    print(f"  - Consolidations performed: {len(consolidations_df)}")
    print(f"  - Unitary UTPs consolidated: {len(unitary_munis_df)}")

if __name__ == "__main__":
    main()
