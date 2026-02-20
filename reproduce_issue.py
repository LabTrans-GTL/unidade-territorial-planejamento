
import pandas as pd
import logging
import sys
from unittest.mock import MagicMock
from src.pipeline.sede_consolidator import SedeConsolidator

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

def reproduce():
    print("--- Starting Reproduction Script ---")
    
    # Mock Graph and Validator
    mock_graph = MagicMock()
    mock_validator = MagicMock()
    mock_analyzer = MagicMock()
    
    # Mock data
    # 409: Belém do São Francisco (Origin) -> 366: Cabrobó (Intermediate) -> 675: Salgueiro (Final)
    
    # DataFrame Metrics
    data = [
        {
            'cd_mun_sede': 409, 'nm_sede': 'Belém do São Francisco', 'utp_id': '409',
            'principal_destino_cd': 366, 'tempo_ate_destino_h': 1.0, 
            'regiao_metropolitana': '', 'tem_alerta_dependencia': True,
            'tem_aeroporto': False, 'turismo': '3 - Apoio', 'regic': 'Centro Local'
        },
        {
            'cd_mun_sede': 366, 'nm_sede': 'Cabrobó', 'utp_id': '366',
            'principal_destino_cd': 675, 'tempo_ate_destino_h': 0.8, 
            'regiao_metropolitana': '', 'tem_alerta_dependencia': True,
            'tem_aeroporto': False, 'turismo': '3 - Apoio', 'regic': 'Centro Local'
        },
        {
            'cd_mun_sede': 675, 'nm_sede': 'Salgueiro', 'utp_id': '675',
            'principal_destino_cd': 999, 'tempo_ate_destino_h': 2.5, # Doesn't move
            'regiao_metropolitana': '', 'tem_alerta_dependencia': False,
            'tem_aeroporto': False, 'turismo': '3 - Apoio', 'regic': 'Centro Sub-Regional B'
        }
    ]
    df_metrics = pd.DataFrame(data)
    
    consolidator = SedeConsolidator(mock_graph, mock_validator, mock_analyzer)
    
    # Mock helper methods to avoid full graph dependency
    consolidator._build_adjacency_graph = MagicMock()
    consolidator.adjacency_graph = MagicMock()
    
    # Mock UTP lookup
    def get_utp(mun_id):
        return str(mun_id) # Simplify: UTP ID = Sede ID for this test
    mock_graph.get_municipality_utp.side_effect = get_utp
    
    # Mock Adjacency Validation (Always True for this test)
    consolidator._validate_utp_adjacency = MagicMock(return_value=True)
    
    # Mock Graph UTP Seeds for "Sede destino não encontrada" check
    mock_graph.utp_seeds = {'409': 409, '366': 366, '675': 675}
    
    print("\n1. Running Filter Candidates...")
    candidates = consolidator._filter_candidates(df_metrics)
    
    print(f"Candidates found: {len(candidates)}")
    for c in candidates:
        print(f"  Approved: {c['sede_origem']} -> {c['sede_destino']}")
        
    print(f"Rejected count: {len(consolidator.rejected_candidates)}")
    for r in consolidator.rejected_candidates:
        print(f"  Rejected: {r['sede_origem']} -> {r['sede_destino']} Reason: {r['motivo_rejeicao']}")

    print("\n2. Running Transitive Consolidation...")
    candidates = consolidator._apply_transitive_consolidation(candidates, df_metrics)
    
    print(f"\nFinal Candidates: {len(candidates)}")
    found = False
    for c in candidates:
        print(f"  {c['sede_origem']} -> {c['sede_destino']} (Transitive: {c.get('transitive', False)})")
        if str(c['sede_origem']) == '409' and str(c['sede_destino']) == '366':
            found = True
            
    if found:
        print("\nSUCCESS: 409 -> 366 was approved via transitive rule!")
    else:
        print("\nFAILURE: 409 -> 366 was NOT approved.")

if __name__ == "__main__":
    reproduce()
