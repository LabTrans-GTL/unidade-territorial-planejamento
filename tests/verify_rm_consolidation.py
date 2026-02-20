
import sys
import os
import pandas as pd
from unittest.mock import MagicMock

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.pipeline.sede_consolidator import SedeConsolidator

def test_rm_logic():
    print("--- Testing RM Logic ---")
    
    # Mock dependencies
    graph = MagicMock()
    # Mock utp_seeds: utp_id -> sede_id
    graph.utp_seeds = {
        '100': 2506905, # Itabaiana
        '200': 2507507  # Joao Pessoa
    }
    graph.get_municipality_utp.side_effect = lambda x: '100' if x == 2506905 else ('200' if x == 2507507 else None)
    
    validator = MagicMock()
    analyzer = MagicMock()
    
    consolidator = SedeConsolidator(graph, validator, analyzer)
    
    # Case 1: Sede Consolidation Logic
    print("\n[Case 1] Sede Consolidation Logic (Itabaiana -> Joao Pessoa)")
    
    # Mock metrics dataframe row
    # Itabaiana: RM = None
    # Joao Pessoa: RM = 'RM de João Pessoa'
    
    row_itabaiana = {
        'cd_mun_sede': 2506905,
        'nm_sede': 'Itabaiana',
        'utp_id': '100',
        'regiao_metropolitana': None, # None/NaN
        'tem_alerta_dependencia': True, # Trigger consolidation
        'principal_destino_cd': 2507507,
        'principal_destino_nm': 'João Pessoa',
        'tempo_ate_destino_h': 1.5, # < 2h, Valid
        'tem_aeroporto': False,
        'turismo': '',
        'regic': 'Centro Local',
        'score': 0
    }
    
    row_joao_pessoa = {
        'cd_mun_sede': 2507507,
        'nm_sede': 'João Pessoa',
        'utp_id': '200',
        'regiao_metropolitana': 'RM de João Pessoa',
        'tem_alerta_dependencia': False,
        'principal_destino_cd': None,
        'tem_aeroporto': True,
        'turismo': '1 - Município Turístico',
        'regic': 'Capital Regional A',
        'score': 2
    }
    
    # Create DF
    df_metrics = pd.DataFrame([row_itabaiana, row_joao_pessoa])
    
    # Run _filter_candidates
    # Since _filter_candidates reads destination metrics from df_metrics lookup:
    candidates = consolidator._filter_candidates(df_metrics, original_sedes={2506905, 2507507})
    
    found = False
    for c in candidates:
        if c['sede_origem'] == 2506905 and c['sede_destino'] == 2507507:
            found = True
            print("  ❌ FAILURE: Candidate ACCEPTED despite RM mismatch (None vs RM JP).")
            break
            
    if not found:
        print("  ✅ SUCCESS: Candidate REJECTED correctly due to RM mismatch.")

    # Case 2: Orphan Cleanup Logic
    print("\n[Case 2] Orphan Cleanup Logic")
    
    # Setup Orphan state
    # Orphan Itabaiana (UTP 100 inactive/orphan) -> Target UTP 200 (Active)
    # Check if logic allows move
    
    # Mock helper method result (simulating internal logic if we can't call private methods easily)
    # But better to test normalized comparison logic if possible.
    # Since _process_orphans_recursive is complex and depends on graph state, 
    # we will focus on verifying if we can add a unit test for the RM normalization helper we plan to add.
    
    if hasattr(consolidator, '_get_rm_canonical'):
         print("  Testing helper method...")
         rm1 = consolidator._get_rm_canonical(None)
         rm2 = consolidator._get_rm_canonical('RM de João Pessoa')
         print(f"  RM(None) -> '{rm1}'")
         print(f"  RM(JP) -> '{rm2}'")
         if rm1 == rm2:
             print("  ❌ FAILURE: Normalized RMs match!")
         else:
             print("  ✅ SUCCESS: Normalized RMs differ.")
    else:
        print("  ℹ️ Helper method _get_rm_canonical not implemented yet.")

if __name__ == "__main__":
    test_rm_logic()
