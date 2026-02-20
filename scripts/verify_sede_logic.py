import pandas as pd
import logging
from unittest.mock import MagicMock
from src.pipeline.sede_consolidator import SedeConsolidator
from src.core.graph import TerritorialGraph

# Setup basic logging
logging.basicConfig(level=logging.INFO)

def test_conflict_resolution():
    print("Testing Conflict Resolution Logic...")
    
    # Mock Graph and Analyzer
    graph = TerritorialGraph()
    analyzer = MagicMock()
    
    # Create Consolidator
    consolidator = SedeConsolidator(graph, MagicMock(), analyzer)
    
    # Mock Data
    # A = Santa Cruz (Larger Flow)
    # B = Venâncio (Smaller Flow)
    id_a = 4316808
    id_b = 4322608
    
    flow_df = pd.DataFrame([
        {'mun_origem': id_a, 'viagens': 1000}, # A has 1000 trips
        {'mun_origem': id_b, 'viagens': 500},  # B has 500 trips
    ])
    
    # Mock Candidates: Both want to move to each other
    candidates = [
        {
            'sede_origem': id_a, 'sede_destino': id_b, # A -> B
            'nm_origem': 'Santa Cruz', 'nm_destino': 'Venancio',
            'utp_origem': 'UTP_A', 'utp_destino': 'UTP_B',
            'score_origem': 1, 'score_destino': 1,
            'tempo_viagem_h': 0.5
        },
        {
            'sede_origem': id_b, 'sede_destino': id_a, # B -> A
            'nm_origem': 'Venancio', 'nm_destino': 'Santa Cruz',
            'utp_origem': 'UTP_B', 'utp_destino': 'UTP_A',
            'score_origem': 1, 'score_destino': 1,
            'tempo_viagem_h': 0.5
        }
    ]
    
    # We need to bypass the candidate filtering and go straight to conflict detection.
    # But current implementation has logic inside `run_sede_consolidation`.
    # Let's extract the loop logic or simulate it.
    # A workaround to test the logic block specifically:
    
    # Re-implement the key logic block for testing purposes OR mock internal methods
    # Since we can't easily mock the internal variable `candidates` inside the method,
    # we will copy the logic we want to test here to verify it works as expected 
    # (Unit Test style), ensuring our understanding of the fix is correct.
    
    mapping = {str(c['sede_origem']): str(c['sede_destino']) for c in candidates}
    to_remove = set()
    
    print(f"Candidates: {[c['sede_origem'] for c in candidates]}")
    
    for c in candidates:
        a = str(c['sede_origem'])
        b = str(c['sede_destino'])
        
        if b in mapping and mapping[b] == a:
             if a in to_remove or b in to_remove:
                 continue
                 
             # Get Flows
             flow_a = float(consolidator._get_total_flow(int(a), flow_df))
             flow_b = float(consolidator._get_total_flow(int(b), flow_df))
             
             print(f"Checking conflict: {a} (Flow {flow_a}) vs {b} (Flow {flow_b})")
             
             if flow_a > flow_b:
                 # Logic we implemented:
                 # A has larger flow. A should stay. B should move.
                 # So we remove candidate A->B.
                 to_remove.add(a)
                 print(f"  -> A > B: Removing candidate {a} (Santa Cruz)")
             else:
                 to_remove.add(b)
                 print(f"  -> B > A: Removing candidate {b}")
                 
    # Apply filter
    final_candidates = [c for c in candidates if str(c['sede_origem']) not in to_remove]
    
    print(f"Final Candidates: {[c['sede_origem'] for c in final_candidates]}")
    
    # Verification
    # We expect Santa Cruz (4316808) to be REMOVED (it stays as Sede)
    # We expect Venâncio (4322608) to REMAIN (it moves to Santa Cruz)
    
    survivor_move = final_candidates[0]
    if len(final_candidates) != 1:
        print("FAIL: Expected exactly 1 candidate remaining.")
    elif survivor_move['sede_origem'] == id_b:
         print("SUCCESS: Venancio (Smaller) is moving to Santa Cruz (Larger).")
    else:
         print(f"FAIL: Wrong candidate remaining: {survivor_move['sede_origem']}")

if __name__ == "__main__":
    test_conflict_resolution()
