import pandas as pd
import logging
import networkx as nx
from typing import Set, Dict, List

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# --- Mocks ---

class MockGraph:
    def __init__(self):
        # Initial State: Each in their own UTP except Salgueiro is target
        self.utp_seeds = {
            "UTP_BELEM": 2601607,
            "UTP_CABROBO": 2603009,
            "UTP_SALGUEIRO": 2612208
        }
        self.mun_utp = {
            2601607: "UTP_BELEM",
            2603009: "UTP_CABROBO",
            2612208: "UTP_SALGUEIRO"
        }
    
    def get_municipality_utp(self, mun_id):
        return self.mun_utp.get(mun_id)
    
    def move_municipality(self, mun_id, new_utp):
        self.mun_utp[mun_id] = new_utp
        logger.info(f"    [MOVE] {mun_id} moved to {new_utp}")

class TestSedeConsolidator:
    def __init__(self, graph):
        self.graph = graph
        self.logger = logger
        # Mock Regic Rank
        self.regic_ranks = {
            'Centro Local': 11,
            'Centro de Zona B': 10,
            'Capital Regional C': 6
        }
    
    def _get_sede_score(self, row: Dict) -> int:
        score = 0
        if row.get('tem_aeroporto'): score += 1
        if "1 - Município Turístico" in str(row.get('turismo', '')): score += 1
        return score
    
    def _get_regic_rank(self, regic_val: str) -> int:
        return self.regic_ranks.get(regic_val, 99)

    def _filter_candidates(self, df: pd.DataFrame) -> List[Dict]:
        """
        Simplified logic - EXACTLY as implemented in the Refactor
        NO Lookahead.
        """
        candidates = []
        
        for _, row in df.iterrows():
            if not row['tem_alerta_dependencia']: continue
            
            sede_origem = row['cd_mun_sede']
            sede_origem_utp = self.graph.get_municipality_utp(sede_origem)
            
            # Destination logic
            sede_destino = row['principal_destino_cd']
            
            # Find current UTP of destination
            utp_destino = self.graph.get_municipality_utp(sede_destino)
            
            if sede_origem_utp == utp_destino:
                continue # Already in same UTP
                
            # Find Sede of Destination UTP
            sede_utp_destino = self.graph.utp_seeds.get(utp_destino)
            
            # Get metrics for Destination Sede (Mock lookup)
            dest_row = df[df['cd_mun_sede'] == sede_utp_destino]
            if dest_row.empty: continue
            dest_row = dest_row.iloc[0]
            
            score_origin = self._get_sede_score(row)
            score_dest = self._get_sede_score(dest_row)
            
            is_candidate = False
            reason = ""
            
            # Logic: Strictly Origin vs Destination Sede
            if score_origin == 0:
                if score_dest == 0:
                    rank_orig = self._get_regic_rank(row['regic'])
                    rank_dest = self._get_regic_rank(dest_row['regic'])
                    if rank_dest < rank_orig:
                        is_candidate = True
                        reason = "Regic Improvement"
                    else:
                        reason = "Rejected: No improvement"
                elif score_dest > 0:
                    is_candidate = True
                    reason = "Score Improvement"
            
            if is_candidate:
                candidates.append({
                     'sede_origem': sede_origem,
                     'sede_destino': sede_destino, # Original Dest (Cabrobó)
                     'nm_origem': row['nm_sede'],
                     'nm_destino': dest_row['nm_sede'], # Actual Sede of UTP (Could be Salgueiro)
                     'utp_origem': sede_origem_utp,
                     'utp_destino': utp_destino, # Target UTP
                     'reason': reason
                })
        return candidates

    def run_simulation(self, df_metrics):
        print("\n--- Simulation Start ---")
        
        for iteration in range(1, 4):
            print(f"\nPass {iteration}")
            candidates = self._filter_candidates(df_metrics)
            
            print(f"  Candidates Found: {len(candidates)}")
            
            if not candidates:
                print("  Converged.")
                break
                
            # Topological Sort Logic
            move_graph = nx.DiGraph()
            cand_map = {}
            for c in candidates:
                u = c['sede_origem']
                v = c['sede_destino']
                # Edge v -> u to process v first
                move_graph.add_edge(v, u) 
                cand_map[u] = c
                
            try:
                ordered_sedes = list(nx.topological_sort(move_graph))
            except:
                ordered_sedes = list(move_graph.nodes())
            
            print(f"  Processing Order: {[id for id in ordered_sedes if id in cand_map]}")

            for sede_id in ordered_sedes:
                if sede_id not in cand_map: continue
                cand = cand_map[sede_id]
                
                # Check current status dynamic check
                curr_utp = self.graph.get_municipality_utp(sede_id)
                target_utp = cand['utp_destino'] # Based on start of pass
                
                # RE-CHECK Target UTP for the destination municipality 
                # (Crucial for Belém -> Cabrobó case)
                actual_dest_utp = self.graph.get_municipality_utp(cand['sede_destino'])
                
                if actual_dest_utp != target_utp:
                    print(f"    [DYN] Target UTP update for {cand['nm_origem']}: {target_utp} -> {actual_dest_utp}")
                    target_utp = actual_dest_utp
                
                if curr_utp == target_utp:
                    print(f"    Already in target UTP. Skipping.")
                    continue
                    
                # Re-Evaluate? In the real code we just trust the move if topology is right
                # Or we could re-evaluate scores. 
                # Here we simulate the Blind Move based on the Candidate Acceptance earlier, 
                # OR we implement the dynamic re-check.
                # Since we stripped the Lookahead, the logic accepted Belém based on Cabrobó's OLD status?
                #
                # WAIT! If Belém -> Cabrobó was ACCEPTED in _filter, it meant Cabrobó was BETTER.
                # But Cabrobó (Score 0) is NOT better than Belém (Score 0).
                # So Belém would initially be REJECTED in Pass 1.
                # Cabrobó -> Salgueiro is ACCEPTED.
                
                # Let's see what happens here.
                
                self.graph.move_municipality(sede_id, target_utp)

# --- Data Setup ---
# Belém -> Cabrobó -> Salgueiro

data = [
    {
        'cd_mun_sede': 2601607, 'nm_sede': 'Belém', 
        'tem_alerta_dependencia': True, 'principal_destino_cd': 2603009, # Cabrobó
        'tem_aeroporto': False, 'turismo': '', 'regic': 'Centro de Zona B'
    },
    {
        'cd_mun_sede': 2603009, 'nm_sede': 'Cabrobó', 
        'tem_alerta_dependencia': True, 'principal_destino_cd': 2612208, # Salgueiro
        'tem_aeroporto': False, 'turismo': '', 'regic': 'Centro de Zona B' 
    },
    {
        'cd_mun_sede': 2612208, 'nm_sede': 'Salgueiro', 
        'tem_alerta_dependencia': False, 'principal_destino_cd': None,
        'tem_aeroporto': False, 'turismo': '', 'regic': 'Capital Regional C' # Better/Score 2 implied
    }
]

df_metrics = pd.DataFrame(data)

# Run
graph = MockGraph()
consolidator = TestSedeConsolidator(graph)
consolidator.run_simulation(df_metrics)

# Validate
print("\n--- Validation ---")
final_utp_belem = graph.get_municipality_utp(2601607)
final_utp_cabrobo = graph.get_municipality_utp(2603009)
final_utp_salgueiro = graph.get_municipality_utp(2612208)

print(f"Belém Final UTP: {final_utp_belem}")
print(f"Cabrobó Final UTP: {final_utp_cabrobo}")
print(f"Salgueiro Final UTP: {final_utp_salgueiro}")

if final_utp_belem == "UTP_SALGUEIRO" and final_utp_cabrobo == "UTP_SALGUEIRO":
    print("SUCCESS: Both consolidated to Salgueiro.")
else:
    print("FAILURE: Did not consolidate correctly.")
