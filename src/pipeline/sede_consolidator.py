import logging
import pandas as pd
import geopandas as gpd
import networkx as nx
import json
from pathlib import Path
from typing import List, Dict, Set, Tuple, Optional
from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator
from src.interface.consolidation_manager import ConsolidationManager
from src.pipeline.sede_analyzer import SedeAnalyzer

class SedeConsolidator:
    """
    Step 6: Consolidation of Sedes based on functional dependency (2h distance + flow)
    and infrastructure scoring (Airport + Tourism).
    """

    def __init__(self, graph: TerritorialGraph, validator: TerritorialValidator, sede_analyzer: SedeAnalyzer):
        self.graph = graph
        self.validator = validator
        self.analyzer = sede_analyzer
        self.logger = logging.getLogger("GeoValida.SedeConsolidator")
        self.consolidation_manager = ConsolidationManager()
        self.adjacency_graph = None  # Will be built from GDF
        
        # Set data directory for exports
        from pathlib import Path
        self.data_dir = Path(__file__).parent.parent.parent / "data" / "03_processed"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
    def _build_adjacency_graph(self, gdf: gpd.GeoDataFrame):
        """Builds a NetworkX graph representing spatial adjacency of municipalities."""
        self.logger.info("Building spatial adjacency graph for pathfinding...")
        self.adjacency_graph = nx.Graph()
        
        if gdf is None or gdf.empty:
            return

        # Ensure we have geometries
        gdf_valid = gdf[gdf.geometry.notna()]
        
        # Use touching/intersection with small buffer to find neighbors
        # Optimized approach: sjoin
        # Project to estimate buffer if needed, but assuming GDF is suitable
        # Using a small buffer helps with imperfect topologies
        
        # Project to metric CRS for buffering (EPSG:3857 is fast and sufficient for adjacency)
        gdf_valid_metric = gdf_valid.to_crs(epsg=3857)
        
        # Use a small buffer (e.g., 100 meters) to handle topology gaps
        buffer_val = 100.0 
        gdf_buff = gdf_valid_metric.copy()
        gdf_buff['geometry'] = gdf_buff.geometry.buffer(buffer_val)
        
        # Self-join to find neighbors
        # Use simple index-based join on metric/buffered geometries
        sjoin = gpd.sjoin(gdf_buff, gdf_buff, how='inner', predicate='intersects')
        
        edges = []
        for idx, row in sjoin.iterrows():
             left = int(row['CD_MUN_left'])
             right = int(row['CD_MUN_right'])
             if left != right:
                 edges.append((left, right))
                 
        self.adjacency_graph.add_edges_from(edges)
        self.logger.info(f"Adjacency graph built: {self.adjacency_graph.number_of_nodes()} nodes, {self.adjacency_graph.number_of_edges()} edges.")


    def _get_regic_rank(self, regic_val: str) -> int:
        """
        Returns numeric rank for REGIC description. Lower is MORE important/relevant.
        Based on standard IBGE hierarchy.
        """
        if not regic_val: return 99
        
        r = str(regic_val).lower().strip()
        
        # Hierarchy Definition (1 = Most Relevant)
        mapping = {
            'grande metrópole nacional': 1,
            'metrópole nacional': 2,
            'metrópole': 3,
            'capital regional a': 4,
            'capital regional b': 5,
            'capital regional c': 6,
            'centro sub-regional a': 7,
            'centro sub-regional b': 8,
            'centro de zona a': 9,
            'centro de zona b': 10,
            'centro local': 11
        }
        
        for k, v in mapping.items():
            if k in r:
                return v
                
        return 99 # Unknown

    def _get_sede_score(self, sede_metrics: Dict) -> int:
        """Calculates score (0-2) based on Airport and Tourism."""
        score = 0
        if sede_metrics.get('tem_aeroporto'):
            score += 1
        
        # Check tourism class.
        # User defined: "1 - Município Turístico"
        turismo = str(sede_metrics.get('turismo', '')).strip()
        if "1 - Município Turístico" in turismo: 
             score += 1
             
        return score

    def _validate_utp_adjacency(self, utp_origem: str, utp_destino: str) -> bool:
        """
        Validates if two UTPs are adjacent (share a border).
        Required to maintain territorial continuity.
        
        IMPORTANT: Adjacency is between UTPs, not between sedes.
        Checks if ANY municipality from origin UTP is adjacent to 
        ANY municipality from destination UTP.
        """
        if self.adjacency_graph is None:
            self.logger.warning("Adjacency graph not built yet!")
            return False
        
        # Get all municipalities in each UTP
        muns_origem = []
        muns_destino = []
        
        for node, data in self.graph.hierarchy.nodes(data=True):
            if data.get('type') == 'municipality':
                node_utp = self.graph.get_municipality_utp(node)
                if node_utp == utp_origem:
                    muns_origem.append(node)
                elif node_utp == utp_destino:
                    muns_destino.append(node)
        
        # Check if any municipality from origem is adjacent to any from destino
        for mun_orig in muns_origem:
            if mun_orig in self.adjacency_graph:
                neighbors = self.adjacency_graph[mun_orig]
                for mun_dest in muns_destino:
                    if mun_dest in neighbors:
                        return True
        
        return False

    def _filter_candidates(self, df_metrics: pd.DataFrame) -> List[Dict]:
        """
        Filters potential consolidation candidates based on simplified rules:
        1. Sede must have main flow to another sede
        2. Travel time <= 2 hours
        3. RM rule: same RM OR both without RM
        4. UTPs must be adjacent
        5. Destination score >= Origin score
        """
        candidates = []
        rejected = []
        
        # Track filtering statistics
        filter_stats = {
            'total_checked': 0,
            'no_alert': 0,
            'invalid_destination': 0,
            'same_utp': 0,
            'travel_time_exceeded': 0,
            'rm_mismatch': 0,
            'not_adjacent': 0,
            'rejected_by_score': 0,
            'accepted': 0
        }
        
        for _, row in df_metrics.iterrows():
            filter_stats['total_checked'] += 1
            
            # Must have dependency alert
            if not row['tem_alerta_dependencia']:
                filter_stats['no_alert'] += 1
                continue
                
            sede_origem = row['cd_mun_sede']
            utp_origem = row['utp_id']
            sede_destino = row['principal_destino_cd']
            
            # Validate destination exists
            if pd.isna(sede_destino):
                filter_stats['invalid_destination'] += 1
                continue
            
            utp_destino = self.graph.get_municipality_utp(sede_destino)
            
            # Check if same UTP (Ensure strict string comparison)
            if str(utp_origem) == str(utp_destino):
                filter_stats['same_utp'] += 1
                continue

            # RULE 1: Travel time <= 2 hours
            tempo_viagem = row.get('tempo_ate_destino_h')
            if tempo_viagem is None or tempo_viagem > 2.0:
                filter_stats['travel_time_exceeded'] += 1
                rejected.append({
                    'sede_origem': sede_origem,
                    'nm_origem': row['nm_sede'],
                    'utp_origem': utp_origem,
                    'sede_destino': sede_utp_destino if sede_utp_destino else sede_destino,
                    'utp_destino': utp_destino,
                    'motivo_rejeicao': f'Tempo de viagem {tempo_viagem:.2f}h > 2h'
                })
                continue

            # RULE 2: RM consistency
            rm_origem = str(row.get('regiao_metropolitana', '')).strip()
            if rm_origem.lower() == 'nan':
                rm_origem = ''
            
            # Get RM of destination sede
            sede_utp_destino = self.graph.utp_seeds.get(utp_destino)
            if not sede_utp_destino:
                filter_stats['invalid_destination'] += 1
                rejected.append({
                    'sede_origem': sede_origem,
                    'nm_origem': row['nm_sede'],
                    'utp_origem': utp_origem,
                    'sede_destino': sede_utp_destino if sede_utp_destino else sede_destino,
                    'utp_destino': utp_destino,
                    'motivo_rejeicao': 'UTP destino não tem sede ativa'
                })
                continue
            
            dest_row = df_metrics[df_metrics['cd_mun_sede'] == sede_utp_destino]
            if dest_row.empty:
                filter_stats['invalid_destination'] += 1
                rejected.append({
                    'sede_origem': sede_origem,
                    'nm_origem': row['nm_sede'],
                    'utp_origem': utp_origem,
                    'sede_destino': sede_utp_destino if sede_utp_destino else sede_destino,
                    'utp_destino': utp_destino,
                    'motivo_rejeicao': 'Sede destino não encontrada em métricas'
                })
                continue
            
            dest_row = dest_row.iloc[0]
            rm_destino = str(dest_row.get('regiao_metropolitana', '')).strip()
            if rm_destino.lower() == 'nan':
                rm_destino = ''

            # RM Rule: Both without RM OR same RM
            if rm_origem or rm_destino:  # At least one has RM
                if rm_origem != rm_destino:
                    filter_stats['rm_mismatch'] += 1
                    rejected.append({
                        'sede_origem': sede_origem,
                        'nm_origem': row['nm_sede'],
                        'utp_origem': utp_origem,
                        'sede_destino': sede_utp_destino if sede_utp_destino else sede_destino,
                        'utp_destino': utp_destino,
                        'rm_origem': rm_origem,
                        'rm_destino': rm_destino,
                        'motivo_rejeicao': f"RM incompatível: '{rm_origem}' != '{rm_destino}'"
                    })
                    continue

            # RULE 3: UTP Adjacency
            if not self._validate_utp_adjacency(utp_origem, utp_destino):
                filter_stats['not_adjacent'] += 1
                rejected.append({
                    'sede_origem': sede_origem,
                    'nm_origem': row['nm_sede'],
                    'utp_origem': utp_origem,
                    'sede_destino': sede_destino,
                    'utp_destino': utp_destino,
                    'motivo_rejeicao': 'UTPs não são adjacentes'
                })
                continue
            
            # RULE 4: Score validation
            # STRICT: Only score=0 origins can consolidate
            score_origem = self._get_sede_score(row)
            score_destino = self._get_sede_score(dest_row)
            
            # Origin MUST have score = 0 (no airport, no tourism)
            if score_origem != 0:
                filter_stats['rejected_by_score'] += 1
                rejected.append({
                    'sede_origem': sede_origem,
                    'nm_origem': row['nm_sede'],
                    'utp_origem': utp_origem,
                    'sede_destino': sede_destino,
                    'nm_destino': dest_row['nm_sede'],
                    'utp_destino': utp_destino,
                    'score_origem': score_origem,
                    'score_destino': score_destino,
                    'tempo_viagem_h': tempo_viagem,
                    'rm_origem': rm_origem,
                    'rm_destino': rm_destino,
                    'motivo_rejeicao': f'Origem tem score {score_origem}, mas só score=0 pode consolidar'
                })
                continue
            
            # If destination has score >= 1 (airport or tourism): APPROVED
            if score_destino >= 1:
                # Will be approved below
                pass
            # If both are score=0: use REGIC as tiebreaker
            elif score_destino == 0:
                regic_origem = self._get_regic_rank(row.get('regic', ''))
                regic_destino = self._get_regic_rank(dest_row.get('regic', ''))
                
                # Destination must have BETTER (lower) REGIC rank
                if regic_destino >= regic_origem:
                    filter_stats['rejected_by_score'] += 1
                    # Store the UTP seed (sede_utp_destino) as the recorded destination
                    # so transitive checks can find approved chains that start at that sede.
                    rejected.append({
                        'sede_origem': sede_origem,
                        'nm_origem': row['nm_sede'],
                        'utp_origem': utp_origem,
                        # record the actual sede (seed) of the destination UTP
                        'sede_destino': sede_utp_destino if sede_utp_destino else sede_destino,
                        'nm_destino': dest_row['nm_sede'],
                        'utp_destino': utp_destino,
                        'score_origem': score_origem,
                        'score_destino': score_destino,
                        'regic_origem': row.get('regic', ''),
                        'regic_destino': dest_row.get('regic', ''),
                        'regic_rank_origem': regic_origem,
                        'regic_rank_destino': regic_destino,
                        'tempo_viagem_h': tempo_viagem,
                        'rm_origem': rm_origem,
                        'rm_destino': rm_destino,
                        'motivo_rejeicao': f'Ambos score=0, mas destino REGIC pior/igual (rank {regic_destino} >= {regic_origem})'
                    })
                    continue
            
            # APPROVED!
            filter_stats['accepted'] += 1
            candidates.append({
                'sede_origem': sede_origem,
                'nm_origem': row['nm_sede'],
                'utp_origem': utp_origem,
                # record the actual seed (sede) of the destination UTP so chains resolve correctly
                'sede_destino': sede_utp_destino,
                'nm_destino': dest_row['nm_sede'],
                'utp_destino': utp_destino,
                'score_origem': score_origem,
                'score_destino': score_destino,
                'tempo_viagem_h': tempo_viagem,
                'rm_origem': rm_origem,
                'rm_destino': rm_destino,
                'motivo_rejeicao': ''
            })
        
        # Log statistics
        self.logger.info(f"\n📈 Candidate Filtering Statistics:")
        self.logger.info(f"   Total sedes checked: {filter_stats['total_checked']}")
        self.logger.info(f"   Filtered out - No dependency alert: {filter_stats['no_alert']}")
        self.logger.info(f"   Filtered out - Invalid destination: {filter_stats['invalid_destination']}")
        self.logger.info(f"   Filtered out - Same UTP: {filter_stats['same_utp']}")
        self.logger.info(f"   Filtered out - Travel time > 2h: {filter_stats['travel_time_exceeded']}")
        self.logger.info(f"   Filtered out - RM mismatch: {filter_stats['rm_mismatch']}")
        self.logger.info(f"   Filtered out - UTPs not adjacent: {filter_stats['not_adjacent']}")
        self.logger.info(f"   Filtered out - Rejected by score: {filter_stats['rejected_by_score']}")
        self.logger.info(f"   ✅ Accepted candidates: {filter_stats['accepted']}")
        
        # Store rejected for CSV export
        self.rejected_candidates = rejected
        
        return candidates
    
    def _find_final_destination(self, sede_id: str, approved_map: Dict[str, Dict]) -> Tuple[str, List[str]]:
        """
        Resolves the complete transitive chain to find the final destination.
        
        Args:
            sede_id: Starting sede ID
            approved_map: Dict mapping sede_origem -> candidate dict
        
        Returns:
            Tuple of (final_destination_sede_id, chain_of_sedes)
        
        Example:
            If A -> B and B -> C and C -> D, returns (D, [A, B, C, D])
        """
        chain = [sede_id]
        current = sede_id
        visited = {sede_id}  # Prevent infinite loops
        
        # Follow the chain until we reach a sede that doesn't move
        while current in approved_map:
            next_sede = str(approved_map[current]['sede_destino'])
            
            # Check for circular reference
            if next_sede in visited:
                self.logger.warning(f"⚠️  Circular reference detected in chain: {chain} -> {next_sede}")
                break
            
            chain.append(next_sede)
            visited.add(next_sede)
            current = next_sede
        
        final_destination = chain[-1]
        return final_destination, chain
    
    def _has_flow_or_time(self, origin: int, dest: int, flow_df: pd.DataFrame, max_time: float = 2.0) -> bool:
        """
        Checks whether there is any recorded flow or travel time between origin and dest.
        Returns True if a flow record exists (viagens>0) or a travel time <= max_time is recorded.
        """
        if flow_df is None:
            return False

        try:
            rows = flow_df[(flow_df['mun_origem'].astype(int) == int(origin)) & (flow_df['mun_destino'].astype(int) == int(dest))]
        except Exception:
            # If columns are missing or types unexpected
            try:
                rows = flow_df[(flow_df['origem'].astype(int) == int(origin)) & (flow_df['destino'].astype(int) == int(dest))]
            except Exception:
                return False

        if rows is None or rows.empty:
            return False

        # Check explicit travel time columns if present
        for col in ['tempo_viagem', 'tempo_ate_destino_h', 'tempo_h', 'tempo']:
            if col in rows.columns:
                try:
                    vals = rows[col].astype(str).str.replace(',', '.').astype(float)
                    if (vals <= max_time).any():
                        return True
                except Exception:
                    # ignore parse errors
                    pass

        # Require explicit travel time <= max_time. Do NOT accept mere presence of flow records
        # as proof of acceptable travel time — that would allow transitive moves without time constraint.
        return False


    def _apply_transitive_consolidation(self, candidates: List[Dict], df_metrics: pd.DataFrame, flow_df: pd.DataFrame = None) -> List[Dict]:
        """
        Applies transitive consolidation rule:
        If A -> B was rejected due to tie (score=0 both, REGIC equal),
        but B -> C (or B -> C -> D -> ...) was approved (final destination has better infrastructure),
        then APPROVE A -> B (because B is validated by its approval to final destination).
        
        IMPORTANT: Score comparison is made between A and the FINAL destination (C), not intermediate (B).
        
        Example: 409 (Belém) -> 366 (Cabrobó) rejected (tie)
                 366 (Cabrobó) -> 675 (Serra Talhada) approved
                 => Check: score(409) vs score(675) - if 675 is better, APPROVE: 409 -> 366
        
        This allows the natural consolidation chain: 409→366→675
        """
        self.logger.info("\n🔗 Checking for transitive consolidation opportunities...")
        
        # Build mapping of approved consolidations: sede_origem -> candidate
        approved_map = {str(c['sede_origem']): c for c in candidates}
        
        # Find rejected candidates due to tie (score=0 both, REGIC equal/worse)
        tie_rejected = [
            r for r in self.rejected_candidates 
            if 'Ambos score=0' in r.get('motivo_rejeicao', '') and 
               'REGIC pior/igual' in r.get('motivo_rejeicao', '')
        ]
        
        new_approved_candidates = []
        transitive_count = 0
        rejected_to_remove = []
        # Specific (sede_b, sede_final) pairs whose B->C candidate must be cancelled
        # because a local A->B fallback was chosen (A has no flow to C).
        # Using pairs avoids collateral cancellation of unrelated chains that share sede_b.
        candidates_to_cancel: Set[Tuple[str, str]] = set()
        
        for rejected in tie_rejected:
            sede_a = str(rejected['sede_origem'])  # e.g., 409 (Belém)
            sede_b = str(rejected['sede_destino'])  # e.g., 366 (Cabrobó)
            utp_a = rejected['utp_origem']
            utp_b = rejected['utp_destino']
            
            # Check if B -> ... -> FINAL exists in approved candidates
            if sede_b in approved_map:
                # Find the complete chain and final destination
                sede_final, chain = self._find_final_destination(sede_b, approved_map)
                chain_str = ' -> '.join(chain)
                
                self.logger.info(f"   🔗 Transitive opportunity: {sede_a} -> {sede_b} (rejected/tie) + chain {chain_str} (approved)")
                
                # Get metrics for A (origin)
                row_a = df_metrics[df_metrics['cd_mun_sede'] == int(sede_a)]
                if row_a.empty:
                    self.logger.info(f"      ❌ Rejected: Sede A ({sede_a}) not found in metrics")
                    continue
                row_a = row_a.iloc[0]
                
                # Get metrics for B (intermediate)
                row_b = df_metrics[df_metrics['cd_mun_sede'] == int(sede_b)]
                if row_b.empty:
                    self.logger.info(f"      ❌ Rejected: Sede B ({sede_b}) not found in metrics")
                    continue
                row_b = row_b.iloc[0]
                
                # CRITICAL: Get metrics for FINAL destination (not intermediate B)
                row_final = df_metrics[df_metrics['cd_mun_sede'] == int(sede_final)]
                if row_final.empty:
                    self.logger.info(f"      ❌ Rejected: Final destination ({sede_final}) not found in metrics")
                    continue
                row_final = row_final.iloc[0]
                
                # Compare A with FINAL destination (not B)
                score_a = self._get_sede_score(row_a)
                score_b = self._get_sede_score(row_b)
                score_final = self._get_sede_score(row_final)
                
                # Validate: A can only move if FINAL destination has better score
                # Origin MUST have score = 0 (already validated in original filter)
                should_approve = False
                approval_reason = ""
                
                if score_final >= 1:
                    # Final destination has infrastructure (airport or tourism)
                    should_approve = True
                    approval_reason = f"Final destination {sede_final} has score {score_final}"
                elif score_final == 0 and score_a == 0:
                    # Both score=0: use REGIC as tiebreaker
                    regic_a = self._get_regic_rank(row_a.get('regic', ''))
                    regic_final = self._get_regic_rank(row_final.get('regic', ''))
                    
                    if regic_final < regic_a:
                        # Final destination has better REGIC
                        should_approve = True
                        approval_reason = f"Final destination {sede_final} has better REGIC (rank {regic_final} < {regic_a})"
                    else:
                        self.logger.info(f"      ❌ Rejected: Final destination REGIC not better (rank {regic_final} >= {regic_a})")
                        continue
                else:
                    self.logger.info(f"      ❌ Rejected: Final destination score ({score_final}) not sufficient")
                    continue
                
                # NEW: Ensure A has recorded flow/tempo to FINAL destination before allowing transitive move
                if not self._has_flow_or_time(int(sede_a), int(sede_final), flow_df):
                    self.logger.info(f"      ❌ Rejected TRANSITIVE: Origin {sede_a} has no recorded flow/time to final destination {sede_final}")

                    # Prefer local consolidation A->B if:
                    # (1) A has recorded flow/time to B AND UTPs are adjacent, OR
                    # (2) the SEDE municipalities of A and B are directly adjacent (share a physical border),
                    #     which is a strong enough geographic signal even without explicit flow data.
                    has_flow_to_b = self._has_flow_or_time(int(sede_a), int(sede_b), flow_df)
                    utps_adjacent = self._validate_utp_adjacency(utp_a, utp_b)
                    sedes_directly_adjacent = (
                        self.adjacency_graph is not None
                        and int(sede_a) in self.adjacency_graph
                        and int(sede_b) in self.adjacency_graph[int(sede_a)]
                    )

                    if (has_flow_to_b and utps_adjacent) or sedes_directly_adjacent:
                        if sedes_directly_adjacent and not (has_flow_to_b and utps_adjacent):
                            local_reason = 'Local preference: sede municipalities are directly adjacent (share border); no flow to final destination.'
                            self.logger.info(
                                f"      ℹ️ Prefer local consolidation: {sede_a} -> {sede_b} "
                                f"(sedes directly adjacent — municipalities share border)"
                            )
                        else:
                            local_reason = 'Local preference: origin has flow/time to intermediate and UTPs are adjacent; no flow to final.'
                            self.logger.info(
                                f"      ℹ️ Prefer local consolidation: {sede_a} -> {sede_b} "
                                f"(has flow/time to intermediate and UTPs adjacent)"
                            )

                        # Create a local candidate pointing to B
                        try:
                            row_b = df_metrics[df_metrics['cd_mun_sede'] == int(sede_b)].iloc[0]
                        except Exception:
                            row_b = None

                        local_candidate = {
                            'sede_origem': int(sede_a),
                            'nm_origem': rejected.get('nm_origem', row_a['nm_sede']),
                            'utp_origem': utp_a,
                            'sede_destino': int(sede_b),
                            'nm_destino': row_b['nm_sede'] if row_b is not None else '',
                            'utp_destino': utp_b,
                            'score_origem': self._get_sede_score(row_a),
                            'score_destino': self._get_sede_score(row_b) if row_b is not None else 0,
                            'tempo_viagem_h': rejected.get('tempo_viagem_h', 0.0),
                            'rm_origem': rejected.get('rm_origem', ''),
                            'rm_destino': rejected.get('rm_destino', ''),
                            'motivo_rejeicao': '',
                            'transitive': False,
                            'transitive_reason': local_reason
                        }
                        new_approved_candidates.append(local_candidate)
                        rejected_to_remove.append(rejected)
                        # Cancel the SPECIFIC B->C move in this chain (not all B->anything).
                        # A has no flow to C, so B absorbs A locally and should NOT move on to C.
                        candidates_to_cancel.add((str(sede_b), str(sede_final)))
                        self.logger.info(
                            f"      🚫 Will cancel specific candidate {sede_b}->{sede_final} "
                            f"(A has no flow to C; B absorbs A locally)."
                        )
                    else:
                        # Neither flow/time+adjacency nor direct municipality border: keep rejected.
                        self.logger.info(
                            f"      ❌ No local fallback available for {sede_a} -> {sede_b}: "
                            f"no flow to final ({sede_final}), no flow to intermediate, and sedes not directly adjacent."
                        )
                    continue
                
                # APPROVE A -> FINAL (approve A moving directly to the final destination)
                # This prevents A from remaining in B after the chain is applied.
                transitive_count += 1
                # Determine UTP of final destination
                utp_final = self.graph.get_municipality_utp(sede_final)

                new_candidate = {
                    'sede_origem': int(sede_a),
                    'nm_origem': rejected.get('nm_origem', row_a['nm_sede']),
                    'utp_origem': utp_a,
                    # Point directly to the final destination sede (C)
                    'sede_destino': int(sede_final),
                    'nm_destino': row_final.get('nm_sede', ''),
                    'utp_destino': utp_final,
                    'score_origem': score_a,
                    'score_destino': score_final,
                    'tempo_viagem_h': rejected.get('tempo_viagem_h', 0.0),
                    'rm_origem': rejected.get('rm_origem', ''),
                    'rm_destino': rejected.get('rm_destino', ''),
                    'motivo_rejeicao': '',
                    'transitive': True,
                    'transitive_reason': f"Chain {chain_str}: {approval_reason}"
                }
                
                new_approved_candidates.append(new_candidate)
                rejected_to_remove.append(rejected)
                
                self.logger.info(f"      ✅ Transitive APPROVAL: {sede_a} ({row_a['nm_sede']}) -> {sede_b} ({row_b['nm_sede']}) [chain: {chain_str}, reason: {approval_reason}]")
        
        # Remove approved rejections from rejected_candidates
        for rej in rejected_to_remove:
            sede_a = str(rej['sede_origem'])
            sede_b = str(rej['sede_destino'])
            self.rejected_candidates = [
                r for r in self.rejected_candidates 
                if not (str(r.get('sede_origem')) == sede_a and str(r.get('sede_destino')) == sede_b)
            ]
        
        # Cancel specific B->C candidates where a local A->B was preferred (A has no flow to C).
        if candidates_to_cancel:
            before = len(candidates)
            cancelled_info = [
                f"{c['sede_origem']} ({c.get('nm_origem', '?')}) -> {c['sede_destino']} ({c.get('nm_destino', '?')})"
                for c in candidates
                if (str(c['sede_origem']), str(c['sede_destino'])) in candidates_to_cancel
            ]
            candidates = [
                c for c in candidates
                if (str(c['sede_origem']), str(c['sede_destino'])) not in candidates_to_cancel
            ]
            after = len(candidates)
            for info in cancelled_info:
                self.logger.info(f"   🚫 Cancelled specific candidate: {info} (A has no flow to C; B absorbs A locally)")
            self.logger.info(
                f"   Candidates reduced {before}->{after} after cancelling {len(candidates_to_cancel)} specific B->C pair(s)."
            )

        if transitive_count > 0 or new_approved_candidates:
            self.logger.info(f"✅ Created {transitive_count} transitive approval(s) and {len(new_approved_candidates) - transitive_count} local fallback(s)")
            candidates.extend(new_approved_candidates)
        else:
            self.logger.info("   No transitive consolidations found")
        
        return candidates

    def _sync_analyzer_with_graph(self):
        """Syncs the analyzer's DataFrame with the current graph state (UTPs and Sedes)."""
        if self.analyzer.df_municipios is None:
            return

        self.logger.info("Syncing Analyzer with Graph State...")
        
        # Build a set of all sede municipalities for fast lookup
        # graph.utp_seeds is a dict: {utp_id: mun_id_sede}
        sede_municipalities = set(self.graph.utp_seeds.values())
        
        updates = {}
        for node in self.graph.hierarchy.nodes():
             # We assume integer nodes are municipalities
             if isinstance(node, int) or (isinstance(node, str) and node.isdigit()):
                 # Verify it is a municipality node (or just trust ID)
                 mun_id = int(node)
                 
                 # Get UTP
                 utp = self.graph.get_municipality_utp(mun_id)
                 
                 # Get Sede Status - check if this municipality is a sede
                 # A municipality is a sede if it's a value in utp_seeds
                 is_sede = mun_id in sede_municipalities
                 
                 updates[mun_id] = {'utp_id': utp, 'sede_utp': is_sede}
        
        # Bulk Update DataFrame
        # Iterate updates is safer/faster than df iterrows for this
        sede_count = 0
        for idx, row in self.analyzer.df_municipios.iterrows():
            mun_id = int(row['cd_mun'])
            if mun_id in updates:
                self.analyzer.df_municipios.at[idx, 'utp_id'] = updates[mun_id]['utp_id']
                self.analyzer.df_municipios.at[idx, 'sede_utp'] = updates[mun_id]['sede_utp']
                if updates[mun_id]['sede_utp']:
                    sede_count += 1
        
        self.logger.info(f"✅ Synced {len(updates)} municipalities. {sede_count} are sedes.")

        # Force analyzer to clear cached metrics if any
        # (calculate_socioeconomic_metrics recalculates from df_municipios every time, so this is sufficient)

    def run_sede_consolidation(self, flow_df: pd.DataFrame, gdf: gpd.GeoDataFrame, map_gen) -> int:
        """
        Executes Simplified Sede Consolidation Pipeline (Single Pass).
        
        Logic:
        1. Identify sedes with main flow to another sede (travel time <= 2h)
        2. Validate RM rules (same RM or both without RM)
        3. Validate UTP adjacency
        4. Validate infrastructure scores
        5. Move entire UTP (sede + all municipalities) to destination UTP
        """
        self.logger.info("Starting Step 6: Sede Consolidation (Simplified)...")
        
        # Initialize
        self.consolidation_manager = ConsolidationManager()
        self.changes_current_run = []
        self.rejected_candidates = []
        
        # Build Adjacency Graph
        self._build_adjacency_graph(gdf)
        
        # Sync analyzer with current graph state
        self._sync_analyzer_with_graph()
        
        # Calculate metrics for all sedes
        df_metrics = self.analyzer.calculate_socioeconomic_metrics()
        
        total_sedes = len(df_metrics)
        sedes_com_alerta = df_metrics['tem_alerta_dependencia'].sum() if 'tem_alerta_dependencia' in df_metrics.columns else 0
        
        self.logger.info(f"📊 Sede Analysis Stats:")
        self.logger.info(f"   Total sedes: {total_sedes}")
        self.logger.info(f"   Sedes with dependency alerts: {sedes_com_alerta}")
        
        if total_sedes == 0:
            self.logger.warning("⚠️  NO SEDES FOUND!")
            self._save_results_and_csv()
            return 0
        
        # Filter candidates using simplified rules
        candidates = self._filter_candidates(df_metrics)
        
        # Apply transitive consolidation rule
        # If A->B rejected (tie) but B->C approved, create A->C
        candidates = self._apply_transitive_consolidation(candidates, df_metrics, flow_df)
        
        # Resolve mutual preference conflicts (A -> B and B -> A).
        # When two sedes prefer each other, only allow the movement of the one
        # whose total originating flow is smaller. If flows are equal, reject both
        # to avoid mutual loss of sede status.
        if candidates and flow_df is not None:
            mapping = {str(c['sede_origem']): str(c['sede_destino']) for c in candidates}
            to_remove = set()
            for c in candidates:
                a = str(c['sede_origem'])
                b = str(c['sede_destino'])
                # Check reciprocal
                if b in mapping and mapping[b] == a:
                    # Already decided
                    if a in to_remove or b in to_remove:
                        continue

                    try:
                        flow_a = float(self._get_total_flow(int(a), flow_df) or 0)
                    except Exception:
                        flow_a = 0.0
                    try:
                        flow_b = float(self._get_total_flow(int(b), flow_df) or 0)
                    except Exception:
                        flow_b = 0.0

                    if flow_a == flow_b:
                        # Tie: reject both
                        to_remove.update([a, b])
                        reason = f'Concorrência recíproca com mesmo fluxo ({flow_a}); nenhum movido.'
                    elif flow_a > flow_b:
                        # A has larger flow (is bigger/stronger).
                        # We want to KEEP A (reject move A->B) and EXECUTE move B->A.
                        # Matches candidate where sede_origem = A
                        to_remove.add(a) 
                        reason = f'Concorrência: {a} tem fluxo maior ({flow_a}) que {b} ({flow_b}); {a} deve ser MANTIDO (removemos A->B) e {b} deve mover (mantemos B->A).'
                    else:
                        # B has larger flow.
                        # We want to KEEP B (reject move B->A) and EXECUTE move A->B.
                        # Matches candidate where sede_origem = B
                        to_remove.add(b)
                        reason = f'Concorrência: {b} tem fluxo maior ({flow_b}) que {a} ({flow_a}); {b} deve ser MANTIDO (removemos B->A) e {a} deve mover (mantemos A->B).'

                    # Register rejected candidate(s)
                    for rem in ( [a, b] if flow_a == flow_b else ([b] if flow_a > flow_b else [a]) ):
                        # find candidate dict
                        rem_cand = next((x for x in candidates if str(x['sede_origem']) == str(rem)), None)
                        if rem_cand:
                            self.rejected_candidates.append({
                                'sede_origem': rem_cand.get('sede_origem', ''),
                                'nm_origem': rem_cand.get('nm_origem', ''),
                                'utp_origem': rem_cand.get('utp_origem', ''),
                                'sede_destino': rem_cand.get('sede_destino', ''),
                                'utp_destino': rem_cand.get('utp_destino', ''),
                                'motivo_rejeicao': reason
                            })

            if to_remove:
                before_count = len(candidates)
                candidates = [c for c in candidates if str(c['sede_origem']) not in to_remove]
                self.logger.info(f"🔁 Resolved {len(to_remove)} reciprocal candidate(s): {to_remove}. Candidates reduced {before_count}->{len(candidates)}")
        elif candidates and flow_df is None:
            self.logger.warning('Fluxo de viagens (flow_df) não fornecido: não foi possível resolver concorrências recíprocas automaticamente.')
        if not candidates:
            self.logger.info("✅ No consolidation candidates found.")
            self._save_results_and_csv()
            return 0
        
        self.logger.info(f"✅ Found {len(candidates)} consolidation candidates")
        
        # Execute consolidations
        total_changes = 0
        # NORMALIZE candidates: ensure utp_destino is resolved from sede_destino when missing
        for c in candidates:
            try:
                # If utp_destino is missing or indicates not found, try to resolve
                utp_dest = c.get('utp_destino')
                sede_dest = c.get('sede_destino')
                if (not utp_dest or str(utp_dest).upper() in ['NAO_ENCONTRADO', 'NAN', '']) and sede_dest:
                    try:
                        resolved = self.graph.get_municipality_utp(int(sede_dest))
                    except Exception:
                        resolved = None
                    if resolved:
                        c['utp_destino'] = resolved
            except Exception:
                # Do not fail consolidation due to normalization issues
                self.logger.debug('Failed to normalize candidate utp_destino for candidate', exc_info=True)

        for cand in candidates:
            sede_origem = cand['sede_origem']
            sede_destino = cand['sede_destino'] if 'sede_destino' in cand else None
            utp_origem = cand['utp_origem']
            utp_destino = cand['utp_destino']
            
            self.logger.info(f"\n🔄 Consolidating: {cand['nm_origem']} (UTP {utp_origem}) -> {cand['nm_destino']} (UTP {utp_destino})")
            
            # Get all municipalities in the origin UTP
            muns_to_move = []
            for node, data in self.graph.hierarchy.nodes(data=True):
                if data.get('type') == 'municipality':
                    # FIX: Type mismatch (int vs str) caused failure to find municipalities
                    if str(self.graph.get_municipality_utp(node)) == str(utp_origem):
                        muns_to_move.append(node)
            
            self.logger.info(f"  Moving entire UTP: {len(muns_to_move)} municipalities from {utp_origem} to {utp_destino}")

            # Preserve original membership to detect full vs partial moves
            original_muns = set(muns_to_move)

            # Move all municipalities
            for mun in muns_to_move:
                self.graph.move_municipality(mun, utp_destino)
                self.logger.debug(f"DEBUG: Moved municipality {mun} from UTP {utp_origem} to UTP {utp_destino}")
                
                # Update GDF for this municipality
                if gdf is not None and 'CD_MUN' in gdf.columns:
                    try:
                        mask = gdf['CD_MUN'].astype(str).str.split('.').str[0] == str(mun)
                        gdf.loc[mask, 'UTP_ID'] = str(utp_destino)
                    except Exception as e:
                        self.logger.error(f"    Failed to update GDF for {mun}: {e}")
            
            # Log consolidation
            is_transitive = cand.get('transitive', False)
            reason_suffix = f" (TRANSITIVE: {cand.get('transitive_reason', '?')})" if is_transitive else ""
            
            # Ensure target UTP is resolved before recording consolidation
            resolved_target_utp = utp_destino
            try:
                if (not resolved_target_utp or str(resolved_target_utp).upper() in ['NAO_ENCONTRADO', 'NAN', ''] ) and sede_destino:
                    resolved = self.graph.get_municipality_utp(int(sede_destino))
                    if resolved and str(resolved).upper() not in ['NAO_ENCONTRADO', 'NAN', '']:
                        resolved_target_utp = resolved
            except Exception:
                # ignore resolution failure and keep original utp_destino
                resolved_target_utp = resolved_target_utp

            cons_entry = self.consolidation_manager.add_consolidation(
                source_utp=utp_origem,
                target_utp=resolved_target_utp,
                reason=f"Sede consolidation (full UTP): Score {cand['score_origem']}->{cand['score_destino']}, Travel {cand['tempo_viagem_h']:.2f}h{reason_suffix}",
                details={
                    "sede_id": sede_origem,
                    "sede_destino": sede_destino,
                    "nm_sede": cand['nm_origem'],
                    "is_sede": True,
                    "municipalities_moved": len(muns_to_move),
                    "score_origem": cand['score_origem'],
                    "score_destino": cand['score_destino'],
                    "tempo_viagem_h": cand['tempo_viagem_h'],
                    "rm_origem": cand.get('rm_origem', ''),
                    "rm_destino": cand.get('rm_destino', ''),
                    "transitive": is_transitive,
                    "transitive_reason": cand.get('transitive_reason', '') if is_transitive else ''
                }
            )
            self.changes_current_run.append(cons_entry)
            

            # Revoga status de sede apenas se a sede de origem realmente mudou de UTP
            if sede_origem in muns_to_move and str(self.graph.get_municipality_utp(sede_origem)) == utp_destino:
                if self.graph.hierarchy.has_node(sede_origem):
                    self.logger.debug(f"DEBUG: Revoking sede flag for municipality {sede_origem} (UTP {utp_origem} -> {utp_destino})")
                    self.graph.hierarchy.nodes[sede_origem]['sede_utp'] = False

                # Decide corretamente sobre o mapeamento de seeds e remoção de UTP
                utp_node = f"UTP_{utp_origem}"

                # Recalcula sucessores da UTP após a movimentação
                successors = list(self.graph.hierarchy.successors(utp_node)) if self.graph.hierarchy.has_node(utp_node) else []

                # Identify remaining municipalities
                remaining_muns = [n for n in successors if self.graph.hierarchy.nodes[n].get('type') == 'municipality']

                # Se a UTP ficou vazia --> remover seed e nó
                if not remaining_muns:
                    if utp_origem in self.graph.utp_seeds:
                        self.logger.debug(f"DEBUG: Deleting utp_seeds[{utp_origem}] -> {self.graph.utp_seeds.get(utp_origem)}")
                        del self.graph.utp_seeds[utp_origem]
                    if self.graph.hierarchy.has_node(utp_node):
                        self.logger.debug(f"DEBUG: Removing UTP node {utp_node} from hierarchy (empty after move)")
                        self.graph.hierarchy.remove_node(utp_node)
                else:
                    # Partial move: apenas reatribuir seed se a seed atual não pertence mais à UTP
                    if utp_origem in self.graph.utp_seeds:
                        current_seed = self.graph.utp_seeds[utp_origem]
                        if (not self.graph.hierarchy.has_node(current_seed)) or (current_seed not in remaining_muns):
                            # Escolher nova sede: preferir município já marcado como sede, senão o primeiro
                            new_sede = None
                            for m in remaining_muns:
                                if self.graph.hierarchy.nodes[m].get('sede_utp'):
                                    new_sede = m
                                    break
                            if new_sede is None and remaining_muns:
                                new_sede = remaining_muns[0]

                            if new_sede is not None:
                                self.logger.debug(f"DEBUG: Reassigning seed for UTP {utp_origem} -> {new_sede} (was {current_seed})")
                                self.graph.utp_seeds[utp_origem] = new_sede
                                if self.graph.hierarchy.has_node(new_sede):
                                    self.graph.hierarchy.nodes[new_sede]['sede_utp'] = True
                    else:
                        # No seed recorded previously: set one from remaining
                        if remaining_muns:
                            new_sede = remaining_muns[0]
                            self.logger.debug(f"DEBUG: Setting missing seed for UTP {utp_origem} -> {new_sede}")
                            self.graph.utp_seeds[utp_origem] = new_sede
                            if self.graph.hierarchy.has_node(new_sede):
                                self.graph.hierarchy.nodes[new_sede]['sede_utp'] = True

            # GARANTIR QUE A UTP DE DESTINO SEMPRE TERÁ UMA SEDE REGISTRADA
            # Se não houver sede registrada para a UTP destino, definir a sede_destino (ou sede_origem se apropriado)
            utp_destino_str = str(utp_destino)
            # CRITICAL FIX: Check if destination UTP has a VALID sede (exists AND belongs to this UTP)
            needs_new_sede = False
            if utp_destino_str not in self.graph.utp_seeds:
                needs_new_sede = True
            else:
                current_sede_id = self.graph.utp_seeds[utp_destino_str]
                # Check if sede node exists
                if not self.graph.hierarchy.has_node(current_sede_id):
                    needs_new_sede = True
                # CRITICAL: Check if sede actually belongs to this UTP
                elif str(self.graph.get_municipality_utp(current_sede_id)) != utp_destino_str:
                    self.logger.warning(f"DEBUG: UTP {utp_destino} sede {current_sede_id} no longer belongs to this UTP! Reassigning...")
                    needs_new_sede = True
            
            if needs_new_sede:
                # Preferir sede_destino se ela está entre os municípios da UTP destino
                if sede_destino in muns_to_move and str(self.graph.get_municipality_utp(sede_destino)) == utp_destino_str:
                    self.logger.debug(f"DEBUG: Setting utp_seeds[{utp_destino_str}] -> {sede_destino} (preferred destination sede)")
                    self.graph.utp_seeds[utp_destino_str] = sede_destino
                    if self.graph.hierarchy.has_node(sede_destino):
                        self.graph.hierarchy.nodes[sede_destino]['sede_utp'] = True
                # Se não, usar sede_origem se ela foi movida para a UTP destino
                elif sede_origem in muns_to_move and str(self.graph.get_municipality_utp(sede_origem)) == utp_destino_str:
                    self.logger.debug(f"DEBUG: Setting utp_seeds[{utp_destino_str}] -> {sede_origem} (sede_origem moved into destino)")
                    self.graph.utp_seeds[utp_destino_str] = sede_origem
                    if self.graph.hierarchy.has_node(sede_origem):
                        self.graph.hierarchy.nodes[sede_origem]['sede_utp'] = True
                # Como fallback, escolher qualquer município da UTP destino
                elif muns_to_move:
                    mun_fallback = muns_to_move[0]
                    self.logger.debug(f"DEBUG: Fallback setting utp_seeds[{utp_destino_str}] -> {mun_fallback}")
                    self.graph.utp_seeds[utp_destino_str] = mun_fallback
                    if self.graph.hierarchy.has_node(mun_fallback):
                        self.graph.hierarchy.nodes[mun_fallback]['sede_utp'] = True
                else:
                    # Last resort: get ANY municipality currently in destination UTP
                    utp_destino_node = f"UTP_{utp_destino}"
                    if self.graph.hierarchy.has_node(utp_destino_node):
                        dest_muns = [n for n in self.graph.hierarchy.successors(utp_destino_node) 
                                    if self.graph.hierarchy.nodes[n].get('type') == 'municipality']
                        if dest_muns:
                            mun_fallback = dest_muns[0]
                            self.logger.warning(f"DEBUG: Emergency fallback setting utp_seeds[{utp_destino_str}] -> {mun_fallback}")
                            self.graph.utp_seeds[utp_destino_str] = mun_fallback
                            if self.graph.hierarchy.has_node(mun_fallback):
                                self.graph.hierarchy.nodes[mun_fallback]['sede_utp'] = True
            
            total_changes += 1
            self.logger.info(f"  ✅ Sede consolidation complete: {len(muns_to_move)} municipalities moved")

        
        # Save results
        self._save_results_and_csv()
        
        # Recolor graph
        self.logger.info("\n🎨 Recalculating graph coloring...")
        try:
            coloring = self.graph.compute_graph_coloring(gdf)
            self.logger.info(f"✅ Coloring updated: {max(coloring.values(), default=0) + 1} colors needed")
            
            if gdf is not None:
                gdf['COLOR_ID'] = gdf['CD_MUN'].astype(int).map(coloring)
            
            # Save coloring
            # Imports moved to top of file
            
            coloring_file = self.data_dir / "post_sede_coloring.json"
            coloring_str_keys = {str(k): v for k, v in coloring.items()}
            
            with open(coloring_file, 'w') as f:
                json.dump(coloring_str_keys, f, indent=2)
            
            self.logger.info(f"💾 Coloring saved to: {coloring_file}")
            
            # CRITICAL: Confirm all active seeds are marked as sede_utp=True in graph nodes
            # This ensures the snapshot reflects the final consolidation state
            count_sedes_marked = 0
            for utp_id, mun_id in self.graph.utp_seeds.items():
                if self.graph.hierarchy.has_node(mun_id):
                    self.graph.hierarchy.nodes[mun_id]['sede_utp'] = True
                    count_sedes_marked += 1
            self.logger.info(f"✅ Marked {count_sedes_marked} municipalities as Active Sedes in Graph.")

            # Export Snapshot Step 6 (Sede Consolidation)
            snapshot_path = self.data_dir / "snapshot_step6_sede_consolidation.json"
            self.graph.export_snapshot(snapshot_path, "Sede Consolidation", gdf)
            
        except Exception as e:
            self.logger.warning(f"⚠️  Error recalculating coloring or saving snapshot: {e}")
        
        self.logger.info(f"\n✅ Sede Consolidation complete: {total_changes} consolidations executed")
        return total_changes

    def _save_results_and_csv(self):
        """Save consolidation results to JSON and CSV files."""
        import pandas as pd
        from pathlib import Path
        
        # Save JSON (existing functionality)
        self.logger.info(f"💾 Saving consolidation results ({len(self.changes_current_run)} changes)...")
        self.consolidation_manager.save_sede_batch(self.changes_current_run if self.changes_current_run else [])
        
        # Generate CSV with all candidates (approved + rejected)
        csv_records = []
        
        # Add approved consolidations
        for change in self.changes_current_run:
            if change.get('details', {}).get('is_sede', False):  # Only record sede movements, not individual municipalities
                # Backwards-compatible: some entries use 'mun_id', others use 'sede_id'
                sede_origem_id = change['details'].get('mun_id', change['details'].get('sede_id', ''))
                csv_records.append({
                    'sede_origem': sede_origem_id,
                    'utp_origem': change['source_utp'],
                    'sede_destino': change['details'].get('sede_destino', ''),
                    'utp_destino': change['target_utp'],
                    'tempo_viagem_h': change['details'].get('tempo_viagem_h', ''),
                    'score_origem': change['details'].get('score_origem', ''),
                    'score_destino': change['details'].get('score_destino', ''),
                    'rm_origem': change['details'].get('rm_origem', ''),
                    'rm_destino': change['details'].get('rm_destino', ''),
                    'status': 'APROVADO',
                    'transitive': 'SIM' if change['details'].get('transitive', False) else 'NAO',
                    'transitive_reason': change['details'].get('transitive_reason', ''),
                    'motivo_rejeicao': ''
                })
        
        # Add rejected candidates
        for rejected in self.rejected_candidates:
            csv_records.append({
                'sede_origem': rejected.get('sede_origem', ''),
                'utp_origem': rejected.get('utp_origem', ''),
                'sede_destino': rejected.get('sede_destino', ''),
                'utp_destino': rejected.get('utp_destino', ''),
                'tempo_viagem_h': rejected.get('tempo_viagem_h', ''),
                'score_origem': rejected.get('score_origem', ''),
                'score_destino': rejected.get('score_destino', ''),
                'rm_origem': rejected.get('rm_origem', ''),
                'rm_destino': rejected.get('rm_destino', ''),
                'status': 'REJEITADO',
                'transitive': 'NAO',
                'transitive_reason': '',
                'motivo_rejeicao': rejected.get('motivo_rejeicao', '')
            })
        
        # Save CSV
        if csv_records:
            df_csv = pd.DataFrame(csv_records)
            csv_path = Path(self.data_dir) / 'sede_consolidation_result.csv'
            df_csv.to_csv(csv_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"💾 CSV saved to: {csv_path} ({len(csv_records)} records)")
        else:
            self.logger.info("No consolidation records to save to CSV")




    def _get_total_flow(self, mun_id: int, flow_df: pd.DataFrame) -> float:
        """Helper to get total flow originating from a municipality."""
        if flow_df is None: return 0
        return flow_df[flow_df['mun_origem'] == mun_id]['viagens'].sum()

    def _export_candidate_analysis_json(self, df_metrics, candidates, pass_num):
        '''Exporta análise detalhada de candidatos para JSON.'''
        import json
        from pathlib import Path
        from datetime import datetime
        
        export_data = {
            'pass_number': pass_num,
            'timestamp': datetime.now().isoformat(),
            'total_sedes_analyzed': len(df_metrics),
            'total_candidates_found': len(candidates),
            'candidates': []
        }
        
        for cand in candidates:
            sede_origem = cand['sede_origem']
            sede_destino = cand['sede_destino']
            origem_row = df_metrics[df_metrics['cd_mun_sede'] == sede_origem]
            dest_row = df_metrics[df_metrics['cd_mun_sede'] == sede_destino]
            if origem_row.empty or dest_row.empty:
                continue
            origem_row = origem_row.iloc[0]
            dest_row = dest_row.iloc[0]
            candidate_data = {
                'approved': True,
                'origem': {'cd_mun': int(sede_origem), 'nm_mun': origem_row['nm_sede'], 'utp_id': cand['utp_origem'], 'uf': origem_row['uf'], 'populacao': int(origem_row['populacao_total_utp']), 'regic': origem_row['regic'], 'tem_aeroporto': bool(origem_row['tem_aeroporto']), 'aeroporto_icao': origem_row.get('aeroporto_icao', ''), 'turismo': origem_row.get('turismo', ''), 'score': cand['score_origem']},
                'destino': {'cd_mun': int(sede_destino), 'nm_mun': dest_row['nm_sede'], 'utp_id': cand['utp_destino'], 'uf': dest_row['uf'], 'populacao': int(dest_row['populacao_total_utp']), 'regic': dest_row['regic'], 'tem_aeroporto': bool(dest_row['tem_aeroporto']), 'aeroporto_icao': dest_row.get('aeroporto_icao', ''), 'turismo': dest_row.get('turismo', ''), 'score': cand['score_dest']},
                'fluxo': {'principal_destino': origem_row.get('principal_destino_nm', ''), 'tempo_h': float(origem_row.get('tempo_ate_destino_h', 0)), 'proporcao': float(origem_row.get('proporcao_fluxo_principal', 0))},
                'reason': cand['reason']
            }
            export_data['candidates'].append(candidate_data)
        output_path = Path(self.data_dir) / f'sede_consolidation_analysis_pass{pass_num}.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        self.logger.info(f'✅ Análise exportada: {output_path}')
        return output_path

