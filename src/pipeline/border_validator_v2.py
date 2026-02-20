# src/pipeline/border_validator_v2.py
"""
Border Validator V2 - Sede-Centric Approach

Logic:
1. For each UTP, identify municipalities that are poorly connected to their current sede:
   - No flow ≤2h to current sede, OR
   - Municipality is on the border with other UTPs
   
2. For these candidates, check if they have better alternatives:
   - Find which other SEDES they have flow ≤2h to
   - Check if any of those sedes belong to ADJACENT UTPs
   - If yes → candidate for relocation
   
3. Execute changes iteratively until convergence, respecting RM rules
"""

import logging
import pandas as pd
import geopandas as gpd
import networkx as nx
from typing import Dict, Set, List, Tuple, Optional
from pathlib import Path

from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator


class BorderValidatorV2:
    """
    Validates and optimizes UTP borders based on flow connectivity to sedes.
    
    This version focuses on identifying poorly-connected municipalities
    and relocating them to UTPs with better flow connections.
    """
    
    def __init__(
        self,
        graph: TerritorialGraph,
        validator: TerritorialValidator,
        data_dir: Path = None,
        impedance_df: pd.DataFrame = None
    ):
        self.graph = graph
        self.validator = validator
        self.logger = logging.getLogger("GeoValida.BorderValidatorV2")
        self.data_dir = data_dir or Path(__file__).parent.parent.parent / "data" / "03_processed"
        self.adjacency_graph = None
        self.impedance_df = impedance_df
        
        # Load impedance if not provided
        if self.impedance_df is None:
            self._load_impedance_data()

    def _load_impedance_data(self):
        """Loads travel time matrix (impedance)."""
        impedance_path = Path(__file__).parent.parent.parent / "data" / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"
        
        if not impedance_path.exists():
            self.logger.warning(f"Impedance file not found: {impedance_path}")
            return
            
        try:
            self.logger.info("Loading impedance data in BorderValidatorV2...")
            self.impedance_df = pd.read_csv(impedance_path, sep=';', encoding='latin-1')
            
            # Normalize columns
            self.impedance_df = self.impedance_df.rename(columns={
                'PAR_IBGE': 'par_ibge',
                'COD_IBGE_ORIGEM': 'origem',
                'COD_IBGE_DESTINO': 'destino',
                'Tempo': 'tempo_horas',
                'COD_IBGE_ORIGEM_1': 'origem_6',
                'COD_IBGE_DESTINO_1': 'destino_6'
            })
            
            # Clean and convert
            self.impedance_df['tempo_horas'] = (
                self.impedance_df['tempo_horas']
                .astype(str)
                .str.replace(',', '.')
                .astype(float)
            )
            
            # Ensure 6-digit columns are int
            for col in ['origem_6', 'destino_6']:
                self.impedance_df[col] = pd.to_numeric(self.impedance_df[col], errors='coerce').fillna(0).astype(int)
                
            self.logger.info(f"Loaded {len(self.impedance_df)} impedance records.")
            
        except Exception as e:
            self.logger.error(f"Failed to load impedance data: {e}")
            self.impedance_df = None
        
    def _build_adjacency_graph(self, gdf: gpd.GeoDataFrame):
        """Builds spatial adjacency graph of municipalities."""
        self.logger.info("Building spatial adjacency graph...")
        self.adjacency_graph = nx.Graph()
        
        if gdf is None or gdf.empty:
            return
        
        gdf_valid = gdf[gdf.geometry.notna()]
        gdf_metric = gdf_valid.to_crs(epsg=3857)
        
        # Buffer 100m for topology gaps
        buffer_val = 100.0
        gdf_buff = gdf_metric.copy()
        gdf_buff['geometry'] = gdf_buff.geometry.buffer(buffer_val)
        
        # Ensure all municipalities appear as nodes (including isolated ones)
        try:
            node_ids = []
            if 'CD_MUN' in gdf_valid.columns:
                # Some CD_MUN values may be strings
                node_ids = [int(x) for x in gdf_valid['CD_MUN'].tolist() if x is not None and str(x) != 'nan']
            if node_ids:
                self.adjacency_graph.add_nodes_from(node_ids)
        except Exception:
            # If any conversion fails, continue — nodes will be created from edges later
            pass

        # Self-join
        sjoin = gpd.sjoin(gdf_buff, gdf_buff, how='inner', predicate='intersects')
        
        edges = []
        for idx, row in sjoin.iterrows():
            left = int(row['CD_MUN_left'])
            right = int(row['CD_MUN_right'])
            if left != right:
                edges.append((left, right))
        
        # Add edges (this will also add missing nodes for any connected pairs)
        if edges:
            self.adjacency_graph.add_edges_from(edges)
        self.logger.info(f"Adjacency graph built: {self.adjacency_graph.number_of_nodes()} nodes, {self.adjacency_graph.number_of_edges()} edges")
    
    def _get_mun_rm(self, mun_id: int) -> Optional[str]:
        """Gets RM of a municipality from graph."""
        if self.graph.hierarchy.has_node(mun_id):
            return self.graph.hierarchy.nodes[mun_id].get('regiao_metropolitana')
        return None
    
    def _validate_rm_compatibility(self, mun_id: int, target_utp: str) -> bool:
        """Validates RM rules for municipality relocation."""
        mun_rm = self._get_mun_rm(mun_id)
        utp_rm = self.validator.get_rm_of_utp(target_utp)
        
        # Normalize values to handle None vs RM_SEM_RM mismatch
        # Treat None, nan, empty string, and "RM_SEM_RM" as equivalent
        def normalize_rm(val):
            if val is None:
                return "RM_SEM_RM"
            val_str = str(val)
            if val_str.lower() in ['none', 'nan', '', 'rm_sem_rm']:
                return "RM_SEM_RM"
            return val_str
            
        norm_mun_rm = normalize_rm(mun_rm)
        norm_utp_rm = normalize_rm(utp_rm)
        
        # Both without RM (normalized to RM_SEM_RM) -> OK
        if norm_mun_rm == "RM_SEM_RM" and norm_utp_rm == "RM_SEM_RM":
            return True
        
        # Same RM -> OK
        if norm_mun_rm == norm_utp_rm:
            return True
        
        # Different RM -> REJECT
        return False
    
    def _has_flow_to_sede(self, mun_id: int, sede_id: int, flow_df: pd.DataFrame, max_time: float = 2.0) -> bool:
        """Checks if municipality has flow ≤max_time to the sede."""
        if flow_df is None or flow_df.empty:
            return False
        
        # Check flow from mun to sede
        flows = flow_df[
            (flow_df['mun_origem'].astype(int) == int(mun_id)) &
            (flow_df['mun_destino'].astype(int) == int(sede_id))
        ]
        
        if flows.empty:
            return False
        
        # Check time constraint
        # Check time constraint using impedance matrix (real time)
        # We need to look up the time, not rely on non-existent column in flow_df
        real_time = self._get_travel_time(mun_id, sede_id)
        
        if real_time is not None:
            return real_time <= max_time
            
        # Fallback: If no time found, treat as INVALID (too far or unknown)
        # Strict mode: missing impedance means we can't verify < 2h
        return False

    def _get_travel_time(self, origin_id: int, dest_id: int) -> Optional[float]:
        """Gets travel time between two municipalities using 6-digit lookup."""
        if self.impedance_df is None:
            return None
            
        # Convert to 6-digit
        orig_6 = int(origin_id) // 10
        dest_6 = int(dest_id) // 10
        
        # Check direct
        row = self.impedance_df[
            (self.impedance_df['origem_6'] == orig_6) & 
            (self.impedance_df['destino_6'] == dest_6)
        ]
        
        if not row.empty:
            return float(row.iloc[0]['tempo_horas'])
            
        return None
    
    def _get_flows_to_sedes(self, mun_id: int, flow_df: pd.DataFrame, max_time: float = 2.0) -> List[Tuple[int, float, float]]:
        """
        Gets all flows from municipality to ANY sede within time limit.
        
        Returns:
            List of (sede_id, flow_value, travel_time) tuples
        """
        if flow_df is None or flow_df.empty:
            return []
        
        # Get all flows from this municipality
        flows = flow_df[flow_df['mun_origem'].astype(int) == int(mun_id)].copy()
        
        if flows.empty:
            return []
        
        # Filter by time
        if 'tempo_viagem' in flows.columns:
            flows = flows[flows['tempo_viagem'] <= max_time]
        
        # Filter only destinations that are sedes
        sede_flows = []
        for _, row in flows.iterrows():
            dest_id = int(row['mun_destino'])
            
            # Check if destination is a sede
            dest_utp = self.graph.get_municipality_utp(dest_id)
            if dest_utp and str(dest_utp) in self.graph.utp_seeds:
                sede_of_utp = self.graph.utp_seeds[str(dest_utp)]
                if int(sede_of_utp) == dest_id:
                    # This is a sede!
                    viagens = float(row['viagens'])
                    tempo = self._get_travel_time(mun_id, dest_id)
                    
                    # Strict check: If no time found, we assume it's > 2h (or invalid)
                    if tempo is None:
                        continue
                    
                    # Filter by max_time if we have valid time data
                    if tempo > max_time:
                         continue

                    sede_flows.append((dest_id, viagens, tempo))
        
        # Sort by flow descending
        sede_flows.sort(key=lambda x: -x[1])
        
        return sede_flows
    
    def _is_adjacent_to_utp(self, mun_id: int, target_utp: str) -> bool:
        """Checks if municipality is adjacent to any municipality in target UTP."""
        if self.adjacency_graph is None or mun_id not in self.adjacency_graph:
            return False
        
        # Get neighbors
        neighbors = list(self.adjacency_graph[mun_id])
        
        # Check if any neighbor belongs to target UTP
        for neighbor in neighbors:
            neighbor_utp = self.graph.get_municipality_utp(neighbor)
            if neighbor_utp == target_utp:
                return True
        
        return False

    def _is_connected_to_sede_within_utp(self, mun_id: int, sede_id: int, utp_muns: List[int]) -> bool:
        """
        Checks whether `mun_id` can reach `sede_id` using only municipalities
        that belong to the same UTP (provided in `utp_muns`). Uses the spatial
        adjacency graph to verify a path exists inside the induced subgraph.
        """
        if self.adjacency_graph is None:
            return False

        # Ensure node types are ints
        try:
            mun_int = int(mun_id)
            sede_int = int(sede_id)
        except Exception:
            return False

        # Build set of UTP municipality nodes (ints)
        utp_node_set = set()
        for m in utp_muns:
            try:
                utp_node_set.add(int(m))
            except Exception:
                continue

        # Both nodes must be part of the induced set and present in adjacency
        if mun_int not in utp_node_set or sede_int not in utp_node_set:
            return False

        # Induce subgraph and check connectivity
        try:
            sub = self.adjacency_graph.subgraph(utp_node_set)
            return nx.has_path(sub, mun_int, sede_int)
        except Exception:
            return False
    
    def _identify_poorly_connected_municipalities(
        self,
        flow_df: pd.DataFrame,
        gdf: gpd.GeoDataFrame
    ) -> Dict[str, Set[int]]:
        """
        Identifies ALL border municipalities for evaluation.
        
        Border municipality = adjacent to other UTPs
        
        Later, we check if they have better flow to a different sede.
        
        Returns:
            Dict mapping UTP_ID -> Set of border municipality IDs
        """
        self.logger.info("Identifying border municipalities...")
        
        border_municipalities = {}
        
        # Get all UTPs
        utp_nodes = [n for n, d in self.graph.hierarchy.nodes(data=True) if d.get('type') == 'utp']
        
        for utp_node in utp_nodes:
            utp_id = utp_node.replace("UTP_", "")
            
            # Get sede
            sede = self.graph.utp_seeds.get(utp_id)
            if not sede:
                continue
            
            # Get all municipalities in this UTP
            muns_in_utp = list(self.graph.hierarchy.successors(utp_node))
            
            border_in_utp = set()
            
            for mun_id in muns_in_utp:
                mun_int = int(mun_id)
                
                # Skip if it's the sede itself
                if mun_int == int(sede):
                    continue
                
                # Check if on border (adjacent to other UTPs)
                is_border = False
                if self.adjacency_graph and mun_int in self.adjacency_graph:
                    neighbors = list(self.adjacency_graph[mun_int])
                    for neighbor in neighbors:
                        neighbor_utp = self.graph.get_municipality_utp(neighbor)
                        if neighbor_utp != utp_id:
                            is_border = True
                            break

                # Add all border municipalities (adjacency-based)
                if is_border:
                    border_in_utp.add(mun_int)
                    continue

                # Additional check: ensure municipality can reach the sede
                # using only municipalities inside the same UTP. If not,
                # consider it poorly connected and include for evaluation.
                try:
                    # muns_in_utp may be strings; pass list of their ints
                    utp_muns_int = [int(x) for x in muns_in_utp]
                except Exception:
                    utp_muns_int = []

                if sede:
                    try:
                        seat_int = int(sede)
                    except Exception:
                        seat_int = None
                else:
                    seat_int = None

                if seat_int is None or not self._is_connected_to_sede_within_utp(mun_int, seat_int, utp_muns_int):
                    # Not reachable through internal UTP connectivity
                    border_in_utp.add(mun_int)
                    self.logger.debug(f"  [DEBUG] Mun {mun_int}: Not connected to sede {sede} within UTP {utp_id}")
            
            if border_in_utp:
                border_municipalities[utp_id] = border_in_utp
                self.logger.info(f"  UTP {utp_id}: {len(border_in_utp)} border municipalities")
        
        return border_municipalities
    
    def _find_better_utp(
        self,
        mun_id: int,
        current_utp: str,
        flow_df: pd.DataFrame
    ) -> Optional[Tuple[str, float, str]]:
        """
        Finds a better UTP by comparing flow to ALL sedes within 2h.
        
        Relocates if:
        - Current sede is unreachable (no flow ≤2h) AND another sede is reachable
        - OR another sede has HIGHER flow than current sede
        - That sede's UTP is ADJACENT
        - RM rules are respected
        
        Returns:
            (target_utp_id, flow_value, reason) or None
        """
        # Get current sede
        current_sede = self.graph.utp_seeds.get(current_utp)
        
        # Get flows to all sedes within 2h
        sede_flows = self._get_flows_to_sedes(mun_id, flow_df, max_time=2.0)
        
        if not sede_flows:
            self.logger.debug(f"  [DEBUG] Mun {mun_id}: No sede flows found")
            return None
        
        self.logger.debug(f"  [DEBUG] Mun {mun_id}: Found {len(sede_flows)} sede flows")
        
        # Check if current sede is reachable (has flow ≤2h)
        current_sede_flow = 0.0
        has_flow_to_current = False
        if current_sede:
            for sede_id, flow_value, _ in sede_flows:
                if int(sede_id) == int(current_sede):
                    current_sede_flow = flow_value
                    has_flow_to_current = True
                    break
        
        self.logger.debug(f"  [DEBUG] Mun {mun_id}: Flow to current sede ({current_sede}): {current_sede_flow} ({'reachable' if has_flow_to_current else 'UNREACHABLE'})")
        
        # Find sede with HIGHEST flow (excluding current)
        # If current sede is unreachable, accept ANY positive flow (set threshold to -1)
        # If current sede is reachable, require strictly higher flow
        best_sede = None
        best_flow = current_sede_flow if has_flow_to_current else -1.0
        best_time = 0.0
        
        rejected_count = {'not_higher': 0, 'not_adjacent': 0, 'rm_mismatch': 0}
        
        for sede_id, flow_value, travel_time in sede_flows:
            # Get UTP of this sede
            sede_utp = self.graph.get_municipality_utp(sede_id)
            
            # Skip if it's the current UTP
            if sede_utp == current_utp:
                continue
            
            # Check if flow is better than current
            if flow_value <= best_flow:
                rejected_count['not_higher'] += 1
                self.logger.debug(f"  [DEBUG] Mun {mun_id}: Sede {sede_id} (UTP {sede_utp}) rejected - flow {flow_value:.0f} not higher than threshold {best_flow:.0f}")
                continue
            
            # Check if this UTP is adjacent
            if not self._is_adjacent_to_utp(mun_id, sede_utp):
                rejected_count['not_adjacent'] += 1
                self.logger.debug(f"  [DEBUG] Mun {mun_id}: Sede {sede_id} (UTP {sede_utp}) rejected - not adjacent")
                continue
            
            # Check RM compatibility
            if not self._validate_rm_compatibility(mun_id, sede_utp):
                rejected_count['rm_mismatch'] += 1
                self.logger.debug(f"  [DEBUG] Mun {mun_id}: Sede {sede_id} (UTP {sede_utp}) rejected - RM mismatch")
                continue
            
            # This is better!
            best_sede = sede_id
            best_flow = flow_value
            best_time = travel_time
            best_utp = sede_utp
            self.logger.debug(f"  [DEBUG] Mun {mun_id}: Sede {sede_id} (UTP {sede_utp}) ACCEPTED - flow {flow_value:.0f} viagens ({travel_time:.2f}h)")
        
        if rejected_count['not_higher'] + rejected_count['not_adjacent'] + rejected_count['rm_mismatch'] > 0:
            self.logger.debug(f"  [DEBUG] Mun {mun_id}: Rejected {rejected_count['not_higher']} (flow), {rejected_count['not_adjacent']} (adjacency), {rejected_count['rm_mismatch']} (RM)")
        
        # Return if found better option
        if best_sede:
            if has_flow_to_current:
                reason = f"Higher flow to sede {best_sede}: {best_flow:.0f} viagens ({best_time:.2f}h) vs current sede: {current_sede_flow:.0f}"
            else:
                reason = f"Flow to sede {best_sede}: {best_flow:.0f} viagens ({best_time:.2f}h) - current sede UNREACHABLE"
            return (best_utp, best_flow, reason)
        
        return None
    
    def _get_main_flow_destination(
        self,
        mun_id: int,
        flow_df: pd.DataFrame,
        max_time: float = 2.0
    ) -> Optional[Tuple[int, float, str]]:
        """
        Encontra o município de destino com maior fluxo dentro do limite de tempo.
        
        Args:
            mun_id: ID do município origem
            flow_df: DataFrame de fluxos
            max_time: Tempo máximo de viagem em horas
            
        Returns:
            (dest_mun_id, flow_value, dest_utp_id) ou None se não encontrar
        """
        if flow_df is None or flow_df.empty:
            return None
        
        # Busca todos os fluxos do município origem
        flows = flow_df[flow_df['mun_origem'].astype(int) == int(mun_id)].copy()
        
        if flows.empty:
            return None
        
        # Validar tempo de viagem usando impedância
        valid_flows = []
        for _, row in flows.iterrows():
            dest_id = int(row['mun_destino'])
            viagens = float(row['viagens'])
            
            # Verificar tempo de viagem
            tempo = self._get_travel_time(mun_id, dest_id)
            
            if tempo is None:
                continue  # Sem dados de tempo, ignora
            
            if tempo > max_time:
                continue  # Tempo maior que o limite
            
            # Buscar UTP do destino
            dest_utp = self.graph.get_municipality_utp(dest_id)
            if dest_utp and dest_utp != "NAO_ENCONTRADO" and dest_utp != "SEM_UTP":
                valid_flows.append((dest_id, viagens, dest_utp, tempo))
        
        if not valid_flows:
            return None
        
        # Ordena por fluxo (descendente) e retorna o maior
        valid_flows.sort(key=lambda x: -x[1])
        dest_id, flow_value, dest_utp, tempo = valid_flows[0]
        
        return (dest_id, flow_value, dest_utp)
    
    def _reallocate_by_main_flow(
        self,
        flow_df: pd.DataFrame,
        gdf: gpd.GeoDataFrame
    ) -> int:
        """
        Realoca municípios de fronteira sem fluxo para sedes, baseado no fluxo principal.
        
        Esta etapa trata municípios que:
        1. Estão na fronteira (adjacentes a outras UTPs)
        2. Não têm fluxo para nenhuma sede
        3. Têm fluxo principal para outro município
        
        Valida adjacência e regras de RM antes de realocar.
        
        Returns:
            Número de realocações realizadas
        """
        self.logger.info("\n" + "="*80)
        self.logger.info("STEP: Realocação por Fluxo Principal (municípios sem fluxo para sedes)")
        self.logger.info("="*80)
        
        changes = 0
        
        # Identificar municípios de fronteira
        border_municipalities = self._identify_poorly_connected_municipalities(flow_df, gdf)
        
        relocations = []
        
        for utp_id, mun_set in border_municipalities.items():
            for mun_id in mun_set:
                nm_mun = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', str(mun_id))
                
                # Verificar se tem fluxo para alguma sede
                sede_flows = self._get_flows_to_sedes(mun_id, flow_df, max_time=2.0)
                
                if sede_flows:
                    # Tem fluxo para sedes, já foi tratado na etapa anterior
                    continue
                
                self.logger.debug(f"  [SEM FLUXO PARA SEDES] {nm_mun} ({mun_id})")
                
                # Buscar fluxo principal para qualquer município
                main_flow = self._get_main_flow_destination(mun_id, flow_df, max_time=2.0)
                
                if not main_flow:
                    self.logger.debug(f"    [REJEITADO] Sem fluxo principal válido")
                    continue
                
                dest_mun_id, flow_value, target_utp = main_flow
                dest_nm = self.graph.hierarchy.nodes.get(dest_mun_id, {}).get('name', str(dest_mun_id))
                
                # Validação 1: Não mover para a mesma UTP
                if target_utp == utp_id:
                    self.logger.debug(f"    [REJEITADO] Fluxo para mesma UTP ({target_utp})")
                    continue
                
                # Validação 2: Adjacência
                if not self._is_adjacent_to_utp(mun_id, target_utp):
                    self.logger.debug(f"    [REJEITADO] UTP {target_utp} não é adjacente")
                    continue
                
                # Validação 3: Regras de RM (INVIOLÁVEIS)
                if not self._validate_rm_compatibility(mun_id, target_utp):
                    self.logger.debug(f"    [REJEITADO] Incompatibilidade de RM")
                    continue
                
                # Município aprovado para realocação
                relocations.append({
                    'mun_id': mun_id,
                    'nm_mun': nm_mun,
                    'origin_utp': utp_id,
                    'target_utp': target_utp,
                    'dest_mun_id': dest_mun_id,
                    'dest_nm': dest_nm,
                    'flow_value': flow_value
                })
        
        # Executar realocações
        if relocations:
            self.logger.info(f"\n📦 Executando {len(relocations)} realocações por fluxo principal...")
            
            for rel in relocations:
                self.logger.info(
                    f"  ✅ {rel['nm_mun']} ({rel['mun_id']}): "
                    f"{rel['origin_utp']} → {rel['target_utp']}"
                )
                self.logger.info(
                    f"     Fluxo principal: {rel['flow_value']:.0f} viagens para "
                    f"{rel['dest_nm']} ({rel['dest_mun_id']})"
                )
                
                # Executar movimento
                self.graph.move_municipality(rel['mun_id'], rel['target_utp'])
                changes += 1
        else:
            self.logger.info("  ℹ️ Nenhum município elegível para realocação por fluxo principal")
        
        self.logger.info(f"\n✅ Realocação por fluxo principal concluída: {changes} mudanças")
        self.logger.info("="*80 + "\n")
        
        return changes
    
    def run_border_validation(
        self,
        flow_df: pd.DataFrame,
        gdf: gpd.GeoDataFrame,
        max_iterations: int = 50
    ) -> int:
        """
        Main border validation loop.
        
        Returns:
            Number of total changes made
        """
        self.logger.info("\n" + "="*80)
        self.logger.info("BORDER VALIDATOR V2 - Sede-Centric Approach")
        self.logger.info("="*80)
        
        # Build adjacency graph
        self._build_adjacency_graph(gdf)
        
        total_changes = 0
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            self.logger.info(f"\n--- ITERATION {iteration} ---")
            
            # Step 1: Identify poorly connected municipalities
            poorly_connected = self._identify_poorly_connected_municipalities(flow_df, gdf)
            
            if not poorly_connected:
                self.logger.info("✅ No poorly connected municipalities found. Convergence achieved!")
                break
            
            # Step 2: Find better UTPs for each poorly connected municipality
            relocations = []
            
            for utp_id, mun_set in poorly_connected.items():
                for mun_id in mun_set:
                    nm_mun = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', str(mun_id))
                    
                    # Find better UTP
                    result = self._find_better_utp(mun_id, utp_id, flow_df)
                    
                    if result:
                        target_utp, flow_value, reason = result
                        relocations.append((mun_id, nm_mun, utp_id, target_utp, flow_value, reason))
            
            if not relocations:
                self.logger.info("✅ No valid relocations found. Convergence achieved!")
                break
            
            # Step 3: Execute relocations
            self.logger.info(f"\n📦 Executing {len(relocations)} relocations...")
            
            for mun_id, nm_mun, origin_utp, target_utp, flow_value, reason in relocations:
                self.logger.info(f"  ✅ {nm_mun} ({mun_id}): {origin_utp} → {target_utp}")
                self.logger.info(f"     Reason: {reason}")
                
                # Move municipality
                self.graph.move_municipality(mun_id, target_utp)
                total_changes += 1
            
            self.logger.info(f"\n📊 Iteration {iteration} complete: {len(relocations)} changes")
        
        # NOVA ETAPA: Realocação por fluxo principal
        # Trata municípios de fronteira que não têm fluxo para sedes
        changes_main_flow = self._reallocate_by_main_flow(flow_df, gdf)
        total_changes += changes_main_flow
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"BORDER VALIDATION COMPLETE")
        self.logger.info(f"  Total iterations: {iteration}")
        self.logger.info(f"  Sede-based relocations: {total_changes - changes_main_flow}")
        self.logger.info(f"  Main flow relocations: {changes_main_flow}")
        self.logger.info(f"  Total changes: {total_changes}")
        self.logger.info(f"{'='*80}\n")
        
        return total_changes
