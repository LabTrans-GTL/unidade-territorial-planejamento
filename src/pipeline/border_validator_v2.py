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
        impedance_df: pd.DataFrame = None,
        consolidator = None
    ):
        self.graph = graph
        self.validator = validator
        self.logger = logging.getLogger("GeoValida.BorderValidatorV2")
        self.data_dir = data_dir or Path(__file__).parent.parent.parent / "data" / "03_processed"
        self.adjacency_graph = None
        self.impedance_df = impedance_df
        # Use injected consolidator if provided; lazy-create fallback otherwise
        self.consolidator = consolidator
        
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
        
        # Self-join
        sjoin = gpd.sjoin(gdf_buff, gdf_buff, how='inner', predicate='intersects')
        
        edges = []
        for idx, row in sjoin.iterrows():
            left = int(row['CD_MUN_left'])
            right = int(row['CD_MUN_right'])
            if left != right:
                edges.append((left, right))
        
        self.adjacency_graph.add_edges_from(edges)
        self.logger.info(f"Adjacency graph built: {self.adjacency_graph.number_of_nodes()} nodes, {self.adjacency_graph.number_of_edges()} edges")
    
    def _get_mun_rm(self, mun_id: int) -> Optional[str]:
        """Gets RM of a municipality from graph."""
        if self.graph.hierarchy.has_node(mun_id):
            return self.graph.hierarchy.nodes[mun_id].get('regiao_metropolitana')
        return None
    
    def _validate_rm_compatibility(self, mun_id: int, target_utp: str) -> bool:
        """Validates RM rules for municipality relocation."""
        return self.validator.validate_rm_compatibility(mun_id, target_utp)
    
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
            # Get sede
            sede = self.graph.utp_seeds.get(utp_id)
            if not sede:
                continue
            
            # Get all municipalities in this UTP
            muns_in_utp = list(self.graph.hierarchy.successors(utp_node))
            is_unitary = len(muns_in_utp) == 1
            
            border_in_utp = set()
            
            for mun_id in muns_in_utp:
                mun_int = int(mun_id)
                
                # Skip if it's the sede AND NOT A UNITARY UTP
                # If it's a unitary UTP, we ALLOW evaluating the sede for relocation
                if mun_int == int(sede) and not is_unitary:
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
                
                # Add all border municipalities
                if is_border:
                    border_in_utp.add(mun_int)
            
            if border_in_utp:
                border_municipalities[utp_id] = border_in_utp
                self.logger.info(f"  UTP {utp_id}: {len(border_in_utp)} border municipalities")
        
        return border_municipalities
    
    def _get_aggregated_flows_to_utps(self, mun_id: int, flow_df: pd.DataFrame, max_time: float = 2.0) -> Dict[str, float]:
        """
        Calculates total flow from municipality to EACH UTP (aggregated).
        Only considers destinations within max_time (2h).
        
        Returns:
            Dict mapping utp_id -> total_flow_value
        """
        if flow_df is None or flow_df.empty:
            return {}
        
        # Get all flows from this municipality
        flows = flow_df[flow_df['mun_origem'].astype(int) == int(mun_id)].copy()
        if flows.empty:
            return {}
        
        utp_totals = {}
        for _, row in flows.iterrows():
            dest_id = int(row['mun_destino'])
            viagens = float(row['viagens'])
            
            # Check travel time
            tempo = self._get_travel_time(mun_id, dest_id)
            if tempo is None or tempo > max_time:
                continue
                
            # Get UTP of destination
            dest_utp = self.graph.get_municipality_utp(dest_id)
            if dest_utp:
                utp_totals[dest_utp] = utp_totals.get(dest_utp, 0.0) + viagens
                
        return utp_totals

    def _find_better_utp(
        self,
        mun_id: int,
        current_utp: str,
        flow_df: pd.DataFrame
    ) -> Optional[Tuple[str, float, str]]:
        """
        Finds a better UTP by comparing DIRECT FLOW TO SEDE of each adjacent UTP.

        Rules:
        1. Compare direct flow from mun_id to each candidate UTP's SEDE (within 2h).
        2. STRICT ACCESSIBILITY: The target UTP's SEDE must be reachable within 2h.
        3. ADJACENCY + RM: Must be adjacent and respect RM rules.
        4. Only moves if flow to target sede > flow to current sede.

        Returns:
            (target_utp_id, flow_to_sede, reason) or None
        """
        if flow_df is None or flow_df.empty:
            return None

        # Flow to current sede (baseline)
        current_sede = self.graph.utp_seeds.get(current_utp)
        current_flow_to_sede = 0.0
        if current_sede:
            rows = flow_df[
                (flow_df['mun_origem'].astype(int) == int(mun_id)) &
                (flow_df['mun_destino'].astype(int) == int(current_sede))
            ]
            current_flow_to_sede = float(rows['viagens'].sum()) if not rows.empty else 0.0

        best_utp = None
        best_flow = current_flow_to_sede
        best_reason = ""

        # Evaluate candidate UTPs that are adjacent
        utp_nodes = [n for n, d in self.graph.hierarchy.nodes(data=True) if d.get('type') == 'utp']
        for utp_node in utp_nodes:
            target_utp = utp_node.replace("UTP_", "")
            if target_utp == current_utp:
                continue

            # Rule A: Adjacency
            if not self._is_adjacent_to_utp(mun_id, target_utp):
                continue

            # Rule B: Sede Accessibility (STRICT ≤2h)
            target_sede = self.graph.utp_seeds.get(target_utp)
            if not target_sede:
                continue

            time_to_sede = self._get_travel_time(mun_id, int(target_sede))
            if time_to_sede is None or time_to_sede > 2.0:
                continue

            # Rule C: RM Compatibility
            if not self._validate_rm_compatibility(mun_id, target_utp):
                continue

            # Rule D: Direct flow to target sede must exceed direct flow to current sede
            rows = flow_df[
                (flow_df['mun_origem'].astype(int) == int(mun_id)) &
                (flow_df['mun_destino'].astype(int) == int(target_sede))
            ]
            flow_to_target_sede = float(rows['viagens'].sum()) if not rows.empty else 0.0

            if flow_to_target_sede <= best_flow:
                continue

            # New best!
            best_utp = target_utp
            best_flow = flow_to_target_sede
            best_reason = (
                f"Flow to sede {target_sede} ({flow_to_target_sede:.0f}) > "
                f"flow to current sede {current_sede} ({current_flow_to_sede:.0f}). "
                f"Sede {target_sede} is {time_to_sede:.2f}h away."
            )

        if best_utp:
            return (best_utp, best_flow, best_reason)

            
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
        gdf: gpd.GeoDataFrame,
        is_loop: bool = False
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
        if not is_loop:
            self.logger.info("\n" + "="*80)
            self.logger.info("STEP: Realocação por Fluxo Principal (municípios sem fluxo para sedes)")
            self.logger.info("="*80)
        else:
            self.logger.info("\n--- Sub-Step: Main Flow Reallocation ---")
        
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
                
                # Município aprovado para realocação - Executar IMEDIATAMENTE
                self.logger.info(
                    f"  ✅ {nm_mun} ({mun_id}): "
                    f"{utp_id} → {target_utp}"
                )
                self.logger.info(
                    f"     Fluxo principal: {flow_value:.0f} viagens para "
                    f"{dest_nm} ({dest_mun_id})"
                )
                
                # Executar movimento imediato para prevenir oscilação (ping-pong)
                self.graph.move_municipality(mun_id, target_utp)
                
                # Update GDF for this municipality
                if gdf is not None and 'CD_MUN' in gdf.columns:
                    try:
                        mask = gdf['CD_MUN'].astype(str).str.split('.').str[0] == str(mun_id)
                        gdf.loc[mask, 'UTP_ID'] = str(target_utp)
                    except Exception as e:
                        self.logger.error(f"      Failed to update GDF for {mun_id}: {e}")

                changes += 1
        else:
            if changes == 0:
                self.logger.info("  ℹ️ Nenhum município elegível para realocação por fluxo principal")
        
        if not is_loop:
            self.logger.info(f"\n✅ Realocação por fluxo principal concluída: {changes} mudanças")
            self.logger.info("="*80 + "\n")
        else:
            self.logger.info(f"--- Sub-step: Main Flow Relocation complete: {changes} changes ---")
        
        return changes

    def _identify_municipalities_far_from_sede(
        self,
        gdf: gpd.GeoDataFrame
    ) -> Dict[str, Set[int]]:
        """
        Identifica municípios que estão a >2h da sede da sua UTP atual.

        Estes municípios foram posicionados por fluxo total (consolidator passo 5),
        mas o critério fundamental das UTPs é a acessibilidade à sede em ≤2h.
        A sede em si nunca é avaliada aqui.

        Returns:
            Dict mapeando utp_id → conjunto de mun_ids inacessíveis
        """
        self.logger.info("Verificando acessibilidade à sede (critério ≤2h)...")
        far_from_sede: Dict[str, Set[int]] = {}

        utp_nodes = [n for n, d in self.graph.hierarchy.nodes(data=True) if d.get('type') == 'utp']
        for utp_node in utp_nodes:
            utp_id = utp_node.replace("UTP_", "")
            sede = self.graph.utp_seeds.get(utp_id)
            if not sede:
                continue

            muns_in_utp = list(self.graph.hierarchy.successors(utp_node))
            if len(muns_in_utp) <= 1:
                continue  # unitárias já tratadas pelo consolidator

            for mun_id in muns_in_utp:
                mun_int = int(mun_id)
                if mun_int == int(sede):
                    continue  # sede nunca avaliada

                time_to_sede = self._get_travel_time(mun_int, int(sede))
                if time_to_sede is None or time_to_sede > 2.0:
                    # Município fora do alcance da sua própria sede
                    nm = self.graph.hierarchy.nodes.get(mun_int, {}).get('name', str(mun_int))
                    self.logger.debug(
                        f"  [{utp_id}] {nm} ({mun_int}): "
                        f"sede {sede} a {'N/A' if time_to_sede is None else f'{time_to_sede:.2f}'}h"
                    )
                    far_from_sede.setdefault(utp_id, set()).add(mun_int)

        total = sum(len(v) for v in far_from_sede.values())
        self.logger.info(f"  Municípios a >2h da sua sede: {total} (em {len(far_from_sede)} UTPs)")
        return far_from_sede

    def _identify_disconnected_from_sede(
        self,
        gdf: gpd.GeoDataFrame
    ) -> Dict[str, Set[int]]:
        """
        Identifica municípios que não possuem caminho de adjacência geográfica
        até a sede DENTRO da própria UTP.

        Isso ocorre quando municípios foram absorvidos por uma UTP grande mas não
        estão conectados ao corpo principal dela via municípios vizinhos da mesma UTP.
        Ex: UTP 125 onde Cajati (sede) é um ilhota rodeada por municipípios de outras UTPs,
        enquanto Itariri/Miracatu/Pedro de Toledo estão em outro componente.

        Returns:
            Dict mapeando utp_id → conjunto de mun_ids desconectados da sede
        """
        self.logger.info("Verificando conectividade intra-UTP (municipípios isolados da sede)...")
        disconnected: Dict[str, Set[int]] = {}

        if gdf is None or gdf.empty:
            return disconnected

        # Preparar GDF para join espacial (uma única vez)
        cd_col = 'CD_MUN' if 'CD_MUN' in gdf.columns else 'cd_mun'
        gdf_m = gdf[[cd_col, 'geometry']].copy().to_crs(epsg=3857)
        gdf_m['_cd_int'] = gdf_m[cd_col].astype(str).str.split('.').str[0].astype(int)
        gdf_buff = gdf_m.copy()
        gdf_buff['geometry'] = gdf_buff.geometry.buffer(100)  # 100m para gaps topológicos

        # Pré-build do sjoin global para evitar repetir por UTP
        sjoin_all = gpd.sjoin(gdf_buff[['_cd_int', 'geometry']],
                              gdf_buff[['_cd_int', 'geometry']],
                              how='inner', predicate='intersects')
        sjoin_all = sjoin_all[sjoin_all['_cd_int_left'] != sjoin_all['_cd_int_right']]

        utp_nodes = [n for n, d in self.graph.hierarchy.nodes(data=True) if d.get('type') == 'utp']
        for utp_node in utp_nodes:
            utp_id = utp_node.replace("UTP_", "")
            sede = self.graph.utp_seeds.get(utp_id)
            if not sede:
                continue

            muns_in_utp = [int(m) for m in self.graph.hierarchy.successors(utp_node)]
            if len(muns_in_utp) <= 1:
                continue  # unitárias não aplicam

            mun_set = set(muns_in_utp)

            # Construir sub-grafo de adjacência INTRA-UTP
            G = nx.Graph()
            G.add_nodes_from(mun_set)

            # Filtrar sjoin para apenas arestas entre municípios desta UTP
            mask = (
                sjoin_all['_cd_int_left'].isin(mun_set) &
                sjoin_all['_cd_int_right'].isin(mun_set)
            )
            for _, row in sjoin_all[mask].iterrows():
                G.add_edge(int(row['_cd_int_left']), int(row['_cd_int_right']))

            sede_int = int(sede)
            if not G.has_node(sede_int):
                continue

            # Municipios conectados à sede dentro da UTP
            connected = nx.node_connected_component(G, sede_int)
            isolated = mun_set - connected - {sede_int}

            if isolated:
                self.logger.info(
                    f"  UTP {utp_id}: {len(isolated)} municípios desconectados da sede {sede}: "
                    f"{[self.graph.hierarchy.nodes.get(m, {}).get('name', str(m)) for m in isolated]}"
                )
                disconnected[utp_id] = isolated

        total = sum(len(v) for v in disconnected.values())
        self.logger.info(f"  Total desconectados: {total} (em {len(disconnected)} UTPs)")
        return disconnected

    def run_border_validation(
        self,
        flow_df: pd.DataFrame,
        gdf: gpd.GeoDataFrame,
        max_iterations: int = 50,
        map_gen: Any = None
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
            iteration_changes = 0
            self.logger.info(f"\n--- ITERATION {iteration} ---")
            
            # Step 1: Identify poorly connected municipalities
            poorly_connected = self._identify_poorly_connected_municipalities(flow_df, gdf)
            
            if not poorly_connected:
                self.logger.info("✅ No poorly connected municipalities found. Convergence achieved!")
                break
            
            # Step 2: Find and execute relocations sequentially
            for utp_id, mun_set in poorly_connected.items():
                for mun_id in mun_set:
                    nm_mun = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', str(mun_id))
                    
                    # Re-check current UTP (might have changed in this same iteration)
                    current_utp = self.graph.get_municipality_utp(mun_id)
                    
                    # Find better UTP
                    result = self._find_better_utp(mun_id, current_utp, flow_df)
                    
                    if result:
                        target_utp, flow_value, reason = result
                        self.logger.info(f"  ✅ {nm_mun} ({mun_id}): {current_utp} → {target_utp}")
                        self.logger.info(f"     Reason: {reason}")
                        
                        # Move municipality IMMEDIATELY to prevent oscillation
                        self.graph.move_municipality(mun_id, target_utp)
                        
                        # Update GDF for this municipality
                        if gdf is not None and 'CD_MUN' in gdf.columns:
                            try:
                                mask = gdf['CD_MUN'].astype(str).str.split('.').str[0] == str(mun_id)
                                gdf.loc[mask, 'UTP_ID'] = str(target_utp)
                            except Exception as e:
                                self.logger.error(f"      Failed to update GDF for {mun_id}: {e}")

                        total_changes += 1
                        iteration_changes += 1

            # ETAPA 2B: Realocação por fluxo principal (Trata fronteiras sem fluxo para sedes)
            # Integrado no loop para permitir convergência
            changes_main_flow = self._reallocate_by_main_flow(flow_df, gdf, is_loop=True)
            total_changes += changes_main_flow
            iteration_changes += changes_main_flow
            
            if iteration_changes == 0:
                self.logger.info("✅ Convergence achieved: No more changes in this iteration.")
                break
        
        self.logger.info(f"\n📊 Iterations finished after {iteration} cycles.")

        # ===== ETAPA FINAL: Acessibilidade à Sede (≤2h) =====
        # Identifica municípios que estão a >2h da sede atual e tenta movê-los
        # para UTPs adjacentes cuja sede seja acessível em ≤2h.
        # Esse critério é o fator determinante das UTPs e não é verificado
        # pelo consolidator (passos 5/7), por isso é feito aqui.
        self.logger.info("\n" + "-"*60)
        self.logger.info("ETAPA FINAL: Verificação de Acessibilidade à Sede (≤2h)")
        self.logger.info("-"*60)

        far_from_sede = self._identify_municipalities_far_from_sede(gdf)
        changes_sede_access = 0

        for utp_id, mun_set in far_from_sede.items():
            for mun_id in mun_set:
                # Re-verificar UTP atual (pode ter mudado nesta etapa)
                current_utp = self.graph.get_municipality_utp(mun_id)
                nm_mun = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', str(mun_id))

                # Verificar se o município já está acessível à sede atual (mudou de UTP)
                current_sede = self.graph.utp_seeds.get(current_utp)
                if current_sede:
                    t = self._get_travel_time(mun_id, int(current_sede))
                    if t is not None and t <= 2.0:
                        continue  # Já está ok, pula

                # Tentar encontrar uma UTP adjacente com sede acessível em ≤2h
                result = self._find_better_utp(mun_id, current_utp, flow_df)
                if result:
                    target_utp, flow_value, reason = result
                    self.logger.info(
                        f"  ✅ [SEDE-ACCESS] {nm_mun} ({mun_id}): "
                        f"{current_utp} → {target_utp} | {reason}"
                    )
                    self.graph.move_municipality(mun_id, target_utp)
                    if gdf is not None and 'CD_MUN' in gdf.columns:
                        try:
                            mask = gdf['CD_MUN'].astype(str).str.split('.').str[0] == str(mun_id)
                            gdf.loc[mask, 'UTP_ID'] = str(target_utp)
                        except Exception as e:
                            self.logger.error(f"      Failed GDF update for {mun_id}: {e}")
                    changes_sede_access += 1
                else:
                    self.logger.warning(
                        f"  ⚠️ [SEDE-ACCESS] {nm_mun} ({mun_id}): "
                        f"UTP {current_utp} — sem alternativa acessível em ≤2h"
                    )

        total_changes += changes_sede_access
        self.logger.info(f"  Realocações por acessibilidade à sede: {changes_sede_access}")

        # ===== ETAPA FINAL 2: Desconectividade Intra-UTP =====
        # Identifica municípios que não possuem caminho de adjacência à sede
        # dentro da própria UTP e tenta relocá-los.
        # Após a relocação, a UTP pode ficar unitária e ser consolidada
        # pelo passo 8.x (consolidator) já existente no manager.
        self.logger.info("\n" + "-"*60)
        self.logger.info("ETAPA FINAL 2: Municípios Desconectados da Sede (Intra-UTP)")
        self.logger.info("-"*60)

        disconnected = self._identify_disconnected_from_sede(gdf)
        changes_disconnected = 0

        for utp_id, mun_set in disconnected.items():
            for mun_id in mun_set:
                current_utp = self.graph.get_municipality_utp(mun_id)
                nm_mun = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', str(mun_id))

                # Tentar relocar usando fluxo para sede (já valida <=2h + adjacência + RM)
                result = self._find_better_utp(mun_id, current_utp, flow_df)
                if result:
                    target_utp, flow_value, reason = result
                    self.logger.info(
                        f"  ✅ [DISCONNECTED] {nm_mun} ({mun_id}): "
                        f"{current_utp} → {target_utp} | {reason}"
                    )
                    self.graph.move_municipality(mun_id, target_utp)
                    if gdf is not None and 'CD_MUN' in gdf.columns:
                        try:
                            mask = gdf['CD_MUN'].astype(str).str.split('.').str[0] == str(mun_id)
                            gdf.loc[mask, 'UTP_ID'] = str(target_utp)
                        except Exception as e:
                            self.logger.error(f"      Failed GDF update for {mun_id}: {e}")
                    changes_disconnected += 1
                else:
                    # Fallback: tentar mover para qualquer UTP adjacente (mesmo sem fluxo superior)
                    # Pois estar desconectado geograficamente já justifica a mudança
                    nm_mun = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', str(mun_id))
                    neighbors = list(self.adjacency_graph[mun_id]) if self.adjacency_graph and mun_id in self.adjacency_graph else []
                    best_fallback = None
                    for neighbor in neighbors:
                        neighbor_utp = self.graph.get_municipality_utp(neighbor)
                        if neighbor_utp == current_utp:
                            continue
                        neighbor_sede = self.graph.utp_seeds.get(neighbor_utp)
                        if not neighbor_sede:
                            continue
                        t = self._get_travel_time(mun_id, int(neighbor_sede))
                        if t is None or t > 2.0:
                            continue
                        if not self._validate_rm_compatibility(mun_id, neighbor_utp):
                            continue
                        best_fallback = neighbor_utp
                        break

                    if best_fallback:
                        self.logger.info(
                            f"  ✅ [DISCONNECTED-FALLBACK] {nm_mun} ({mun_id}): "
                            f"{current_utp} → {best_fallback} (via adjacência, sem fluxo superior)"
                        )
                        self.graph.move_municipality(mun_id, best_fallback)
                        if gdf is not None and 'CD_MUN' in gdf.columns:
                            try:
                                mask = gdf['CD_MUN'].astype(str).str.split('.').str[0] == str(mun_id)
                                gdf.loc[mask, 'UTP_ID'] = str(best_fallback)
                            except Exception as e:
                                self.logger.error(f"      Failed GDF update for {mun_id}: {e}")
                        changes_disconnected += 1
                    else:
                        self.logger.warning(
                            f"  ⚠️ [DISCONNECTED] {nm_mun} ({mun_id}): sem alternativa válida"
                        )

        total_changes += changes_disconnected
        self.logger.info(f"  Realocações por desconectividade: {changes_disconnected}")

        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"BORDER VALIDATION COMPLETE")
        self.logger.info(f"  Total iterations: {iteration}")
        self.logger.info(f"  Sede-based relocations: {total_changes - changes_main_flow - changes_sede_access - changes_disconnected}")
        self.logger.info(f"  Main flow relocations: {changes_main_flow}")
        self.logger.info(f"  Sede accessibility relocations: {changes_sede_access}")
        self.logger.info(f"  Disconnected municipality relocations: {changes_disconnected}")
        self.logger.info(f"  Total changes: {total_changes}")
        self.logger.info(f"{'='*80}\n")
        
        return total_changes
