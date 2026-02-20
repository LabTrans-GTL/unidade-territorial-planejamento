# src/pipeline/isolated_municipality_resolver.py
"""
Step 8.5: Isolated Municipality Resolution

Identifies municipalities that are disconnected from their UTP's sede
(no path through adjacent municipalities) and reconnects them to appropriate
adjacent UTPs based on flow, travel time, and REGIC scoring.
"""

import logging
import pandas as pd
import geopandas as gpd
import networkx as nx
from typing import Dict, Set, List, Tuple, Optional
from pathlib import Path

from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator
from src.interface.consolidation_manager import ConsolidationManager


class IsolatedMunicipalityResolver:
    """
    Resolves municipalities that are disconnected from their UTP's sede.
    
    This typically happens after sede consolidation when only the sede moves
    but other municipalities remain in the origin UTP without a connection.
    """
    
    def __init__(
        self,
        graph: TerritorialGraph,
        validator: TerritorialValidator,
        consolidation_manager: ConsolidationManager
    ):
        self.graph = graph
        self.validator = validator
        self.consolidation_manager = consolidation_manager
        self.logger = logging.getLogger("GeoValida.IsolatedResolver")
        self.adjacency_graph = None
        
    def _build_adjacency_graph(self, gdf: gpd.GeoDataFrame):
        """Builds a NetworkX graph representing spatial adjacency of municipalities."""
        self.logger.info("Building spatial adjacency graph for connectivity analysis...")
        self.adjacency_graph = nx.Graph()
        
        if gdf is None or gdf.empty:
            return
        
        # Ensure we have geometries
        gdf_valid = gdf[gdf.geometry.notna()]
        
        # Project to metric CRS for buffering (EPSG:3857 is fast)
        gdf_valid_metric = gdf_valid.to_crs(epsg=3857)
        
        # Use a small buffer (100 meters) to handle topology gaps
        buffer_val = 100.0
        gdf_buff = gdf_valid_metric.copy()
        gdf_buff['geometry'] = gdf_buff.geometry.buffer(buffer_val)
        
        # Self-join to find neighbors
        sjoin = gpd.sjoin(gdf_buff, gdf_buff, how='inner', predicate='intersects')
        
        edges = []
        for idx, row in sjoin.iterrows():
            left = int(row['CD_MUN_left'])
            right = int(row['CD_MUN_right'])
            if left != right:
                edges.append((left, right))
        
        self.adjacency_graph.add_edges_from(edges)
        self.logger.info(f"Adjacency graph built: {self.adjacency_graph.number_of_nodes()} nodes, {self.adjacency_graph.number_of_edges()} edges.")
    
    def identify_isolated_municipalities(self, gdf: gpd.GeoDataFrame) -> Dict[str, Set[int]]:
        """
        Identifies municipalities that have no path to their UTP's sede.
        
        Args:
            gdf: GeoDataFrame with municipality geometries
            
        Returns:
            Dict mapping UTP_ID -> Set of isolated municipality IDs
        """
        isolated_by_utp = {}
        
        # Build adjacency graph if not already built
        if self.adjacency_graph is None:
            self._build_adjacency_graph(gdf)
        
        # Get all UTPs
        utp_nodes = [n for n, d in self.graph.hierarchy.nodes(data=True) if d.get('type') == 'utp']
        
        for utp_node in utp_nodes:
            utp_id = utp_node.replace("UTP_", "")
            
            # Get sede for this UTP
            sede = self.graph.utp_seeds.get(utp_id)
            
            # Get all municipalities in this UTP
            muns_in_utp = list(self.graph.hierarchy.successors(utp_node))
            
            if not muns_in_utp:
                continue
            
            # If no sede, all municipalities are "isolated"
            if not sede or sede not in muns_in_utp:
                self.logger.warning(f"UTP {utp_id} has no sede! All {len(muns_in_utp)} municipalities are isolated.")
                isolated_by_utp[utp_id] = set(int(m) for m in muns_in_utp)
                continue
            
            # Build subgraph of only municipalities in this UTP
            utp_subgraph = nx.Graph()
            for mun in muns_in_utp:
                mun_int = int(mun)
                if mun_int in self.adjacency_graph:
                    # Add edges only to other municipalities in the same UTP
                    for neighbor in self.adjacency_graph[mun_int]:
                        if neighbor in [int(m) for m in muns_in_utp]:
                            utp_subgraph.add_edge(mun_int, neighbor)
            
            # Find connected components
            if utp_subgraph.number_of_nodes() == 0:
                # No adjacency data
                continue
            
            components = list(nx.connected_components(utp_subgraph))
            
            if len(components) <= 1:
                # All connected, no islands
                continue
            
            # Find which component contains the sede
            sede_int = int(sede)
            sede_component = None
            for comp in components:
                if sede_int in comp:
                    sede_component = comp
                    break
            
            if sede_component is None:
                self.logger.warning(f"UTP {utp_id}: Sede {sede} not in any component!")
                continue
            
            # All other components are isolated
            isolated = set()
            for comp in components:
                if comp != sede_component:
                    isolated.update(comp)
            
            if isolated:
                isolated_by_utp[utp_id] = isolated
                self.logger.warning(f"UTP {utp_id}: Found {len(isolated)} isolated municipalities (sede: {sede})")
        
        return isolated_by_utp
    
    def _get_ranked_flows(self, mun_id: int, flow_df: pd.DataFrame, max_time: float = 2.0, top_n: int = 5) -> List[Tuple[int, float, float]]:
        """
        Gets top N ranked flows from a municipality.
        
        Args:
            mun_id: Origin municipality ID
            flow_df: Flow dataframe
            max_time: Maximum travel time in hours
            top_n: Number of top flows to return
            
        Returns:
            List of (dest_mun_id, flow_value, travel_time) tuples, sorted by flow descending
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
        
        # Sort by flow descending and take top N
        flows = flows.nlargest(top_n, 'viagens')
        
        results = []
        for _, row in flows.iterrows():
            dest_mun = int(row['mun_destino'])
            viagens = float(row['viagens'])
            tempo = float(row.get('tempo_viagem', 0))
            results.append((dest_mun, viagens, tempo))
        
        return results
    
    def find_reconnection_candidates(
        self,
        mun_id: int,
        flow_df: pd.DataFrame,
        gdf: gpd.GeoDataFrame
    ) -> List[Dict]:
        """
        Finds and ranks potential reconnection candidates for an isolated municipality.
        
        Criteria:
        1. UTP must be adjacent to the municipality
        2. Either:
           - Sede is reachable within 2h, OR
           - Municipality has ranked flow to any municipality in that UTP within 2h
        3. RM rules must be satisfied
        
        Args:
            mun_id: Isolated municipality ID
            flow_df: Flow dataframe
            gdf: GeoDataFrame
            
        Returns:
            List of candidate dicts, sorted by score (best first)
        """
        if self.adjacency_graph is None:
            self._build_adjacency_graph(gdf)
        
        # Get adjacent municipalities
        if mun_id not in self.adjacency_graph:
            self.logger.warning(f"Municipality {mun_id} not in adjacency graph!")
            return []
        
        adjacent_muns = list(self.adjacency_graph[mun_id])
        
        # Get UTPs of adjacent municipalities
        adjacent_utps = set()
        for adj_mun in adjacent_muns:
            utp = self.graph.get_municipality_utp(adj_mun)
            if utp:
                adjacent_utps.add(utp)
        
        # Get current UTP to exclude it
        current_utp = self.graph.get_municipality_utp(mun_id)
        adjacent_utps.discard(current_utp)
        
        if not adjacent_utps:
            return []
        
        # Get ranked flows from this municipality
        ranked_flows = self._get_ranked_flows(mun_id, flow_df, max_time=2.0, top_n=5)
        
        candidates = []
        
        for utp_id in adjacent_utps:
            # Check RM rules
            if not self._validate_rm_rules(mun_id, utp_id):
                continue
            
            # Get sede of candidate UTP
            sede = self.graph.utp_seeds.get(utp_id)
            
            # Get all municipalities in candidate UTP
            muns_in_utp = [int(m) for m in self.graph.hierarchy.successors(f"UTP_{utp_id}")]
            
            # Scoring system
            score = 0.0
            reason_parts = []
            
            # Check if sede is reachable within 2h
            if sede:
                # Try to find flow to sede
                sede_flow = [f for f in ranked_flows if f[0] == int(sede)]
                if sede_flow:
                    score += sede_flow[0][1]  # Add flow value
                    reason_parts.append(f"Fluxo para sede: {sede_flow[0][1]:.0f} viagens")
            
            # Check for flows to ANY municipality in this UTP (ranked flows <= 2h)
            utp_flows = [f for f in ranked_flows if f[0] in muns_in_utp]
            if utp_flows:
                total_utp_flow = sum(f[1] for f in utp_flows)
                score += total_utp_flow
                reason_parts.append(f"Fluxo total para UTP: {total_utp_flow:.0f} viagens")
            
            # REGIC bonus
            regic_score = self.validator.get_utp_regic_score(utp_id)
            regic_bonus = 1000 / (regic_score + 1)  # Lower REGIC rank = higher bonus
            score += regic_bonus
            
            if score > 0:
                candidates.append({
                    'utp_id': utp_id,
                    'score': score,
                    'reason': ' | '.join(reason_parts) if reason_parts else 'REGIC fallback',
                    'regic_rank': regic_score,
                    'flows_to_utp': utp_flows
                })
        
        # Sort by score descending
        candidates.sort(key=lambda x: -x['score'])
        
        return candidates
    
    def _validate_rm_rules(self, mun_id: int, dest_utp: str) -> bool:
        """
        Validates RM consistency rules.
        
        Rules:
        - If Mun has RM and Dest UTP has different RM -> REJECT
        - Otherwise -> ACCEPT
        """
        # Get municipality RM from graph nodes
        mun_rm = None
        if self.graph.hierarchy.has_node(mun_id):
            mun_rm = self.graph.hierarchy.nodes[mun_id].get('regiao_metropolitana')
        
        # Get destination UTP RM
        utp_rm = self.validator.get_rm_of_utp(dest_utp)
        
        # Both without RM -> OK
        if not mun_rm and not utp_rm:
            return True
        
        # Same RM -> OK
        if mun_rm == utp_rm:
            return True
        
        # Different RM -> REJECT
        return False
    
    def _find_adjacent_utps_fallback(self, mun_id: int, respect_rm: bool = True) -> List[Dict]:
        """
        Fallback strategy: Find ANY adjacent UTP for reconnection.
        
        Used when flow-based reconnection fails. Simply connects to any
        geographically adjacent UTP, optionally respecting RM rules.
        
        Args:
            mun_id: Isolated municipality ID
            respect_rm: If True, only return RM-compatible UTPs
            
        Returns:
            List of candidate dicts with UTP info
        """
        if self.adjacency_graph is None or mun_id not in self.adjacency_graph:
            return []
        
        # Get adjacent municipalities
        adjacent_muns = list(self.adjacency_graph[mun_id])
        
        # Get their UTPs
        adjacent_utps = set()
        for adj_mun in adjacent_muns:
            utp = self.graph.get_municipality_utp(adj_mun)
            if utp and utp != self.graph.get_municipality_utp(mun_id):
                adjacent_utps.add(utp)
        
        if not adjacent_utps:
            return []
        
        # Filter by RM if requested
        candidates = []
        for utp_id in adjacent_utps:
            # Check RM rules if requested
            if respect_rm and not self._validate_rm_rules(mun_id, utp_id):
                continue
            
            # Get REGIC score for ranking
            regic_score = self.validator.get_utp_regic_score(utp_id)
            
            # Simple scoring: prefer higher REGIC (lower number)
            score = 1000 / (regic_score + 1)
            
            candidates.append({
                'utp_id': utp_id,
                'score': score,
                'reason': f'Adjacent fallback ({"RM-compatible" if respect_rm else "RM-ignored"})',
                'regic_rank': regic_score,
                'flows_to_utp': []  # No flow data in fallback
            })
        
        # Sort by REGIC score (lower is better)
        candidates.sort(key=lambda x: x['regic_rank'])
        
        return candidates
    
    def run_isolated_resolution(
        self,
        flow_df: pd.DataFrame,
        gdf: gpd.GeoDataFrame,
        map_gen
    ) -> int:
        """
        Main process to identify and resolve isolated municipalities.
        
        Args:
            flow_df: Flow dataframe
            gdf: GeoDataFrame
            map_gen: Map generator for syncing
            
        Returns:
            Number of municipalities reconnected
        """
        self.logger.info("Step 8.5: Starting Isolated Municipality Resolution...")
        
        # Build adjacency graph
        self._build_adjacency_graph(gdf)
        
        # Identify isolated municipalities
        isolated_by_utp = self.identify_isolated_municipalities(gdf)
        
        if not isolated_by_utp:
            self.logger.info("✅ No isolated municipalities found!")
            return 0
        
        total_isolated = sum(len(muns) for muns in isolated_by_utp.values())
        self.logger.info(f"Found {total_isolated} isolated municipalities across {len(isolated_by_utp)} UTPs")
        
        # Resolve each isolated municipality
        total_reconnected = 0
        total_unresolved = 0
        
        for utp_id, isolated_muns in isolated_by_utp.items():
            self.logger.info(f"\n--- Resolving {len(isolated_muns)} isolated municipalities in UTP {utp_id} ---")
            
            for mun_id in isolated_muns:
                nm_mun = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', str(mun_id))
                
                # Find reconnection candidates (flow-based)
                candidates = self.find_reconnection_candidates(mun_id, flow_df, gdf)
                
                # FALLBACK 1: If no flow-based candidates, try adjacent UTPs respecting RM
                if not candidates:
                    self.logger.info(f"  🔄 {nm_mun} ({mun_id}): No flow candidates, trying adjacent UTPs (RM-compatible)...")
                    candidates = self._find_adjacent_utps_fallback(mun_id, respect_rm=True)
                
                # FALLBACK 2: If still no candidates, try adjacent UTPs ignoring RM
                if not candidates:
                    self.logger.info(f"  🔄 {nm_mun} ({mun_id}): No RM-compatible adjacents, trying ANY adjacent UTP...")
                    candidates = self._find_adjacent_utps_fallback(mun_id, respect_rm=False)
                
                if not candidates:
                    self.logger.warning(f"  ❌ {nm_mun} ({mun_id}): TRULY isolated - no adjacent UTPs found!")
                    total_unresolved += 1
                    continue
                
                # Use best candidate
                best = candidates[0]
                target_utp = best['utp_id']
                
                self.logger.info(f"  ✅ Reconnecting: {nm_mun} ({mun_id}) -> UTP {target_utp}")
                self.logger.info(f"     Reason: {best['reason']}")
                self.logger.info(f"     Score: {best['score']:.2f}, REGIC: {best['regic_rank']}")
                
                # Move municipality
                self.graph.move_municipality(mun_id, target_utp)
                
                # Update GDF
                if gdf is not None and 'CD_MUN' in gdf.columns:
                    try:
                        mask = gdf['CD_MUN'].astype(str).str.split('.').str[0] == str(mun_id)
                        gdf.loc[mask, 'UTP_ID'] = str(target_utp)
                    except Exception as e:
                        self.logger.error(f"      Failed to update GDF for {mun_id}: {e}")
                
                # Log consolidation
                self.consolidation_manager.add_consolidation(
                    source_utp=utp_id,
                    target_utp=target_utp,
                    reason="Isolated Municipality Resolution",
                    details={
                        "mun_id": mun_id,
                        "nm_mun": nm_mun,
                        "score": best['score'],
                        "regic_rank": best['regic_rank'],
                        "flows_to_utp": len(best['flows_to_utp'])
                    }
                )
                
                total_reconnected += 1
        
        self.logger.info(f"\n✅ Isolated Resolution complete:")
        self.logger.info(f"   Reconnected: {total_reconnected}")
        self.logger.info(f"   Unresolved: {total_unresolved}")
        
        return total_reconnected
