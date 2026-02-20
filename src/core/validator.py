# -*- coding: utf-8 -*-
import logging
from typing import List

import networkx as nx
import geopandas as gpd
import shapely.geometry as sgeom
import shapely.ops as ops

from .graph import TerritorialGraph


class TerritorialValidator:
    # Ranking atualizado baseado nos nomes presentes em SEDE+regic
    REGIC_RANK = {
        'Metrópole Nacional': 1,
        'Metrópole': 2,
        'Capital Regional A': 3,
        'Capital Regional B': 4,
        'Capital Regional C': 5,
        'Centro Sub-Regional A': 6,
        'Centro Sub-Regional B': 7,
        'Centro de Zona A': 8,
        'Centro de Zona B': 9,
        'Centro Local': 10,
        'Sem Dados': 98,
        '6': 99 
    }

    def __init__(self, graph: TerritorialGraph):
        self.graph = graph
        self.logger = logging.getLogger("TerritorialValidator")

    def _get_buffer_value(self, gdf: gpd.GeoDataFrame) -> float:
        """Determina o valor do buffer ideal com base no CRS (Graus vs Metros)."""
        if gdf.crs and gdf.crs.is_projected:
            return 500.0  # 500 metros para coordenadas projetadas (ex: EPSG:5880)
        return 0.05  # ~5km para coordenadas geográficas (ex: SIRGAS 2000 / WGS84)

    def get_regic_score(self, cd_mun: int) -> int:
        """Retorna o peso hierárquico usando as descrições carregadas no grafo."""
        level_desc = self.graph.mun_regic.get(int(cd_mun), '6')
        return self.REGIC_RANK.get(level_desc, 99)

    def get_utp_regic_score(self, utp_id: str) -> int:
        """Retorna o score REGIC de uma UTP baseado em seu município sede."""
        if not hasattr(self.graph, 'utp_seeds'):
            return 999
        
        sede_mun = self.graph.utp_seeds.get(utp_id)
        if not sede_mun:
            return 999
        
        return self.get_regic_score(sede_mun)


    def _safe_get_geometry(self, gdf: gpd.GeoDataFrame, filter_col: str, value):
        """Retorna a geometria de forma segura, tratando inconsistências de tipos."""
        if gdf is None or gdf.empty:
            return None
        
        # CORREÇÃO: Converte ambos para string para garantir o match (evita erro Int vs String)
        rows = gdf.loc[gdf[filter_col].astype(str) == str(value)]
        
        if rows.empty:
            self.logger.debug(f"Aviso: Valor {value} não encontrado na coluna {filter_col}.")
            return None
        try:
            geom = rows.geometry.iloc[0]
            if geom is None or geom.is_empty:
                return None
            return geom
        except Exception as e:
            self.logger.error(f"Erro ao recuperar geometria: {e}")
            return None

    def get_shared_boundary_length(self, mun_id: int, target_utp_id: str, gdf: gpd.GeoDataFrame) -> float:
        """Calcula o comprimento da fronteira partilhada em metros."""
        if gdf is None or gdf.empty:
            return 0.0

        mun_geom = self._safe_get_geometry(gdf, 'CD_MUN', mun_id)
        if mun_geom is None:
            return 0.0

        target_rows = gdf.loc[gdf['UTP_ID'].astype(str) == str(target_utp_id)]
        if target_rows.empty:
            return 0.0

        try:
            target_utp_geom = target_rows.unary_union
        except Exception:
            return 0.0

        if target_utp_geom is None or target_utp_geom.is_empty:
            return 0.0

        try:
            source_crs = gdf.crs
            if source_crs is None:
                return 0.0

            mun_series = gpd.GeoSeries([mun_geom], crs=source_crs).to_crs(epsg=5880)
            target_series = gpd.GeoSeries([target_utp_geom], crs=source_crs).to_crs(epsg=5880)

            mun_proj = mun_series.iloc[0]
            target_proj = target_series.unary_union
            
            shared = mun_proj.boundary.intersection(target_proj)
            length = getattr(shared, 'length', 0.0)
            return float(length) if length is not None else 0.0
        except Exception as e:
            self.logger.debug(f"Erro no cálculo de fronteira: {e}")
            return 0.0

    def get_rm_of_utp(self, utp_id: str) -> str:
        utp_node = f"UTP_{utp_id}"
        if not self.graph.hierarchy.has_node(utp_node):
            return "NAO_ENCONTRADA"
        parents = list(self.graph.hierarchy.predecessors(utp_node))
        return parents[0] if parents else "SEM_RM"

    def is_change_allowed(self, mun_id: int, target_utp_id: str, gdf: gpd.GeoDataFrame) -> bool:
        """Verifica RM e Adjacência Geográfica."""
        current_utp = self.graph.get_municipality_utp(mun_id)
        rm_origin = self.get_rm_of_utp(current_utp)
        rm_dest = self.get_rm_of_utp(target_utp_id)

        if rm_origin != rm_dest:
            return False

        mun_geom = self._safe_get_geometry(gdf, 'CD_MUN', mun_id)
        if mun_geom is None:
            return False

        target_geoms = gdf.loc[gdf['UTP_ID'].astype(str) == str(target_utp_id)]
        if target_geoms.empty:
            return False

        try:
            # CORREÇÃO: Buffer dinâmico
            buf_val = self._get_buffer_value(gdf)
            return target_geoms.geometry.intersects(mun_geom.buffer(buf_val)).any()
        except Exception:
            return False

    def is_adjacent_to_any_in_utp(self, mun_id: int, target_utp_id: str, gdf: gpd.GeoDataFrame) -> bool:
        """Verifica se o município toca QUALQUER município da UTP de destino."""
        if gdf is None or gdf.empty:
            return False

        mun_geom = self._safe_get_geometry(gdf, 'CD_MUN', mun_id)
        if mun_geom is None:
            return False

        target_geoms = gdf.loc[gdf['UTP_ID'].astype(str) == str(target_utp_id)]
        if target_geoms.empty:
            return False

        try:
            # CORREÇÃO: Buffer dinâmico para garantir detecção em fronteiras imperfeitas
            buf_val = self._get_buffer_value(gdf)
            return target_geoms.geometry.intersects(mun_geom.buffer(buf_val)).any()
        except Exception:
            return False

    def is_non_rm_utp(self, utp_id: str) -> bool:
        rm_node = self.get_rm_of_utp(utp_id)
        return rm_node == "RM_SEM_RM"

    def get_neighboring_utps(self, mun_id: int, gdf: gpd.GeoDataFrame) -> List[str]:
        """Retorna IDs de UTPs vizinhas, corrigindo falhas de detecção por escala."""
        if gdf is None or gdf.empty:
            return []

        mun_geom = self._safe_get_geometry(gdf, 'CD_MUN', mun_id)
        if mun_geom is None:
            return []

        try:
            # CORREÇÃO: Buffer dinâmico baseado no CRS
            buf_val = self._get_buffer_value(gdf)
            
            # Detecção de interseção com buffer para capturar vizinhos com 'gaps'
            neighbors = gdf.loc[gdf.geometry.intersects(mun_geom.buffer(buf_val))]
            
            # CORREÇÃO: Normalização do UTP_ID para string
            return neighbors['UTP_ID'].dropna().unique().astype(str).tolist()
        except Exception as e:
            self.logger.debug(f"Erro ao buscar vizinhos: {e}")
            return []

    def validate_utp_contiguity(self, utp_id: str, gdf_mun_utp: gpd.GeoDataFrame, sede_id: int) -> List[int]:
        """Detecta municípios isolados dentro de uma UTP."""
        if gdf_mun_utp is None or gdf_mun_utp.empty:
            return []

        gdf = gdf_mun_utp[['CD_MUN', 'geometry']].reset_index(drop=True).copy()
        gdf['CD_MUN'] = gdf['CD_MUN'].astype(int)

        edges = set()
        try:
            # Uso de buffer pequeno para compensar erros de topologia no sjoin
            gdf_buffer = gdf.copy()
            gdf_buffer['geometry'] = gdf_buffer.geometry.buffer(self._get_buffer_value(gdf_mun_utp) / 10)
            
            sjoin = gpd.sjoin(gdf_buffer, gdf_buffer, how='inner', predicate='intersects')
            for _, row in sjoin.iterrows():
                a, b = int(row['CD_MUN_left']), int(row['CD_MUN_right'])
                if a != b:
                    edges.add(tuple(sorted((a, b))))
        except Exception:
            # Fallback
            for i, r in gdf.iterrows():
                candidates = gdf.loc[gdf.geometry.touches(r.geometry), 'CD_MUN'].tolist()
                for c in candidates:
                    if int(c) != int(r['CD_MUN']):
                        edges.add(tuple(sorted((int(r['CD_MUN']), int(c)))))

        G = nx.Graph()
        municipios_ids = gdf['CD_MUN'].tolist()
        G.add_nodes_from(municipios_ids)
        G.add_edges_from(edges)

        if int(sede_id) not in G:
            return []

        alcancaveis = nx.node_connected_component(G, int(sede_id))
        return [m for m in municipios_ids if m not in alcancaveis]