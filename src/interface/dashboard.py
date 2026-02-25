# src/interface/dashboard.py
import os
import streamlit as st
import pandas as pd
import logging
from pathlib import Path

# Imports de ferramentas e carregadores
from src.utils import DataLoader
from src.interface.consolidation_loader import ConsolidationLoader
from src.interface.snapshot_loader import SnapshotLoader

# Imports das utilidades de visualização
from src.interface.view_utils import (
    DASHBOARD_CSS,
    get_geodataframe,
    get_derived_rm_geodataframe,
    get_derived_state_geodataframe,
    get_territorial_graph,
    PASTEL_PALETTE
)

# Imports das abas (Módulos separados)
from src.interface.v8_0_initial_view import render_v8_0_initial
from src.interface.v8_1_unitary_view import render_v8_1_unitary
from src.interface.v8_2_sedes_view import render_v8_2_sedes
from src.interface.v8_3_centralization_view import render_v8_3_centralization
from src.interface.influence_view import render_influence_analysis_tab

try:
    import psutil as _psutil
except ImportError:
    _psutil = None

# Configuração da Página
st.set_page_config(
    page_title="Unidade Territorial de Planejamento",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Aplicar CSS
st.markdown(DASHBOARD_CSS, unsafe_allow_html=True)

def render_dashboard(manager):
    """Dashboard principal que orquestra as diferentes versões do território."""
    
    consolidation_loader = ConsolidationLoader()
    snapshot_loader = SnapshotLoader()
    
    col1, _, col3 = st.columns([2, 1, 1])
    with col1:
        st.title("Unidade Territorial de Planejamento")
        st.markdown("Visualização da distribuição inicial e pós-consolidação de UTPs")
    
    with col3:
        status_class = "status-executed" if consolidation_loader.is_executed() else "status-pending"
        status_text = "Consolidado" if consolidation_loader.is_executed() else "Pendente"
        st.markdown(f"<div class='status-badge {status_class}'>{status_text}</div>", unsafe_allow_html=True)
    
    # === SIDEBAR ===
    with st.sidebar:
        st.markdown("### Filtros")
        st.markdown("---")
        
        data_loader = DataLoader()
        df_municipios = data_loader.get_municipios_dataframe()
        metadata = data_loader.get_metadata()
        
        if df_municipios.empty:
            st.error("Falha ao carregar dados.")
            return
        
        if 'utp_id' in df_municipios.columns:
            df_municipios['utp_id'] = df_municipios['utp_id'].astype(str)
        
        # Mapeamento para display
        df_sedes_map = df_municipios[df_municipios['sede_utp'] == True][['utp_id', 'nm_mun']].set_index('utp_id')
        utp_sede_map = df_sedes_map['nm_mun'].to_dict()
        
        # Filtros
        ufs = sorted(df_municipios['uf'].unique().tolist())
        all_ufs = st.checkbox("Brasil Completo", value=True)
        selected_ufs = ufs if all_ufs else st.multiselect("Estados (UF)", ufs, default=[])
        
        st.markdown("---")
        df_municipios['display_name'] = df_municipios['nm_mun'] + " (" + df_municipios['uf'] + ")"
        mun_options = sorted(df_municipios['display_name'].unique().tolist())
        selected_muns_search = st.multiselect("Buscar Município", mun_options)
        
        forced_utps = []
        if selected_muns_search:
            mask = df_municipios['display_name'].isin(selected_muns_search)
            forced_utps = df_municipios[mask]['utp_id'].unique().tolist()
            if forced_utps:
                st.info(f"Visualizando {len(forced_utps)} UTP(s) da busca.")
        
        utps_list = sorted(df_municipios[df_municipios['uf'].isin(selected_ufs)]['utp_id'].unique().tolist())
        all_utps = st.checkbox("Todas as UTPs", value=False)
        selected_utps = utps_list if all_utps else st.multiselect(
            "UTPs", utps_list, format_func=lambda x: f"{x} - {utp_sede_map.get(x, 'N/A')}"
        )
        
        st.markdown("---")
        if _psutil:
            mem = _psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2
            st.metric("RAM em uso", f"{mem:.0f} MB")
    
    # Aplicar filtros logicamente
    df_filtered = df_municipios[df_municipios['uf'].isin(selected_ufs)].copy()
    if forced_utps:
        df_filtered = df_municipios[df_municipios['utp_id'].isin(forced_utps)].copy()
    elif selected_utps:
        df_filtered = df_filtered[df_filtered['utp_id'].isin(selected_utps)]
    
    # Carregar GeoDataFrames
    maps_dir = Path("data/04_maps")
    gdf = get_geodataframe(maps_dir / "municipalities_optimized.geojson", df_municipios)
    gdf_rm = get_derived_rm_geodataframe(maps_dir / "rm_boundaries_optimized.geojson")
    gdf_states = get_derived_state_geodataframe(maps_dir / "state_boundaries_optimized.geojson")
    
    # Seletor de Versão
    list_versions = [
        "Versão 8.0 - Distribuição Inicial",
        "Versão 8.1 - UTPs unitárias",
        "Versão 8.2 - Dependência entre Sedes",
        "Versão 8.3 - Centralização das Sedes",
        "Versão 8.4 - Análise de Influência"
    ]
    
    selected_version = st.radio("Versão do Território", list_versions, horizontal=True, index=4)
    
    # Delegar renderização para os módulos específicos
    if selected_version == list_versions[0]:
        render_v8_0_initial(df_municipios, df_filtered, selected_ufs, selected_utps, gdf, gdf_rm, gdf_states, snapshot_loader, PASTEL_PALETTE)
    elif selected_version == list_versions[1]:
        render_v8_1_unitary(df_municipios, df_filtered, selected_ufs, selected_utps, gdf, gdf_rm, gdf_states, snapshot_loader, consolidation_loader, PASTEL_PALETTE)
    elif selected_version == list_versions[2]:
        render_v8_2_sedes(df_municipios, selected_ufs, selected_utps, utps_list, ufs, gdf, gdf_rm, gdf_states, snapshot_loader, consolidation_loader, PASTEL_PALETTE)
    elif selected_version == list_versions[3]:
        render_v8_3_centralization(df_municipios, df_filtered, selected_ufs, selected_utps, utps_list, gdf, gdf_rm, gdf_states, snapshot_loader, consolidation_loader, PASTEL_PALETTE)
    elif selected_version == list_versions[4]:
        render_influence_analysis_tab(df_municipios, gdf, snapshot_loader)
