# src/interface/v8_0_initial_view.py
import streamlit as st
import gc
from src.interface.view_utils import (
    render_territorial_config_table,
    create_enriched_utp_summary,
    load_or_compute_coloring,
    get_state_boundaries
)
from src.interface.map_flow_render import render_map_with_flow_popups

def render_v8_0_initial(df_municipios, df_filtered, selected_ufs, selected_utps, gdf, gdf_rm, gdf_states_optimized, snapshot_loader, PASTEL_PALETTE):
    st.markdown("### <span class='step-badge step-initial'>Versão 8.0</span> Distribuição Inicial", unsafe_allow_html=True)
    st.markdown("""
    **Antes da v8, o maior desafio era a integridade referencial. Com base no estudo da versão 7, foi possível efetuar as seguintes melhorias**
    
    *   **Continuidade:** 3 UTPs não possuíram municípios conexos territorialmente, totalizando 24 municípios.
    *   **Região Metropolitana:** 169 UTPs apresentavam discrepância entre as regiões metropolitanas, totalizando 2154 municípios.
    """)
    st.markdown("---")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Municípios", df_filtered['cd_mun'].nunique(), f"{df_municipios['cd_mun'].nunique()} total")
    with col2:
        st.metric("UTPs", df_filtered['utp_id'].nunique())
    with col3:
        st.metric("Estados", df_filtered['uf'].nunique())
    
    st.markdown("---")
    st.markdown("#### Mapa Interativo")
    
    col_ctrl1, col_ctrl2 = st.columns(2)
    with col_ctrl1:
        show_rm_borders = st.checkbox("Mostrar contornos de RMs", value=False, key='show_rm_tab1')
    with col_ctrl2:
        show_state_borders = st.checkbox("Mostrar limites Estaduais", value=False, key='show_state_tab1')

    gdf_initial = snapshot_loader.get_geodataframe_for_step('step1', gdf)
    gdf_display = gdf_initial if gdf_initial is not None else gdf

    if gdf_display is not None:
        gdf_filtered_map = gdf_display[gdf_display['uf'].isin(selected_ufs)].copy()
        if selected_utps:
            gdf_filtered_map = gdf_filtered_map[gdf_filtered_map['utp_id'].isin(selected_utps)]
        
        gdf_states_filtered = None
        if show_state_borders:
            gdf_all_states = gdf_states_optimized if gdf_states_optimized is not None else get_state_boundaries(gdf)
            if gdf_all_states is not None:
                gdf_states_filtered = gdf_all_states[gdf_all_states['uf'].isin(selected_ufs)] if selected_ufs else gdf_all_states

        global_colors_initial = load_or_compute_coloring(gdf, "initial_coloring.json")
        map_html = render_map_with_flow_popups(
            gdf_filtered_map, df_municipios, title="Distribuição por UTP (Inicial)", 
            global_colors=global_colors_initial, gdf_rm=gdf_rm, 
            show_rm_borders=show_rm_borders, show_state_borders=show_state_borders,
            gdf_states=gdf_states_filtered, PASTEL_PALETTE=PASTEL_PALETTE, step_key='step1'
        )
        if map_html:
            st.components.v1.html(map_html, height=600, scrolling=False)
    
    st.markdown("---")
    st.markdown("#### Configuração Territorial")
    _allowed_muns_tab1 = set(df_filtered['cd_mun'].astype(str).tolist()) if not df_filtered.empty else None
    df_config_tab1 = render_territorial_config_table('step1', snapshot_loader, _allowed_muns_tab1)
    if not df_config_tab1.empty:
        st.dataframe(df_config_tab1, hide_index=True, width='stretch', height=400)
        del df_config_tab1
        gc.collect()
    
    st.markdown("---")
    st.markdown("#### Resumo das UTPs")
    utp_summary = create_enriched_utp_summary(df_filtered)
    if not utp_summary.empty:
        st.dataframe(utp_summary, width='stretch', hide_index=True, height=600)
    else:
        st.info("Nenhuma UTP encontrada com os filtros selecionados.")
