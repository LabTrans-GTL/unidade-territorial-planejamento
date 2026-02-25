# src/interface/v8_1_unitary_view.py
import streamlit as st
import pandas as pd
import json
import logging
import gc
from datetime import datetime
from src.interface.view_utils import (
    render_territorial_config_table,
    load_or_compute_coloring,
    get_state_boundaries
)
from src.interface.map_flow_render import render_map_with_flow_popups

def render_v8_1_unitary(df_municipios, df_filtered, selected_ufs, selected_utps, gdf, gdf_rm, gdf_states_optimized, snapshot_loader, consolidation_loader, PASTEL_PALETTE):
    st.markdown("### <span class='step-badge step-final'>Versão 8.1</span> UTPs unitárias", unsafe_allow_html=True)
    st.markdown("""
    **O objetivo central é garantir que nenhum município permaneça isolado em uma UTP própria, a menos que não haja candidatos adjacentes válidos.**
    """)
    
    _df_metrics_tab2 = snapshot_loader.get_snapshot_dataframe('step5')
    _allowed_cd_mun_str = df_filtered['cd_mun'].astype(str).unique() if not df_filtered.empty else []

    if not _df_metrics_tab2.empty and len(_allowed_cd_mun_str) > 0:
        _df_metrics_tab2 = _df_metrics_tab2[_df_metrics_tab2['cd_mun'].astype(str).isin(_allowed_cd_mun_str)].copy()

    if not _df_metrics_tab2.empty:
        _df_metrics_tab2['cd_mun'] = _df_metrics_tab2['cd_mun'].astype(str)
        _df_mun_uf = df_municipios[['cd_mun', 'uf']].copy()
        _df_mun_uf['cd_mun'] = _df_mun_uf['cd_mun'].astype(str)
        _df_m = _df_metrics_tab2.merge(_df_mun_uf, on='cd_mun', how='left')
    else:
        _df_m = df_filtered

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Municípios", _df_m['cd_mun'].nunique(), f"{df_municipios['cd_mun'].nunique()} total")
    with col2:
        st.metric("UTPs", _df_m['utp_id'].nunique())
    with col3:
        st.metric("Estados", _df_m['uf'].nunique())

    st.markdown("---")

    if consolidation_loader.is_executed():
        st.markdown("#### Mapa Pós-Consolidação")
        col_ctrl1, col_ctrl2 = st.columns(2)
        with col_ctrl1:
            show_rm_borders_tab2 = st.checkbox("Mostrar contornos de RMs", value=False, key='show_rm_tab2')
        with col_ctrl2:
            show_state_borders_tab2 = st.checkbox("Mostrar limites Estaduais", value=False, key='show_state_tab2')

        if gdf is not None:
            gdf_consolidated = snapshot_loader.get_geodataframe_for_step('step5', gdf[gdf['uf'].isin(selected_ufs)].copy())
            if gdf_consolidated is None:
                gdf_consolidated = consolidation_loader.apply_post_unitary_to_dataframe(gdf[gdf['uf'].isin(selected_ufs)].copy())
            
            if selected_utps:
                gdf_consolidated = gdf_consolidated[gdf_consolidated['utp_id'].isin(selected_utps)]
            
            colors_consolidated = {}
            if 'color_id' in gdf_consolidated.columns:
                _col_cd = 'CD_MUN' if 'CD_MUN' in gdf_consolidated.columns else 'cd_mun'
                colors_consolidated = dict(zip(gdf_consolidated[_col_cd].astype(int), gdf_consolidated['color_id'].astype(int)))
            
            if not colors_consolidated:
                 colors_consolidated = load_or_compute_coloring(gdf_consolidated, "consolidated_coloring.json")
            
            gdf_states_filtered = None
            if show_state_borders_tab2:
                gdf_all_states = gdf_states_optimized if gdf_states_optimized is not None else get_state_boundaries(gdf)
                if gdf_all_states is not None:
                    gdf_states_filtered = gdf_all_states[gdf_all_states['uf'].isin(selected_ufs)] if selected_ufs else gdf_all_states

            map_html = render_map_with_flow_popups(
                gdf_consolidated, df_municipios, title="Distribuição Consolidada (Snapshot)", 
                global_colors=colors_consolidated, gdf_rm=gdf_rm, 
                show_rm_borders=show_rm_borders_tab2, show_state_borders=show_state_borders_tab2,
                gdf_states=gdf_states_filtered, PASTEL_PALETTE=PASTEL_PALETTE, step_key='step5'
            )
            if map_html:
                st.components.v1.html(map_html, height=600, scrolling=False)
        
        st.markdown("---")
        st.markdown("#### Configuração Territorial")
        _allowed_muns_tab2 = set(df_filtered['cd_mun'].astype(str).tolist()) if not df_filtered.empty else None
        df_config_tab2 = render_territorial_config_table('step5', snapshot_loader, _allowed_muns_tab2)
        if not df_config_tab2.empty:
            st.dataframe(df_config_tab2, hide_index=True, width='stretch', height=400)
            del df_config_tab2
            gc.collect()

        st.markdown("---")
        st.markdown("#### Registro de Consolidações")
        post_unitary_consolidations = consolidation_loader.get_post_unitary_consolidations()
        if post_unitary_consolidations:
            df_consolidations = pd.DataFrame([
                {
                    "ID": i + 1,
                    "UTP Origem": c["source_utp"],
                    "UTP Destino": c["target_utp"],
                    "Motivo": c.get("reason", "N/A"),
                    "Data": c["timestamp"][:10],
                    "Hora": c["timestamp"][11:19]
                }
                for i, c in enumerate(post_unitary_consolidations)
            ])
            st.dataframe(df_consolidations, width='stretch', hide_index=True)
        
        result_json = json.dumps(consolidation_loader.result, ensure_ascii=False, indent=2)
        st.download_button(
            label="Baixar Resultado de Consolidação",
            data=result_json,
            file_name=f"consolidation_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )
