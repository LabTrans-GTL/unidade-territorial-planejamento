# src/interface/v8_3_centralization_view.py
import streamlit as st
import pandas as pd
import json
import logging
import gc
import folium
from pathlib import Path
from datetime import datetime
from src.interface.view_utils import (
    render_territorial_config_table,
    get_state_boundaries
)
from src.interface.map_flow_render import render_map_with_flow_popups
from src.interface.flow_utils import get_top_municipalities_in_utp

def render_v8_3_centralization(df_municipios, df_filtered, selected_ufs, selected_utps, utps_list, gdf, gdf_rm, gdf_states_optimized, snapshot_loader, consolidation_loader, PASTEL_PALETTE):
    st.markdown("### <span class='step-badge step-final'>Versão 8.3</span> Centralização das Sedes", unsafe_allow_html=True)
    st.markdown("""
    **Última etapa que garante que todos os municípios de uma mesma UTP tenham a sua própria sede como referencial.**
    """)
    st.markdown("---")
    
    st.markdown("### Análise de Fluxos por UTP")
    df_step8_with_flows = snapshot_loader.get_complete_dataframe_with_flows('step8')
    
    with st.expander("Municípios com Maior Fluxo por UTP", expanded=False):
        available_utps = sorted(df_step8_with_flows['utp_id'].unique().tolist()) if not df_step8_with_flows.empty else []
        if available_utps:
            selected_utp_for_flow = st.selectbox("Selecione a UTP:", options=available_utps, key="utp_flow_selector")
            if selected_utp_for_flow:
                df_utp_flows = get_top_municipalities_in_utp(df_step8_with_flows, selected_utp_for_flow, top_n=10)
                if not df_utp_flows.empty:
                    df_display = df_utp_flows.copy()
                    df_display['total_flow'] = df_display['total_flow'].apply(lambda x: f"{x:,}")
                    df_display = df_display.rename(columns={'nm_mun': 'Município', 'total_flow': 'Fluxo Total'})
                    st.dataframe(df_display[['Município', 'Fluxo Total']], hide_index=True, width='stretch')
        else:
            st.warning("Nenhuma UTP disponível nos filtros selecionados.")
    
    st.markdown("---")
    
    if gdf is not None:
         gdf_borders = snapshot_loader.get_geodataframe_for_step('step8', gdf[gdf['uf'].isin(selected_ufs)].copy())
         if gdf_borders is not None:
             if selected_utps:
                 gdf_borders = gdf_borders[gdf_borders['utp_id'].isin(selected_utps)]
             
             st.subheader("Estado Final Pós-Validação (Snapshot)")
             if not gdf_borders.empty:
                col1, col2, col3 = st.columns(3)
                with col1:
                    col_cd = 'CD_MUN' if 'CD_MUN' in gdf_borders.columns else 'cd_mun'
                    current_unique = gdf_borders[col_cd].astype(str).nunique()
                    st.metric("Municípios", current_unique, f"{df_municipios['cd_mun'].nunique()} total")
                with col2:
                    st.metric("UTPs", gdf_borders['utp_id'].nunique(), f"{len(utps_list)} total")
                with col3:
                    st.metric("Estados", gdf_borders['uf'].nunique())
                st.markdown("---")
             
             colors_borders = {}
             if 'color_id' in gdf_borders.columns:
                 _col_cd = 'CD_MUN' if 'CD_MUN' in gdf_borders.columns else 'cd_mun'
                 colors_borders = dict(zip(gdf_borders[_col_cd].astype(int), gdf_borders['color_id'].astype(int)))
             
             col_ctrl1, col_ctrl2 = st.columns(2)
             with col_ctrl1:
                 show_rm_borders_tab4 = st.checkbox("Mostrar contornos de RMs", value=False, key='show_rm_tab4')
             with col_ctrl2:
                 show_state_borders_tab4 = st.checkbox("Mostrar limites Estaduais", value=False, key='show_state_tab4')

             gdf_states_filtered = None
             if show_state_borders_tab4:
                gdf_all_states = gdf_states_optimized if gdf_states_optimized is not None else get_state_boundaries(gdf)
                if gdf_all_states is not None:
                    gdf_states_filtered = gdf_all_states[gdf_all_states['uf'].isin(selected_ufs)] if selected_ufs else gdf_all_states

             map_html = render_map_with_flow_popups(
                 gdf_borders, df_step8_with_flows, title="Validação Fronteiras (Snapshot)",
                 global_colors=colors_borders, gdf_rm=gdf_rm, 
                 show_rm_borders=show_rm_borders_tab4, show_state_borders=show_state_borders_tab4,
                 gdf_states=gdf_states_filtered, PASTEL_PALETTE=PASTEL_PALETTE, step_key='step8'
             )
             if map_html:
                 st.components.v1.html(map_html, height=600, scrolling=False)
             
             st.markdown("---")
             st.markdown("#### Configuração Territorial")
             _allowed_muns_tab4 = set(df_filtered['cd_mun'].astype(str).tolist()) if not df_filtered.empty else None
             df_config_tab4 = render_territorial_config_table('step8', snapshot_loader, _allowed_muns_tab4)
             if not df_config_tab4.empty:
                 st.dataframe(df_config_tab4, hide_index=True, width='stretch', height=400)
                 del df_config_tab4
                 gc.collect()

    borders_json_path = Path("data/03_processed/border_validation_result.json")
    if borders_json_path.exists():
        try:
            with open(borders_json_path, 'r', encoding='utf-8') as f:
                borders_data = json.load(f)
            relocations = borders_data.get('relocations', [])
            rejections = borders_data.get('rejections', [])
            transitive_chains = borders_data.get('transitive_chains', [])
            
            if transitive_chains:
                st.markdown("#### 🔗 Cadeias Transitivas Detectadas")
                for i, chain in enumerate(transitive_chains, 1):
                    st.info(f"**Cadeia {i}:** {' → '.join(chain['chain'])} → **UTP {chain['final_utp']}**")

            subtab1, subtab2 = st.tabs(["Realocações", "Rejeições"])
            with subtab1:
                relocations_df = pd.DataFrame(relocations) if relocations else pd.DataFrame()
                if not relocations_df.empty:
                    st.dataframe(relocations_df, hide_index=True, width='stretch', height=400)
            with subtab2:
                if rejections:
                    st.dataframe(pd.DataFrame(rejections), hide_index=True, width='stretch', height=400)
                else:
                    st.success("✅ Nenhuma rejeição!")
            
            borders_json = json.dumps(borders_data, ensure_ascii=False, indent=2)
            st.download_button("Baixar Resultados (JSON)", data=borders_json, file_name=f"border_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        except Exception as e:
            st.error(f"Erro ao carregar dados: {e}")
    else:
        st.warning("⚠️ Dados de validação de fronteiras não encontrados")
