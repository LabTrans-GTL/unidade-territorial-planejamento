# src/interface/v8_2_sedes_view.py
import streamlit as st
import pandas as pd
import gc
import folium
from src.interface.view_utils import (
    render_territorial_config_table,
    load_or_compute_coloring,
    get_state_boundaries
)
from src.interface.map_flow_render import render_map_with_flow_popups

def render_v8_2_sedes(df_municipios, selected_ufs, selected_utps, utps_list, ufs, gdf, gdf_rm, gdf_states_optimized, snapshot_loader, consolidation_loader, PASTEL_PALETTE):
    st.markdown("### <span class='step-badge step-final'>Versão 8.2</span> Dependência entre Sedes", unsafe_allow_html=True)
    st.markdown("""
    **O objetivo desta etapa é fundir territórios quando a sede de uma UTP demonstra uma dependência funcional em relação a outra sede vizinha.**
    """)
    st.markdown("---")

    sede_executed = consolidation_loader.is_sede_executed()
    
    if sede_executed:
        sede_result = consolidation_loader.get_sede_result()
        sede_consolidations = sede_result.get('consolidations', [])
        
        if gdf is not None:
            gdf_sliced = gdf[gdf['uf'].isin(selected_ufs)].copy()
            if selected_utps:
                gdf_sliced = gdf_sliced[gdf_sliced['utp_id'].isin(selected_utps)]
                
            gdf_final = snapshot_loader.get_geodataframe_for_step('step6', gdf_sliced)
            
            if gdf_final is not None and not gdf_final.empty:
                col1, col2, col3 = st.columns(3)
                with col1:
                    col_cd = 'CD_MUN' if 'CD_MUN' in gdf_final.columns else 'cd_mun'
                    current_unique = gdf_final[col_cd].astype(str).nunique()
                    st.metric("Municípios", current_unique, f"{df_municipios['cd_mun'].nunique()} total")
                with col2:
                    st.metric("UTPs", gdf_final['utp_id'].nunique(), f"{len(utps_list)} total")
                with col3:
                    st.metric("Estados", gdf_final['uf'].nunique(), f"{len(ufs)} total")
                
                st.markdown("---")
                col_ctrl1, col_ctrl2 = st.columns(2)
                with col_ctrl1:
                    show_rm_borders_tab3 = st.checkbox("Mostrar contornos de RMs", value=False, key='show_rm_tab3')
                with col_ctrl2:
                    show_state_borders_tab3 = st.checkbox("Mostrar limites Estaduais", value=False, key='show_state_tab3')

                colors_final = {}
                if 'color_id' in gdf_final.columns:
                    _col_cd = 'CD_MUN' if 'CD_MUN' in gdf_final.columns else 'cd_mun'
                    colors_final = dict(zip(gdf_final[_col_cd].astype(int), gdf_final['color_id'].astype(int)))
                else:
                    colors_final = load_or_compute_coloring(gdf_final, "post_sede_coloring.json")

                gdf_states_filtered = None
                if show_state_borders_tab3:
                    gdf_all_states = gdf_states_optimized if gdf_states_optimized is not None else get_state_boundaries(gdf)
                    if gdf_all_states is not None:
                        gdf_states_filtered = gdf_all_states[gdf_all_states['uf'].isin(selected_ufs)] if selected_ufs else gdf_all_states

                map_html = render_map_with_flow_popups(
                    gdf_final, df_municipios, title="Final (Snapshot)", 
                    global_colors=colors_final, gdf_rm=gdf_rm, 
                    show_rm_borders=show_rm_borders_tab3, show_state_borders=show_state_borders_tab3,
                    gdf_states=gdf_states_filtered, PASTEL_PALETTE=PASTEL_PALETTE, step_key='step6'
                )
                if map_html:
                    st.components.v1.html(map_html, height=600, scrolling=False)
            
            st.markdown("---")
            st.markdown("#### Configuração Territorial")
            _allowed_muns_tab3 = set(df_municipios[df_municipios['uf'].isin(selected_ufs)]['cd_mun'].astype(str).tolist())
            df_config_tab3 = render_territorial_config_table('step6', snapshot_loader, _allowed_muns_tab3)
            if not df_config_tab3.empty:
                st.dataframe(df_config_tab3, hide_index=True, width='stretch', height=400)
                del df_config_tab3
                gc.collect()
            
            st.markdown("---")
            st.markdown("#### Detalhes das Alterações de Sedes")
            changes_data = []
            for c in sede_consolidations:
                details = c.get('details', {})
                changes_data.append({
                    "Município": details.get('nm_mun', str(details.get('mun_id'))),
                    "UTP Origem": c['source_utp'],
                    "UTP Destino": c['target_utp'],
                    "Tipo": "Sede da UTP" if details.get('is_sede') else "Município Componente",
                    "Motivo": c['reason']
                })
            st.dataframe(pd.DataFrame(changes_data), hide_index=True, width='stretch')
            gc.collect()
        else:
            st.warning("Mapa indisponível")
    else:
        st.info("Nenhuma consolidação de sedes encontrada.")
