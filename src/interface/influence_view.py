# src/interface/influence_view.py
import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
import json
from pathlib import Path
from src.interface.map_flow_render import render_map_with_flow_popups
from src.interface.view_utils import load_or_compute_coloring, PASTEL_PALETTE

def render_influence_analysis_tab(df_municipios, gdf, snapshot_loader):
    """
    Renders the "Estudo de Caso - Análise de Influência" tab with Step 8 UTP context.
    Minimalist version: Chain members in gray, background in UTP colors.
    """
    st.markdown("### <span class='step-badge step-final'>Estudo de Caso</span> Análise de Hierarquia e Influência", unsafe_allow_html=True)
    st.markdown("""
    Esta visualização analisa as **Cadeias de Influência** (fluxos OD até 2h) 
    sobrepostas à configuração final das UTPs (**Passo 8**). 
    Este estudo auxilia na identificação de municípios que, embora integrados a uma UTP, 
    possuem vínculos de fluxo mais fortes com polos de outras regiões.
    """)
    
    with st.expander("Metodologia Aplicada"):
        st.markdown("""
        O processo de identificação das cadeias de influência segue quatro critérios estritos:
        
        1.  **Filtro de Impedância**: Considera apenas fluxos de origem-destino com tempo de viagem por terra **inferior a 2 horas**.
        2.  **Fluxo Principal Estrito**: Para cada município, identifica-se o destino com o **maior volume de viagens** (Polo Principal) dentro do limite de tempo.
        3.  **Consistência Regional (RM)**: O vínculo de influência só é validado se ambos os municípios pertencerem à mesma **Região Metropolitana (RM)** ou se ambos estiverem **fora de qualquer RM**. Caso o polo absoluto não respeite este critério, o município é classificado como sem influência definida (não se busca o segundo colocado).
        4.  **Classificação Hierárquica**:
            *   **Primária (Núcleo)**: Formada por pares de municípios com fluxos principais recíprocos (A → B e B → A).
            *   **Secundária (Dependente)**: Municípios cujo fluxo principal é direcionada a um membro de um Núcleo.
            *   **Terciária (Satélite)**: Municípios cujo fluxo principal é direcionada a um membro da Cadeia Secundária.
        """)

    st.markdown("---")

    # 1. Carregar dados da análise
    data_dir = Path("data/03_processed")
    path_full = data_dir / "analise_hierarquia_influencia.csv"
    path_resumo = data_dir / "resumo_cadeias_influencia.csv"

    if not path_full.exists() or not path_resumo.exists():
        st.warning("⚠️ Os arquivos de análise de influência não foram encontrados. Execute `run_influence_analysis.py` primeiro.")
        return

    df_full = pd.read_csv(path_full, sep=';')
    df_resumo = pd.read_csv(path_resumo, sep=';')

    # 2. Seletor de Cadeia (Resumo)
    st.markdown("#### 🔗 Selecione uma Cadeia de Influência para Visualizar")
    
    # Reordenar colunas para mostrar Sedes em destaque
    cols_order = ['id_cadeia', 'nucleo', 'qtd_municipios', 'tem_sede', 'qtd_sedes', 'utp_dominante', 'coesao_utp_perc', 'diagnostico_geral']
    df_resumo = df_resumo[cols_order]

    event = st.dataframe(
        df_resumo,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        height=300
    )

    selected_chain_id = None
    if event and len(event.selection.rows) > 0:
        idx = event.selection.rows[0]
        selected_chain_id = df_resumo.iloc[idx]['id_cadeia']
    else:
        st.info("💡 Selecione uma linha na tabela acima para visualizar a cadeia no contexto das UTPs.")
        return

    # 3. Métricas da Cadeia Selecionada
    chain_data = df_resumo[df_resumo['id_cadeia'] == selected_chain_id].iloc[0]
    df_members = df_full[df_full['id_cadeia'] == selected_chain_id].copy()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Municípios na Cadeia", chain_data['qtd_municipios'])
    with col2:
        st.metric("UTP Dominante", chain_data['utp_dominante'])
    with col3:
        st.metric("Coesão UTP", f"{chain_data['coesao_utp_perc']}%")
    with col4:
        st.metric("Status", "Mista" if chain_data['coesao_utp_perc'] < 100 else "Coesa")

    st.markdown(f"**Diagnóstico:** {chain_data['diagnostico_geral']}")
    st.markdown("---")

    # 4. Contexto do Mapa (Step 8)
    st.markdown(f"#### Mapa da Cadeia: {chain_data['nucleo']} (Contexto UTP Passo 8)")
    
    col_map, col_details = st.columns([2, 1])

    with col_map:
        if gdf is not None:
            # 4.1 Identificar UTPs envolvidas no Passo 8
            involved_utp_ids = [str(u) for u in df_members['utp_origem'].unique()]
            
            # Carregar snapshot do Passo 8
            df_step8 = snapshot_loader.get_snapshot_dataframe('step8')
            if df_step8.empty:
                df_step8 = df_municipios
            
            # Filtrar municípios das UTPs envolvidas
            df_step8['utp_id'] = df_step8['utp_id'].astype(str)
            df_context = df_step8[df_step8['utp_id'].isin(involved_utp_ids)]
            context_ids = df_context['cd_mun'].astype(str).tolist()
            
            # Criar GDF de contexto
            gdf_context = snapshot_loader.get_geodataframe_for_step('step8', gdf[gdf['CD_MUN'].astype(str).isin(context_ids)].copy())
            if gdf_context is None:
                gdf_context = gdf[gdf['CD_MUN'].astype(str).isin(context_ids)].copy()

            # 4.2 Configurar Cores (Background UTPs + Chain Members Gray)
            chain_ids = [int(hid) for hid in df_members['cd_mun'].tolist()]
            
            # Cores base do Step 8
            colors_step8 = {}
            if 'color_id' in gdf_context.columns:
                _col_cd = 'CD_MUN' if 'CD_MUN' in gdf_context.columns else 'cd_mun'
                colors_step8 = dict(zip(gdf_context[_col_cd].astype(int), gdf_context['color_id'].astype(int)))
            else:
                colors_step8 = load_or_compute_coloring(gdf_context, "post_sede_coloring.json")
            
            # SOBRESCREVER membros da cadeia com CINZA (#d9d9d9)
            for cid in chain_ids:
                colors_step8[cid] = '#808080' # Cinza mais nítido que o light gray para contraste

            # 4.3 Renderizar mapa (Sem contorno, sem poluição)
            map_html = render_map_with_flow_popups(
                gdf_context,
                df_step8,
                title=f"Cadeia {selected_chain_id}",
                global_colors=colors_step8,
                show_rm_borders=False, # Reduzir poluição visual
                show_state_borders=False,
                PASTEL_PALETTE=PASTEL_PALETTE,
                step_key='step8'
            )
            if map_html:
                st.components.v1.html(map_html, height=550, scrolling=False)
        else:
            st.error("GeoDataFrame não carregado.")

    with col_details:
        st.markdown("**Hierarquia na Cadeia**")
        st.dataframe(
            df_members[['nm_mun', 'hierarquia', 'sede_utp', 'utp_origem', 'sugestao_analise']],
            hide_index=True,
            use_container_width=True,
            height=450
        )

    st.markdown("---")
    st.markdown("#### Recomendações Técnicas")
    
    for _, row in df_members[df_members['sugestao_analise'] != "Integridade Territorial: Cadeia totalmente contida na mesma UTP."].iterrows():
        st.warning(f"**{row['nm_mun']}**: {row['sugestao_analise']}")
