# src/interface/components/sede_comparison.py
"""
Componentes de visualização para análise comparativa entre sedes.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional


def render_sede_table(df: pd.DataFrame, show_alerts_only: bool = False) -> None:
    """
    Renderiza tabela interativa de sedes com formatação condicional.
    
    Args:
        df: DataFrame com dados das sedes
        show_alerts_only: Se True, mostra apenas sedes com alerta
    """
    if df.empty:
        st.info("Nenhum dado disponível para visualização.")
        return
    
    df_display = df.copy()
    
    # Filtrar apenas alertas se solicitado
    if show_alerts_only:
        df_display = df_display[df_display['Alerta'] == 'SIM']
        
        if df_display.empty:
            st.success("Nenhum alerta de dependência detectado!")
            return
    
    # Ordenar por população (padrão)
    df_display = df_display.sort_values('População', ascending=False)
    
    # Exibir tabela com formatação
    st.dataframe(
        df_display,
        width='stretch',
        hide_index=True,
        column_config={
            'UTP': st.column_config.TextColumn('UTP', width='small'),
            'Sede': st.column_config.TextColumn('Sede', width='medium'),
            'UF': st.column_config.TextColumn('UF', width='small'),
            'REGIC': st.column_config.TextColumn('REGIC', width='medium'),
            'População': st.column_config.NumberColumn('População', format='%d'),
            'Nº Municípios': st.column_config.NumberColumn('Nº Mun.', width='small'),
            'Viagens': st.column_config.NumberColumn('Viagens', format='%d'),
            'Aeroporto': st.column_config.TextColumn('Aeroporto', width='small'),
            'Turismo': st.column_config.TextColumn('Turismo', width='small'),
            'Principal Destino': st.column_config.TextColumn('Principal Destino', width='medium'),
            'Fluxo (%)': st.column_config.NumberColumn('Fluxo (%)', format='%.1f%%'),
            'Tempo (h)': st.column_config.NumberColumn('Tempo (h)', format='%.2f'),
            'Alerta': st.column_config.TextColumn('Alerta', width='small')
        }
    )


def render_dependency_alerts(df: pd.DataFrame) -> None:
    """
    Renderiza cards de alertas de dependência com destaque visual.
    
    Args:
        df: DataFrame com dados das sedes
    """
    df_alerts = df[df['Alerta'] == 'SIM'].copy()
    
    if df_alerts.empty:
        st.success("**Nenhuma dependência funcional detectada**")
        st.caption("Todas as sedes têm autonomia ou fluxos principais para destinos >2h de distância")
        return
    
    st.warning(f"**{len(df_alerts)} alertas de dependência detectados**")
    
    # Exibir cada alerta em um expander
    for _, row in df_alerts.iterrows():
        with st.expander(f"ALERTA: {row['Sede']} ({row['UF']}) → {row['Principal Destino']}"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Sede de Origem", row['Sede'])
                st.caption(f"UTP: {row['UTP']}")
                st.caption(f"REGIC: {row['REGIC']}")
            
            with col2:
                st.metric("Principal Destino", row['Principal Destino'])
                st.caption(f"Proporção do Fluxo: {row['Fluxo (%)']}%")
                st.caption(f"Tempo de Viagem: {row['Tempo (h)']}h")
            
            with col3:
                st.metric("População UTP", f"{int(row['População']):,}")
                st.caption(f"Municípios: {row['Nº Municípios']}")
                st.caption(f"Total Viagens: {int(row['Viagens']):,}")
            
            st.markdown("---")
            st.markdown("""
            **Recomendação:** Esta sede apresenta forte dependência funcional de outro centro urbano. 
            Considere avaliar a consolidação ou reclassificação desta UTP.
            """)


def render_socioeconomic_charts(df: pd.DataFrame) -> None:
    """
    Renderiza gráficos de comparação socioeconômica usando Plotly.
    
    Args:
        df: DataFrame com dados das sedes
    """
    if df.empty:
        return
    
    # Gráfico 1: Top 15 Sedes por População
    st.markdown("#### Top 15 Sedes por População")
    
    df_top_pop = df.nlargest(15, 'População').copy()
    
    # Adicionar cor baseada em alerta
    df_top_pop['cor'] = df_top_pop['Alerta'].map({
        'SIM': '#ff6b6b',  # Vermelho
        '': '#4CAF50'  # Verde
    })
    
    fig_pop = go.Figure()
    fig_pop.add_trace(go.Bar(
        x=df_top_pop['População'],
        y=df_top_pop['Sede'],
        orientation='h',
        marker=dict(color=df_top_pop['cor']),
        text=df_top_pop['População'].apply(lambda x: f'{x:,.0f}'),
        textposition='outside',
        hovertemplate='<b>%{y}</b><br>População: %{x:,.0f}<extra></extra>'
    ))
    
    fig_pop.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        xaxis_title='População Total da UTP',
        yaxis_title='',
        height=500,
        showlegend=False,
        margin=dict(l=10, r=10, t=30, b=10)
    )
    
    st.plotly_chart(fig_pop, width='stretch')



def render_regic_distribution(df: pd.DataFrame) -> None:
    """
    Renderiza distribuição de sedes por classificação REGIC.
    
    Args:
        df: DataFrame com dados das sedes
    """
    if df.empty or 'REGIC' not in df.columns:
        return
    
    st.markdown("#### Distribuição por Classificação REGIC")
    
    # Filtrar apenas sedes com classificação REGIC
    df_regic = df[df['REGIC'] != ''].copy()
    
    if df_regic.empty:
        st.info("Nenhuma sede com classificação REGIC disponível")
        return
    
    # Contar por classificação
    regic_counts = df_regic.groupby('REGIC').size().reset_index(name='Quantidade')
    regic_counts = regic_counts.sort_values('Quantidade', ascending=False)
    
    # Criar gráfico de barras
    fig_regic = px.bar(
        regic_counts,
        x='REGIC',
        y='Quantidade',
        text='Quantidade',
        color='Quantidade',
        color_continuous_scale='Blues'
    )
    
    fig_regic.update_traces(textposition='outside')
    fig_regic.update_layout(
        xaxis_title='Classificação REGIC',
        yaxis_title='Número de Sedes',
        showlegend=False,
        height=400,
        margin=dict(l=10, r=10, t=10, b=10)
    )
    
    fig_regic.update_xaxes(tickangle=45)
    
    st.plotly_chart(fig_regic, width='stretch')


def render_origin_destination_table(df: pd.DataFrame, show_alerts_only: bool = False) -> None:
    """
    Renderiza tabela comparativa no formato origem-destino.
    
    Mostra dados de origem e destino lado a lado para facilitar
    a identificação de qual sede tem mais relevância.
    
    Args:
        df: DataFrame com dados origem-destino (do export_origin_destination _comparison)
        show_alerts_only: Se True, mostra apenas pares com alerta
    """
    if df.empty:
        st.info("Nenhuma relação origem-destino detectada.")
        st.caption("Não há sedes cujo principal fluxo vai para outra sede.")
        return
    
    df_display = df.copy()
    
    # Filtrar apenas alertas se solicitado
    if show_alerts_only:
        df_display = df_display[df_display['Alerta'] == 'SIM']
        
        if df_display.empty:
            st.success("Nenhum alerta de dependência detectado!")
            return
    
    # Exibir contagem
    st.caption(f"**{len(df_display)} relações origem-destino** (ordenadas por % de fluxo)")
    
    # Configurar colunas com agrupamento visual (colunas intercaladas)
    st.dataframe(
        df_display,
        width='stretch',
        hide_index=True,
        column_config={
            # UTP (intercalado)
            'Origem_UTP': st.column_config.TextColumn('🔵 UTP', width='small', help='UTP de origem'),
            'Destino_UTP': st.column_config.TextColumn('🟢 UTP', width='small', help='UTP de destino'),
            
            # Sede (intercalado)
            'Origem_Sede': st.column_config.TextColumn('🔵 Sede', width='medium', help='Sede de origem'),
            'Destino_Sede': st.column_config.TextColumn('🟢 Sede', width='medium', help='Sede de destino'),
            
            # UF (intercalado)
            'Origem_UF': st.column_config.TextColumn('🔵 UF', width='small'),
            'Destino_UF': st.column_config.TextColumn('🟢 UF', width='small'),
            
            # REGIC (intercalado)
            'Origem_REGIC': st.column_config.TextColumn('🔵 REGIC', width='small'),
            'Destino_REGIC': st.column_config.TextColumn('🟢 REGIC', width='small'),
            
            # População (intercalado + delta)
            'Origem_População': st.column_config.NumberColumn('🔵 Pop.', format='%d', help='População total da UTP de origem'),
            'Destino_População': st.column_config.NumberColumn('🟢 Pop.', format='%d', help='População total da UTP de destino'),
            'Δ_População': st.column_config.NumberColumn('Δ Pop.', format='%+d', help='Diferença populacional (Destino - Origem)'),
            
            # Municípios (intercalado)
            'Origem_Municípios': st.column_config.NumberColumn('🔵 Mun.', width='small', help='Número de municípios'),
            'Destino_Municípios': st.column_config.NumberColumn('🟢 Mun.', width='small', help='Número de municípios'),
            
            # Viagens (intercalado + delta)
            'Origem_Viagens': st.column_config.NumberColumn('🔵 Viag.', format='%d', help='Total de viagens da UTP'),
            'Destino_Viagens': st.column_config.NumberColumn('🟢 Viag.', format='%d', help='Total de viagens da UTP'),
            'Δ_Viagens': st.column_config.NumberColumn('Δ Viag.', format='%+d', help='Diferença de viagens (Destino - Origem)'),
            
            # Aeroporto (intercalado)
            'Origem_Aeroporto': st.column_config.TextColumn('🔵 Aero', width='small'),
            'Destino_Aeroporto': st.column_config.TextColumn('🟢 Aero', width='small'),
            
            # ICAO (intercalado)
            'Origem_ICAO': st.column_config.TextColumn('🔵 ICAO', width='small'),
            'Destino_ICAO': st.column_config.TextColumn('🟢 ICAO', width='small'),
            
            # Turismo (intercalado)
            'Origem_Turismo': st.column_config.TextColumn('🔵 Turismo', width='small'),
            'Destino_Turismo': st.column_config.TextColumn('🟢 Turismo', width='small'),
            
            # Relação
            'Fluxo_%': st.column_config.NumberColumn('📊 Fluxo (%)', format='%.1f%%', help='% do fluxo da origem que vai para o destino'),
            'Tempo_h': st.column_config.NumberColumn('⏱️ Tempo (h)', format='%.2f', help='Tempo de viagem'),
            'Alerta': st.column_config.TextColumn('Alerta', width='small'),
            
            # Razão
            'Razão_Pop': st.column_config.NumberColumn('Razão Pop.', format='%.2fx', help='População Destino / População Origem')
        },
        height=600
    )
    
    # Legenda explicativa
    st.markdown("---")
    st.markdown("""
    **📖 Como interpretar:**
    - 🔵 **Origem**: Sede que tem dependência (fluxo principal sai desta sede)
    - 🟢 **Destino**: Sede que recebe o fluxo principal
    - **Δ Positivo**: Destino é maior que origem (dependência esperada)
    - **Δ Negativo**: Origem é maior que destino (situação atípica)
    - **Razão \u003e 1**: Destino é mais populoso que origem
    - **Razão \u003c 1**: Origem é mais populosa que destino
    """)


def render_comprehensive_table(df: pd.DataFrame, show_alerts_only: bool = False) -> None:
    """
    Renderiza tabela comparativa COMPLETA no formato origem-destino.
    
    Suporta mais de 70 colunas de indicadores socioeconômicos.
    
    Args:
        df: DataFrame com dados origem-destino (do export_comprehensive_dependency_table)
        show_alerts_only: Se True, mostra apenas pares com alerta
    """
    if df.empty:
        st.info("Nenhuma relação origem-destino detectada.")
        return
    
    df_display = df.copy()
    
    # Filtrar apenas alertas se solicitado
    if show_alerts_only:
        # Verifica qual coluna de alerta existe
        if 'ALERTA_DEPENDENCIA' in df_display.columns:
            # Filtrar por string não vazia (contém emoji ou texto "SIM")
            df_display = df_display[df_display['ALERTA_DEPENDENCIA'].astype(str).str.len() > 0]
        elif 'Alerta' in df_display.columns:
            df_display = df_display[df_display['Alerta'] == 'SIM']
            
        if df_display.empty:
            st.success("Nenhum alerta de dependência detectado!")
            return
    
    # Exibir contagem
    st.caption(f"**{len(df_display)} relações origem-destino** (ordenadas por % de fluxo)")
    
    # Configuração de colunas mapeada para melhor visualização
    column_config = {
        # --- Identificação (Fixa) ---
        'nome_municipio_origem': st.column_config.TextColumn('🔵 Origem', width='medium'),
        'nome_municipio_destino': st.column_config.TextColumn('🟢 Destino', width='medium'),
        'UTP_ORIGEM': st.column_config.TextColumn('UTP Orig.', width='small'),
        'UTP_DESTINO': st.column_config.TextColumn('UTP Dest.', width='small'),
        'UF_ORIGEM': st.column_config.TextColumn('UF Orig.', width='small'),
        'UF_DESTINO': st.column_config.TextColumn('UF Dest.', width='small'),
        
        # --- Relação ---
        'proporcao_fluxo_pct': st.column_config.ProgressColumn(
            'Fluxo (%)', 
            format='%.1f%%', 
            min_value=0, 
            max_value=100,
            help='Proporção de viagens da origem para o destino'
        ),
        'qtd_viagens': st.column_config.NumberColumn('Viagens', format='%d'), # Viagens específicas Origem->Destino
        'Tempo': st.column_config.NumberColumn('Tempo (h)', format='%.2f'),
        'ALERTA_DEPENDENCIA': st.column_config.TextColumn('Alerta', help='Indicador de dependência crítica'),
        'observacao': st.column_config.TextColumn('Obs.', width='large'),
        
        # --- População ---
        'PopulacaoSede_Origem': st.column_config.NumberColumn('🔵 Pop. Sede', format='%d'),
        'PopulacaoSede_Destino': st.column_config.NumberColumn('🟢 Pop. Sede', format='%d'),
        
        # --- Aeroporto ---
        'AeroportoICAO_Origem': st.column_config.TextColumn('🔵 Aero', width='small'),
        'AeroportoICAO_Destino': st.column_config.TextColumn('🟢 Aero', width='small'),
        'AeroportoPassageiros_Origem': st.column_config.NumberColumn('🔵 Pax Aero', format='%d'),
        'AeroportoPassageiros_Destino': st.column_config.NumberColumn('🟢 Pax Aero', format='%d'),
        
        # --- Turismo ---
        'ClassificacaoTurismo_Origem': st.column_config.TextColumn('🔵 Turismo', width='small'),
        'ClassificacaoTurismo_Destino': st.column_config.TextColumn('🟢 Turismo', width='small'),
        'RegiaoTuristica_Origem': st.column_config.TextColumn('🔵 Reg. Tur.', width='small'),
        'RegiaoTuristica_Destino': st.column_config.TextColumn('🟢 Reg. Tur.', width='small'),
        
        # --- Economia ---
        'RendaPerCapita_Origem': st.column_config.NumberColumn('🔵 Índice Renda PC', format='%.2f'),
        'RendaPerCapita_Destino': st.column_config.NumberColumn('🟢 Índice Renda PC', format='%.2f'),
        'ICE_R_Origem': st.column_config.NumberColumn('🔵 ICE-R', format='%.2f'),
        'ICE_R_Destino': st.column_config.NumberColumn('🟢 ICE-R', format='%.2f'),
        
        # --- Saúde ---
        'Medicos100MilHab_Origem': st.column_config.NumberColumn('🔵 Méd./100k', format='%.1f'),
        'Medicos100MilHab_Destino': st.column_config.NumberColumn('🟢 Méd./100k', format='%.1f'),
        'Leitos100MilHab_Origem': st.column_config.NumberColumn('🔵 Leitos/100k', format='%.1f'),
        'Leitos100MilHab_Destino': st.column_config.NumberColumn('🟢 Leitos/100k', format='%.1f'),
        
        # --- Conectividade ---
        'Cobertura4G_Origem': st.column_config.NumberColumn('🔵 4G (%)', format='%.1f%%'),
        'Cobertura4G_Destino': st.column_config.NumberColumn('🟢 4G (%)', format='%.1f%%'),
        'DensidadeBandaLarga_Origem': st.column_config.NumberColumn('🔵 Band. Larg.', format='%.1f'),
        'DensidadeBandaLarga_Destino': st.column_config.NumberColumn('🟢 Band. Larg.', format='%.1f'),
    }
    
    # Seleção de colunas para exibir (ordem lógica)
    cols_to_show = [
        'nome_municipio_origem', 'nome_municipio_destino', 
        'proporcao_fluxo_pct', 'qtd_viagens', 'Tempo', 'ALERTA_DEPENDENCIA',
        
        'UF_ORIGEM', 'UF_DESTINO',
        'UTP_ORIGEM', 'UTP_DESTINO',
        
        'PopulacaoSede_Origem', 'PopulacaoSede_Destino',
        
        'ClassificacaoTurismo_Origem', 'ClassificacaoTurismo_Destino',
        'RegiaoTuristica_Origem', 'RegiaoTuristica_Destino',
        
        'AeroportoICAO_Origem', 'AeroportoICAO_Destino',
        
        'RendaPerCapita_Origem', 'RendaPerCapita_Destino',
        'ICE_R_Origem', 'ICE_R_Destino',
        
        'Medicos100MilHab_Origem', 'Medicos100MilHab_Destino',
        'Cobertura4G_Origem', 'Cobertura4G_Destino'
    ]
    
    # Filtrar apenas colunas que existem no DF
    cols_existing = [c for c in cols_to_show if c in df_display.columns]
    
    # Adicionar observação se existir
    if 'observacao' in df_display.columns:
        cols_existing.append('observacao')
        
    st.dataframe(
        df_display[cols_existing],
        width='stretch',
        hide_index=True,
        column_config=column_config,
        height=600
    )
    
    with st.expander("Ver todas as colunas disponíveis (Tabela Bruta)"):
        st.dataframe(df_display, use_container_width=True)

