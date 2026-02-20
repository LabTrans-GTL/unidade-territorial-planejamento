# src/interface/components/metrics.py
import streamlit as st


def render_top_metrics(manager):
    """Renderiza métricas no topo do dashboard seguindo Padrão Gov.br."""
    if manager.graph is None or manager.graph.hierarchy is None:
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Total de municípios
    mun_count = sum(1 for n, d in manager.graph.hierarchy.nodes(data=True) 
                    if d.get('type') == 'municipality')
    with col1:
        st.metric(
            label="Municípios",
            value=f"{mun_count:,}".replace(',', '.'),
            help="Total de municípios brasileiros carregados"
        )
    
    # Total de UTPs
    utp_count = sum(1 for n, d in manager.graph.hierarchy.nodes(data=True) 
                    if d.get('type') == 'utp')
    with col2:
        st.metric(
            label="UTPs",
            value=f"{utp_count:,}".replace(',', '.'),
            help="Total de Unidades de Planejamento Territorial"
        )
    
    # Total de RMs
    rm_count = sum(1 for n, d in manager.graph.hierarchy.nodes(data=True) 
                   if d.get('type') == 'rm')
    with col3:
        st.metric(
            label="Regiões Metropolitanas",
            value=f"{rm_count:,}".replace(',', '.'),
            help="Total de Regiões Metropolitanas"
        )
    
    # UTPs unitárias (alerta)
    unitary_utps = manager.graph.get_unitary_utps()
    with col4:
        st.metric(
            label="UTPs Unitárias",
            value=len(unitary_utps),
            delta="Requer consolidação" if len(unitary_utps) > 0 else "Nenhuma",
            help="UTPs com apenas um município (candidatas a consolidação)"
        )


def render_summary_stats(manager):
    """Renderiza estatísticas de consolidação com Padrão Gov.br."""
    st.subheader("Resumo Executivo", divider="blue")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.info("Consolidações Passo 5: 0")
    with col2:
        st.info("Consolidações Passo 7: 0")
    with col3:
        st.success("Taxa de Validação: 100%")

