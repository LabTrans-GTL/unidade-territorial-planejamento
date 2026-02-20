# src/interface/dashboard.py
import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
import json
import logging
from pathlib import Path
from datetime import datetime
from src.utils import DataLoader
from src.interface.consolidation_loader import ConsolidationLoader
from src.interface.snapshot_loader import SnapshotLoader
from src.run_consolidation import run_consolidation
from src.pipeline.sede_analyzer import SedeAnalyzer
from src.interface.components import sede_comparison
from src.core.graph import TerritorialGraph
from src.interface.flow_utils import (
    get_top_municipalities_in_utp,
    get_top_destinations_for_municipality,
    format_flow_popup_html,
    get_municipality_total_flow
)
from src.interface.map_flow_render import render_map_with_flow_popups


# ===== CONFIGURAÇÃO DA PÁGINA =====
st.set_page_config(
    page_title="Unidade Territorial de Planejamento",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado
st.markdown("""
<style>
    :root {
        --primary-color: #1351B4;
    }
    [data-testid="stMetricValue"] {
        color: #1351B4;
        font-weight: bold;
    }
    .step-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.85rem;
    }
    .step-initial { background-color: #e3f2fd; color: #1351B4; }
    .step-final { background-color: #e8f5e9; color: #2e7d32; }
    .status-badge {
        display: inline-block;
        padding: 6px 16px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.9rem;
    }
    .status-executed { background-color: #d4edda; color: #155724; }
    .status-pending { background-color: #fff3cd; color: #856404; }
    
    /* Forçar alinhamento à esquerda das tabs */
    .stTabs {
        width: 100% !important;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        justify-content: flex-start !important;
        gap: 1rem !important; /* Reduzido de 2rem */
        width: 100% !important;
        margin-left: 0 !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: auto !important;
        white-space: pre-wrap !important;
        background-color: transparent !important;
        border: none !important;
        padding-left: 0 !important;
        padding-right: 0 !important;
        margin-right: 1.5rem !important; /* Espaçamento entre tabs */
        flex-grow: 0 !important; /* Impede que estiquem */
    }

    /* Container das tabs - removendo centralização do Streamlit */
    div[data-testid="stHorizontalBlock"] > div:has([data-baseweb="tab-list"]) {
        width: 100% !important;
    }
    
    /* Garantir que o indicador de seleção acompanhe */
    .stTabs [data-baseweb="tab-highlight"] {
        background-color: #1351B4 !important;
    }
</style>
""", unsafe_allow_html=True)

# Paleta Pastel (Cores suaves e agradáveis)
from src.interface.palette import get_palette

# Carregar Paleta Ativa
PASTEL_PALETTE = get_palette()


@st.cache_data(show_spinner="Carregando mapa...", hash_funcs={gpd.GeoDataFrame: id, pd.DataFrame: id})
def get_geodataframe(optimized_geojson_path, df_municipios):
    """
    Carrega o GeoDataFrame pré-processado de municípios.
    
    Se o arquivo otimizado não existir (gerado pelo pipeline main.py),
    exibe um aviso e retorna None.
    """
    if not optimized_geojson_path.exists():
        st.warning("""
        **GeoDataFrame otimizado não encontrado!**
        
        Para melhor performance, execute o pipeline completo:
        ```bash
        python main.py
        ```
        Isso irá pré-processar e salvar os GeoDataFrames otimizados.
        """)
        return None

    try:
        # Carregar GeoJSON pré-processado
        gdf = gpd.read_file(optimized_geojson_path)
        
        # Atualizar com dados mais recentes do df_municipios
        # (caso o initialization.json tenha sido alterado após o pré-processamento)
        df_mun_copy = df_municipios.copy()
        df_mun_copy['cd_mun'] = df_mun_copy['cd_mun'].astype(str)
        gdf['CD_MUN'] = gdf['CD_MUN'].astype(str)
        
        # Re-merge para garantir dados atualizados
        gdf = gdf.drop(columns=['uf', 'utp_id', 'sede_utp', 'regiao_metropolitana', 'nm_sede'], errors='ignore')
        gdf = gdf.merge(
            df_mun_copy[['cd_mun', 'uf', 'utp_id', 'sede_utp', 'regiao_metropolitana', 'nm_mun']], 
            left_on='CD_MUN', right_on='cd_mun', how='left'
        )
        
        # Recalcular nomes das sedes
        df_sedes = df_mun_copy[df_mun_copy['sede_utp'] == True][['utp_id', 'nm_mun']].set_index('utp_id')
        sede_mapper = df_sedes['nm_mun'].to_dict()
        gdf['nm_sede'] = gdf['utp_id'].map(sede_mapper).fillna('')
        gdf['regiao_metropolitana'] = gdf['regiao_metropolitana'].fillna('')
        
        return gdf
    except Exception as e:
        st.error(f"Erro ao carregar mapa otimizado: {e}")
        return None


@st.cache_data(show_spinner="Carregando RMs...", hash_funcs={gpd.GeoDataFrame: id})
def get_derived_rm_geodataframe(optimized_rm_geojson_path):
    """
    Carrega o GeoDataFrame pré-processado de Regiões Metropolitanas.
    
    Se o arquivo otimizado não existir (gerado pelo pipeline main.py),
    retorna None silenciosamente (RMs são opcionais).
    """
    if not optimized_rm_geojson_path.exists():
        logging.info("GeoDataFrame de RMs otimizado não encontrado (opcional)")
        return None
    
    try:
        # Carregar GeoJSON pré-processado
        gdf_rm = gpd.read_file(optimized_rm_geojson_path)
        return gdf_rm
        
    except Exception as e:
        logging.error(f"Erro ao carregar RMs otimizadas: {e}")
        return None


@st.cache_data(show_spinner="Carregando Estados...", hash_funcs={gpd.GeoDataFrame: id})
def get_derived_state_geodataframe(optimized_state_geojson_path):
    """
    Carrega o GeoDataFrame pré-processado de Estados.
    """
    if not optimized_state_geojson_path.exists():
        return None
    
    try:
        gdf_states = gpd.read_file(optimized_state_geojson_path)
        return gdf_states
    except Exception as e:
        logging.error(f"Erro ao carregar Estados otimizados: {e}")
        return None


@st.cache_resource(show_spinner="Construindo Grafo Territorial...")
def get_territorial_graph(df_municipios):
    """
    Cria e cacheia o grafo territorial completo.
    Evita recriação a cada renderização.
    """
    if df_municipios is None or df_municipios.empty:
        return None
        
    try:
        graph = TerritorialGraph()
        # Carregar estrutura do grafo a partir dos dados
        for _, row in df_municipios.iterrows():
            cd_mun = int(row['cd_mun'])
            nm_mun = row.get('nm_mun', str(cd_mun))
            utp_id = str(row.get('utp_id', 'SEM_UTP'))
            rm_name = row.get('regiao_metropolitana', '')
            
            if not rm_name or str(rm_name).strip() == '':
                rm_name = "SEM_RM"
            
            # Criar hierarquia no grafo
            rm_node = f"RM_{rm_name}"
            if not graph.hierarchy.has_node(rm_node):
                graph.hierarchy.add_node(rm_node, type='rm', name=rm_name)
                graph.hierarchy.add_edge(graph.root, rm_node)
            
            utp_node = f"UTP_{utp_id}"
            if not graph.hierarchy.has_node(utp_node):
                graph.hierarchy.add_node(utp_node, type='utp', utp_id=utp_id)
                graph.hierarchy.add_edge(rm_node, utp_node)
            
            graph.hierarchy.add_node(cd_mun, type='municipality', name=nm_mun)
            graph.hierarchy.add_edge(utp_node, cd_mun)
        
        logging.info(f"Grafo territorial criado: {len(graph.hierarchy.nodes)} nós")
        return graph
    except Exception as e:
        logging.error(f"Erro ao criar grafo territorial: {e}")
        return None


@st.cache_data(show_spinner="Carregando coloração pré-calculada...", hash_funcs={gpd.GeoDataFrame: id, pd.DataFrame: id})
def load_or_compute_coloring(gdf, cache_filename="initial_coloring.json"):
    """
    Carrega a coloração pré-calculada do cache.
    
    O cache é gerado pela Etapa 3 do main.py (scripts/s03_precompute_coloring.py).
    Se o cache não existir, retorna um dicionário vazio e exibe um aviso.
    
    Args:
        gdf: GeoDataFrame (não usado, mantido para compatibilidade)
        cache_filename: Nome do arquivo de cache ("initial_coloring.json" ou "consolidated_coloring.json")
    
    Returns:
        Dict: mapeamento cd_mun (int) -> color_index (int)
    """
    cache_path = Path(__file__).parent.parent.parent / "data" / cache_filename
    
    # Check alternate location (03_processed) if not in root data
    if not cache_path.exists():
        alt_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / cache_filename
        if alt_path.exists():
            cache_path = alt_path

    # Tentar carregar do arquivo
    if cache_path.exists():
        try:
            with open(cache_path, "r") as f:
                coloring_str_keys = json.load(f)
                # JSON chaves são sempre strings, converter para int
                coloring = {int(k): v for k, v in coloring_str_keys.items()}
                logging.info(f"✅ Coloração carregada do cache: {len(coloring)} municípios")
                return coloring
        except Exception as e:
            logging.error(f"❌ Erro ao ler cache de coloração: {e}")
            st.error(f"Erro ao carregar cache de coloração: {e}")
            return {}
    
    # Cache não existe - avisar usuário
    logging.warning("⚠️ Cache de coloração não encontrado!")
    st.warning("""
    **Cache de coloração não encontrado!**
    
    Para otimizar o carregamento do dashboard, execute o pipeline completo:
    
    ```bash
    python main.py
    ```
    
    Isso irá pré-calcular a coloração e salvar em cache.
    """)
    
    return {}


@st.cache_data(show_spinner="Calculando contornos estaduais...", hash_funcs={gpd.GeoDataFrame: id})
def get_state_boundaries(gdf):
    """
    Calcula os contornos dos estados dissolvendo os municípios.
    """
    if gdf is None or gdf.empty:
        return None
        
    try:
        # Dissolver por UF
        gdf_states = gdf[['uf', 'geometry']].dissolve(by='uf').reset_index()
        return gdf_states
    except Exception as e:
        logging.error(f"Erro ao calcular contornos estaduais: {e}")
        return None






def render_territorial_config_table(step_key: str, snapshot_loader: "SnapshotLoader", allowed_cd_mun: set = None) -> pd.DataFrame:
    """
    Builds a territorial configuration table from a snapshot.

    Each row represents one UTP with:
      - UTP: utp_id
      - Sede: name of the municipality where sede_utp == True
      - Qtd. Municípios: total municipalities in the UTP
      - Municípios: comma-separated list of municipality names

    Args:
        step_key: snapshot key ('step1', 'step5', 'step6', 'step8')
        snapshot_loader: SnapshotLoader instance
        allowed_cd_mun: optional set of cd_mun strings to filter municipalities.
                        If None, all municipalities in the snapshot are shown.

    Returns:
        DataFrame with the columns above, sorted by UTP.
    """
    data = snapshot_loader.load_snapshot(step_key)
    if not data:
        return pd.DataFrame()

    nodes = data.get("nodes", {})

    # Collect municipality-level nodes only
    utps: dict[str, dict] = {}  # utp_id -> {sede, municipios: list}
    for node_id, attrs in nodes.items():
        if not node_id.isdigit():
            continue
        if allowed_cd_mun is not None and node_id not in allowed_cd_mun:
            continue

        utp_id = str(attrs.get("utp_id", "SEM_UTP"))
        name = attrs.get("name", node_id)
        is_sede = bool(attrs.get("sede_utp", False))

        if utp_id not in utps:
            utps[utp_id] = {"sede": None, "municipios": []}

        utps[utp_id]["municipios"].append(name)
        if is_sede:
            utps[utp_id]["sede"] = name

    if not utps:
        return pd.DataFrame()

    rows = []
    for utp_id, info in utps.items():
        muns_sorted = sorted(info["municipios"])
        sede = info["sede"] or "—"
        rows.append({
            "UTP": utp_id,
            "Sede": sede,
            "Qtd. Municípios": len(muns_sorted),
            "Municípios": ", ".join(muns_sorted),
        })

    df = pd.DataFrame(rows).sort_values("UTP").reset_index(drop=True)
    return df


def create_enriched_utp_summary(df_municipios):
    """
    Cria resumo enriquecido das UTPs com métricas territoriais relevantes.
    
    Args:
        df_municipios: DataFrame com dados dos municípios
        
    Returns:
        DataFrame com métricas agregadas por UTP
    """
    if df_municipios.empty:
        return pd.DataFrame()
    
    # Preparar dados
    df = df_municipios.copy()
    
    # Garantir types numéricos
    numeric_cols = ['populacao_2022', 'area_km2']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Agregar viagens por município
    df['total_viagens'] = 0
    if 'modais' in df.columns:
        df['total_viagens'] = df['modais'].apply(
            lambda x: sum(x.values()) if isinstance(x, dict) else 0
        )
    
    # Identificar modal dominante
    def get_modal_dominante(modais):
        if not isinstance(modais, dict) or not modais:
            return ''
        max_modal = max(modais.items(), key=lambda x: x[1])
        if max_modal[1] == 0:
            return ''
        # Simplificar nomes
        modal_map = {
            'rodoviaria_coletiva': 'Rod. Coletiva',
            'rodoviaria_particular': 'Rod. Particular',
            'aeroviaria': 'Aérea',
            'ferroviaria': 'Ferroviária',
            'hidroviaria': 'Hidroviária'
        }
        return modal_map.get(max_modal[0], max_modal[0])
    
    df['modal_dominante'] = df['modais'].apply(get_modal_dominante) if 'modais' in df.columns else ''
    
    # Agrupar por UTP
    summary_list = []
    
    for utp_id, group in df.groupby('utp_id'):
        # Identificar sede
        sede_row = group[group['sede_utp'] == True]
        if sede_row.empty:
            continue
        
        sede = sede_row.iloc[0]
        
        # População
        pop_total = group['populacao_2022'].sum()
        
        # Maior município
        maior_mun = group.loc[group['populacao_2022'].idxmax()]
        maior_mun_nome = f"{maior_mun['nm_mun']} ({maior_mun['populacao_2022']:,.0f})"
        
        # Turismo
        turismo_cat = sede.get('turismo_classificacao', '')
        if pd.isna(turismo_cat) or str(turismo_cat).strip() == '':
            turismo_cat = '-'
        else:
            # Simplificar categoria (pegar apenas primeira parte antes do hífen)
            turismo_cat = str(turismo_cat).split('-')[0].strip()
        
        # Aeroportos na UTP
        aeroportos_info = []
        for _, mun in group.iterrows():
            if 'aeroporto' in mun and isinstance(mun['aeroporto'], dict):
                aero = mun['aeroporto']
                icao = aero.get('sigla', '') or aero.get('icao', '')
                passageiros = aero.get('passageiros_anual', 0)
                
                if icao and str(icao).strip() not in ['', 'nan', 'None']:
                    aeroportos_info.append({
                        'icao': str(icao),
                        'passageiros': int(passageiros) if passageiros else 0,
                        'municipio': mun['nm_mun']
                    })
        
        # Determinar principal aeroporto e formatar
        if aeroportos_info:
            # Ordenar por passageiros
            aeroportos_info.sort(key=lambda x: x['passageiros'], reverse=True)
            principal = aeroportos_info[0]
            
            # Formatar passageiros
            if principal['passageiros'] > 1000000:
                pass_fmt = f"{principal['passageiros']/1000000:.1f}M"
            elif principal['passageiros'] > 1000:
                pass_fmt = f"{principal['passageiros']/1000:.0f}k"
            else:
                pass_fmt = str(principal['passageiros'])
            
            if len(aeroportos_info) == 1:
                aeroporto_display = f"{principal['icao']} ({pass_fmt})"
            else:
                aeroporto_display = f"{len(aeroportos_info)} aeros | {principal['icao']} ({pass_fmt})"
        else:
            aeroporto_display = '-'
        
        # Viagens
        viagens_total = group['total_viagens'].sum()
        
        # Modal dominante da UTP (baseado na sede)
        modal_dom = sede.get('modal_dominante', '-')
        
        # REGIC
        regic = sede.get('regic', '-')
        if pd.isna(regic) or str(regic).strip() == '':
            regic = '-'
        
        # Região Metropolitana
        rm = sede.get('regiao_metropolitana', '')
        if pd.isna(rm) or str(rm).strip() == '':
            rm = '-'
        
        summary_list.append({
            'UTP': utp_id,
            'Sede': sede['nm_mun'],
            'UF': sede['uf'],
            'Municípios': len(group),
            'População': int(pop_total),
            'Maior Município': maior_mun_nome,
            'REGIC': regic,
            'RM': rm,
            'Turismo': turismo_cat,
            'Aeroportos': aeroporto_display,
            'Viagens': int(viagens_total),
            'Modal': modal_dom if modal_dom else '-'
        })
    
    # Criar DataFrame
    summary_df = pd.DataFrame(summary_list)
    
    if summary_df.empty:
        return summary_df
    
    # Ordenar por população (decrescente)
    summary_df = summary_df.sort_values('População', ascending=False)
    
    # Formatar colunas numéricas para display
    summary_df_display = summary_df.copy()
    summary_df_display['População'] = summary_df_display['População'].apply(lambda x: f"{x:,}")
    summary_df_display['Viagens'] = summary_df_display['Viagens'].apply(
        lambda x: f"{x:,}" if x > 0 else '-'
    )
    
    return summary_df_display


def analyze_unitary_utps(df_municipios):
    """
    Identifica UTPs que possuem apenas 1 município.
    
    Returns:
        DataFrame com lista de UTPs unitárias e seus detalhes
    """
    utp_counts = df_municipios.groupby('utp_id').size().reset_index(name='num_municipios')
    unitary_utps = utp_counts[utp_counts['num_municipios'] == 1]['utp_id'].tolist()
    
    if not unitary_utps:
        return pd.DataFrame()
    
    # Buscar detalhes dos municípios únicos
    df_unitary = df_municipios[df_municipios['utp_id'].isin(unitary_utps)].copy()
    
    result = df_unitary[['utp_id', 'nm_mun', 'uf', 'populacao_2022', 'regiao_metropolitana']].copy()
    result.columns = ['UTP', 'Município', 'UF', 'População', 'RM']
    result['População'] = result['População'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else '-')
    result['RM'] = result['RM'].fillna('-')
    
    return result.sort_values('UTP')


def analyze_non_contiguous_utps(gdf):
    """
    Identifica UTPs cujos municípios não são geograficamente contíguos.
    
    Usa análise espacial para verificar se todos os municípios de uma UTP
    formam uma região conectada.
    
    Returns:
        Dict com UTP_ID -> lista de componentes desconectados
    """
    if gdf is None or gdf.empty:
        return {}
    
    from shapely.ops import unary_union
    
    non_contiguous = {}
    
    # Agrupar por UTP
    for utp_id, group in gdf.groupby('utp_id'):
        if len(group) <= 1:
            continue  # UTP unitária, não há como ser não-contígua
        
        # Criar união das geometrias
        try:
            # Dissolve para criar geometria única da UTP
            utp_geom = group.geometry.unary_union
            
            # Verificar se é MultiPolygon (indica descontinuidade)
            if utp_geom.geom_type == 'MultiPolygon':
                # Contar componentes desconectados
                num_components = len(utp_geom.geoms)
                
                # Identificar quais municípios estão em cada componente
                components_info = []
                for i, component in enumerate(utp_geom.geoms):
                    # Encontrar municípios que intersectam este componente
                    municipalities_in_component = []
                    for idx, row in group.iterrows():
                        if row.geometry.intersects(component):
                            municipalities_in_component.append(row['NM_MUN'])
                    
                    if municipalities_in_component:
                        components_info.append(municipalities_in_component)
                
                if num_components > 1:
                    non_contiguous[utp_id] = {
                        'num_components': num_components,
                        'components': components_info,
                        'num_municipalities': len(group)
                    }
        except Exception as e:
            logging.warning(f"Erro ao analisar contiguidade da UTP {utp_id}: {e}")
            continue
    
    return non_contiguous





def render_dashboard(manager):
    """Dashboard com visualização do pipeline de consolidação territorial."""
    
    # === LOAD CONSOLIDATION CACHE ===
    consolidation_loader = ConsolidationLoader()
    snapshot_loader = SnapshotLoader()
    
    # Flags de controle de fluxo (inicializa se não existir).columns([2, 1, 1])
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.title("Unidade Territorial de Planejamento")
        st.markdown("Visualização da distribuição inicial e pós-consolidação de UTPs")
    
    with col3:
        status_class = "status-executed" if consolidation_loader.is_executed() else "status-pending"
        status_text = "Consolidado" if consolidation_loader.is_executed() else "Pendente"
        st.markdown(f"<div class='status-badge {status_class}'>{status_text}</div>", unsafe_allow_html=True)
    
    # === SIDEBAR - FILTROS & CONTROLES ===
    with st.sidebar:
        st.markdown("### Filtros")
        st.markdown("---")
        
        # Carregar dados
        data_loader = DataLoader()
        df_municipios = data_loader.get_municipios_dataframe()
        metadata = data_loader.get_metadata()
        
        if df_municipios.empty:
            st.error("Falha ao carregar dados.")
            return
        
        # Garantir types consistentes
        if 'utp_id' in df_municipios.columns:
            df_municipios['utp_id'] = df_municipios['utp_id'].astype(str)
        
        # Filtro por UF
        ufs = sorted(df_municipios['uf'].unique().tolist())
        all_ufs = st.checkbox("Brasil Completo", value=True)
        
        if all_ufs:
            selected_ufs = ufs
            st.multiselect("Estados (UF)", ufs, default=ufs, disabled=True)
        else:
            selected_ufs = st.multiselect("Estados (UF)", ufs, default=[])
        
        # Filtro por Município (Novo)
        st.markdown("---")
        # Criar lista formatada "Nome (UF)"
        df_municipios['display_name'] = df_municipios['nm_mun'] + " (" + df_municipios['uf'] + ")"
        mun_options = sorted(df_municipios['display_name'].unique().tolist())
        
        selected_muns_search = st.multiselect(
            "Buscar Município", 
            mun_options,
            help="Selecione um ou mais municípios para visualizar suas UTPs completas."
        )
        
        # Lógica de Filtro Reverso: Município -> UTP
        # Se municípios forem selecionados, eles REDEFINEM a lista de UTPs selecionadas
        forced_utps_from_search = []
        if selected_muns_search:
            # Extrair nomes puros (remove UF) - mas melhor usar ID se possível, aqui vamos pelo display_name
            # Filtrar DF original
            mask_search = df_municipios['display_name'].isin(selected_muns_search)
            forced_utps_from_search = df_municipios[mask_search]['utp_id'].unique().tolist()
            
            # Atualizar/Forçar UTPs selecionadas
            # Nota: Isso vai apenas impactar o filtro visual, não altera o widget de multiselect acima
            # para não quebrar o estado do Streamlit. Apenas usamos na lógica de filtragem.
            if forced_utps_from_search:
                st.info(f"Visualizando {len(forced_utps_from_search)} UTP(s) referente(s) à busca.")
        
        # Filtro por UTP (Mantido, mas com lógica condicional)
        if selected_ufs:
            df_utp_options = df_municipios[df_municipios['uf'].isin(selected_ufs)]
        else:
            df_utp_options = df_municipios
            
        utps_list = sorted(df_utp_options['utp_id'].unique().tolist())
        all_utps = st.checkbox("Todas as UTPs", value=False)
        
        if all_utps:
            selected_utps = utps_list
            st.multiselect("UTPs", utps_list, default=utps_list, disabled=True)
        else:
            # Se houver busca por muni, podemos pré-selecionar ou apenas ignorar este campo na lógica final
            default_utps = []
            selected_utps = st.multiselect("UTPs", utps_list, default=default_utps)
        
        st.markdown("---")
        st.caption(f"Dados de: {metadata.get('timestamp', 'N/A')[:10]}")
        

    
    # Aplicar filtros
    # Aplicar filtros
    df_filtered = df_municipios[df_municipios['uf'].isin(selected_ufs)].copy()
    
    # Lógica de prioridade: Busca por Município > Filtro de UTP
    if forced_utps_from_search:
        # Se buscou município, ignora o filtro de UTP manual e mostra as UTPs da busca
        # Mas mantemos o filtro de UF? Geralmente user quer ver o resultado da busca independente da UF
        # Vamos priorizar a busca globalmente
        df_filtered = df_municipios[df_municipios['utp_id'].isin(forced_utps_from_search)].copy()
    elif selected_utps:
        df_filtered = df_filtered[df_filtered['utp_id'].isin(selected_utps)]
    
    
    # Carregar GeoDataFrames otimizados (gerados pelo pipeline main.py)
    maps_dir = Path(__file__).parent.parent.parent / "data" / "04_maps"
    optimized_municipalities_path = maps_dir / "municipalities_optimized.geojson"
    optimized_rm_path = maps_dir / "rm_boundaries_optimized.geojson"
    
    gdf = get_geodataframe(optimized_municipalities_path, df_municipios)
    gdf_rm = get_derived_rm_geodataframe(optimized_rm_path)
    
    # Carregar Estados otimizados
    optimized_state_path = maps_dir / "state_boundaries_optimized.geojson"
    gdf_states_optimized = get_derived_state_geodataframe(optimized_state_path)

    
    # 1. Preparar DataFrame limpo para o grafo (sem dicts para evitar erro de hash)
    # Selecionamos apenas as colunas necessárias para a estrutura topológica
    topology_cols = ['cd_mun', 'nm_mun', 'utp_id', 'regiao_metropolitana']
    df_topology = df_municipios[topology_cols].copy()
    
    # Criar e cachear grafo territorial usando o DF limpo
    graph = get_territorial_graph(df_topology)
    
    # 2. Carregar ou calcular coloração GLOBAL (Persistente em arquivo)
    # ATENÇÃO: Carregamos aqui a INITIAL por padrão, mas cada tab pode pedir a sua
    global_colors_initial = load_or_compute_coloring(gdf, "initial_coloring.json") if gdf is not None else {}
    
    # === TABS ===
    # === TABS ===
    tab1, tab2, tab3, tab4 = st.tabs([
        "Versão 8.0 - Distribuição Inicial",
        "Versão 8.1 - UTPs unitárias",
        "Versão 8.2 - Dependência entre Sedes",
        "Versão 8.3 - Centralização das Sedes"
    ])
    
    # ==== TAB 1: DISTRIBUIÇÃO INICIAL ====
    with tab1:
        st.markdown("### <span class='step-badge step-initial'>Versão 8.0</span> Distribuição Inicial", unsafe_allow_html=True)
        st.markdown("""
        **Antes da v8, o maior desafio era a integridade referencial. Com base no estudo da versão 7, foi possível efetuar as seguintes melhorias**
        
        *   **Continuidade:** 3 UTPs não possuíram municípios conexos territorialmente, totalizando 24 municípios.
        *   **Região Metropolitana:** 169 UTPs apresentavam discrepância entre as regiões metropolitanas, totalizando 2154 municípios.
        
        *Esta é configuração inicial considerada pela ferramenta, para as demais consolidações de versões.*
        """)
        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            # Usar contagem única de CDs para evitar contar shapefiles duplicados
            total_unique = df_municipios['cd_mun'].nunique()
            current_unique = df_filtered['cd_mun'].nunique()
            st.metric("Municípios", current_unique, f"{total_unique} total")
        with col2:
            st.metric("UTPs", len(df_filtered['utp_id'].unique()), f"{len(utps_list)} total")
        with col3:
            st.metric("Estados", len(df_filtered['uf'].unique()), f"{len(ufs)} total")
        
        st.markdown("---")
        st.markdown("#### Mapa Interativo")
        
        # Controle de visualização de contornos
        col_ctrl1, col_ctrl2 = st.columns(2)
        with col_ctrl1:
            show_rm_borders = st.checkbox(
                "Mostrar contornos de Regiões Metropolitanas",
                value=False,
                key='show_rm_tab1',
                help="Ativa/desativa a visualização dos contornos das Regiões Metropolitanas"
            )
        with col_ctrl2:
            show_state_borders = st.checkbox(
                "Mostrar limites Estaduais",
                value=False,
                key='show_state_tab1',
                help="Ativa/desativa a visualização dos limites dos Estados"
            )
        
        # Tentar carregar snapshot do estado inicial (Step 1)
        # Se não existir, usa o gdf base (que já é o inicial carregado dos inputs)
        gdf_initial = snapshot_loader.get_geodataframe_for_step('step1', gdf)
        gdf_display = gdf_initial if gdf_initial is not None else gdf

        if gdf_display is not None:
            gdf_filtered = gdf_display[gdf_display['uf'].isin(selected_ufs)].copy()
            if selected_utps:
                gdf_filtered = gdf_filtered[gdf_filtered['utp_id'].isin(selected_utps)]
            
            # Preparar contornos de estado (filtrado pelos estados selecionados)
            gdf_states_filtered = None
            if show_state_borders:
                # 1. Tentar usar otimizado
                if gdf_states_optimized is not None:
                    gdf_all_states = gdf_states_optimized
                # 2. Fallback: calcular do GDF atual
                elif gdf is not None:
                    gdf_all_states = get_state_boundaries(gdf)
                else:
                    gdf_all_states = None
                    
                if gdf_all_states is not None:
                    # Filtrar apenas estados selecionados ou visíveis no filtro atual
                    if selected_ufs:
                        gdf_states_filtered = gdf_all_states[gdf_all_states['uf'].isin(selected_ufs)]
                    else:
                        gdf_states_filtered = gdf_all_states

            # Renderizar mapa usando render_map_with_flow_popups
            m = render_map_with_flow_popups(
                gdf_filtered, 
                df_municipios, 
                title="Distribuição por UTP (Inicial)", 
                global_colors=global_colors_initial, 
                gdf_rm=gdf_rm, 
                show_rm_borders=show_rm_borders,
                show_state_borders=show_state_borders,
                gdf_states=gdf_states_filtered,
                PASTEL_PALETTE=PASTEL_PALETTE,
                step_key='step1'
            )
            if m:
                map_html = m._repr_html_()
                st.components.v1.html(map_html, height=600, scrolling=False)
        
        st.markdown("---")
        st.markdown("#### Configuração Territorial")
        st.caption("UTPs, sedes e municípios conforme o snapshot desta versão (v8.0 – inicial)")
        _allowed_muns_tab1 = set(df_filtered['cd_mun'].astype(str).tolist()) if not df_filtered.empty else None
        df_config_tab1 = render_territorial_config_table('step1', snapshot_loader, _allowed_muns_tab1)
        if not df_config_tab1.empty:
            st.dataframe(df_config_tab1, hide_index=True, use_container_width=True, height=400)
        else:
            st.info("Snapshot da versão inicial não encontrado. Execute o pipeline completo.")
        
        st.markdown("---")
        st.markdown("#### Resumo das UTPs")
        st.caption("Características socioeconômicas e territoriais agregadas por UTP")
        
        # Criar resumo enriquecido
        utp_summary = create_enriched_utp_summary(df_filtered)
        
        if not utp_summary.empty:
            st.markdown(f"**{len(utp_summary)} UTPs (ordenadas por população)**")
            
            # Mostrar todas as UTPs
            st.dataframe(
                utp_summary,
                width='stretch',
                hide_index=True,
                height=600
            )
            
            # Estatísticas rápidas
            st.markdown("---")
            col1, col2, col3, col4 = st.columns(4)
            
            # Converter população de volta para número para estatísticas
            pop_values = df_filtered.groupby('utp_id')['populacao_2022'].sum()
            
            with col1:
                st.metric("População Média", f"{pop_values.mean():,.0f}")
            with col2:
                st.metric("População Mediana", f"{pop_values.median():,.0f}")
            with col3:
                utps_com_aero = (utp_summary['Aeroportos'] != '-').sum()
                st.metric("UTPs com Aeroporto", utps_com_aero)
            with col4:
                # Contar total de aeroportos
                total_aeros = 0
                for val in utp_summary['Aeroportos']:
                    if val != '-':
                        if 'aeros' in val:
                            # Extrair número antes de 'aeros'
                            total_aeros += int(val.split(' ')[0])
                        else:
                            total_aeros += 1
                st.metric("Total de Aeroportos", total_aeros)
        else:
            st.info("Nenhuma UTP encontrada com os filtros selecionados.")


    
    # ==== TAB 2: PÓS-CONSOLIDAÇÃO ====
    with tab2:
        st.markdown("### <span class='step-badge step-final'>Versão 8.1</span> UTPs unitárias", unsafe_allow_html=True)
        col_title, col_btn = st.columns([3, 1])
        with col_title:
            st.markdown("""
            **O objetivo central é garantir que nenhum município permaneça isolado em uma UTP própria, a menos que não haja candidatos adjacentes válidos. O processo segue uma hierarquia de critérios:**
            
            1.  **Consolidação Funcional Orientada a Fluxos:** esta etapa utiliza a matriz OD para mover a UTP unitária para uma UTP vizinha com a qual possua maior iteração.
            2.  **Consolidação Territorial de Último Recurso:** após as tentativas baseadas em fluxos, as UTPs unitárias remanescentes são resolvidas via REGIC com a UTP vizinha de maior importância.
            """)
        with col_btn:
            if st.button("Rodar Pipeline", width='stretch', key="btn_tab_run"):
                with st.spinner("Executando..."):
                    if run_consolidation():
                        st.success("Feito!")
                        st.rerun()
                    else:
                        st.error("Erro!")
        # === MÉTRICAS PADRONIZADAS (sempre visíveis) ===
        # Verifica diretamente o snapshot step5 — independente do consolidation_result.json
        _df_metrics_tab2 = snapshot_loader.get_snapshot_dataframe('step5')
        if not _df_metrics_tab2.empty and selected_ufs:
            _df_metrics_tab2 = _df_metrics_tab2[
                _df_metrics_tab2['cd_mun'].isin(df_filtered['cd_mun'])
            ].copy()

        if not _df_metrics_tab2.empty:
            _src_label = "v8.1 – pós consolidação de unitárias"
            _df_m = _df_metrics_tab2
        else:
            _src_label = "v8.0 – distribuição inicial (snapshot v8.1 não encontrado)"
            _df_m = df_filtered

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Municípios", _df_m['cd_mun'].nunique(), f"{df_municipios['cd_mun'].nunique()} total")
        with col2:
            st.metric("UTPs", _df_m['utp_id'].nunique(), f"{len(utps_list)} total")
        with col3:
            st.metric("Estados", _df_m['uf'].nunique(), f"{len(ufs)} total")
        st.caption(f"📊 Dados: {_src_label}")

        st.markdown("---")


        if consolidation_loader.is_executed():
            # IMPORTANTE: Usar snapshot pós-unitárias (Steps 5+7) ao invés do resultado completo
            # Isso garante que as consolidações de sedes NÃO apareçam nesta aba
            post_unitary_consolidations = consolidation_loader.get_post_unitary_consolidations()
            post_unitary_mapping = consolidation_loader.get_post_unitary_mapping()
            
            # Calcular estatísticas baseadas no snapshot pós-unitárias
            # MUDANÇA CRÍTICA: Usar Snapshot Step 5 (Post-Unitary) direto
            # Isso garante que vemos exatamente o que foi salvo
            df_consolidated = snapshot_loader.get_snapshot_dataframe('step5')
            if df_consolidated.empty:
                # Fallback se snapshot nao existir (ainda nao rodou pipeline novo)
                df_consolidated = consolidation_loader.apply_post_unitary_to_dataframe(df_filtered)
            else:
                # Filtrar DF do snapshot pelos filtros da UI se necessario (ex: UFs)
                if selected_ufs:
                     df_consolidated = df_consolidated[df_consolidated['cd_mun'].isin(df_filtered['cd_mun'])].copy()
            
            st.markdown("---")
            st.markdown("#### Mapa Pós-Consolidação")
            
            # Controle de visualização de contornos de RM
            col_ctrl1, col_ctrl2 = st.columns(2)
            with col_ctrl1:
                show_rm_borders_tab2 = st.checkbox(
                    "Mostrar contornos de Regiões Metropolitanas",
                    value=False,
                    key='show_rm_tab2',
                    help="Ativa/desativa a visualização dos contornos das RMs"
                )
            with col_ctrl2:
                show_state_borders_tab2 = st.checkbox(
                    "Mostrar limites Estaduais",
                    value=False,
                    key='show_state_tab2',
                    help="Ativa/desativa a visualização dos limites dos Estados"
                )

            if gdf is not None:
                # Tentar carregar GDF do snapshot
                gdf_consolidated = snapshot_loader.get_geodataframe_for_step('step5', gdf[gdf['uf'].isin(selected_ufs)].copy())
                
                if gdf_consolidated is None:
                    # Fallback
                    gdf_consolidated = consolidation_loader.apply_post_unitary_to_dataframe(
                        gdf[gdf['uf'].isin(selected_ufs)].copy()
                    )
                
                if selected_utps:
                    gdf_consolidated = gdf_consolidated[
                        gdf_consolidated['utp_id'].isin(selected_utps)
                    ]
                
                # Calcular coloração CONSOLIDADA sobre o frame consolidado (ou usar do snapshot)
                # O snapshot loader já traz color_id. Podemos usar ele.
                colors_consolidated = {}
                
                # Check if we have valid coloring in the snapshot
                has_valid_coloring = False
                if 'color_id' in gdf_consolidated.columns:
                     unique_colors = gdf_consolidated['color_id'].unique()
                     # If we have more than 1 color, OR if we have 1 color but it's not 0 (which is default/fallback)
                     # actually 0 is a valid color index, but if ALL are 0, it's suspicious for a large map.
                     if len(unique_colors) > 1:
                         has_valid_coloring = True
                     elif len(unique_colors) == 1 and unique_colors[0] != 0:
                         has_valid_coloring = True
                     
                     if has_valid_coloring:
                         for _, row in gdf_consolidated.iterrows():
                             # Access CD_MUN safely (standardized in loader)
                             col_name = 'CD_MUN' if 'CD_MUN' in row else 'cd_mun'
                             colors_consolidated[int(row[col_name])] = int(row['color_id'])
                
                # Fallback: Load from external cache if snapshot coloring is missing or seems invalid (monochromatic 0)
                if not has_valid_coloring:
                     logging.warning("⚠️ Snapshot coloring seems invalid or missing. Loading from consolidated_coloring.json fallback.")
                     colors_consolidated = load_or_compute_coloring(gdf_consolidated, "consolidated_coloring.json")
                
                # Preparar contornos de estado
                gdf_states_filtered = None
                if show_state_borders_tab2:
                    if gdf_states_optimized is not None:
                        gdf_all_states = gdf_states_optimized
                    elif gdf is not None:
                        gdf_all_states = get_state_boundaries(gdf)
                    else:
                        gdf_all_states = None

                    if gdf_all_states is not None and selected_ufs:
                        gdf_states_filtered = gdf_all_states[gdf_all_states['uf'].isin(selected_ufs)]
                    elif gdf_all_states is not None:
                        gdf_states_filtered = gdf_all_states

                # Renderizar mapa com opção de mostrar contornos de RM
                # Renderizar mapa (TAB 2: Pós Consolidação)
                m = render_map_with_flow_popups(
                    gdf_consolidated,
                    df_municipios, 
                    title="Distribuição Consolidada (Snapshot)", 
                    global_colors=colors_consolidated,
                    gdf_rm=gdf_rm, 
                    show_rm_borders=show_rm_borders_tab2,
                    show_state_borders=show_state_borders_tab2,
                    gdf_states=gdf_states_filtered,
                    PASTEL_PALETTE=PASTEL_PALETTE,
                    step_key='step5'
                )
                if m:
                    map_html = m._repr_html_()
                    st.components.v1.html(map_html, height=600, scrolling=False)
            
            st.markdown("---")
            st.markdown("#### Configuração Territorial")
            st.caption("UTPs, sedes e municípios conforme o snapshot desta versão (v8.1 – pós UTPs unitárias)")
            _allowed_muns_tab2 = set(df_filtered['cd_mun'].astype(str).tolist()) if not df_filtered.empty else None
            df_config_tab2 = render_territorial_config_table('step5', snapshot_loader, _allowed_muns_tab2)
            if not df_config_tab2.empty:
                st.dataframe(df_config_tab2, hide_index=True, use_container_width=True, height=400)
            else:
                st.info("Snapshot da versão 8.1 não encontrado. Execute o pipeline.")
            
            st.markdown("---")
            st.markdown("#### Registro de Consolidações")
            st.caption("Consolidações de UTPs unitárias (Steps 5+7) - SEM incluir consolidação de sedes")
            
            # Preparar dados para planilha - USAR DADOS PÓS-UNITÁRIAS
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
            
            # Download do resultado
            result_json = json.dumps(consolidation_loader.result, ensure_ascii=False, indent=2)
            st.download_button(
                label="Baixar Resultado de Consolidação",
                data=result_json,
                file_name=f"consolidation_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
            


        

        



            

            

                

            


                



    # ==== TAB 3: CONSOLIDAÇÃO SEDES ====
    with tab3:
        st.markdown("### <span class='step-badge step-final'>Versão 8.2</span> Dependência entre Sedes", unsafe_allow_html=True)
        st.markdown("""
        **O objetivo desta etapa é fundir territórios quando a sede de uma UTP demonstra uma dependência funcional em relação a outra sede vizinha. Para que um UTP seja absorvida por outra, aplicamos quatro filtros sequenciais:**
        
        1.  **Dependência de fluxo e tempo:** a sede da UTP “A” deve ter seu fluxo principal de viagens voltado para a sede da UTP “B”, com um tempo de deslocamento de <= 2h.
        2.  **Compatibilidade de RM:** ambas as sedes pertencem à mesma Região Metropolitana ou ambas não pertencem a nenhuma.
        3.  **Adjacência Geográfica:** As UTPs devem compartilhar uma fronteira física com a UTP de destino.
        4.  **Pontuação de Infraestrutura:** A UTP de destino deve possuir um nível de infraestrutura (Aeroportos, Turismo e REGIC) superior a UTP de origem.
        """)
        st.markdown("---")

        # Verificar se existe resultado dedicado de Sedes
        sede_executed = consolidation_loader.is_sede_executed()
        
        if sede_executed:
             sede_result = consolidation_loader.get_sede_result()
             sede_consolidations = sede_result.get('consolidations', [])
             sede_mapping = sede_result.get('utps_mapping', {})
             
             if gdf is not None:
                 # Filtrar GDF
                 gdf_sliced = gdf[gdf['uf'].isin(selected_ufs)].copy()
                 if selected_utps:
                     gdf_sliced = gdf_sliced[gdf_sliced['utp_id'].isin(selected_utps)]
                     
                 # Aplicar consolidação TOTAL via Snapshot Step 6
                 gdf_final = snapshot_loader.get_geodataframe_for_step('step6', gdf_sliced)
                 
                 if gdf_final is None:
                     # Fallback
                     gdf_final = consolidation_loader.apply_consolidations_to_dataframe(gdf_sliced, custom_mapping=total_mapping)
                 
                 # === MÉTRICAS PADRONIZADAS ===
                 if not gdf_final.empty:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        # Contagem única de municípios (coluna pode variar caixa dependendo da fonte, normalizar)
                        col_cd = 'CD_MUN' if 'CD_MUN' in gdf_final.columns else 'cd_mun'
                        current_unique = gdf_final[col_cd].astype(str).nunique()
                        total_unique = df_municipios['cd_mun'].nunique()
                        st.metric("Municípios", current_unique, f"{total_unique} total")
                    with col2:
                        current_utps = gdf_final['utp_id'].nunique()
                        st.metric("UTPs", current_utps, f"{len(utps_list)} total")
                    with col3:
                        current_ufs = gdf_final['uf'].nunique()
                        st.metric("Estados", current_ufs, f"{len(ufs)} total")
                 
                 st.markdown("---")

                 # Controle de visualização de contornos
                 col_ctrl1, col_ctrl2 = st.columns(2)
                 with col_ctrl1:
                     show_rm_borders_tab3 = st.checkbox(
                         "Mostrar contornos de Regiões Metropolitanas",
                         value=False,
                         key='show_rm_tab3'
                     )
                 with col_ctrl2:
                     show_state_borders_tab3 = st.checkbox(
                         "Mostrar limites Estaduais",
                         value=False,
                         key='show_state_tab3'
                     )

                 # Renderizar
                 # Tentar carregar coloração final específica se existir, senão usa a consolidada padrão
                 colors_final = {}
                 if 'color_id' in gdf_final.columns:
                     for _, row in gdf_final.iterrows():
                         col_name = 'CD_MUN' if 'CD_MUN' in row else 'cd_mun'
                         colors_final[int(row[col_name])] = int(row['color_id'])
                 else:
                     colors_final = load_or_compute_coloring(gdf_final, "post_sede_coloring.json")

                 # Preparar contornos de estado
                 gdf_states_filtered = None
                 if show_state_borders_tab3:
                    if gdf_states_optimized is not None:
                        gdf_all_states = gdf_states_optimized
                    elif gdf is not None:
                        gdf_all_states = get_state_boundaries(gdf)
                    else:
                        gdf_all_states = None

                    if gdf_all_states is not None and selected_ufs:
                        gdf_states_filtered = gdf_all_states[gdf_all_states['uf'].isin(selected_ufs)]
                    elif gdf_all_states is not None:
                        gdf_states_filtered = gdf_all_states

                 # Renderizar mapa usando render_map_with_flow_popups
                 m = render_map_with_flow_popups(
                     gdf_final, 
                     df_municipios,
                     title="Final (Snapshot)", 
                     global_colors=colors_final, 
                     gdf_rm=gdf_rm, 
                     show_rm_borders=show_rm_borders_tab3,
                     show_state_borders=show_state_borders_tab3,
                     gdf_states=gdf_states_filtered,
                     PASTEL_PALETTE=PASTEL_PALETTE,
                     step_key='step6'
                 )
                 if m:
                      map_html = m._repr_html_()
                      st.components.v1.html(map_html, height=600, scrolling=False)
             else:
                 st.warning("Mapa indisponível")
             
             st.markdown("---")
             st.markdown("#### Configuração Territorial")
             st.caption("UTPs, sedes e municípios conforme o snapshot desta versão (v8.2 – pós consolidação de sedes)")
             _allowed_muns_tab3 = set(df_filtered['cd_mun'].astype(str).tolist()) if not df_filtered.empty else None
             df_config_tab3 = render_territorial_config_table('step6', snapshot_loader, _allowed_muns_tab3)
             if not df_config_tab3.empty:
                 st.dataframe(df_config_tab3, hide_index=True, use_container_width=True, height=400)
             else:
                 st.info("Snapshot da versão 8.2 não encontrado. Execute o pipeline.")
             
             st.markdown("---")
             # Tabela de Mudanças Específicas desta Etapa
             st.markdown("#### Detalhes das Alterações de Sedes")
             
             # Criar tabela detalhada
             changes_data = []
             for c in sede_consolidations:
                 # Detalhes estão em 'details'
                 details = c.get('details', {})
                 mun_id = details.get('mun_id')
                 nm_mun = details.get('nm_mun', str(mun_id))
                 
                 changes_data.append({
                     "Município": nm_mun,
                     "UTP Origem": c['source_utp'],
                     "UTP Destino": c['target_utp'],
                     "Tipo": "Sede da UTP" if details.get('is_sede') else "Município Componente",
                     "Motivo": c['reason']
                 })
             
             st.dataframe(pd.DataFrame(changes_data), hide_index=True, width='stretch')

        else:
             st.info("Nenhuma consolidação de sedes encontrada.")
             st.caption("Execute a Etapa 6 do pipeline e certifique-se que houve consolidações.")
    
    # ==== TAB 4: DUPLA ADERÊNCIA / CENTRALIZAÇÃO ====
    with tab4:
        st.markdown("### <span class='step-badge step-final'>Versão 8.3</span> Centralização das Sedes", unsafe_allow_html=True)
        st.markdown("""
        **Última etapa que garante que todos os municípios de uma mesma UTP tenham a sua própria sede como referencial. Desta forma, o algoritmo pretende:**
        
        1.  **Buscar Municípios limítrofes:** listar dentro de uma UTP todos os munícipios de fronteira que não possuam outra sede como principal, dentro do fluxo de 2h.
        2.  **Validar e registrar o movimento:** garantir que as mudanças respeitem as regras invioláveis e persistir a mudança, para a atualização do estado do grafo.
        3.  **Iterar até a inexistência de movimentos:** permitir que o algoritmo execute novamente até que todos os munícipios limítrofes queiram pertencer a sua UTP atual ou que as mudanças estejam bloqueadas pelas regras invioláveis.
        
        *Etapa ainda em discussão. Duas versões implementadas, mas com melhorias a serem feitas devido a quebra de descontinuidade.*
        """)
        st.markdown("---")
        
        # === SEÇÃO DE ANÁLISE DE FLUXO ===
        st.markdown("### Análise de Fluxos por UTP")
        st.markdown("Visualize os municípios com maior fluxo dentro de cada UTP.")
        
        # Carregar dados com UTP atualizada (step8) + dados de fluxo (initialization)
        df_step8_with_flows = snapshot_loader.get_complete_dataframe_with_flows('step8')
        
        # Painel de análise de fluxo por UTP
        with st.expander("Municípios com Maior Fluxo por UTP", expanded=False):
            st.caption("Selecione uma UTP para visualizar os municípios ordenados por volume total de fluxo")
            st.caption("ℹ️ Dados baseados no estado final após validação de fronteiras (Step 8)")
            
            # Determinar UTPs disponíveis para seleção
            # Use df_step8_with_flows for available UTPs (not df_filtered) to ensure
            # flow data is available even when UI filters are applied
            available_utps = sorted(df_step8_with_flows['utp_id'].unique().tolist()) if not df_step8_with_flows.empty else []
            
            if available_utps:
                # Seletor de UTP
                selected_utp_for_flow = st.selectbox(
                    "Selecione a UTP:",
                    options=available_utps,
                    key="utp_flow_selector"
                )
                
                if selected_utp_for_flow:
                    # Obter dados de fluxo para a UTP selecionada (usando dados do step8)
                    df_utp_flows = get_top_municipalities_in_utp(df_step8_with_flows, selected_utp_for_flow, top_n=10)
                    
                    if not df_utp_flows.empty:
                        st.markdown(f"#### Top 10 Municípios por Fluxo - UTP {selected_utp_for_flow}")
                        
                        # Exibir tabela formatada
                        df_display = df_utp_flows.copy()
                        df_display['total_flow'] = df_display['total_flow'].apply(lambda x: f"{x:,}")
                        df_display['rodoviaria_coletiva'] = df_display['rodoviaria_coletiva'].apply(lambda x: f"{x:,}")
                        df_display['rodoviaria_particular'] = df_display['rodoviaria_particular'].apply(lambda x: f"{x:,}")
                        df_display['aeroviaria'] = df_display['aeroviaria'].apply(lambda x: f"{x:,}")
                        
                        df_display = df_display.rename(columns={
                            'nm_mun': 'Município',
                            'total_flow': 'Fluxo Total',
                            'rodoviaria_coletiva': 'Rod. Coletiva',
                            'rodoviaria_particular': 'Rod. Particular',
                            'aeroviaria': 'Aérea'
                        })
                        
                        st.dataframe(
                            df_display[['Município', 'Fluxo Total', 'Rod. Coletiva', 'Rod. Particular', 'Aérea']],
                            hide_index=True,
                            width='stretch'
                        )
                    else:
                        st.info("Nenhum dado de fluxo disponível para esta UTP.")
            else:
                st.warning("Nenhuma UTP disponível nos filtros selecionados.")
        
        st.markdown("---")
        
        # Visualizar Mapa Snapshot Step 8
        if gdf is not None:
             gdf_borders = snapshot_loader.get_geodataframe_for_step('step8', gdf[gdf['uf'].isin(selected_ufs)].copy())
             
             if gdf_borders is not None:
                 if selected_utps:
                     gdf_borders = gdf_borders[gdf_borders['utp_id'].isin(selected_utps)]
                     
                 st.subheader("Estado Final Pós-Validação (Snapshot)")
                 st.caption("Clique em um município no mapa para ver os 5 principais destinos de fluxo")
                 
                 # === MÉTRICAS PADRONIZADAS (MOVIDO PARA CIMA DO MAPA) ===
                 if not gdf_borders.empty:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        col_cd = 'CD_MUN' if 'CD_MUN' in gdf_borders.columns else 'cd_mun'
                        current_unique = gdf_borders[col_cd].astype(str).nunique()
                        total_unique = df_municipios['cd_mun'].nunique()
                        st.metric("Municípios", current_unique, f"{total_unique} total")
                    with col2:
                        current_utps = gdf_borders['utp_id'].nunique()
                        st.metric("UTPs", current_utps, f"{len(utps_list)} total")
                    with col3:
                        current_ufs = gdf_borders['uf'].nunique()
                        st.metric("Estados", current_ufs, f"{len(ufs)} total")
                    
                    st.markdown("---")
                 
                 colors_borders = {}
                 if 'color_id' in gdf_borders.columns:
                     for _, row in gdf_borders.iterrows():
                         col_name = 'CD_MUN' if 'CD_MUN' in row else 'cd_mun'
                         colors_borders[int(row[col_name])] = int(row['color_id'])
                 
                 # Controle de visualização de contornos
                 col_ctrl1, col_ctrl2 = st.columns(2)
                 with col_ctrl1:
                     show_rm_borders_tab4 = st.checkbox(
                         "Mostrar contornos de Regiões Metropolitanas",
                         value=False,
                         key='show_rm_tab4'
                     )
                 with col_ctrl2:
                     show_state_borders_tab4 = st.checkbox(
                         "Mostrar limites Estaduais",
                         value=False,
                         key='show_state_tab4'
                     )

                 # Preparar contornos de estado
                 gdf_states_filtered = None
                 if show_state_borders_tab4:
                    if gdf_states_optimized is not None:
                        gdf_all_states = gdf_states_optimized
                    elif gdf is not None:
                        gdf_all_states = get_state_boundaries(gdf)
                    else:
                        gdf_all_states = None

                    if gdf_all_states is not None and selected_ufs:
                        gdf_states_filtered = gdf_all_states[gdf_all_states['uf'].isin(selected_ufs)]
                    elif gdf_all_states is not None:
                        gdf_states_filtered = gdf_all_states

                 # Usar a nova função de renderização com popups de fluxo
                 # IMPORTANTE: Usar df_step8_with_flows para que os popups mostrem
                 # os fluxos baseados nas UTPs atualizadas do Step 8
                 try:
                     map_with_flows = render_map_with_flow_popups(
                         gdf_borders,
                         df_step8_with_flows,  # Dados com UTP atualizada + fluxos
                         title="Validação Fronteiras (Snapshot)",
                         global_colors=colors_borders,
                         gdf_rm=gdf_rm, 
                         show_rm_borders=show_rm_borders_tab4,
                         show_state_borders=show_state_borders_tab4,
                         gdf_states=gdf_states_filtered,
                         PASTEL_PALETTE=PASTEL_PALETTE,
                         step_key='step8'
                     )
                     
                     if map_with_flows:
                         map_html = map_with_flows._repr_html_()
                         st.components.v1.html(map_html, height=600, scrolling=False)
                 except Exception as e:
                     logging.error(f"Erro ao renderizar mapa com fluxos: {e}")
                     st.error(f"Erro ao renderizar mapa: {e}")
                     st.error(f"Erro ao renderizar mapa: {e}")
                 
                 st.markdown("---")
                 st.markdown("#### Configuração Territorial")
                 st.caption("UTPs, sedes e municípios conforme o snapshot desta versão (v8.3 – pós validação de fronteiras)")
                 _allowed_muns_tab4 = set(df_filtered['cd_mun'].astype(str).tolist()) if not df_filtered.empty else None
                 df_config_tab4 = render_territorial_config_table('step8', snapshot_loader, _allowed_muns_tab4)
                 if not df_config_tab4.empty:
                     st.dataframe(df_config_tab4, hide_index=True, use_container_width=True, height=400)
                 else:
                     st.info("Snapshot da versão 8.3 não encontrado. Execute o pipeline.")
                 
                 st.markdown("---")
        
        # Carregar dados de validação de fronteiras
        borders_json_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "border_validation_result.json"
        
        if borders_json_path.exists():
            try:
                with open(borders_json_path, 'r', encoding='utf-8') as f:
                    borders_data = json.load(f)
                
                metadata = borders_data.get('metadata', {})
                relocations = borders_data.get('relocations', [])
                rejections = borders_data.get('rejections', [])
                transitive_chains = borders_data.get('transitive_chains', [])
                
                # === MÉTRICAS PADRONIZADAS (REMOVIDO DAQUI) ===
                # (Movido para cima do mapa)
                
                st.markdown("---")
                
                # === CADEIAS TRANSITIVAS ===
                if transitive_chains:
                    st.markdown("#### 🔗 Cadeias Transitivas Detectadas")
                    st.caption(f"{len(transitive_chains)} cadeia(s) transitiva(s) resolvida(s)")
                    
                    for i, chain in enumerate(transitive_chains, 1):
                        chain_str = " → ".join(chain['chain'])
                        final_utp = chain['final_utp']
                        st.info(f"**Cadeia {i}:** {chain_str} → **UTP {final_utp}**")
                    
                    st.markdown("---")
                
                # === TABS INTERNAS: REALOCAÇÕES VS REJEIÇÕES ===
                subtab1, subtab2 = st.tabs(["Realocações", "Rejeições"])
                
                with subtab1:
                    st.markdown("#### Municípios Realocados")
                    
                if relocations:
                    # Preparar dados para tabela (Usando lista atual)
                    relocations_df = pd.DataFrame([
                        {
                            "Município": r['mun_name'],
                            "CD_MUN": r['mun_id'],
                            "UTP Origem": r['utp_origem'],
                            "UTP Destino": r['utp_destino'],
                            "Iteração": r.get('iteration', 1),
                            "Motivo": r.get('reason', 'N/A')
                        }
                        for r in relocations
                    ])
                else:
                    # Tentar carregar HISTÓRICO da ConsolidationManager se a lista atual estiver vazia
                    # Isso garante que a tabela mostre o histórico mesmo se a última execução foi limpa
                    
                    history_consolidations = consolidation_loader.get_consolidations()
                    border_history = []
                    
                    for c in history_consolidations:
                        # Identificar registros de validação de fronteira pelo "reason" ou "details"
                        reason = str(c.get('reason', ''))
                        details = c.get('details', {})
                        
                        if "Border validation" in reason or details.get('step') == 'border_validation':
                             # Extrair dados do formato do ConsolidationManager
                             mun_nm = details.get('municipality_name', str(details.get('municipality_id', 'Unknown')))
                             
                             border_history.append({
                                "Município": mun_nm,
                                "CD_MUN": details.get('municipality_id', 0),
                                "UTP Origem": c['source_utp'],
                                "UTP Destino": c['target_utp'],
                                "Iteração": details.get('iteration', 1),
                                "Motivo": reason
                             })
                    
                    if border_history:
                        relocations_df = pd.DataFrame(border_history)
                        st.info(f"Mostrando histórico acumulado de {len(relocations_df)} realocações (execuções anteriores)")
                    else:
                        relocations_df = pd.DataFrame()

                # Só renderizar se tiver dados (seja atual ou histórico)
                if not relocations_df.empty:
                        
                    # Estatísticas
                    st.markdown(f"**{len(relocations_df)} municípios realocados**")
                        
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("UTPs Origem Únicas", relocations_df['UTP Origem'].nunique())
                    with col2:
                        st.metric("UTPs Destino Únicas", relocations_df['UTP Destino'].nunique())
                    
                    # Distribuição por iteração
                    st.markdown("**Distribuição por Iteração:**")
                    iter_counts = relocations_df['Iteração'].value_counts().sort_index()
                    for iter_num, count in iter_counts.items():
                        st.write(f"- Iteração {iter_num}: {count} realocações")
                    
                    st.markdown("---")
                    st.markdown("**Detalhes:**")
                    st.dataframe(relocations_df, hide_index=True, width='stretch', height=400)
                    
                    # Visualização no mapa
                    if gdf is not None:
                        st.markdown("---")
                        st.markdown("#### Mapa de Municípios Realocados")
                        
                        # Destacar municípios realocados
                        # Precisamos extrair IDs da tabela consolidada
                        if 'CD_MUN' in relocations_df.columns:
                             relocated_ids = set(relocations_df['CD_MUN'].unique())
                             gdf_highlight = gdf[gdf['CD_MUN'].isin(relocated_ids)].copy()
                             
                             if not gdf_highlight.empty:
                                 st.caption(f"Destacando {len(gdf_highlight)} municípios realocados")
                                 
                                 # Criar mapa básico
                                 m = folium.Map(
                                     location=[-15, -55],
                                     zoom_start=4,
                                     tiles="CartoDB positron"
                                 )
                                 
                                 # Adicionar municípios realocados em destaque
                                 folium.GeoJson(
                                     gdf_highlight.to_json(),
                                     style_function=lambda x: {
                                         'fillColor': '#FF6B6B',
                                         'color': '#C92A2A',
                                         'weight': 2,
                                         'fillOpacity': 0.7
                                     },
                                     tooltip=folium.GeoJsonTooltip(
                                         fields=['NM_MUN', 'utp_id', 'uf'],
                                         aliases=['Município:', 'UTP:', 'UF:']
                                     )
                                 ).add_to(m)
                                 
                                 # Fit bounds
                                 if not gdf_highlight.empty:
                                     bounds = gdf_highlight.total_bounds
                                     m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])
                                 
                                 map_html = m._repr_html_()
                                 st.components.v1.html(map_html, height=500, scrolling=False)

                else:
                    if not relocations and not 'border_history' in locals():
                         st.info("Nenhuma realocação foi realizada.")
                         st.caption("Todas as fronteiras já estão otimizadas!")
                
                with subtab2:
                    st.markdown("#### Propostas de Realocação Rejeitadas")
                    
                    if rejections:
                        # Preparar dados para tabela
                        rejections_df = pd.DataFrame([
                            {
                                "Município": r['mun_name'],
                                "CD_MUN": r['mun_id'],
                                "UTP Atual": r['utp_origem'],
                                "UTP Proposta": r.get('proposed_utp', 'N/A'),
                                "Motivo": r.get('reason', 'N/A'),
                                "Detalhes": r.get('details', ''),
                                "Iteração": r.get('iteration', 1)
                            }
                            for r in rejections
                        ])
                        
                        st.markdown(f"**{len(rejections_df)} propostas rejeitadas**")
                        
                        # Análise de motivos
                        st.markdown("**Distribuição de Motivos de Rejeição:**")
                        reason_counts = rejections_df['Motivo'].value_counts()
                        for reason, count in reason_counts.items():
                            percentage = (count / len(rejections_df)) * 100
                            st.write(f"- {reason}: {count} ({percentage:.1f}%)")
                        
                        st.markdown("---")
                        st.markdown("**Detalhes:**")
                        st.dataframe(rejections_df, hide_index=True, width='stretch', height=400)
                    else:
                        st.success("✅ Nenhuma rejeição! Todas as propostas foram aceitas.")
                
                # === DOWNLOAD ===
                st.markdown("---")
                col1, col2 = st.columns(2)
                
                with col1:
                    # Download JSON
                    borders_json = json.dumps(borders_data, ensure_ascii=False, indent=2)
                    st.download_button(
                        label="Baixar Resultados (JSON)",
                        data=borders_json,
                        file_name=f"border_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )
                
                with col2:
                    # Download CSV
                    csv_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "border_validation_result.csv"
                    if csv_path.exists():
                        with open(csv_path, 'r', encoding='utf-8-sig') as f:
                            csv_data = f.read()
                        st.download_button(
                            label="Baixar Resultados (CSV)",
                            data=csv_data,
                            file_name=f"border_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                
            except Exception as e:
                st.error(f"Erro ao carregar dados de validação de fronteiras: {e}")
                import traceback
                st.code(traceback.format_exc())
        else:
            st.warning("⚠️ Dados de validação de fronteiras não encontrados")
            st.info("""
            **Como gerar os dados:**
            1. Execute o pipeline completo: `python main.py`
            2. Ou execute apenas a consolidação: `python src/run_consolidation.py`
            
            O Step 8 (Validação de Fronteiras) será executado automaticamente e gerará os arquivos:
            - `data/03_processed/border_validation_result.json`
            - `data/03_processed/border_validation_result.csv`
            """)



