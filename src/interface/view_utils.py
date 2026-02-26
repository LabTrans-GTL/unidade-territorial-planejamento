# src/interface/view_utils.py
import os
import streamlit as st
import pandas as pd
import geopandas as gpd
import json
import logging
from pathlib import Path
from src.core.graph import TerritorialGraph
from src.interface.palette import get_palette

# Paleta Pastel (Cores suaves e agradáveis)
PASTEL_PALETTE = get_palette()

# CSS customizado para o dashboard
DASHBOARD_CSS = """
<style>
    [data-testid="stAppViewBlockContainer"] {
        max-width: 100% !important;
        padding-left: 5rem !important;
        padding-right: 5rem !important;
    }
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
    
    .stTabs {
        width: 100% !important;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        justify-content: flex-start !important;
        gap: 1rem !important;
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
        margin-right: 1.5rem !important;
        flex-grow: 0 !important;
    }

    div[data-testid="stHorizontalBlock"] > div:has([data-baseweb="tab-list"]) {
        width: 100% !important;
    }
    
    .stTabs [data-baseweb="tab-highlight"] {
        background-color: #1351B4 !important;
    }
</style>
"""

@st.cache_resource(show_spinner="Carregando mapa...", hash_funcs={pd.DataFrame: id})
def get_geodataframe(optimized_geojson_path, df_municipios):
    if not optimized_geojson_path.exists():
        st.warning("**GeoDataFrame otimizado não encontrado!**")
        return None

    try:
        gdf = gpd.read_file(optimized_geojson_path)
        df_mun_copy = df_municipios.copy()
        df_mun_copy['cd_mun'] = df_mun_copy['cd_mun'].astype(str)
        gdf['CD_MUN'] = gdf['CD_MUN'].astype(str)
        
        gdf = gdf.drop(columns=['uf', 'utp_id', 'sede_utp', 'regiao_metropolitana', 'nm_sede'], errors='ignore')
        gdf = gdf.merge(
            df_mun_copy[['cd_mun', 'uf', 'utp_id', 'sede_utp', 'regiao_metropolitana', 'nm_mun']], 
            left_on='CD_MUN', right_on='cd_mun', how='left'
        )
        
        df_sedes = df_mun_copy[df_mun_copy['sede_utp'] == True][['utp_id', 'nm_mun']].set_index('utp_id')
        sede_mapper = df_sedes['nm_mun'].to_dict()
        gdf['nm_sede'] = gdf['utp_id'].map(sede_mapper).fillna('')
        gdf['regiao_metropolitana'] = gdf['regiao_metropolitana'].fillna('')
        
        import gc
        gdf['geometry'] = gdf['geometry'].simplify(0.001, preserve_topology=True)
        del df_mun_copy, df_sedes, sede_mapper
        gc.collect()
        
        return gdf
    except Exception as e:
        st.error(f"Erro ao carregar mapa otimizado: {e}")
        return None

@st.cache_resource(show_spinner="Carregando RMs...")
def get_derived_rm_geodataframe(optimized_rm_geojson_path):
    if not optimized_rm_geojson_path.exists():
        return None
    try:
        return gpd.read_file(optimized_rm_geojson_path)
    except Exception as e:
        logging.error(f"Erro ao carregar RMs otimizadas: {e}")
        return None

@st.cache_resource(show_spinner="Carregando Estados...")
def get_derived_state_geodataframe(optimized_state_geojson_path):
    if not optimized_state_geojson_path.exists():
        return None
    try:
        return gpd.read_file(optimized_state_geojson_path)
    except Exception as e:
        logging.error(f"Erro ao carregar Estados otimizados: {e}")
        return None

@st.cache_resource(show_spinner="Construindo Grafo Territorial...", hash_funcs={pd.DataFrame: id}, ttl=3600)
def get_territorial_graph(df_municipios):
    if df_municipios is None or df_municipios.empty:
        return None
    try:
        graph = TerritorialGraph()
        for _, row in df_municipios.iterrows():
            cd_mun = int(row['cd_mun'])
            nm_mun = row.get('nm_mun', str(cd_mun))
            utp_id = str(row.get('utp_id', 'SEM_UTP'))
            rm_name = row.get('regiao_metropolitana', '')
            if not rm_name or str(rm_name).strip() == '':
                rm_name = "SEM_RM"
            
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
        return graph
    except Exception as e:
        logging.error(f"Erro ao criar grafo territorial: {e}")
        return None

@st.cache_data(show_spinner="Carregando coloração pré-calculada...", hash_funcs={gpd.GeoDataFrame: id}, max_entries=10, ttl=3600)
def load_or_compute_coloring(gdf, cache_filename="initial_coloring.json"):
    cache_path = Path(__file__).parent.parent.parent / "data" / cache_filename
    if not cache_path.exists():
        alt_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / cache_filename
        if alt_path.exists():
            cache_path = alt_path

    if cache_path.exists():
        try:
            with open(cache_path, "r") as f:
                coloring_str_keys = json.load(f)
                return {int(k): v for k, v in coloring_str_keys.items()}
        except Exception as e:
            logging.error(f"Erro ao ler cache de coloração: {e}")
            return {}
    return {}

@st.cache_resource(show_spinner="Calculando contornos estaduais...")
def get_state_boundaries(gdf):
    if gdf is None or gdf.empty:
        return None
    try:
        return gdf[['uf', 'geometry']].dissolve(by='uf').reset_index()
    except Exception as e:
        logging.error(f"Erro ao calcular contornos estaduais: {e}")
        return None

def render_territorial_config_table(step_key: str, snapshot_loader, allowed_cd_mun: set = None) -> pd.DataFrame:
    data = snapshot_loader.load_snapshot(step_key)
    if not data:
        return pd.DataFrame()

    nodes = data.get("nodes", {})
    utps = {}
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

    return pd.DataFrame(rows).sort_values("UTP").reset_index(drop=True)

def create_enriched_utp_summary(df_municipios):
    if df_municipios.empty:
        return pd.DataFrame()
    
    df = df_municipios.copy()
    numeric_cols = ['populacao_2022', 'area_km2']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    df['total_viagens'] = 0
    if 'modais' in df.columns:
        df['total_viagens'] = df['modais'].apply(
            lambda x: sum(x.values()) if isinstance(x, dict) else 0
        )
    
    def get_modal_dominante(modais):
        if not isinstance(modais, dict) or not modais:
            return ''
        max_modal = max(modais.items(), key=lambda x: x[1])
        if max_modal[1] == 0:
            return ''
        modal_map = {
            'rodoviaria_coletiva': 'Rod. Coletiva',
            'rodoviaria_particular': 'Rod. Particular',
            'aeroviaria': 'Aérea',
            'ferroviaria': 'Ferroviária',
            'hidroviaria': 'Hidroviária'
        }
        return modal_map.get(max_modal[0], max_modal[0])
    
    df['modal_dominante'] = df['modais'].apply(get_modal_dominante) if 'modais' in df.columns else ''
    
    summary_list = []
    for utp_id, group in df.groupby('utp_id'):
        sede_row = group[group['sede_utp'] == True]
        if sede_row.empty:
            continue
        sede = sede_row.iloc[0]
        pop_total = group['populacao_2022'].sum()
        maior_mun = group.loc[group['populacao_2022'].idxmax()]
        maior_mun_nome = f"{maior_mun['nm_mun']} ({maior_mun['populacao_2022']:,.0f})"
        
        turismo_cat = sede.get('turismo_classificacao', '-')
        if not pd.isna(turismo_cat) and str(turismo_cat).strip() != '':
            turismo_cat = str(turismo_cat).split('-')[0].strip()
        
        aeroportos_info = []
        for _, mun in group.iterrows():
            if 'aeroporto' in mun and isinstance(mun['aeroporto'], dict):
                aero = mun['aeroporto']
                icao = aero.get('sigla', '') or aero.get('icao', '')
                passageiros = aero.get('passageiros_anual', 0)
                if icao and str(icao).strip() not in ['', 'nan', 'None']:
                    aeroportos_info.append({
                        'icao': str(icao),
                        'passageiros': int(passageiros) if passageiros else 0
                    })
        
        if aeroportos_info:
            aeroportos_info.sort(key=lambda x: x['passageiros'], reverse=True)
            principal = aeroportos_info[0]
            if principal['passageiros'] > 1000000:
                pass_fmt = f"{principal['passageiros']/1000000:.1f}M"
            elif principal['passageiros'] > 1000:
                pass_fmt = f"{principal['passageiros']/1000:.0f}k"
            else:
                pass_fmt = str(principal['passageiros'])
            aeroporto_display = f"{len(aeroportos_info)} aeros | {principal['icao']} ({pass_fmt})" if len(aeroportos_info) > 1 else f"{principal['icao']} ({pass_fmt})"
        else:
            aeroporto_display = '-'
        
        summary_list.append({
            'UTP': utp_id,
            'Sede': sede['nm_mun'],
            'UF': sede['uf'],
            'Municípios': len(group),
            'População': int(pop_total),
            'Maior Município': maior_mun_nome,
            'REGIC': sede.get('regic', '-'),
            'RM': sede.get('regiao_metropolitana', '-'),
            'Turismo': turismo_cat,
            'Aeroportos': aeroporto_display,
            'Viagens': int(group['total_viagens'].sum()),
            'Modal': sede.get('modal_dominante', '-')
        })
    
    summary_df = pd.DataFrame(summary_list).sort_values('População', ascending=False)
    if summary_df.empty:
        return summary_df
    
    summary_df['População'] = summary_df['População'].apply(lambda x: f"{x:,}")
    summary_df['Viagens'] = summary_df['Viagens'].apply(lambda x: f"{x:,}" if x > 0 else '-')
    return summary_df

def analyze_unitary_utps(df_municipios):
    utp_counts = df_municipios.groupby('utp_id').size().reset_index(name='num_municipios')
    unitary_utps = utp_counts[utp_counts['num_municipios'] == 1]['utp_id'].tolist()
    if not unitary_utps:
        return pd.DataFrame()
    df_unitary = df_municipios[df_municipios['utp_id'].isin(unitary_utps)].copy()
    result = df_unitary[['utp_id', 'nm_mun', 'uf', 'populacao_2022', 'regiao_metropolitana']].copy()
    result.columns = ['UTP', 'Município', 'UF', 'População', 'RM']
    result['População'] = result['População'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else '-')
    result['RM'] = result['RM'].fillna('-')
    return result.sort_values('UTP')
