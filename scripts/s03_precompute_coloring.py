#!/usr/bin/env python3
"""
Script para pré-calcular a coloração de grafos dos municípios.

Este script é a ETAPA 3 do pipeline GeoValida e deve ser executado após:
- Etapa 1: Criação do initialization.json
- Etapa 2: Consolidação das UTPs

O resultado é salvo em data/map_coloring_cache.json e é utilizado pelo
dashboard para renderizar mapas rapidamente sem necessidade de recalcular
a coloração toda vez que o app é iniciado.
"""

import json
import logging
import sys
from pathlib import Path
import geopandas as gpd
import pandas as pd

# Adicionar raiz do projeto ao path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.core.graph import TerritorialGraph

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
DATA_DIR = PROJECT_ROOT / "data"
INIT_JSON_PATH = DATA_DIR / "initialization.json"
SHAPEFILE_PATH = DATA_DIR / "01_raw" / "shapefiles" / "BR_Municipios_2024.shp"
INITIAL_COLORING_PATH = DATA_DIR / "initial_coloring.json"
CONSOLIDATED_COLORING_PATH = DATA_DIR / "consolidated_coloring.json"


def load_initialization_data():
    """Carrega o arquivo initialization.json."""
    logger.info(f"Carregando initialization.json de {INIT_JSON_PATH}...")
    
    if not INIT_JSON_PATH.exists():
        logger.error(f"Arquivo {INIT_JSON_PATH} não encontrado!")
        logger.error("Execute primeiro: python main.py (etapas 1 e 2)")
        return None
    
    try:
        with open(INIT_JSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        municipios = data.get('municipios', [])
        logger.info(f"  ✓ {len(municipios)} municípios carregados")
        return municipios
    except Exception as e:
        logger.error(f"Erro ao carregar initialization.json: {e}")
        return None


def apply_consolidation_mapping(municipios_list):
    """
    Aplica o mapeamento de consolidações aos municípios se consolidation_result.json existir.
    
    Retorna:
        tuple: (municipios_atualizados, num_consolidations)
    """
    CONSOLIDATION_PATH = DATA_DIR / "consolidation_result.json"
    
    # Verifica se existe arquivo de consolidação
    if not CONSOLIDATION_PATH.exists():
        logger.info("  consolidation_result.json não encontrado.")
        logger.info("  Calculando coloração INICIAL (pré-consolidação)...")
        return municipios_list, 0
    
    try:
        # Carregar mapeamento de consolidações
        with open(CONSOLIDATION_PATH, 'r', encoding='utf-8') as f:
            consolidation_data = json.load(f)
        
        utps_mapping = consolidation_data.get('utps_mapping', {})
        
        if not utps_mapping:
            logger.info("  Nenhuma consolidação encontrada no arquivo.")
            logger.info("  Calculando coloração INICIAL...")
            return municipios_list, 0
        
        logger.info(f"  ✓ Carregado mapeamento com {len(utps_mapping)} consolidações")
        logger.info("  Aplicando consolidações ao mapeamento de UTPs...")
        
        # Aplicar mapeamento aos municípios
        municipios_updated = []
        changes_count = 0
        
        for mun in municipios_list:
            mun_copy = mun.copy()
            old_utp = str(mun_copy.get('utp_id', ''))
            
            # Se a UTP foi consolidada, atualizar para a UTP alvo
            if old_utp in utps_mapping:
                new_utp = utps_mapping[old_utp]
                mun_copy['utp_id'] = new_utp
                changes_count += 1
            
            municipios_updated.append(mun_copy)
        
        logger.info(f"  ✓ {changes_count} municípios tiveram suas UTPs atualizadas")
        logger.info("  Calculando coloração FINAL (pós-consolidação)...")
        return municipios_updated, len(utps_mapping)
        
    except Exception as e:
        logger.error(f"Erro ao aplicar consolidação: {e}")
        logger.info("  Prosseguindo com coloração inicial...")
        return municipios_list, 0


def load_shapefile(municipios_list):
    """Carrega e processa o shapefile com dados de municípios."""
    logger.info(f"Carregando shapefile de {SHAPEFILE_PATH}...")
    
    if not SHAPEFILE_PATH.exists():
        logger.error(f"Shapefile não encontrado: {SHAPEFILE_PATH}")
        return None
    
    try:
        # Carregar shapefile
        gdf = gpd.read_file(SHAPEFILE_PATH)
        logger.info(f"  ✓ Shapefile carregado com {len(gdf)} geometrias")
        
        # Criar DataFrame de municípios
        df_mun = pd.DataFrame(municipios_list)
        
        # === LIMPEZA DE DADOS ===
        logger.info("  Limpando dados de entrada...")
        
        # Tamanho original
        original_size = len(df_mun)
        
        # 1. Remover registros com UTP inválida (nan, None, vazio)
        df_mun = df_mun[df_mun['utp_id'].notna()].copy()
        invalid_utp_count = original_size - len(df_mun)
        
        # 2. Remover duplicatas, mantendo apenas o primeiro registro de cada município
        df_mun = df_mun.drop_duplicates(subset=['cd_mun'], keep='first')
        duplicate_count = original_size - invalid_utp_count - len(df_mun)
        
        logger.info(f"  ✓ Removidos {invalid_utp_count} registros com UTP inválida")
        logger.info(f"  ✓ Removidas {duplicate_count} duplicatas")
        logger.info(f"  ✓ {len(df_mun)} municípios únicos e válidos")
        
        # Converter tipos para matching
        gdf['CD_MUN'] = gdf['CD_MUN'].astype(str)
        df_mun['cd_mun'] = df_mun['cd_mun'].astype(str)
        
        # Merge para incluir utp_id no GeoDataFrame
        gdf_merged = gdf.merge(
            df_mun[['cd_mun', 'utp_id']], 
            left_on='CD_MUN', 
            right_on='cd_mun',
            how='inner'
        )
        
        # Renomear para formato esperado pelo TerritorialGraph
        gdf_merged = gdf_merged.rename(columns={'utp_id': 'UTP_ID'})
        
        logger.info(f"  ✓ Merged: {len(gdf_merged)} municípios com geometrias e UTP")
        
        return gdf_merged
        
    except Exception as e:
        logger.error(f"Erro ao processar shapefile: {e}")
        return None


def compute_coloring(gdf):
    """Calcula a coloração de grafos usando TerritorialGraph."""
    logger.info("Calculando coloração de grafos...")
    logger.info("  (Este processo pode demorar alguns minutos...)")
    
    try:
        # Criar instância do grafo
        graph = TerritorialGraph()
        
        # Computar coloração
        coloring = graph.compute_graph_coloring(gdf)
        
        num_colors = len(set(coloring.values()))
        logger.info(f"  ✓ Coloração concluída com {num_colors} cores distintas")
        logger.info(f"  ✓ {len(coloring)} municípios coloridos")
        
        return coloring
        
    except Exception as e:
        logger.error(f"Erro ao calcular coloração: {e}")
        return None


def save_coloring_cache(coloring, output_path, cache_type=""):
    """Salva a coloração em cache JSON."""
    type_label = f" ({cache_type})" if cache_type else ""
    logger.info(f"Salvando cache de coloração{type_label} em {output_path}...")
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(coloring, f, ensure_ascii=False, indent=2)
        
        file_size = output_path.stat().st_size / 1024  # KB
        logger.info(f"  ✓ Cache salvo com sucesso ({file_size:.2f} KB)")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar cache: {e}")
        return False


def main():
    """Função principal."""
    logger.info("=" * 80)
    logger.info("ETAPA 3: PRÉ-CÁLCULO DE COLORAÇÃO DE GRAFOS")
    logger.info("=" * 80)
    
    # 1. Carregar dados de inicialização
    municipios_base = load_initialization_data()
    if municipios_base is None:
        return 1
    
    # === COLORAÇÃO INICIAL (PRÉ-CONSOLIDAÇÃO) ===
    logger.info("\n" + "─" * 80)
    logger.info("FASE 1: Calculando coloração INICIAL (pré-consolidação)...")
    logger.info("─" * 80)
    
    # 2a. Carregar e processar shapefile com dados INICIAIS
    gdf_initial = load_shapefile(municipios_base)
    if gdf_initial is None:
        return 1
    
    # 3a. Calcular coloração inicial
    coloring_initial = compute_coloring(gdf_initial)
    if coloring_initial is None:
        return 1
    
    # 4a. Salvar cache inicial
    success_initial = save_coloring_cache(coloring_initial, INITIAL_COLORING_PATH, "INICIAL")
    if not success_initial:
        return 1
    
    # === COLORAÇÃO PÓS-CONSOLIDAÇÃO ===
    logger.info("\n" + "─" * 80)
    logger.info("FASE 2: Calculando coloração PÓS-CONSOLIDAÇÃO...")
    logger.info("─" * 80)
    
    # 2b. Aplicar consolidações (se existirem)
    municipios_consolidated, num_consolidations = apply_consolidation_mapping(municipios_base)
    
    if num_consolidations > 0:
        # 3b. Carregar e processar shapefile com dados CONSOLIDADOS
        gdf_consolidated = load_shapefile(municipios_consolidated)
        if gdf_consolidated is None:
            return 1
        
        # 4b. Calcular coloração consolidada
        coloring_consolidated = compute_coloring(gdf_consolidated)
        if coloring_consolidated is None:
            return 1
    else:
        logger.info("  Nenhuma consolidação encontrada - usando coloração inicial")
        coloring_consolidated = coloring_initial
    
    # 5b. Salvar cache consolidado
    success_consolidated = save_coloring_cache(
        coloring_consolidated, 
        CONSOLIDATED_COLORING_PATH, 
        "PÓS-CONSOLIDAÇÃO"
    )
    if not success_consolidated:
        return 1
    
    # === RESUMO FINAL ===
    logger.info("\n" + "=" * 80)
    logger.info("✓ ETAPA 3 CONCLUÍDA COM SUCESSO")
    logger.info("=" * 80)
    logger.info(f"\n📄 Arquivos gerados:")
    logger.info(f"   • {INITIAL_COLORING_PATH.name} - Coloração inicial (pré-consolidação)")
    logger.info(f"   • {CONSOLIDATED_COLORING_PATH.name} - Coloração pós-consolidação")
    logger.info("\n💡 Os caches de coloração agora estão disponíveis para o dashboard!")
    logger.info("   Execute: streamlit run app.py")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
