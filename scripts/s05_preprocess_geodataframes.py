#!/usr/bin/env python3
"""
Script para pré-processar e salvar GeoDataFrames otimizados.

Este script é a ETAPA 5 do pipeline GeoValida e deve ser executado após:
- Etapa 1: Criação do initialization.json
- Etapa 2: Consolidação das UTPs
- Etapa 3: Pré-cálculo de coloração

O resultado são arquivos GeoJSON otimizados salvos em data/04_maps/:
- municipalities_optimized.geojson: Municípios simplificados com dados de UTP
- rm_boundaries_optimized.geojson: Contornos de RMs dissolvidos

Isso elimina o processamento pesado no dashboard, acelerando o carregamento.
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

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
DATA_DIR = PROJECT_ROOT / "data"
MAPS_DIR = DATA_DIR / "04_maps"
INIT_JSON_PATH = DATA_DIR / "initialization.json"
SHAPEFILE_PATH = DATA_DIR / "01_raw" / "shapefiles" / "BR_Municipios_2024.shp"

OUTPUT_MUNICIPALITIES = MAPS_DIR / "municipalities_optimized.geojson"
OUTPUT_RM_BOUNDARIES = MAPS_DIR / "rm_boundaries_optimized.geojson"
OUTPUT_STATE_BOUNDARIES = MAPS_DIR / "state_boundaries_optimized.geojson"


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


def process_municipalities_geodataframe(shapefile_path, municipios_list):
    """
    Processa e simplifica o GeoDataFrame de municípios.
    Similar à função get_geodataframe() do dashboard, mas salva o resultado.
    """
    logger.info("Processando GeoDataFrame de municípios...")
    
    if not shapefile_path.exists():
        logger.error(f"Shapefile não encontrado: {shapefile_path}")
        return None
    
    try:
        # 1. Carregar shapefile
        logger.info("  Carregando shapefile...")
        gdf = gpd.read_file(shapefile_path)
        logger.info(f"    ✓ {len(gdf)} geometrias carregadas")
        
        # 2. Reprojetar para WGS84 (EPSG:4326) - Folium espera este CRS
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            logger.info("  Reprojetando para WGS84 (EPSG:4326)...")
            gdf = gdf.to_crs(epsg=4326)
            logger.info("    ✓ Reprojeção concluída")
        
        # 3. Preparar dados de municípios
        df_mun = pd.DataFrame(municipios_list)
        
        # Converter IDs para string para matching
        gdf['CD_MUN'] = gdf['CD_MUN'].astype(str)
        df_mun['cd_mun'] = df_mun['cd_mun'].astype(str)
        
        # 4. Merge com dados do initialization.json
        logger.info("  Mesclando com dados de municípios...")
        gdf = gdf.merge(
            df_mun[['cd_mun', 'uf', 'utp_id', 'sede_utp', 'regiao_metropolitana', 'nm_mun']], 
            left_on='CD_MUN', 
            right_on='cd_mun', 
            how='left'
        )
        
        # Preencher campos vazios
        gdf['regiao_metropolitana'] = gdf['regiao_metropolitana'].fillna('')
        
        # 5. Identificar nomes das sedes
        df_sedes = df_mun[df_mun['sede_utp'] == True][['utp_id', 'nm_mun']].set_index('utp_id')
        sede_mapper = df_sedes['nm_mun'].to_dict()
        gdf['nm_sede'] = gdf['utp_id'].map(sede_mapper).fillna('')
        
        # 6. Simplificar geometria - tolerance de 0.002 graus (~200m)
        logger.info("  Simplificando geometrias...")
        gdf['geometry'] = gdf.geometry.simplify(tolerance=0.002, preserve_topology=True)
        logger.info("    ✓ Simplificação concluída")
        
        # 7. Manter apenas colunas essenciais
        cols_to_keep = ['NM_MUN', 'CD_MUN', 'geometry', 'uf', 'utp_id', 'sede_utp', 'regiao_metropolitana', 'nm_sede']
        existing_cols = [c for c in cols_to_keep if c in gdf.columns]
        gdf = gdf[existing_cols]
        
        logger.info(f"  ✓ GeoDataFrame processado: {len(gdf)} municípios")
        return gdf
        
    except Exception as e:
        logger.error(f"Erro ao processar GeoDataFrame de municípios: {e}")
        return None


def process_rm_geodataframe(shapefile_path, municipios_list):
    """
    Gera geometrias de RMs dissolvendo os municípios.
    Similar à função get_derived_rm_geodataframe() do dashboard, mas salva o resultado.
    """
    logger.info("Processando GeoDataFrame de Regiões Metropolitanas...")
    
    if not shapefile_path.exists():
        logger.error(f"Shapefile não encontrado: {shapefile_path}")
        return None
    
    try:
        # 1. Carregar shapefile bruto (sem simplificação prévia)
        logger.info("  Carregando shapefile bruto...")
        gdf_raw = gpd.read_file(shapefile_path)
        
        # 2. Preparar dados
        df_mun = pd.DataFrame(municipios_list)
        gdf_raw['CD_MUN'] = gdf_raw['CD_MUN'].astype(str)
        df_mun['cd_mun'] = df_mun['cd_mun'].astype(str)
        
        # 3. Merge para pegar a região metropolitana
        logger.info("  Mesclando com dados de RMs...")
        gdf_merged = gdf_raw.merge(
            df_mun[['cd_mun', 'regiao_metropolitana', 'uf']], 
            left_on='CD_MUN', 
            right_on='cd_mun', 
            how='inner'
        )
        
        # 4. Filtrar apenas RMs válidas
        gdf_rm_source = gdf_merged[
            gdf_merged['regiao_metropolitana'].notna() & 
            (gdf_merged['regiao_metropolitana'] != '') &
            (gdf_merged['regiao_metropolitana'] != '-')
        ].copy()
        
        if gdf_rm_source.empty:
            logger.warning("  Nenhuma RM encontrada nos dados")
            return None
        
        logger.info(f"  Encontradas RMs em {len(gdf_rm_source)} municípios")
        
        # 5. Dissolver (União das geometrias) por RM
        logger.info("  Dissolvendo geometrias por RM...")
        gdf_rm_source['count'] = 1
        gdf_dissolved = gdf_rm_source.dissolve(
            by=['regiao_metropolitana', 'uf'], 
            aggfunc={'count': 'sum'}
        ).reset_index()
        
        logger.info(f"    ✓ {len(gdf_dissolved)} RMs processadas")
        
        # 6. Simplificar o contorno resultante
        logger.info("  Simplificando contornos de RMs...")
        gdf_dissolved['geometry'] = gdf_dissolved.geometry.simplify(tolerance=0.002, preserve_topology=True)
        logger.info("    ✓ Simplificação concluída")
        
        # 7. Reprojetar para WGS84 se necessário
        if gdf_dissolved.crs and gdf_dissolved.crs.to_epsg() != 4326:
            gdf_dissolved = gdf_dissolved.to_crs(epsg=4326)
        
        logger.info(f"  ✓ GeoDataFrame de RMs processado: {len(gdf_dissolved)} regiões")
        return gdf_dissolved
        
    except Exception as e:
        logger.error(f"Erro ao processar GeoDataFrame de RMs: {e}")
        return None


def save_geodataframe(gdf, output_path, description):
    """Salva GeoDataFrame em formato GeoJSON."""
    logger.info(f"Salvando {description} em {output_path}...")
    
    try:
        # Criar diretório se não existir
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Salvar como GeoJSON
        gdf.to_file(output_path, driver='GeoJSON')
        
        # Estatísticas do arquivo
        file_size = output_path.stat().st_size / (1024 * 1024)  # MB
        logger.info(f"  ✓ Salvo com sucesso ({file_size:.2f} MB)")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar {description}: {e}")
        return False



def process_state_geodataframe(shapefile_path, municipios_list):
    """
    Gera geometrias de Estados dissolvendo os municípios.
    """
    logger.info("Processando GeoDataFrame de Estados...")
    
    if not shapefile_path.exists():
        logger.error(f"Shapefile não encontrado: {shapefile_path}")
        return None
    
    try:
        # 1. Carregar shapefile bruto (sem simplificação prévia)
        logger.info("  Carregando shapefile bruto...")
        gdf_raw = gpd.read_file(shapefile_path)
        
        # 2. Preparar dados
        df_mun = pd.DataFrame(municipios_list)
        gdf_raw['CD_MUN'] = gdf_raw['CD_MUN'].astype(str)
        df_mun['cd_mun'] = df_mun['cd_mun'].astype(str)
        
        # 3. Merge para pegar a UF
        logger.info("  Mesclando com dados de UF...")
        gdf_merged = gdf_raw.merge(
            df_mun[['cd_mun', 'uf']], 
            left_on='CD_MUN', 
            right_on='cd_mun', 
            how='inner'
        )
        
        if gdf_merged.empty:
            logger.warning("  Nenhum estado encontrado nos dados")
            return None
        
        # 4. Dissolver (União das geometrias) por UF
        logger.info("  Dissolvendo geometrias por UF...")
        
        # Correção de topologia: buffer(0) para fechar gaps microscópicos
        gdf_merged['geometry'] = gdf_merged.geometry.buffer(0)
        
        gdf_dissolved = gdf_merged.dissolve(by='uf').reset_index()
        
        logger.info(f"    ✓ {len(gdf_dissolved)} Estados processados")
        
        # 5. Simplificar o contorno resultante
        logger.info("  Simplificando contornos de Estados...")
        gdf_dissolved['geometry'] = gdf_dissolved.geometry.simplify(tolerance=0.002, preserve_topology=True)
        logger.info("    ✓ Simplificação concluída")
        
        # 6. Reprojetar para WGS84 se necessário
        if gdf_dissolved.crs and gdf_dissolved.crs.to_epsg() != 4326:
            gdf_dissolved = gdf_dissolved.to_crs(epsg=4326)
        
        logger.info(f"  ✓ GeoDataFrame de Estados processado: {len(gdf_dissolved)} UFs")
        return gdf_dissolved
        
    except Exception as e:
        logger.error(f"Erro ao processar GeoDataFrame de Estados: {e}")
        return None


def main():
    """Função principal."""
    logger.info("=" * 80)
    logger.info("ETAPA 5: PRÉ-PROCESSAMENTO DE GEODATAFRAMES")
    logger.info("=" * 80)
    
    # 1. Carregar dados de inicialização
    municipios = load_initialization_data()
    if municipios is None:
        return 1
    
    # 2. Processar GeoDataFrame de municípios
    gdf_municipalities = process_municipalities_geodataframe(SHAPEFILE_PATH, municipios)
    if gdf_municipalities is None:
        return 1
    
    # 3. Salvar GeoDataFrame de municípios
    success = save_geodataframe(gdf_municipalities, OUTPUT_MUNICIPALITIES, "municípios otimizados")
    if not success:
        return 1
    
    # 4. Processar GeoDataFrame de RMs
    gdf_rm = process_rm_geodataframe(SHAPEFILE_PATH, municipios)
    if gdf_rm is not None:
        # 5. Salvar GeoDataFrame de RMs
        save_geodataframe(gdf_rm, OUTPUT_RM_BOUNDARIES, "contornos de RMs")
    else:
        logger.info("  (Pulando salvamento de RMs - nenhuma RM encontrada)")
        
    # 6. Processar GeoDataFrame de Estados
    gdf_states = process_state_geodataframe(SHAPEFILE_PATH, municipios)
    if gdf_states is not None:
        save_geodataframe(gdf_states, OUTPUT_STATE_BOUNDARIES, "contornos de Estados")

    
    logger.info("\n" + "=" * 80)
    logger.info("✓ ETAPA 5 CONCLUÍDA COM SUCESSO")
    logger.info("=" * 80)
    logger.info("\n💡 GeoDataFrames otimizados agora disponíveis!")
    logger.info("   Execute: streamlit run app.py")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
