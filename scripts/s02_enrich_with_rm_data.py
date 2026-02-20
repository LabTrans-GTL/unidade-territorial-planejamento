#!/usr/bin/env python3
"""
Script para criar o arquivo initialization.json a partir dos dados brutos (01_raw).
Simula o processo de carregamento inicial que o main.py faz, mas persiste o resultado em JSON.

Também enriquece os dados com informações socioeconômicas (Base_Categorização).

Uso: python scripts/create_initialization.py
"""
import json
import pandas as pd
from pathlib import Path
import logging
import sys
from datetime import datetime

# Adicionar raiz do projeto ao path para importar src
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.config import FILES, DATA_DIR
from src.core.graph import TerritorialGraph
import geopandas as gpd

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("CreateInitialization")

# --- Funções de Enriquecimento ---

def convert_brazilian_number(value):
    """Converte número brasileiro (vírgula decimal, ponto milhar) para float."""
    if pd.isna(value):
        return None
    
    value_str = str(value).strip()
    if value_str == '' or value_str.lower() in ['nan', 'none']:
        return None
    
    try:
        # Remove pontos (separador de milhar) e substitui vírgula por ponto
        value_str = value_str.replace('.', '').replace(',', '.')
        return float(value_str)
    except:
        return None

def load_enrichment_data():
    """Carrega dados do Base_Categorização."""
    base_cat_path = DATA_DIR / "01_raw" / "Base_Categorização(Base Organizada Normalizada).csv"
    logger.info(f"Carregando Base_Categorização de {base_cat_path}...")
    
    if not base_cat_path.exists():
        logger.error(f"Arquivo não encontrado: {base_cat_path}")
        return None

    try:
        df = pd.read_csv(
            base_cat_path,
            sep=';',
            encoding='utf-8',
            skiprows=2  # Pular as 2 linhas de cabeçalho
        )
        
        # Converter código IBGE para matching
        df['cd_mun_int'] = df['md_cod_mun'].astype(float).astype('Int64')
        
        enrichment_dict = {}
        for _, row in df.iterrows():
            if pd.isna(row['cd_mun_int']):
                continue
                
            cd_mun = int(row['cd_mun_int'])
            
            enrichment_dict[cd_mun] = {
                # População e área
                'populacao_2022': convert_brazilian_number(row['md_populacao_2022']),
                'area_km2': convert_brazilian_number(row['md_area_km2']),
                'turismo_classificacao': str(row['Categoria']) if pd.notna(row['Categoria']) else '',
                'regiao_turistica': str(row['md_regiao_turistica']) if pd.notna(row['md_regiao_turistica']) else '',
                
                # Infraestrutura
                'aeroportos_100km': convert_brazilian_number(row['in_aeroportos_100km']),
                'aeroportos_internacionais_100km': convert_brazilian_number(row['in_aeroportos_inter_100km']),
                
                # Economia
                'renda_per_capita': convert_brazilian_number(row['ee_renda_pc']),
                'cobertura_4g_pct': convert_brazilian_number(row['ci_part_rede_4g']),
                
                # Adicione mais campos aqui se necessário
            }
            
        logger.info(f"  ✓ {len(enrichment_dict)} municípios carregados para enriquecimento")
        return enrichment_dict
        
    except Exception as e:
        logger.error(f"Erro ao carregar dados de enriquecimento: {e}")
        return None

# --- Funções Principais ---

def load_raw_data():
    """Carrega os dataframes brutos conforme configurado em src.config."""
    
    # 1. Carregar UTP Base
    utp_path = FILES['utp_base']
    logger.info(f"Carregando UTP Base de {utp_path}...")
    if not utp_path.exists():
        logger.error(f"Arquivo não encontrado: {utp_path}")
        return None, None, None

    if str(utp_path).endswith('.xlsx'):
        df_utp = pd.read_excel(utp_path, dtype=str)
    else:
        df_utp = pd.read_csv(utp_path, sep=',', encoding='latin1', on_bad_lines='skip', engine='python', dtype=str)
    
    logger.info(f"  ✓ UTP: {len(df_utp)} linhas carregadas")

    # 2. Carregar SEDE + REGIC
    regic_path = FILES['sede_regic']
    logger.info(f"Carregando SEDE+REGIC de {regic_path}...")
    if not regic_path.exists():
        logger.error(f"Arquivo não encontrado: {regic_path}")
        return None, None, None
        
    if str(regic_path).endswith('.xlsx'):
        df_regic = pd.read_excel(regic_path, dtype=str)
    else:
        df_regic = pd.read_csv(regic_path, sep=',', encoding='latin1', on_bad_lines='skip', engine='python', dtype=str)
        
    logger.info(f"  ✓ REGIC: {len(df_regic)} linhas carregadas")

    # 3. Carregar Composição RM (NOVO)
    rm_path = FILES['rm_composition']
    logger.info(f"Carregando Composição RM de {rm_path}...")
    df_rm = None
    if rm_path.exists():
        if str(rm_path).endswith('.xlsx'):
            df_rm = pd.read_excel(rm_path, dtype=str)
        else:
            df_rm = pd.read_csv(rm_path, sep=',', encoding='latin1', on_bad_lines='skip', engine='python', dtype=str)
        logger.info(f"  ✓ RM: {len(df_rm)} linhas carregadas")
    else:
        logger.warning(f"Arquivo de RM não encontrado: {rm_path}")
    
    return df_utp, df_regic, df_rm

def process_data(df_utp, df_regic, df_rm):
    """Processa e combina os dados para o formato do JSON."""
    logger.info("Processando dados...")
    
    # Carregar dados de enriquecimento
    enrichment_dict = load_enrichment_data()
    
    # Normalizar nomes de colunas
    df_utp.columns = df_utp.columns.str.lower().str.strip()
    df_regic.columns = df_regic.columns.str.lower().str.strip()
    
    # Preparar dicionário de RMs
    rm_dict = {}
    if df_rm is not None:
        try:
            # Normalizar colunas do df_rm também
            df_rm_cols = {c: c.upper().strip() for c in df_rm.columns}
            df_rm.rename(columns=df_rm_cols, inplace=True)
            
            # Mapear COD_MUN -> NOME_CATMETROPOL
            # Colunas esperadas: 'COD_MUN', 'NOME_CATMETROPOL'
            for _, row in df_rm.iterrows():
                try:
                    c_mun = str(row.get('COD_MUN', '')).strip()
                    if not c_mun or c_mun == 'nan':
                        continue
                        
                    # Tratar código de 7 dígitos vs 6 dígitos se necessário
                    # No initialization usamos 7 dígitos geralmente
                    c_mun_int = int(float(c_mun))
                    
                    nm_cat = row.get('NOME_CATMETROPOL', '')
                    if pd.isna(nm_cat):
                        nm_cat = ''
                    else:
                        nm_cat = str(nm_cat).strip()
                    
                    if nm_cat:
                        rm_dict[c_mun_int] = nm_cat
                        # Também salvar versao com 6 dígitos por precaução
                        rm_dict[int(str(c_mun_int)[:6])] = nm_cat
                        
                except Exception as e_rm:
                    continue
            logger.info(f"  ✓ {len(rm_dict)} mapeamentos de RM carregados")
        except Exception as e:
            logger.error(f"Erro ao processar arquivo de RMs: {e}")

    # Criar dicionário de municípios
    municipios = {}
    
    # Processar UTP
    possible_cd = [c for c in df_utp.columns if 'cd_mun' in c or 'cod' in c]
    col_cd_mun = possible_cd[0] if possible_cd else 'cd_mun'
    
    possible_nm = [c for c in df_utp.columns if 'nm_mun' in c or 'nome' in c]
    col_nm_mun = possible_nm[0] if possible_nm else 'nm_mun'
    
    possible_utp = [c for c in df_utp.columns if 'utp' in c]
    col_utp = possible_utp[0] if possible_utp else 'utp_id'
    
    # Removendo deteccao antiga de RM do arquivo UTP pois estava incorreta/vazia
    # possible_rm = [c for c in df_utp.columns if 'metropolitana' in c or 'rm' in c]
    # col_rm = possible_rm[0] if possible_rm else None

    logger.info(f"Colunas detectadas: CD={col_cd_mun}, NM={col_nm_mun}, UTP={col_utp}")

    for _, row in df_utp.iterrows():
        try:
            if col_cd_mun not in row:
                continue
                
            cd_mun = int(float(row[col_cd_mun]))
            
            # Buscar RM do dicionário separado
            rm_val = rm_dict.get(cd_mun, '')
            if not rm_val:
                # Tentar com 6 dígitos
                rm_val = rm_dict.get(int(str(cd_mun)[:6]), '')
                
            mun_data = {
                'cd_mun': cd_mun,
                'nm_mun': str(row[col_nm_mun]).strip() if col_nm_mun in row else '',
                'utp_id': str(row[col_utp]).strip() if col_utp in row else '',
                'regiao_metropolitana': rm_val,
                'uf': str(row.get('uf', '')).strip(),
                'sede_utp': False,
                'regic': ''
            }
            
            # Aplicar enriquecimento se disponível
            if enrichment_dict and cd_mun in enrichment_dict:
                mun_data.update(enrichment_dict[cd_mun])
                
            municipios[cd_mun] = mun_data
        except ValueError:
            continue

    # Processar REGIC
    possible_cd_regic = [c for c in df_regic.columns if 'cd_mun' in c or 'cod' in c]
    col_cd_sede = possible_cd_regic[0] if possible_cd_regic else 'cd_mun'
    
    possible_regic = [c for c in df_regic.columns if 'regic' in c]
    col_regic = possible_regic[0] if possible_regic else 'regic'
    
    sedes_count = 0
    for _, row in df_regic.iterrows():
        try:
            if col_cd_sede not in row:
                continue
                
            cd_mun = int(float(row[col_cd_sede]))
            if cd_mun in municipios:
                municipios[cd_mun]['sede_utp'] = True
                municipios[cd_mun]['regic'] = str(row[col_regic]).strip() if col_regic in row else ''
                sedes_count += 1
        except ValueError:
            continue
            
    logger.info(f"  ✓ {len(municipios)} municípios processados e consolidados")
    logger.info(f"  ✓ {sedes_count} sedes identificadas")
    
    return list(municipios.values())

def main():
    logger.info("=" * 80)
    logger.info("CRIAÇÃO DO INITIALIZATION.JSON (COM ENRIQUECIMENTO)")
    logger.info("=" * 80)
    
    df_utp, df_regic, df_rm = load_raw_data()
    
    if df_utp is None or df_regic is None:
        logger.error("Falha ao carregar dados brutos.")
        return 1
        
    municipios_list = process_data(df_utp, df_regic, df_rm)
    
    # Criar estrutura final
    data = {
        "metadata": {
            "created_at": datetime.now().isoformat(),
            "source_files": [
                str(FILES['utp_base'].name), 
                str(FILES['sede_regic'].name),
                str(FILES['rm_composition'].name),
                "Base_Categorização(Base Organizada Normalizada).csv"
            ],
            "total_municipios": len(municipios_list)
        },
        "municipios": municipios_list,
        "utps": [] 
    }
    
    # Salvar JSON
    output_path = DATA_DIR / "initialization.json"
    logger.info(f"Salvando em {output_path}...")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
    logger.info("✅ Arquivo initialization.json criado com sucesso!")
    
    # --- NOVO: PRÉ-CALCULAR COLORAÇÃO INICIAL ---
    logger.info("=" * 80)
    logger.info("PRÉ-CALCULO DA COLORAÇÃO INICIAL")
    
    try:
        # 1. Carregar Shapefile
        shp_path = FILES['shapefiles'] / "BR_Municipios_2024.shp"
        if not shp_path.exists():
            logger.error(f"Shapefile não encontrado em {shp_path}")
        else:
            logger.info("Carregando shapefile para coloração...")
            gdf = gpd.read_file(shp_path)
            
            # 2. Criar DataFrame de Municípios (necessário para merge)
            df_mun = pd.DataFrame(municipios_list)
            
            # 3. Merge para ter UTP_ID no GeoDataFrame
            # Converter tipos para garantir match
            gdf['CD_MUN'] = gdf['CD_MUN'].astype(str)
            df_mun['cd_mun'] = df_mun['cd_mun'].astype(str)
            
            gdf_merged = gdf.merge(
                df_mun[['cd_mun', 'utp_id']], 
                left_on='CD_MUN', 
                right_on='cd_mun',
                how='inner'
            )
            
            # Renomear colunas para o esperado pelo TerritorialGraph
            gdf_merged = gdf_merged.rename(columns={'utp_id': 'UTP_ID'})
            
            # 4. Calcular Coloração
            logger.info("Calculando coloração...")
            graph = TerritorialGraph()
            coloring = graph.compute_graph_coloring(gdf_merged)
            
            # 5. Salvar em arquivo específico
            coloring_path = DATA_DIR / "initial_coloring.json"
            with open(coloring_path, 'w') as f:
                json.dump(coloring, f)
                
            logger.info(f"✅ Coloração inicial salva em {coloring_path}")
            
    except Exception as e:
        logger.error(f"Erro ao calcular coloração inicial: {e}")
        # Não falhar o script principal por causa disso, apenas logar
        
    return 0

if __name__ == "__main__":
    sys.exit(main())
