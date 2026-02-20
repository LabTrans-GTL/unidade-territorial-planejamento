#!/usr/bin/env python3
"""
Script para enriquecer initialization.json com dados do Base_Categorização.
Adiciona população, categoria de turismo, e outros indicadores socioeconômicos.

Uso: python scripts/enrich_initialization.py
"""
import json
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Configurações de caminhos
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "01_raw"
INIT_JSON = DATA_DIR / "initialization.json"
BASE_CAT = RAW_DIR / "Base_Categorização(Base Organizada Normalizada).csv"


def load_base_categorizacao():
    """Carrega dados do Base_Categorização."""
    logger.info(f"Carregando Base_Categorização de {BASE_CAT}...")
    
    df = pd.read_csv(
        BASE_CAT,
        sep=';',
        encoding='utf-8',
        skiprows=2  # Pular as 2 linhas de cabeçalho
    )
    
    logger.info(f"  ✓ {len(df)} municípios carregados")
    logger.info(f"  ✓ {len(df.columns)} colunas disponíveis")
    
    return df


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


def enrich_initialization_json():
    """Enriquece o initialization.json com dados do Base_Categorização."""
    
    # 1. Carregar initialization.json
    if not INIT_JSON.exists():
        logger.error(f"Arquivo {INIT_JSON} não encontrado!")
        logger.error("Execute primeiro o processo de criação do initialization.json")
        return False
    
    logger.info(f"Carregando {INIT_JSON}...")
    with open(INIT_JSON, 'r', encoding='utf-8') as f:
        init_data = json.load(f)
    
    logger.info(f"  ✓ {len(init_data.get('municipios', []))} municípios no initialization.json")
    
    # 2. Carregar Base_Categorização
    df_cat = load_base_categorizacao()
    
    # 3. Converter código do município para int para matching
    df_cat['cd_mun_int'] = df_cat['md_cod_mun'].astype(float).astype('Int64')
    
    # 4. Criar dicionário de lookup por código do município
    cat_dict = {}
    for _, row in df_cat.iterrows():
        # Pular se código do município for NA
        if pd.isna(row['cd_mun_int']):
            continue
            
        cd_mun = int(row['cd_mun_int'])
        
        # Dados essenciais
        cat_dict[cd_mun] = {
            # População e área
            'populacao_2022': convert_brazilian_number(row['md_populacao_2022']),
            'area_km2': convert_brazilian_number(row['md_area_km2']),
            'turismo_classificacao': str(row['Categoria']) if pd.notna(row['Categoria']) else '',
            'regiao_turistica': str(row['md_regiao_turistica']) if pd.notna(row['md_regiao_turistica']) else '',
            
            # Infraestrutura de transporte (dados mais relevantes para análise de sedes)
            'aeroportos_100km': convert_brazilian_number(row['in_aeroportos_100km']),
            'aeroportos_internacionais_100km': convert_brazilian_number(row['in_aeroportos_inter_100km']),
            'rodoviarias': convert_brazilian_number(row['in_rodoviarias']),
            
            # Estrutura econômica
            'estabelecimentos_formais_mil_hab': convert_brazilian_number(row['ee_estab_formais']),
            'ocupacoes_formais_mil_hab': convert_brazilian_number(row['ee_ocup_formais']),
            'renda_per_capita': convert_brazilian_number(row['ee_renda_pc']),
            'remuneracao_media': convert_brazilian_number(row['ee_rem_med_formais']),
            'ice_r': convert_brazilian_number(row['ee_ice_r']),  # Índice de Competitividade Econômica Regional
            
            # Serviços turísticos
            'densidade_leitos_hospedagem': convert_brazilian_number(row['st_dens_leitos_hospedagem']),
            'densidade_estabelecimentos_hospedagem': convert_brazilian_number(row['st_dens_estab_hospedagem']),
            'avaliacao_media_hospedagem': convert_brazilian_number(row['st_av_hospedagem']),
            'avaliacao_media_restaurante': convert_brazilian_number(row['st_av_restaurante']),
            
            # Especialização turística
            'estabelecimentos_turismo_mil_hab': convert_brazilian_number(row['et_estab_turismo']),
            'ocupacoes_turismo_mil_hab': convert_brazilian_number(row['et_ocup_turismo']),
            'quociente_locacional_turismo': convert_brazilian_number(row['et_ql_turismo']),
            
            # Recursos naturais e culturais
            'area_conservacao_ambiental_pct': convert_brazilian_number(row['rc_area_conserv']),
            'densidade_patrimonio_cultural': convert_brazilian_number(row['rc_dens_patri_cult']),
            
            # Conectividade
            'cobertura_4g_pct': convert_brazilian_number(row['ci_part_rede_4g']),
            'cobertura_5g_pct': convert_brazilian_number(row['ci_part_rede_5g']),
            'densidade_banda_larga': convert_brazilian_number(row['ci_dens_banda_fixa']),
            
            # Saúde
            'medicos_100mil_hab': convert_brazilian_number(row['sa_medicos']),
            'leitos_hospitalares_100mil_hab': convert_brazilian_number(row['sa_leitos_hospitalar']),
            'estabelecimentos_saude_100mil_hab': convert_brazilian_number(row['sa_estab_saude']),
            'leitos_uti_100mil_hab': convert_brazilian_number(row['sa_leitos_uti']),
            
            # Segurança
            'taxa_homicidios_100mil_hab': convert_brazilian_number(row['se_tx_homicidios']),
            
            # Demanda turística
            'demanda_turistica': convert_brazilian_number(row['de_demanda_turistica']),
            'passageiros_onibus_turismo': convert_brazilian_number(row['de_passageiros_bus_turismo']),
        }
    
    logger.info(f"  ✓ {len(cat_dict)} municípios processados do Base_Categorização")
    
    # 5. Enriquecer dados dos municípios
    enriched_count = 0
    missing_count = 0
    
    for municipio in init_data.get('municipios', []):
        cd_mun = municipio.get('cd_mun')
        
        if cd_mun in cat_dict:
            # Merge dos dados
            municipio.update(cat_dict[cd_mun])
            enriched_count += 1
        else:
            missing_count += 1
            logger.warning(f"  ⚠ Município {cd_mun} ({municipio.get('nm_mun', 'Unknown')}) não encontrado no Base_Categorização")
    
    logger.info(f"\n📊 Estatísticas do enriquecimento:")
    logger.info(f"  ✓ {enriched_count} municípios enriquecidos com sucesso")
    logger.info(f"  ⚠ {missing_count} municípios não encontrados no Base_Categorização")
    
    # 6. Salvar backup do original
    backup_path = INIT_JSON.with_suffix('.json.backup')
    logger.info(f"\n💾 Criando backup em {backup_path}...")
    with open(backup_path, 'w', encoding='utf-8') as f:
        json.dump(init_data, f, ensure_ascii=False, indent=2)
    
    # 7. Salvar versão enriquecida
    logger.info(f"💾 Salvando initialization.json enriquecido...")
    with open(INIT_JSON, 'w', encoding='utf-8') as f:
        json.dump(init_data, f, ensure_ascii=False, indent=2)
    
    logger.info("\n✅ Enriquecimento concluído com sucesso!")
    logger.info(f"\n📁 Arquivos gerados:")
    logger.info(f"  - {INIT_JSON} (versão enriquecida)")
    logger.info(f"  - {backup_path} (backup do original)")
    
    # 8. Mostrar amostra de dados enriquecidos
    if init_data.get('municipios'):
        sample = init_data['municipios'][0]
        logger.info(f"\n📋 Amostra de dados enriquecidos para {sample.get('nm_mun', 'N/A')}:")
        logger.info(f"  População 2022: {sample.get('populacao_2022', 'N/A'):,}")
        logger.info(f"  Categoria Turismo: {sample.get('turismo_classificacao', 'N/A')}")
        logger.info(f"  Aeroportos 100km: {sample.get('aeroportos_100km', 'N/A')}")
        logger.info(f"  Renda per capita: R$ {sample.get('renda_per_capita', 'N/A')}")
        logger.info(f"  Cobertura 4G: {sample.get('cobertura_4g_pct', 'N/A')}%")
    
    return True


def main():
    """Função principal."""
    logger.info("=" * 80)
    logger.info("ENRIQUECIMENTO DE INITIALIZATION.JSON COM BASE_CATEGORIZAÇÃO")
    logger.info("=" * 80)
    logger.info("")
    
    success = enrich_initialization_json()
    
    if success:
        logger.info("\n💡 Próximo passo: Recarregue o Streamlit dashboard para ver os dados atualizados!")
        logger.info("   streamlit run src/interface/dashboard.py")
    else:
        logger.error("\n❌ Enriquecimento falhou!")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
