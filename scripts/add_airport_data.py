#!/usr/bin/env python3
"""
Script para adicionar dados de aeroportos comerciais ao initialization.json
Lê de: Aeros_comercial(Planilha1).csv
"""
import json
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "01_raw"
INIT_JSON = DATA_DIR / "initialization.json"
AEROS_FILE = RAW_DIR / "Aeros_comercial(Planilha1).csv"


def load_airport_data():
    """Carrega dados de aeroportos comerciais."""
    logger.info(f"Carregando dados de aeroportos de {AEROS_FILE}...")
    
    # Tentar diferentes delimitadores e encodings
    for sep in [';', ',']:
        for encoding in ['latin1', 'utf-8', 'cp1252']:
            try:
                df = pd.read_csv(AEROS_FILE, sep=sep, encoding=encoding)
                # Verificar se tem as colunas esperadas
                if len(df.columns) > 3:
                    logger.info(f"  ✓ Carregado com sep='{sep}', encoding='{encoding}'")
                    logger.info(f"  ✓ {len(df)} registros de aeroportos")
                    logger.info(f"  ✓ Colunas: {df.columns.tolist()}")
                    return df
            except:
                continue
    
    logger.error("Falha ao carregar arquivo de aeroportos!")
    return None


def integrate_airport_data():
    """Integra dados de aeroportos ao initialization.json."""
    
    # 1. Carregar dados de aeroportos
    df_aero = load_airport_data()
    if df_aero is None:
        return False
    
    # 2. Processar nome das colunas (remover BOM e espaços)
    df_aero.columns = df_aero.columns.str.strip().str.replace('\ufeff', '')
    logger.info(f"Colunas processadas: {df_aero.columns.tolist()}")
    
    # 3. Identificar coluna de código IBGE
    cod_col = None
    for col in df_aero.columns:
        if 'COD' in col.upper() and 'IBGE' in col.upper():
            cod_col = col
            break
    
    if not cod_col:
        logger.error("Coluna de código IBGE não encontrada!")
        return False
    
    logger.info(f"Usando coluna: {cod_col}")
    
    # 4. Criar dicionário: código município -> nome aeroporto(s)
    airport_dict = {}
    for _, row in df_aero.iterrows():
        try:
            cd_mun = int(str(row[cod_col]).strip())
            icao = row.get('ICAO', row.get('﻿ICAO', 'N/A'))
            cidade = row.get('Cidade', 'N/A')
            
            if cd_mun not in airport_dict:
                airport_dict[cd_mun] = []
            
            airport_dict[cd_mun].append({
                'icao': str(icao).strip(),
                'cidade': str(cidade).strip()
            })
        except:
            continue
    
    logger.info(f"  ✓ {len(airport_dict)} municípios com aeroportos identificados")
    
    # 5. Carregar initialization.json
    if not INIT_JSON.exists():
        logger.error(f"Arquivo {INIT_JSON} não encontrado!")
        return False
    
    logger.info(f"Carregando {INIT_JSON}...")
    with open(INIT_JSON, 'r', encoding='utf-8') as f:
        init_data = json.load(f)
    
    # 6. Integrar dados de aeroporto
    updated_count = 0
    for municipio in init_data.get('municipios', []):
        cd_mun = municipio.get('cd_mun')
        
        if cd_mun in airport_dict:
            # Adicionar informação de aeroporto
            airports = airport_dict[cd_mun]
            municipio['aeroporto'] = airports[0]['icao']  # Código ICAO do primeiro aeroporto
            municipio['aeroportos_lista'] = airports  # Lista completa de aeroportos
            municipio['tem_aeroporto'] = True
            municipio['num_aeroportos'] = len(airports)
            updated_count += 1
        else:
            municipio['tem_aeroporto'] = False
            municipio['num_aeroportos'] = 0
    
    logger.info(f"\n📊 Estatísticas:")
    logger.info(f"  ✓ {updated_count} municípios com aeroportos comerciais")
    
    # 7. Criar backup
    backup_path = INIT_JSON.with_suffix('.json.backup2')
    logger.info(f"\n💾 Criando backup em {backup_path}...")
    with open(backup_path, 'w', encoding='utf-8') as f:
        json.dump(init_data, f, ensure_ascii=False, indent=2)
    
    # 8. Salvar
    logger.info(f"💾 Salvando initialization.json atualizado...")
    with open(INIT_JSON, 'w', encoding='utf-8') as f:
        json.dump(init_data, f, ensure_ascii=False, indent=2)
    
    logger.info("\n✅ Dados de aeroportos integrados com sucesso!")
    
    # 9. Mostrar alguns exemplos
    examples = [m for m in init_data['municipios'] if m.get('tem_aeroporto')][:5]
    if examples:
        logger.info(f"\n📋 Exemplos de municípios com aeroporto:")
        for m in examples:
            logger.info(f"  • {m['nm_mun']} ({m['uf']}): {m.get('aeroporto', 'N/A')}")
    
    return True


def main():
    logger.info("=" * 80)
    logger.info("INTEGRAÇÃO DE DADOS DE AEROPORTOS COMERCIAIS")
    logger.info("=" * 80)
    logger.info("")
    
    success = integrate_airport_data()
    
    if success:
        logger.info("\n💡 Dados de aeroportos integrados!")
    else:
        logger.error("\n❌ Falha na integração!")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
