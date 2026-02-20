#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de Inicialização: Consolida dados de múltiplas fontes em um JSON unificado.
Objetivo: Criar um arquivo base para facilitar o carregamento inicial e processamentos futuros.

Estrutura esperada:
{
  "municipios": [
    {
      "cd_mun": int,
      "nm_mun": str,
      "uf": str,
      "regiao_metropolitana": str,
      "utp_id": str,
      "sede_utp": bool,
      "regic": str,
      "regic_nivel": str,
      "turismo_classificacao": str,
      "aeroporto": {
        "sigla": str,
        "passageiros_anual": int
      },
      "populacao_2022": int,
      "modais": {
        "rodoviaria_coletiva": int,
        "rodoviaria_particular": int,
        "aeroviaria": int,
        "ferroviaria": int,
        "hidroviaria": int
      },
      "impedancias": {
        "06h": float,
        "filtradas_2h": float
      }
    }
  ],
  "utps": [
    {
      "utp_id": str,
      "nome": str,
      "municipios": [int],
      "sede_cd_mun": int,
      "total_municipios": int
    }
  ],
  "metadata": {
    "timestamp": str,
    "total_municipios": int,
    "total_utps": int,
    "fontes": [str]
  }
}
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
RAW_DATA_DIR = Path(__file__).parent / "01_raw"
OUTPUT_DIR = Path(__file__).parent
OUTPUT_FILE = OUTPUT_DIR / "initialization.json"


def load_utp_base():
    """Carrega base de UTPs com municípios."""
    logger.info("Carregando base de UTPs...")
    file_path = RAW_DATA_DIR / "UTP_FINAL.xlsx"
    
    try:
        df = pd.read_excel(file_path)
        logger.info(f"  ✓ Carregadas {len(df)} linhas de UTP")
        return df
    except Exception as e:
        logger.error(f"  ✗ Erro ao carregar UTP: {e}")
        return pd.DataFrame()


def load_sede_regic():
    """Carrega dados de sede e REGIC."""
    logger.info("Carregando dados de Sede e REGIC...")
    file_path = RAW_DATA_DIR / "SEDE+regic.xlsx"
    
    try:
        df = pd.read_excel(file_path)
        logger.info(f"  ✓ Carregadas {len(df)} sedes")
        return df
    except Exception as e:
        logger.error(f"  ✗ Erro ao carregar Sede+REGIC: {e}")
        return pd.DataFrame()


def load_turismo():
    """Carrega classificação de turismo."""
    logger.info("Carregando classificação de turismo...")
    file_path = RAW_DATA_DIR / "UTP_TURISMO.xlsx"
    
    try:
        df = pd.read_excel(file_path)
        logger.info(f"  ✓ Carregadas {len(df)} classificações de turismo")
        return df
    except Exception as e:
        logger.error(f"  ✗ Erro ao carregar turismo: {e}")
        return pd.DataFrame()


def load_categorization():
    """Carrega categorização base."""
    logger.info("Carregando categorização base...")
    file_path = RAW_DATA_DIR / "Base_Categorização(Base Organizada Normalizada).csv"
    
    try:
        df = pd.read_csv(file_path, sep=';', encoding='utf-8')
        logger.info(f"  ✓ Carregadas {len(df)} categorias")
        return df
    except Exception as e:
        logger.error(f"  ✗ Erro ao carregar categorização: {e}")
        return pd.DataFrame()


def load_airports():
    """Carrega dados de aeroportos comerciais."""
    logger.info("Carregando dados de aeroportos comerciais...")
    file_path = RAW_DATA_DIR / "Aeros_comercial(Planilha1).csv"
    
    try:
        df = pd.read_csv(file_path, sep=';', encoding='utf-8')
        logger.info(f"  ✓ Carregados {len(df)} aeroportos")
        return df
    except Exception as e:
        logger.error(f"  ✗ Erro ao carregar aeroportos: {e}")
        return pd.DataFrame()


def load_impedances():
    """Carrega dados de impedância."""
    logger.info("Carregando dados de impedância...")
    impedances = {}
    
    try:
        # Impedância 06h
        file_06h = RAW_DATA_DIR / "impedance" / "impedancias_06h_18_08_22.csv"
        df_06h = pd.read_csv(file_06h, sep=';', encoding='utf-8-sig')
        impedances['06h'] = df_06h
        logger.info(f"  ✓ Carregadas {len(df_06h)} impedâncias 06h")
        
        # Impedância filtrada 2h
        file_2h = RAW_DATA_DIR / "impedance" / "impedancias_filtradas_2h.csv"
        df_2h = pd.read_csv(file_2h, sep=';', encoding='utf-8-sig')
        impedances['filtradas_2h'] = df_2h
        logger.info(f"  ✓ Carregadas {len(df_2h)} impedâncias filtradas 2h")
        
        return impedances
    except Exception as e:
        logger.error(f"  ✗ Erro ao carregar impedâncias: {e}")
        return {}


def load_modal_matrices():
    """Carrega matrizes de modais (viagens por tipo)."""
    logger.info("Carregando matrizes de modais...")
    modals = {}
    
    modals_files = {
        'rodoviaria_coletiva': 'base_dados_rodoviaria_coletiva_2023.csv',
        'rodoviaria_particular': 'base_dados_rodoviaria_particular_2023.csv',
        'aeroviaria': 'base_dados_aeroviaria_2023.csv',
        'ferroviaria': 'base_dados_ferroviaria_2023.csv',
        'hidroviaria': 'base_dados_hidroviaria_2023.csv'
    }
    
    for modal_name, filename in modals_files.items():
        try:
            file_path = RAW_DATA_DIR / "person-matrix-data" / filename
            if file_path.exists():
                # Tentar com separador padrão (vírgula)
                try:
                    df = pd.read_csv(file_path, sep=',', encoding='utf-8')
                except:
                    df = pd.read_csv(file_path, sep=';', encoding='utf-8')
                
                modals[modal_name] = df
                logger.info(f"  ✓ Carregados {len(df)} registros de {modal_name}")
            else:
                logger.warning(f"  ⚠ Arquivo não encontrado: {filename}")
        except Exception as e:
            logger.error(f"  ✗ Erro ao carregar {modal_name}: {e}")
    
    return modals


def load_population():
    """Carrega dados de população 2022."""
    logger.info("Carregando dados de população 2022...")
    
    try:
        # Tentar encontrar arquivo de população
        pop_files = list(RAW_DATA_DIR.glob('*POP*.xlsx'))
        if not pop_files:
            logger.warning("  ⚠ Arquivo de população não encontrado")
            return pd.DataFrame()
        
        df = pd.read_excel(pop_files[0], header=1)  # Skip first header row
        logger.info(f"  ✓ Carregada população de {len(df)} municípios")
        return df
    except Exception as e:
        logger.error(f"  ✗ Erro ao carregar população: {e}")
        return pd.DataFrame()


def consolidate_data(
    df_utp, df_sede, df_turismo, df_categorizacao, 
    df_airports, impedances, modals, df_population
):
    """Consolida todos os dados em estrutura JSON (versão otimizada)."""
    logger.info("\nConsolidando dados...")
    
    # Dicionários de lookup - PRÉ-PROCESSADOS
    airports_lookup = {}
    if not df_airports.empty:
        try:
            # Detectar coluna de CD_MUN
            cd_col = next((col for col in df_airports.columns if 'cd' in col.lower() or 'mun' in col.lower()), df_airports.columns[0])
            sigla_col = next((col for col in df_airports.columns if 'sigla' in col.lower() or 'aero' in col.lower()), None)
            pass_col = next((col for col in df_airports.columns if 'pass' in col.lower() or 'viag' in col.lower()), None)
            
            if sigla_col:
                df_airports[cd_col] = pd.to_numeric(df_airports[cd_col], errors='coerce')
                airports_lookup = df_airports.dropna(subset=[cd_col]).set_index(cd_col).to_dict('index')
        except Exception as e:
            logger.warning(f"Erro ao processar aeroportos: {e}")
    
    turismo_lookup = {}
    if not df_turismo.empty:
        try:
            cd_col = next((col for col in df_turismo.columns if 'cd' in col.lower()), df_turismo.columns[0])
            class_col = next((col for col in df_turismo.columns if 'class' in col.lower() or 'turismo' in col.lower()), None)
            if class_col:
                df_turismo[cd_col] = pd.to_numeric(df_turismo[cd_col], errors='coerce')
                turismo_lookup = df_turismo.dropna(subset=[cd_col]).set_index(cd_col)[class_col].to_dict()
        except Exception as e:
            logger.warning(f"Erro ao processar turismo: {e}")
    
    # Lookup de impedâncias (AGREGADO por origem)
    impedance_lookup = {}
    if '06h' in impedances and not impedances['06h'].empty:
        try:
            df_imp = impedances['06h'].copy()
            
            # Handle header as data (file has: BR430690-BR431112 | 5,337... as header)
            # Get header and convert to data
            header_row = {col: df_imp.columns[i] for i, col in enumerate(df_imp.columns)}
            if len(df_imp.columns) == 2:
                # First header is origin-destination, second is impedance value
                origin_dest_header = df_imp.columns[0]
                impedance_header = df_imp.columns[1]
                
                # Add header row to data
                header_data = pd.DataFrame([header_row])
                df_imp = pd.concat([header_data, df_imp], ignore_index=True)
            
            # Rename columns
            df_imp.columns = ['origin_dest', 'impedance']
            
            # Extract origin from origin-destination pairs
            df_imp['origin'] = df_imp['origin_dest'].astype(str).str.split('-').str[0]
            
            # Convert to numeric, replacing comma with dot for decimal
            df_imp['impedance'] = df_imp['impedance'].astype(str).str.replace(',', '.').apply(pd.to_numeric, errors='coerce')
            df_imp['origin'] = df_imp['origin'].apply(pd.to_numeric, errors='coerce')
            df_imp = df_imp.dropna(subset=['origin', 'impedance'])
            
            # Aggregate by origin (average impedance)
            if len(df_imp) > 0:
                impedance_lookup = df_imp.groupby('origin')['impedance'].mean().to_dict()
                logger.info(f"  ✓ Processadas impedâncias para {len(impedance_lookup)} municípios")
            else:
                logger.warning("  ⚠ Nenhuma impedância válida encontrada")
        except Exception as e:
            logger.warning(f"Erro ao processar impedâncias 06h: {e}")
    
    
    # Lookup de modais (AGREGADO por origem)
    modal_lookup = {}
    for modal_name, df_modal in modals.items():
        if df_modal is not None and not df_modal.empty:
            try:
                # Modal files have columns: mun_origem, mun_destino, viagens
                if 'mun_origem' in df_modal.columns and 'viagens' in df_modal.columns:
                    # Group by origin and sum trips
                    df_agg = df_modal.groupby('mun_origem')['viagens'].sum().reset_index()
                    df_agg['mun_origem'] = pd.to_numeric(df_agg['mun_origem'], errors='coerce')
                    df_agg['viagens'] = pd.to_numeric(df_agg['viagens'], errors='coerce')
                    df_agg = df_agg.dropna()
                    
                    for origin, trips in zip(df_agg['mun_origem'], df_agg['viagens']):
                        origin = int(origin)
                        if origin not in modal_lookup:
                            modal_lookup[origin] = {}
                        modal_lookup[origin][modal_name] = int(trips)
                    
                    logger.info(f"  ✓ Processadas viagens de {modal_name} para {len(set(df_agg['mun_origem']))} municípios")
                else:
                    logger.warning(f"  ⚠ Colunas esperadas não encontradas em {modal_name}: {list(df_modal.columns)}")
            except Exception as e:
                logger.warning(f"Erro ao processar modal {modal_name}: {e}")
    
    
    população_lookup = {}
    if not df_population.empty:
        try:
            cd_col = next((col for col in df_population.columns if 'cd' in col.lower()), df_population.columns[0])
            pop_col = next((col for col in df_population.columns if 'pop' in col.lower()), None)
            if pop_col:
                df_population[cd_col] = pd.to_numeric(df_population[cd_col], errors='coerce')
                df_population[pop_col] = pd.to_numeric(df_population[pop_col], errors='coerce')
                população_lookup = df_population.dropna(subset=[cd_col]).set_index(cd_col)[pop_col].to_dict()
        except Exception as e:
            logger.warning(f"Erro ao processar população: {e}")
    
    # Criar lookup de sedes
    sede_lookup = {}
    regic_lookup = {}
    if not df_sede.empty:
        try:
            for _, row in df_sede.iterrows():
                cd_mun = int(row.get('CD_MUN', 0))
                utp_id = str(row.get('UTPs_PAN_3', ''))
                regic = str(row.get('REGIC', ''))
                sede_lookup[utp_id] = cd_mun
                regic_lookup[cd_mun] = regic
        except:
            pass
    
    # === PROCESSAMENTO EM LOTE ===
    municipios_data = []
    utps_data = {}
    
    logger.info("  Processando municípios...")
    total_rows = len(df_utp)
    
    for idx, (_, row) in enumerate(df_utp.iterrows()):
        if (idx + 1) % 1000 == 0:
            logger.info(f"    {idx + 1}/{total_rows} municípios processados...")
        
        try:
            cd_mun = int(row.get('CD_MUN', 0))
            nm_mun = row.get('NM_MUN', '')
            uf = row.get('UF', '')
            regiao_metrop = row.get('NM_CONCU', row.get('REGIAO_METROPOLITANA', ''))
            utp_id = str(row.get('UTPs_PAN_3', ''))
            
            # Verificar se é sede
            is_sede = cd_mun == sede_lookup.get(utp_id)
            
            # Obter aeroporto
            aero_data = None
            if cd_mun in airports_lookup:
                try:
                    aero_info = airports_lookup[cd_mun]
                    sigla = next((v for k, v in aero_info.items() if 'sigla' in k.lower()), '')
                    passageiros = next((int(v) if isinstance(v, (int, float)) else 0 
                                       for k, v in aero_info.items() if 'pass' in k.lower()), 0)
                    aero_data = {'sigla': sigla, 'passageiros_anual': passageiros}
                except:
                    pass
            
            # Estrutura do município
            mun_data = {
                'cd_mun': cd_mun,
                'nm_mun': nm_mun,
                'uf': uf,
                'regiao_metropolitana': regiao_metrop if pd.notna(regiao_metrop) else '',
                'utp_id': utp_id,
                'sede_utp': is_sede,
                'regic': regic_lookup.get(cd_mun, ''),
                'turismo_classificacao': turismo_lookup.get(cd_mun, ''),
                'aeroporto': aero_data,
                'populacao_2022': int(população_lookup.get(cd_mun, 0)) or 0,
                'modais': {
                    'rodoviaria_coletiva': modal_lookup.get(cd_mun, {}).get('rodoviaria_coletiva', 0),
                    'rodoviaria_particular': modal_lookup.get(cd_mun, {}).get('rodoviaria_particular', 0),
                    'aeroviaria': modal_lookup.get(cd_mun, {}).get('aeroviaria', 0),
                    'ferroviaria': modal_lookup.get(cd_mun, {}).get('ferroviaria', 0),
                    'hidroviaria': modal_lookup.get(cd_mun, {}).get('hidroviaria', 0)
                },
                'impedancia_media_06h': impedance_lookup.get(cd_mun, 0)
            }
            
            municipios_data.append(mun_data)
            
            # Agregar dados de UTP
            if utp_id not in utps_data:
                utps_data[utp_id] = {
                    'utp_id': utp_id,
                    'municipios': [],
                    'sede_cd_mun': None,
                    'total_municipios': 0
                }
            
            utps_data[utp_id]['municipios'].append(cd_mun)
            if is_sede:
                utps_data[utp_id]['sede_cd_mun'] = cd_mun
            
        except Exception as e:
            logger.warning(f"Erro ao processar linha {idx}: {e}")
            continue
    
    # Finalizar dados de UTP
    for utp_id, utp_info in utps_data.items():
        utp_info['total_municipios'] = len(utp_info['municipios'])
    
    logger.info(f"  ✓ Consolidados {len(municipios_data)} municípios")
    logger.info(f"  ✓ Consolidadas {len(utps_data)} UTPs")
    
    return {
        'municipios': municipios_data,
        'utps': list(utps_data.values()),
        'metadata': {
            'timestamp': datetime.now().isoformat(),
            'total_municipios': len(municipios_data),
            'total_utps': len(utps_data),
            'fontes': [
                'UTP_FINAL.xlsx',
                'SEDE+regic.xlsx',
                'UTP_TURISMO.xlsx',
                'Base_Categorização.csv',
                'Aeros_comercial.csv',
                'impedance/*.csv',
                'person-matrix-data/*.csv',
                'POP2022_Municipios.xlsx'
            ]
        }
    }


def save_json(data, output_file):
    """Salva dados em JSON."""
    logger.info(f"\nSalvando dados em {output_file}...")
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        logger.info(f"  ✓ JSON salvo com sucesso ({file_size_mb:.2f} MB)")
        return True
    except Exception as e:
        logger.error(f"  ✗ Erro ao salvar JSON: {e}")
        return False


def main():
    """Executa o script completo."""
    logger.info("=" * 80)
    logger.info("INICIALIZADOR DE DADOS - GeoValida")
    logger.info("=" * 80)
    
    # Carregar todos os dados
    df_utp = load_utp_base()
    df_sede = load_sede_regic()
    df_turismo = load_turismo()
    df_categorizacao = load_categorization()
    df_airports = load_airports()
    impedances = load_impedances()
    modals = load_modal_matrices()
    df_population = load_population()
    
    # Consolidar
    data = consolidate_data(
        df_utp, df_sede, df_turismo, df_categorizacao,
        df_airports, impedances, modals, df_population
    )
    
    # Salvar
    success = save_json(data, OUTPUT_FILE)
    
    logger.info("\n" + "=" * 80)
    if success:
        logger.info("✓ INICIALIZAÇÃO CONCLUÍDA COM SUCESSO")
    else:
        logger.info("✗ ERRO NA INICIALIZAÇÃO")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
