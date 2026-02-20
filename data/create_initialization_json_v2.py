#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de Inicialização: Consolida dados de múltiplas fontes em um JSON unificado.
Versão 2.0 - Com suporte completo a modais origem-destino e impedâncias
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
    """Carrega dados de SEDE e REGIC."""
    logger.info("Carregando dados de Sede e REGIC...")
    file_path = RAW_DATA_DIR / "SEDE+regic.xlsx"
    try:
        df = pd.read_excel(file_path)
        logger.info(f"  ✓ Carregadas {len(df)} sedes")
        return df
    except Exception as e:
        logger.error(f"  ✗ Erro ao carregar SEDE: {e}")
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
        
        # Garantir que as colunas esperadas existem
        expected_cols = ['ICAO', 'Cidade', 'Passageiros processados', 'COD IBGE']
        missing_cols = [col for col in expected_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"  ⚠ Colunas ausentes no arquivo de aeroportos: {missing_cols}")
        
        # Converter COD IBGE para numérico
        if 'COD IBGE' in df.columns:
            df['COD IBGE'] = pd.to_numeric(df['COD IBGE'], errors='coerce')
        
        # Converter passageiros para numérico
        if 'Passageiros processados' in df.columns:
            df['Passageiros processados'] = pd.to_numeric(df['Passageiros processados'], errors='coerce')
        
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
                # Usar separador vírgula para modais
                df = pd.read_csv(file_path, sep=',', encoding='utf-8')
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


def load_metropolitan_regions():
    """Carrega dados de regiões metropolitanas."""
    logger.info("Carregando dados de regiões metropolitanas...")
    file_path = RAW_DATA_DIR / "Composicao_RM_2024.xlsx"
    
    try:
        if not file_path.exists():
            logger.warning(f"  ⚠ Arquivo {file_path.name} não encontrado")
            return pd.DataFrame()
        
        df = pd.read_excel(file_path)
        
        # Verificar colunas necessárias
        required_cols = ['COD_MUN', 'NOME_RECMETROPOL']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"  ⚠ Colunas ausentes: {missing_cols}")
            return pd.DataFrame()
        
        # Converter COD_MUN para numérico
        df['COD_MUN'] = pd.to_numeric(df['COD_MUN'], errors='coerce')
        df = df.dropna(subset=['COD_MUN'])
        
        # Manter apenas a primeira ocorrência de cada município (caso haja duplicatas)
        df = df.drop_duplicates(subset=['COD_MUN'], keep='first')
        
        logger.info(f"  ✓ Carregadas {len(df)} municípios em regiões metropolitanas")
        return df
    except Exception as e:
        logger.error(f"  ✗ Erro ao carregar regiões metropolitanas: {e}")
        return pd.DataFrame()


def consolidate_data(
    df_utp, df_sede, df_turismo, df_categorizacao, 
    df_airports, impedances, modals, df_population, df_rm
):
    """Consolida todos os dados em estrutura JSON."""
    logger.info("\nConsolidando dados...")
    
    # ========== PREPARAR LOOKUPS ==========
    
    # Lookup de aeroportos
    airports_lookup = {}
    if not df_airports.empty:
        try:
            # Usar colunas explícitas do arquivo Aeros_comercial
            if 'COD IBGE' in df_airports.columns:
                # Remover linhas com COD IBGE inválido
                df_airports_clean = df_airports.dropna(subset=['COD IBGE']).copy()
                
                # Criar dicionário estruturado: {cod_ibge: {icao, cidade, passageiros}}
                for _, row in df_airports_clean.iterrows():
                    cod_ibge = int(row['COD IBGE'])
                    airports_lookup[cod_ibge] = {
                        'icao': row.get('ICAO', ''),
                        'cidade': row.get('Cidade', ''),
                        'passageiros_anual': int(row.get('Passageiros processados', 0)) if pd.notna(row.get('Passageiros processados')) else 0
                    }
                
                logger.info(f"  ✓ Processados {len(airports_lookup)} aeroportos comerciais")
        except Exception as e:
            logger.warning(f"Erro ao processar aeroportos: {e}")
    
    # Lookup de turismo
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
    
    
    # Lookup de impedâncias 2h filtradas (por origem)
    impedance_2h_lookup = {}
    if 'filtradas_2h' in impedances and not impedances['filtradas_2h'].empty:
        try:
            df_imp2 = impedances['filtradas_2h'].copy()
            if 'COD_IBGE_ORIGEM' in df_imp2.columns and 'Tempo' in df_imp2.columns:
                df_imp2['COD_IBGE_ORIGEM'] = pd.to_numeric(df_imp2['COD_IBGE_ORIGEM'], errors='coerce')
                df_imp2['Tempo'] = df_imp2['Tempo'].astype(str).str.replace(',', '.').apply(pd.to_numeric, errors='coerce')
                impedance_2h_lookup = df_imp2.groupby('COD_IBGE_ORIGEM')['Tempo'].mean().to_dict()
                logger.info(f"  ✓ Processadas impedâncias 2h para {len(impedance_2h_lookup)} municípios")
        except Exception as e:
            logger.warning(f"Erro ao processar impedâncias 2h: {e}")
    
    # Lookup de modais (armazenar toda matriz origem-destino-viagens)
    modal_data = {}  # {modal: {origin: {destination: trips}}}
    modal_by_origin = {}  # {origin: {modal: total_trips}}
    
    for modal_name, df_modal in modals.items():
        if df_modal is not None and not df_modal.empty:
            try:
                if 'mun_origem' in df_modal.columns and 'viagens' in df_modal.columns:
                    modal_data[modal_name] = {}
                    
                    df_modal['mun_origem'] = pd.to_numeric(df_modal['mun_origem'], errors='coerce')
                    df_modal['mun_destino'] = pd.to_numeric(df_modal['mun_destino'], errors='coerce')
                    df_modal['viagens'] = pd.to_numeric(df_modal['viagens'], errors='coerce')
                    df_modal = df_modal.dropna()
                    
                    # Montar matriz origem-destino
                    for _, row in df_modal.iterrows():
                        try:
                            origin = int(row['mun_origem'])
                            dest = int(row['mun_destino'])
                            trips = int(row['viagens'])
                            
                            # Armazenar na matriz completa
                            if origin not in modal_data[modal_name]:
                                modal_data[modal_name][origin] = {}
                            modal_data[modal_name][origin][dest] = trips
                            
                            # Também agregar por origem para resumo
                            if origin not in modal_by_origin:
                                modal_by_origin[origin] = {}
                            if modal_name not in modal_by_origin[origin]:
                                modal_by_origin[origin][modal_name] = 0
                            modal_by_origin[origin][modal_name] += trips
                        except:
                            pass
            except Exception as e:
                logger.warning(f"Erro ao processar modal {modal_name}: {e}")
    
    população_lookup = {}
    if not df_population.empty:
        try:
            # Detectar colunas
            cd_col = next((col for col in df_population.columns if 'cod' in col.lower() and 'munic' in col.lower()), None)
            pop_col = next((col for col in df_population.columns if 'popula' in col.lower()), None)
            
            if cd_col and pop_col:
                df_population[cd_col] = pd.to_numeric(df_population[cd_col], errors='coerce')
                df_population[pop_col] = pd.to_numeric(df_population[pop_col], errors='coerce')
                população_lookup = df_population.dropna(subset=[cd_col, pop_col]).set_index(cd_col)[pop_col].to_dict()
                logger.info(f"    Carregada população de {len(população_lookup)} municípios")
        except Exception as e:
            logger.warning(f"Erro ao processar população: {e}")
    
    # Lookup de sedes
    sede_lookup = {}
    regic_lookup = {}
    if not df_sede.empty:
        try:
            for _, row in df_sede.iterrows():
                try:
                    cd_mun = int(row.get('CD_MUN', 0))
                    utp_id = str(row.get('UTPs_PAN_3', ''))
                    regic = str(row.get('REGIC', ''))
                    if cd_mun > 0 and utp_id:
                        sede_lookup[utp_id] = cd_mun
                        regic_lookup[cd_mun] = regic
                except:
                    pass
        except Exception as e:
            logger.warning(f"Erro ao processar sedes: {e}")
    
    # ========== PROCESSAR MUNICÍPIOS ==========
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
            uf = row.get('SIGLA_UF', '')  # Corrigido: UF não existe, usar SIGLA_UF
            utp_id = str(row.get('UTPs_PAN_3', ''))
            
            # Buscar região metropolitana do arquivo Composicao_RM_2024.xlsx
            regiao_metrop = ''
            if not df_rm.empty:
                rm_row = df_rm[df_rm['COD_MUN'] == cd_mun]
                if not rm_row.empty:
                    regiao_metrop = str(rm_row.iloc[0]['NOME_RECMETROPOL'])
            
            # Verificar se é sede
            is_sede = cd_mun == sede_lookup.get(utp_id)
            
            # Estrutura do município
            mun_data = {
                'cd_mun': cd_mun,
                'nm_mun': nm_mun,
                'uf': uf,
                'regiao_metropolitana': str(regiao_metrop) if pd.notna(regiao_metrop) else '',
                'utp_id': utp_id,
                'sede_utp': is_sede,
                'regic': regic_lookup.get(cd_mun, ''),
                'turismo_classificacao': turismo_lookup.get(cd_mun, ''),
                'aeroporto': None,
                'populacao_2022': int(população_lookup.get(cd_mun, 0)),
                'modais': {
                    'rodoviaria_coletiva': modal_by_origin.get(cd_mun, {}).get('rodoviaria_coletiva', 0),
                    'rodoviaria_particular': modal_by_origin.get(cd_mun, {}).get('rodoviaria_particular', 0),
                    'aeroviaria': modal_by_origin.get(cd_mun, {}).get('aeroviaria', 0),
                    'ferroviaria': modal_by_origin.get(cd_mun, {}).get('ferroviaria', 0),
                    'hidroviaria': modal_by_origin.get(cd_mun, {}).get('hidroviaria', 0)
                },
                'impedancia_2h_filtrada': impedance_2h_lookup.get(cd_mun, None),
                'modal_matriz': {}  # Matriz origem-destino por modal
            }
            
            # Adicionar aeroporto se existir
            if cd_mun in airports_lookup:
                try:
                    aero_info = airports_lookup[cd_mun]
                    mun_data['aeroporto'] = {
                        'icao': str(aero_info.get('icao', '')),
                        'cidade': str(aero_info.get('cidade', '')),
                        'passageiros_anual': int(aero_info.get('passageiros_anual', 0))
                    }
                except Exception as e:
                    logger.warning(f"Erro ao adicionar aeroporto para município {cd_mun}: {e}")
            
            # Adicionar matriz modal (origem-destino-viagens)
            for modal_name in modal_data:
                if cd_mun in modal_data[modal_name]:
                    mun_data['modal_matriz'][modal_name] = modal_data[modal_name][cd_mun]
            
            municipios_data.append(mun_data)
            
            # Registrar no UTP
            if utp_id not in utps_data:
                utps_data[utp_id] = {
                    'utp_id': utp_id,
                    'municipios': [],
                    'sede_cd_mun': sede_lookup.get(utp_id, 0),
                }
            utps_data[utp_id]['municipios'].append(cd_mun)
            
        except Exception as e:
            logger.warning(f"Erro ao processar município na linha {idx + 1}: {e}")
    
    logger.info(f"  ✓ Consolidados {len(municipios_data)} municípios")
    
    # Adicionar total de municípios aos UTPs
    utps_list = []
    for utp_id, utp_info in utps_data.items():
        utp_info['total_municipios'] = len(utp_info['municipios'])
        utps_list.append(utp_info)
    
    logger.info(f"  ✓ Consolidadas {len(utps_list)} UTPs")
    
    return municipios_data, utps_list


def save_json(municipios, utps):
    """Salva dados em JSON."""
    logger.info(f"\nSalvando dados em {OUTPUT_FILE}...")
    
    metadata = {
        'timestamp': datetime.now().isoformat(),
        'total_municipios': len(municipios),
        'total_utps': len(utps),
        'fontes': [
            'UTP_FINAL.xlsx',
            'SEDE+regic.xlsx',
            'UTP_TURISMO.xlsx',
            'Base_Categorização.csv',
            'Aeros_comercial.csv',
            'Composicao_RM_2024.xlsx',  # Adicionado fonte de RMs
            'impedance/impedancias_06h_18_08_22.csv',
            'impedance/impedancias_filtradas_2h.csv',
            'person-matrix-data/*.csv',
            'POP2022_Municipios.xlsx'
        ]
    }
    
    data = {
        'municipios': municipios,
        'utps': utps,
        'metadata': metadata
    }
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    file_size = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    logger.info(f"  ✓ JSON salvo com sucesso ({file_size:.2f} MB)")


def main():
    """Função principal."""
    logger.info("=" * 80)
    logger.info("INICIALIZADOR DE DADOS - GeoValida v2.0")
    logger.info("=" * 80)
    
    # Carregar dados
    df_utp = load_utp_base()
    df_sede = load_sede_regic()
    df_turismo = load_turismo()
    df_categorizacao = load_categorization()
    df_airports = load_airports()
    impedances = load_impedances()
    modals = load_modal_matrices()
    df_population = load_population()
    df_rm = load_metropolitan_regions()  # Nova função para carregar RMs
    
    # Consolidar
    municipios, utps = consolidate_data(
        df_utp, df_sede, df_turismo, df_categorizacao,
        df_airports, impedances, modals, df_population, df_rm
    )
    
    # Salvar
    save_json(municipios, utps)
    
    logger.info("\n" + "=" * 80)
    logger.info("✓ INICIALIZAÇÃO CONCLUÍDA COM SUCESSO")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
