# src/pipeline/sede_analyzer.py
"""
Módulo para análise de dependências entre sedes (UTPs).
Identifica hierarquias e relações de dependência baseadas em dados socioeconômicos e fluxos.
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json


class SedeAnalyzer:
    """
    Analisa dependências entre sedes de UTPs usando dados socioeconômicos e padrões de fluxo.
    
    O principal critério de alerta é quando o fluxo principal de uma sede vai para 
    outra sede a até 2 horas de distância, indicando possível dependência funcional.
    """
    
    def __init__(self, data_path: Optional[Path] = None, consolidation_loader=None, map_generator=None):
        """
        Inicializa o analisador de sedes.
        
        Args:
            data_path: Caminho para o diretório de dados. Se None, usa o padrão do projeto.
            consolidation_loader: Opcional. ConsolidationLoader para aplicar consolidações.
            map_generator: Opcional. UTPMapGenerator com geometrias carregadas (para fallback de distância).
        """
        self.logger = logging.getLogger("GeoValida.SedeAnalyzer")
        
        if data_path is None:
            self.data_path = Path(__file__).parent.parent.parent / "data"
        else:
            self.data_path = Path(data_path)
        
        # DataFrames principais
        self.df_municipios = None
        self.df_impedance = None
        self.df_sede_analysis = None
        
        # Componentes externos
        self.consolidation_loader = consolidation_loader
        self.map_generator = map_generator
        
        # Dados agregados
        self.sede_metrics = {}
        self.dependency_alerts = []
    
        # Dados agregados
        self.sede_metrics = {}
        self.dependency_alerts = []
    
    def load_initialization_data(self) -> bool:
        """
        Carrega dados do initialization.json que contém informações consolidadas.
        
        Se consolidation_loader foi fornecido no __init__, aplica as consolidações
        territoriais aos dados antes de prosseguir com a análise.
        
        Returns:
            True se carregou com sucesso, False caso contrário.
        """
        json_path = self.data_path / "initialization.json"
        
        if not json_path.exists():
            self.logger.error(f"Arquivo {json_path} não encontrado")
            return False
        
        try:
            self.logger.info(f"Carregando dados de {json_path}...")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Converter lista de municípios para DataFrame
            municipios = data.get('municipios', [])
            self.df_municipios = pd.DataFrame(municipios)
            
            # Aplicar consolidações territoriais se fornecidas
            if self.consolidation_loader and self.consolidation_loader.is_executed():
                self.logger.info("📍 Aplicando consolidações territoriais aos dados...")
                original_count = len(self.df_municipios)
                
                # Aplicar consolidações
                self.df_municipios = self.consolidation_loader.apply_consolidations_to_dataframe(
                    self.df_municipios
                )
                
                summary = self.consolidation_loader.get_summary()
                self.logger.info(f"  ✓ Consolidações aplicadas: {summary['total_consolidations']} movimentações")
                self.logger.info(f"  ✓ UTPs reduzidas: {summary['unique_sources']} → {summary['unique_targets']}")
                self.logger.info(f"  ⚠️  ANÁLISE SERÁ BASEADA NA CONFIGURAÇÃO PÓS-CONSOLIDAÇÃO")
            else:
                self.logger.info("  ℹ️  Usando configuração inicial (sem consolidações)")
            
            self.logger.info(f"✓ Carregados {len(self.df_municipios)} municípios")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar initialization.json: {e}")
            return False
    
    def load_impedance_data(self) -> bool:
        """
        Carrega matriz de impedância (tempo de viagem) filtrada para até 2h.
        
        Returns:
            True se carregou com sucesso, False caso contrário.
        """
        impedance_path = self.data_path / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"
        
        if not impedance_path.exists():
            self.logger.warning(f"Arquivo de impedância não encontrado: {impedance_path}")
            return False
        
        try:
            self.logger.info(f"Carregando matriz de impedância de {impedance_path}...")
            
            # Load CSV with correct encoding (latin-1 instead of utf-8)
            self.df_impedance = pd.read_csv(
                impedance_path, 
                sep=';',
                encoding='latin-1'  # Fix encoding
            )
            
            # Drop empty columns (Unnamed: 6, Unnamed: 7)
            self.df_impedance = self.df_impedance.dropna(axis=1, how='all')
            
            # The CSV already has proper column names:
            # PAR_IBGE, COD_IBGE_ORIGEM, COD_IBGE_DESTINO, Tempo,
            # COD_IBGE_ORIGEM_1 (6-digit), COD_IBGE_DESTINO_1 (6-digit)
            
            # We'll rename to match what the code expects:
            # 'origem' (7-digit), 'destino' (7-digit), 'tempo_horas', 'origem_6', 'destino_6'
            self.df_impedance = self.df_impedance.rename(columns={
                'PAR_IBGE': 'par_ibge',
                'COD_IBGE_ORIGEM': 'origem',
                'COD_IBGE_DESTINO': 'destino',
                'Tempo': 'tempo_horas',
                'COD_IBGE_ORIGEM_1': 'origem_6',
                'COD_IBGE_DESTINO_1': 'destino_6'
            })
            
            # Converter coluna tempo_horas de formato brasileiro (vírgula) para float
            self.df_impedance['tempo_horas'] = (
                self.df_impedance['tempo_horas']
                .astype(str)
                .str.replace(',', '.')
                .astype(float)
            )
            
            # CRITICAL: Ensure 6-digit keys are integers
            # The CSV already provides 6-digit codes, but we need to ensure they're int
            self.df_impedance['origem'] = pd.to_numeric(self.df_impedance['origem'], errors='coerce').fillna(0).astype(int)
            self.df_impedance['destino'] = pd.to_numeric(self.df_impedance['destino'], errors='coerce').fillna(0).astype(int)
            self.df_impedance['origem_6'] = pd.to_numeric(self.df_impedance['origem_6'], errors='coerce').fillna(0).astype(int)
            self.df_impedance['destino_6'] = pd.to_numeric(self.df_impedance['destino_6'], errors='coerce').fillna(0).astype(int)
            
            self.logger.info(f"✓ Carregados {len(self.df_impedance)} pares origem-destino (≤2h)")
            return True
            
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar matriz de impedância: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            self.df_impedance = pd.DataFrame()  # Ensure it's not None
            return False


    def get_main_flow_destination(self, cd_mun_origem: int) -> Tuple[Optional[int], float, int, int]:
        """
        Identifica o principal destino de fluxo para um município origem.
        
        Args:
            cd_mun_origem: Código IBGE do município de origem
            
        Returns:
            Tupla com (cd_mun_destino, proporcao_fluxo, total_viagens, viagens_para_destino)
        """
        if self.df_municipios is None:
            return None, 0.0, 0, 0
        
        # Buscar município
        mun = self.df_municipios[self.df_municipios['cd_mun'] == cd_mun_origem]
        if mun.empty:
            return None, 0.0, 0
        
        # Obter modal_matriz (fluxos por destino)
        modal_matriz = mun.iloc[0].get('modal_matriz', {})
        
        # Agregar todos os modais
        flows_by_dest = {}
        total_viagens = 0
        
        for modal, destinos in modal_matriz.items():
            if isinstance(destinos, dict):
                for dest, viagens in destinos.items():
                    dest_int = int(dest)
                    flows_by_dest[dest_int] = flows_by_dest.get(dest_int, 0) + viagens
                    total_viagens += viagens
        
        if not flows_by_dest or total_viagens == 0:
            return None, 0.0, 0, 0
        
        # Encontrar destino com maior fluxo
        max_dest = max(flows_by_dest.items(), key=lambda x: x[1])
        cd_mun_destino = max_dest[0]
        viagens_para_destino = max_dest[1]  # Número de viagens para este destino específico
        proporcao = viagens_para_destino / total_viagens
        
        return cd_mun_destino, proporcao, total_viagens, viagens_para_destino
    
    def get_travel_time(self, cd_origem: int, cd_destino: int) -> Optional[float]:
        """
        Obtém o tempo de viagem entre dois municípios.
        Fallback: Calcula distância geodésica se não houver na matriz.
        """
        # 1. Tentar matriz de impedância usando 6-digit keys
        if self.df_impedance is not None:
            origem_6 = int(cd_origem) // 10
            destino_6 = int(cd_destino) // 10
            
            pair = self.df_impedance[
                (self.df_impedance['origem_6'] == origem_6) & 
                (self.df_impedance['destino_6'] == destino_6)
            ]
            
            if not pair.empty:
                return float(pair.iloc[0]['tempo_horas'])
        
        return None

    def is_sede(self, cd_mun: int) -> bool:
        """
        Verifica se um município é sede de UTP.
        
        Args:
            cd_mun: Código IBGE do município
            
        Returns:
            True se é sede, False caso contrário
        """
        if self.df_municipios is None:
            return False
        
        mun = self.df_municipios[self.df_municipios['cd_mun'] == cd_mun]
        if mun.empty:
            return False
        
        return bool(mun.iloc[0].get('sede_utp', False))
    
    def check_dependency_criteria(self, cd_sede_origem: int) -> Optional[Dict]:
        """
        Verifica se uma sede atende ao critério de dependência:
        - O principal fluxo vai para outra sede
        - O tempo de viagem é ≤ 2h
        
        Args:
            cd_sede_origem: Código IBGE da sede de origem
            
        Returns:
            Dict com informações do alerta, ou None se não há dependência
        """
        # 1. Obter principal destino
        cd_destino, proporcao, total_viagens, viagens_para_destino = self.get_main_flow_destination(cd_sede_origem)
        
        if cd_destino is None:
            return None
        
        # 2. Verificar se o destino é uma sede
        if not self.is_sede(cd_destino):
            return None
        
        # 3. Verificar tempo de viagem
        tempo = self.get_travel_time(cd_sede_origem, cd_destino)
        
        if tempo is None or tempo > 2.0:
            return None
        
        # Critério atendido - criar alerta
        mun_origem = self.df_municipios[self.df_municipios['cd_mun'] == cd_sede_origem].iloc[0]
        mun_destino = self.df_municipios[self.df_municipios['cd_mun'] == cd_destino].iloc[0]
        
        return {
            'sede_origem': cd_sede_origem,
            'nm_sede_origem': mun_origem['nm_mun'],
            'utp_origem': mun_origem['utp_id'],
            'sede_destino': cd_destino,
            'nm_sede_destino': mun_destino['nm_mun'],
            'utp_destino': mun_destino['utp_id'],
            'proporcao_fluxo': proporcao,
            'total_viagens': total_viagens,
            'viagens_para_destino': viagens_para_destino,
            'tempo_horas': tempo,
            'alerta': 'DEPENDÊNCIA FUNCIONAL DETECTADA'
        }
    
    def calculate_socioeconomic_metrics(self) -> pd.DataFrame:
        """
        Calcula métricas socioeconômicas agregadas por UTP (sede).
        
        Returns:
            DataFrame com métricas por sede
        """
        if self.df_municipios is None:
            return pd.DataFrame()
        
        # Ensure impedance data is loaded for travel time calculations
        if self.df_impedance is None:
            self.load_impedance_data()
        
        # Filtrar apenas sedes
        df_sedes = self.df_municipios[self.df_municipios['sede_utp'] == True].copy()
        
        metrics_list = []
        
        for _, sede in df_sedes.iterrows():
            cd_mun = sede['cd_mun']
            utp_id = sede['utp_id']
            
            # Agregar dados de todos os municípios da UTP
            municipios_utp = self.df_municipios[self.df_municipios['utp_id'] == utp_id]
            
            # Calcular métricas agregadas
            pop_total = municipios_utp['populacao_2022'].sum()
            num_municipios = len(municipios_utp)
            
            # Obter viagens apenas da sede (não de toda a UTP)
            # Isso garante que a análise de dependências mostre viagens sede-a-sede
            modais_sede = sede.get('modais', {})
            total_viagens_sede = 0
            if isinstance(modais_sede, dict):
                total_viagens_sede = sum(modais_sede.values())
            
            # Verificar se a sede possui aeroporto comercial
            # Extrair dados completos do aeroporto quando disponível
            aeroporto_info = sede.get('aeroporto')
            tem_aeroporto = False
            aeroporto_icao = ''
            aeroporto_passageiros = 0
            
            if aeroporto_info is not None and isinstance(aeroporto_info, dict):
                # Nova estrutura: {'icao': 'SBSP', 'cidade': 'São Paulo', 'passageiros_anual': 24252637}
                icao = aeroporto_info.get('icao', '')
                passageiros = aeroporto_info.get('passageiros_anual', 0)
                
                if icao and str(icao).strip() != '' and str(icao).lower() not in ['nan', 'none']:
                    tem_aeroporto = True
                    aeroporto_icao = str(icao)
                    aeroporto_passageiros = int(passageiros) if passageiros else 0

            
            # Obter classificação de turismo da sede
            # Nota: Pode estar vazio se dados não foram carregados
            turismo_classificacao = sede.get('turismo_classificacao', '')
            if turismo_classificacao and str(turismo_classificacao).lower() not in ['nan', 'none', '']:
                turismo_classificacao = str(turismo_classificacao)
            else:
                turismo_classificacao = ''
            
            # Obter principal destino da sede (para onde mais viajam seus habitantes)
            cd_destino, prop_fluxo, viagens_sede, viagens_para_destino = self.get_main_flow_destination(cd_mun)
            
            # Obter nome do destino principal e tempo de viagem
            nm_destino = ''
            tempo_destino = None
            if cd_destino is not None:
                dest_mun = self.df_municipios[self.df_municipios['cd_mun'] == cd_destino]
                if not dest_mun.empty:
                    nm_destino = dest_mun.iloc[0]['nm_mun']
                tempo_destino = self.get_travel_time(cd_mun, cd_destino)
            
            # Verificar se há alerta de dependência funcional
            # (sede depende de outra sede a até 2h de distância)
            alerta = self.check_dependency_criteria(cd_mun)
            
            # Criar dicionário base com campos essenciais
            sede_metrics = {
                # Identificação
                'utp_id': utp_id,
                'cd_mun_sede': cd_mun,
                'cd_mun_6dig': str(cd_mun)[:6] if len(str(cd_mun)) > 6 else str(cd_mun),
                'nm_sede': sede['nm_mun'],
                'uf': sede['uf'],
                'regic': sede.get('regic', ''),
                'regiao_metropolitana': sede.get('regiao_metropolitana', ''),
                
                # População e área
                'populacao_total_utp': int(pop_total),
                'populacao_sede': int(sede.get('populacao_2022', 0)),
                'area_km2': sede.get('area_km2'),
                'num_municipios': num_municipios,
                
                # Viagens
                'total_viagens': total_viagens_sede,
                
                # Aeroporto
                'tem_aeroporto': tem_aeroporto,
                'aeroporto_icao': aeroporto_icao,
                'aeroporto_passageiros': aeroporto_passageiros,
                'aeroportos_100km': sede.get('aeroportos_100km'),
                'aeroportos_internacionais_100km': sede.get('aeroportos_internacionais_100km'),
                
                # Turismo
                'turismo': turismo_classificacao,
                'regiao_turistica': sede.get('regiao_turistica', ''),
                'densidade_leitos_hospedagem': sede.get('densidade_leitos_hospedagem'),
                'densidade_estabelecimentos_hospedagem': sede.get('densidade_estabelecimentos_hospedagem'),
                'avaliacao_media_hospedagem': sede.get('avaliacao_media_hospedagem'),
                'avaliacao_media_restaurante': sede.get('avaliacao_media_restaurante'),
                'estabelecimentos_turismo_mil_hab': sede.get('estabelecimentos_turismo_mil_hab'),
                'ocupacoes_turismo_mil_hab': sede.get('ocupacoes_turismo_mil_hab'),
                'quociente_locacional_turismo': sede.get('quociente_locacional_turismo'),
                'demanda_turistica': sede.get('demanda_turistica'),
                'passageiros_onibus_turismo': sede.get('passageiros_onibus_turismo'),
                
                # Infraestrutura de transporte
                'rodoviarias': sede.get('rodoviarias'),
                
                # Economia
                'estabelecimentos_formais_mil_hab': sede.get('estabelecimentos_formais_mil_hab'),
                'ocupacoes_formais_mil_hab': sede.get('ocupacoes_formais_mil_hab'),
                'renda_per_capita': sede.get('renda_per_capita'),
                'remuneracao_media': sede.get('remuneracao_media'),
                'ice_r': sede.get('ice_r'),
                
                # Recursos naturais e culturais
                'area_conservacao_ambiental_pct': sede.get('area_conservacao_ambiental_pct'),
                'densidade_patrimonio_cultural': sede.get('densidade_patrimonio_cultural'),
                
                # Conectividade
                'cobertura_4g_pct': sede.get('cobertura_4g_pct'),
                'cobertura_5g_pct': sede.get('cobertura_5g_pct'),
                'densidade_banda_larga': sede.get('densidade_banda_larga'),
                
                # Saúde
                'medicos_100mil_hab': sede.get('medicos_100mil_hab'),
                'leitos_hospitalares_100mil_hab': sede.get('leitos_hospitalares_100mil_hab'),
                'estabelecimentos_saude_100mil_hab': sede.get('estabelecimentos_saude_100mil_hab'),
                'leitos_uti_100mil_hab': sede.get('leitos_uti_100mil_hab'),
                
                # Segurança
                'taxa_homicidios_100mil_hab': sede.get('taxa_homicidios_100mil_hab'),
                
                # Fluxo e dependência
                'principal_destino_cd': cd_destino,
                'principal_destino_nm': nm_destino,
                'proporcao_fluxo_principal': prop_fluxo,
                'viagens_para_destino': viagens_para_destino if cd_destino is not None else 0,
                'tempo_ate_destino_h': tempo_destino,
                'tem_alerta_dependencia': alerta is not None,
                'alerta_detalhes': alerta
            }
            
            metrics_list.append(sede_metrics)
        
        self.df_sede_analysis = pd.DataFrame(metrics_list)
        return self.df_sede_analysis
    
    def analyze_sede_dependencies(self) -> Dict:
        """
        Executa análise completa de dependências entre sedes.
        
        Returns:
            Dicionário com resumo da análise
        """
        self.logger.info("Iniciando análise de dependências entre sedes...")
        
        # 1. Carregar dados (se ainda não carregados)
        if self.df_municipios is None:
            if not self.load_initialization_data():
                self.logger.error("Falha ao carregar dados de inicialização")
                return {'success': False, 'error': 'Falha ao carregar dados'}
        else:
             self.logger.info(f"Usando DataFrame pré-carregado com {len(self.df_municipios)} municípios.")
        
        # 2. Carregar impedância
        self.load_impedance_data()
        
        # 3. Calcular métricas
        df_metrics = self.calculate_socioeconomic_metrics()
        
        # 4. Identificar alertas
        self.dependency_alerts = df_metrics[df_metrics['tem_alerta_dependencia']].to_dict('records')
        
        summary = {
            'success': True,
            'total_sedes': len(df_metrics),
            'total_alertas': len(self.dependency_alerts),
            'utps_analisadas': df_metrics['utp_id'].nunique(),
            'populacao_total': int(df_metrics['populacao_total_utp'].sum()),
            'sedes_com_aeroporto': int(df_metrics['tem_aeroporto'].sum())
        }
        
        self.logger.info(f"✅ Análise concluída: {summary['total_sedes']} sedes, {summary['total_alertas']} alertas")
        
        return summary
    
    def export_sede_comparison_table(self) -> pd.DataFrame:
        """
        Exporta tabela comparativa entre sedes para visualização.
        
        Returns:
            DataFrame formatado para exibição no dashboard
        """
        if self.df_sede_analysis is None:
            return pd.DataFrame()
        
        # Selecionar e renomear colunas para exibição
        df_display = self.df_sede_analysis[[
            'utp_id',
            'nm_sede',
            'uf',
            'regic',
            'populacao_total_utp',
            'num_municipios',
            'total_viagens',
            'tem_aeroporto',
            'aeroporto_icao',
            'turismo',
            'principal_destino_nm',
            'proporcao_fluxo_principal',
            'tempo_ate_destino_h',
            'tem_alerta_dependencia'
        ]].copy()
        
        # Renomear para nomes amigáveis
        df_display.columns = [
            'UTP',
            'Sede',
            'UF',
            'REGIC',
            'População',
            'Nº Municípios',
            'Viagens',
            'Aeroporto',
            'ICAO',
            'Turismo',
            'Principal Destino',
            'Fluxo (%)',
            'Tempo (h)',
            'Alerta'
        ]
        
        # Formatar percentual
        df_display['Fluxo (%)'] = (df_display['Fluxo (%)'] * 100).round(1)
        
        # Formatar tempo
        df_display['Tempo (h)'] = df_display['Tempo (h)'].round(2)
        
        # Converter booleanos para símbolos
        df_display['Aeroporto'] = df_display['Aeroporto'].map({True: 'Sim', False: ''})
        df_display['Alerta'] = df_display['Alerta'].map({True: 'SIM', False: ''})
        
        return df_display
    
    def export_origin_destination_comparison(self) -> pd.DataFrame:
        """
        Exporta tabela comparativa no formato origem-destino.
        
        Para cada sede que tem fluxo principal para outra sede, cria uma linha
        mostrando dados comparativos entre origem e destino lado a lado.
        Colunas são organizadas para facilitar comparação visual.
        
        Returns:
            DataFrame formatado com colunas origem-destino intercaladas para comparação
        """
        if self.df_sede_analysis is None:
            return pd.DataFrame()
        
        # Filtrar apenas sedes que têm principal destino que também é sede
        df_with_destinations = self.df_sede_analysis.copy()
        
        # Criar lista para armazenar comparações
        comparisons = []
        
        for _, row_origem in df_with_destinations.iterrows():
            cd_destino = row_origem['principal_destino_cd']
            
            # Verificar se o destino é uma sede válida
            if pd.isna(cd_destino):
                continue
            
            # Buscar dados da sede de destino
            row_destino = df_with_destinations[
                df_with_destinations['cd_mun_sede'] == cd_destino
            ]
            
            if row_destino.empty:
                # Destino não é uma sede, pular
                continue
            
            row_destino = row_destino.iloc[0]
            
            # Calcular diferenças
            delta_pop = row_destino['populacao_total_utp'] - row_origem['populacao_total_utp']
            delta_viagens = row_destino['total_viagens'] - row_origem['total_viagens']
            
            # Criar linha comparativa com colunas intercaladas para facilitar comparação
            comparison = {
                # UTP
                'Origem_UTP': row_origem['utp_id'],
                'Destino_UTP': row_destino['utp_id'],
                
                # Sede
                'Origem_Sede': row_origem['nm_sede'],
                'Destino_Sede': row_destino['nm_sede'],
                
                # UF
                'Origem_UF': row_origem['uf'],
                'Destino_UF': row_destino['uf'],
                
                # REGIC
                'Origem_REGIC': row_origem['regic'] if row_origem['regic'] else '-',
                'Destino_REGIC': row_destino['regic'] if row_destino['regic'] else '-',
                
                # População
                'Origem_População': int(row_origem['populacao_total_utp']),
                'Destino_População': int(row_destino['populacao_total_utp']),
                'Δ_População': int(delta_pop),
                
                # Municípios
                'Origem_Municípios': row_origem['num_municipios'],
                'Destino_Municípios': row_destino['num_municipios'],
                
                # Viagens
                'Origem_Viagens': int(row_origem['total_viagens']),
                'Destino_Viagens': int(row_destino['total_viagens']),
                'Δ_Viagens': int(delta_viagens),
                
                # Aeroporto
                'Origem_Aeroporto': 'Sim' if row_origem['tem_aeroporto'] else '-',
                'Destino_Aeroporto': 'Sim' if row_destino['tem_aeroporto'] else '-',
                
                # ICAO
                'Origem_ICAO': row_origem['aeroporto_icao'] if row_origem['tem_aeroporto'] and row_origem['aeroporto_icao'] else '-',
                'Destino_ICAO': row_destino['aeroporto_icao'] if row_destino['tem_aeroporto'] and row_destino['aeroporto_icao'] else '-',
                
                # Turismo
                'Origem_Turismo': row_origem['turismo'] if row_origem['turismo'] and str(row_origem['turismo']).strip() != '' else '-',
                'Destino_Turismo': row_destino['turismo'] if row_destino['turismo'] and str(row_destino['turismo']).strip() != '' else '-',
                
                # Relação
                'Fluxo_%': round(row_origem['proporcao_fluxo_principal'] * 100, 1),
                'Tempo_h': round(row_origem['tempo_ate_destino_h'], 2) if pd.notna(row_origem['tempo_ate_destino_h']) else None,
                'Alerta': '⚠️ SIM' if row_origem['tem_alerta_dependencia'] else '',
                
                # Razão populacional
                'Razão_Pop': round(row_destino['populacao_total_utp'] / row_origem['populacao_total_utp'], 2) if row_origem['populacao_total_utp'] > 0 else 0
            }
            
            comparisons.append(comparison)
        
        df_comparison = pd.DataFrame(comparisons)
        
        # Ordenar por fluxo percentual (maior dependência primeiro)
        if not df_comparison.empty:
            df_comparison = df_comparison.sort_values('Fluxo_%', ascending=False)
        
        return df_comparison
    
    def export_comprehensive_dependency_table(self) -> pd.DataFrame:
        """
        Exporta tabela COMPLETA de análise de dependências no formato origem-destino.
        
        Inclui TODOS os indicadores socioeconômicos disponíveis para análises detalhadas.
        Formato: id_ibge_origem, nome_municipio_origem, UTP_ORIGEM, id_ibge_destino,
        nome_municipio_destino, UTP_DESTINO, qtd_viagens, Tempo, + 100+ indicadores
        
        Returns:
            DataFrame com análise completa origem-destino
        """
        if self.df_sede_analysis is None:
            return pd.DataFrame()
        
        df_with_destinations = self.df_sede_analysis.copy()
        comprehensive_data = []
        
        for _, row_origem in df_with_destinations.iterrows():
            cd_destino = row_origem['principal_destino_cd']
            
            if pd.isna(cd_destino):
                continue
            
            row_destino = df_with_destinations[
                df_with_destinations['cd_mun_sede'] == cd_destino
            ]
            
            if row_destino.empty:
                continue
            
            row_destino = row_destino.iloc[0]
            mesma_utp = row_origem['utp_id'] == row_destino['utp_id']
            
            # Criar linha completa com TODOS os dados
            comprehensive_row = {
                # Identificação
                'id_ibge_origem': int(row_origem['cd_mun_sede']),
                'id_origem_6dig': row_origem['cd_mun_6dig'],
                'nome_municipio_origem': row_origem['nm_sede'],
                'UTP_ORIGEM': row_origem['utp_id'],
                'SEDE_ORIGEM': True,
                'UF_ORIGEM': row_origem['uf'],
                'REGIC_ORIGEM': row_origem['regic'] if row_origem['regic'] else '',
                'id_ibge_destino': int(row_destino['cd_mun_sede']),
                'id_destino_6dig': row_destino['cd_mun_6dig'],
                'nome_municipio_destino': row_destino['nm_sede'],
                'UTP_DESTINO': row_destino['utp_id'],
                'SEDE_DESTINO': True,
                'UF_DESTINO': row_destino['uf'],
                'REGIC_DESTINO': row_destino['regic'] if row_destino['regic'] else '',
                
                # Relação/Fluxo
                'qtd_viagens': int(row_origem['viagens_para_destino']),
                'total_viagens_origem': int(row_origem['total_viagens']),
                'proporcao_fluxo_pct': round(row_origem['proporcao_fluxo_principal'] * 100, 2),
                'Tempo': round(row_origem['tempo_ate_destino_h'], 2) if pd.notna(row_origem['tempo_ate_destino_h']) else None,
                'MESMA_UTP_FLAG': mesma_utp,
                'ALERTA_DEPENDENCIA': '⚠️ SIM' if row_origem['tem_alerta_dependencia'] else '',
                
                # População
                'PopulacaoUTP_Origem': int(row_origem['populacao_total_utp']),
                'PopulacaoSede_Origem': int(row_origem['populacao_sede']),
                'NumMunicipios_Origem': int(row_origem['num_municipios']),
                'PopulacaoUTP_Destino': int(row_destino['populacao_total_utp']),
                'PopulacaoSede_Destino': int(row_destino['populacao_sede']),
                'NumMunicipios_Destino': int(row_destino['num_municipios']),
                
                # Aeroporto
                'Aeroporto_Origem': 'Sim' if row_origem['tem_aeroporto'] else '',
                'AeroportoICAO_Origem': row_origem['aeroporto_icao'] if row_origem['tem_aeroporto'] else '',
                'AeroportoPassageiros_Origem': int(row_origem['aeroporto_passageiros']) if row_origem['tem_aeroporto'] else 0,
                'Aeroportos100km_Origem': row_origem['aeroportos_100km'],
                'Aeroporto_Destino': 'Sim' if row_destino['tem_aeroporto'] else '',
                'AeroportoICAO_Destino': row_destino['aeroporto_icao'] if row_destino['tem_aeroporto'] else '',
                'AeroportoPassageiros_Destino': int(row_destino['aeroporto_passageiros']) if row_destino['tem_aeroporto'] else 0,
                'Aeroportos100km_Destino': row_destino['aeroportos_100km'],
                
                # Turismo
                'ClassificacaoTurismo_Origem': row_origem['turismo'] if row_origem['turismo'] else '',
                'RegiaoTuristica_Origem': row_origem['regiao_turistica'],
                'DensidadeLeitosHospedagem_Origem': row_origem['densidade_leitos_hospedagem'],
                'EstabTurismoMilHab_Origem': row_origem['estabelecimentos_turismo_mil_hab'],
                'DemandaTuristica_Origem': row_origem['demanda_turistica'],
                'ClassificacaoTurismo_Destino': row_destino['turismo'] if row_destino['turismo'] else '',
                'RegiaoTuristica_Destino': row_destino['regiao_turistica'],
                'DensidadeLeitosHospedagem_Destino': row_destino['densidade_leitos_hospedagem'],
                'EstabTurismoMilHab_Destino': row_destino['estabelecimentos_turismo_mil_hab'],
                'DemandaTuristica_Destino': row_destino['demanda_turistica'],
                
                # Infraestrutura
                'Rodoviarias_Origem': row_origem['rodoviarias'],
                'RegiaoMetropolitana_Origem': row_origem['regiao_metropolitana'],
                'Rodoviarias_Destino': row_destino['rodoviarias'],
                'RegiaoMetropolitana_Destino': row_destino['regiao_metropolitana'],
                
                # Economia
                'EstabFormaisMilHab_Origem': row_origem['estabelecimentos_formais_mil_hab'],
                'RendaPerCapita_Origem': row_origem['renda_per_capita'],
                'RemuneracaoMedia_Origem': row_origem['remuneracao_media'],
                'ICE_R_Origem': row_origem['ice_r'],
                'EstabFormaisMilHab_Destino': row_destino['estabelecimentos_formais_mil_hab'],
                'RendaPerCapita_Destino': row_destino['renda_per_capita'],
                'RemuneracaoMedia_Destino': row_destino['remuneracao_media'],
                'ICE_R_Destino': row_destino['ice_r'],
                
                # Conectividade
                'Cobertura4G_Origem': row_origem['cobertura_4g_pct'],
                'Cobertura5G_Origem': row_origem['cobertura_5g_pct'],
                'DensidadeBandaLarga_Origem': row_origem['densidade_banda_larga'],
                'Cobertura4G_Destino': row_destino['cobertura_4g_pct'],
                'Cobertura5G_Destino': row_destino['cobertura_5g_pct'],
                'DensidadeBandaLarga_Destino': row_destino['densidade_banda_larga'],
                
                # Saúde
                'Medicos100MilHab_Origem': row_origem['medicos_100mil_hab'],
                'Leitos100MilHab_Origem': row_origem['leitos_hospitalares_100mil_hab'],
                'LeitosUTI100MilHab_Origem': row_origem['leitos_uti_100mil_hab'],
                'Medicos100MilHab_Destino': row_destino['medicos_100mil_hab'],
                'Leitos100MilHab_Destino': row_destino['leitos_hospitalares_100mil_hab'],
                'LeitosUTI100MilHab_Destino': row_destino['leitos_uti_100mil_hab'],
                
                # Segurança
                'TaxaHomicidios_Origem': row_origem['taxa_homicidios_100mil_hab'],
                'TaxaHomicidios_Destino': row_destino['taxa_homicidios_100mil_hab'],
                
                # Observação
                'observacao': row_origem['alerta_detalhes'].get('alerta', '') if row_origem['alerta_detalhes'] else ''
            }
            
            comprehensive_data.append(comprehensive_row)
        
        df_comprehensive = pd.DataFrame(comprehensive_data)
        
        if not df_comprehensive.empty:
            df_comprehensive = df_comprehensive.sort_values('proporcao_fluxo_pct', ascending=False)
        
        return df_comprehensive
    
    def export_to_json(self, output_path: Path) -> bool:
        """
        Exporta resultados da análise para JSON persistente.
        
        Este método salva toda a análise de dependências em um arquivo JSON
        para carregamento rápido pelo dashboard, evitando recalcular a cada sessão.
        
        Args:
            output_path: Caminho completo para o arquivo JSON de saída
            
        Returns:
            True se exportação foi bem-sucedida, False caso contrário
        """
        if self.df_sede_analysis is None:
            self.logger.error("Nenhuma análise disponível para exportar. Execute analyze_sede_dependencies() primeiro.")
            return False
        
        try:
            from datetime import datetime
            
            # Preparar dados para serialização
            df_export = self.df_sede_analysis.copy()
            
            # Converter tipos para JSON-serializáveis
            for col in df_export.columns:
                if df_export[col].dtype == 'object':
                    df_export[col] = df_export[col].fillna('')
                elif df_export[col].dtype in ['int64', 'Int64']:
                    df_export[col] = df_export[col].fillna(0).astype(int)
                elif df_export[col].dtype in ['float64', 'Float64']:
                    df_export[col] = df_export[col].fillna(0.0)
            
            # Estrutura do JSON
            export_data = {
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'consolidation_applied': self.consolidation_loader is not None and self.consolidation_loader.is_executed(),
                    'total_sedes': len(df_export),
                    'version': '2.0'  # Versão 2.0 com tabela completa
                },
                'summary': {
                    'success': True,
                    'total_sedes': len(df_export),
                    'total_alertas': int(df_export['tem_alerta_dependencia'].sum()),
                    'utps_analisadas': df_export['utp_id'].nunique(),
                    'populacao_total': int(df_export['populacao_total_utp'].sum()),
                    'sedes_com_aeroporto': int(df_export['tem_aeroporto'].sum())
                },
                'sede_analysis': df_export.to_dict('records')
            }
            
            # Adicionar tabela completa de dependências origem-destino
            try:
                df_comprehensive = self.export_comprehensive_dependency_table()
                if not df_comprehensive.empty:
                    # Converter para JSON-serializável
                    df_comp_export = df_comprehensive.copy()
                    for col in df_comp_export.columns:
                        if df_comp_export[col].dtype == 'object':
                            df_comp_export[col] = df_comp_export[col].fillna('')
                        elif df_comp_export[col].dtype in ['int64', 'Int64']:
                            df_comp_export[col] = df_comp_export[col].fillna(0).astype(int)
                        elif df_comp_export[col].dtype in ['float64', 'Float64']:
                            df_comp_export[col] = df_comp_export[col].fillna(0.0)
                    
                    export_data['comprehensive_dependency_table'] = df_comp_export.to_dict('records')
                    self.logger.info(f"   📊 Tabela completa: {len(df_comp_export)} relações origem-destino, {len(df_comp_export.columns)} colunas")
            except Exception as e:
                self.logger.warning(f"   ⚠️ Erro ao exportar tabela completa: {e}")
            
            # Salvar JSON
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"✅ Análise exportada para {output_path}")
            self.logger.info(f"   📊 {len(df_export)} sedes, {export_data['summary']['total_alertas']} alertas")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao exportar análise para JSON: {e}")
            return False
