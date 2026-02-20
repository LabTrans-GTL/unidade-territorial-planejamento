# src/pipeline/analyzer.py
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple


class ODAnalyzer:
    """Analisa Matriz de Origem-Destino para identificar fluxos principais."""

    def __init__(self):
        self.logger = logging.getLogger("GeoValida.ODAnalyzer")
        self.full_flow_df = None

    def run_full_analysis(self) -> pd.DataFrame:
        """Executa análise completa dos fluxos OD carregando dados de múltiplos modais."""
        self.logger.info("Iniciando análise de Matriz OD...")
        
        data_path = Path(__file__).parent.parent.parent / "data" / "01_raw" / "person-matrix-data"
        
        # Arquivos de fluxo por modal
        modal_files = [
            "base_dados_aeroviaria_2023.csv",
            "base_dados_ferroviaria_2023.csv",
            "base_dados_hidroviaria_2023.csv",
            "base_dados_rodoviaria_coletiva_2023.csv",
            "base_dados_rodoviaria_particular_2023.csv"
        ]
        
        dataframes = []
        
        for modal_file in modal_files:
            file_path = data_path / modal_file
            if not file_path.exists():
                self.logger.warning(f"Arquivo não encontrado: {file_path}")
                continue
            
            try:
                # Tentar diferentes separadores e encodings
                df = None
                for sep in [',', ';', '\t']:
                    for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
                        try:
                            df = pd.read_csv(file_path, encoding=encoding, sep=sep, nrows=1)
                            if len(df.columns) > 1:  # Se conseguiu splittar as colunas
                                break
                        except:
                            continue
                    if df is not None and len(df.columns) > 1:
                        break
                
                if df is None or len(df.columns) < 2:
                    self.logger.warning(f"Não conseguiu detectar formato de {file_path}")
                    continue
                
                # Recarregar com os parâmetros corretos
                df = pd.read_csv(file_path, encoding=encoding, sep=sep)
                
                # Normalizar nomes de colunas
                df.columns = df.columns.str.strip().str.lower()
                
                # Mapear colunas esperadas
                if 'mun_origem' in df.columns and 'mun_destino' in df.columns and 'viagens' in df.columns:
                    df_modal = df[['mun_origem', 'mun_destino', 'viagens']].copy()
                elif 'cd_mun_origem' in df.columns and 'cd_mun_destino' in df.columns:
                    df_modal = df[['cd_mun_origem', 'cd_mun_destino', 'viagens']].copy()
                    df_modal.columns = ['mun_origem', 'mun_destino', 'viagens']
                else:
                    self.logger.warning(f"Colunas não encontradas em {modal_file}: {df.columns.tolist()}")
                    continue
                
                dataframes.append(df_modal)
                self.logger.info(f"✓ Carregado {modal_file}: {len(df_modal)} registros")
            
            except Exception as e:
                self.logger.warning(f"Erro ao carregar {modal_file}: {e}")
                continue
        
        if not dataframes:
            self.logger.warning("Nenhum arquivo de fluxo foi carregado!")
            self.full_flow_df = pd.DataFrame({
                'mun_origem': [],
                'mun_destino': [],
                'viagens': []
            })
            return self.full_flow_df
        
        # Consolidar todos os modais
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        # Agrupar por origem-destino (somar viagens de modais diferentes)
        combined_df = combined_df.groupby(['mun_origem', 'mun_destino'])['viagens'].sum().reset_index()
        
        # Calcular proporção de fluxo por origem
        combined_df['total_origem'] = combined_df.groupby('mun_origem')['viagens'].transform('sum')
        combined_df['proporcao'] = combined_df['viagens'] / combined_df['total_origem']
        
        self.full_flow_df = combined_df
        
        self.logger.info(f"✅ Análise OD concluída: {len(self.full_flow_df)} registros com {combined_df['mun_origem'].nunique()} municípios de origem")
        return self.full_flow_df

    def get_main_destination(self, origin_mun: int, threshold: float = 0.1) -> Tuple[int, float]:
        """Retorna o destino principal para um município origem.
        
        Args:
            origin_mun: Código do município de origem
            threshold: Proporção mínima de fluxo para considerar como principal
            
        Returns:
            (cd_mun_destino, proporcao_fluxo)
        """
        if self.full_flow_df is None or self.full_flow_df.empty:
            return None, 0.0
        
        flows = self.full_flow_df[self.full_flow_df['mun_origem'] == origin_mun]
        if flows.empty:
            return None, 0.0
        
        top = flows.nlargest(1, 'proporcao')
        if top.empty or top['proporcao'].values[0] < threshold:
            return None, 0.0
        
        return int(top['mun_destino'].values[0]), float(top['proporcao'].values[0])

    def filter_significant_flows(self, min_proportion: float = 0.05) -> pd.DataFrame:
        """Filtra fluxos significativos (acima do threshold)."""
        if self.full_flow_df is None or self.full_flow_df.empty:
            return pd.DataFrame()
        
        return self.full_flow_df[self.full_flow_df['proporcao'] >= min_proportion]
