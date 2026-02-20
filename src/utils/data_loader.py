"""
Módulo utilitário para carregar dados do initialization.json
"""
import json
import pandas as pd
from pathlib import Path
import logging
from typing import Dict, Tuple, Optional

logger = logging.getLogger(__name__)


class DataLoader:
    """Carrega dados pré-consolidados do initialization.json"""
    
    _instance = None
    _data_cache = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @staticmethod
    def find_json_path() -> Optional[Path]:
        """Encontra o caminho do initialization.json"""
        # Procurar em várias localizações possíveis
        possible_paths = [
            Path(__file__).parent.parent.parent / "data" / "initialization.json",
            Path.cwd() / "data" / "initialization.json",
            Path.cwd() / "initialization.json",
        ]
        
        for path in possible_paths:
            if path.exists():
                return path
        
        return None
    
    @classmethod
    def load_data(cls) -> Optional[Dict]:
        """Carrega os dados do JSON (com cache)"""
        if cls._data_cache is not None:
            return cls._data_cache
        
        json_path = cls.find_json_path()
        
        if not json_path:
            logger.warning("initialization.json não encontrado em nenhuma localização esperada")
            return None
        
        try:
            logger.info(f"Carregando dados de {json_path}")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            cls._data_cache = data
            logger.info(f"✓ Dados carregados com sucesso ({len(data.get('municipios', []))} municípios)")
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao fazer parsing do JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {type(e).__name__}: {e}")
            return None
    
    @classmethod
    def get_municipios_dataframe(cls) -> pd.DataFrame:
        """Retorna DataFrame de municipios"""
        data = cls.load_data()
        
        if data is None:
            return pd.DataFrame()
        
        municipios = data.get('municipios', [])
        return pd.DataFrame(municipios)
    
    @classmethod
    def get_utps_dataframe(cls) -> pd.DataFrame:
        """Retorna DataFrame de UTPs"""
        data = cls.load_data()
        
        if data is None:
            return pd.DataFrame()
        
        utps = data.get('utps', [])
        return pd.DataFrame(utps)
    
    @classmethod
    def get_metadata(cls) -> Dict:
        """Retorna metadata dos dados"""
        data = cls.load_data()
        
        if data is None:
            return {}
        
        return data.get('metadata', {})
    
    @classmethod
    def get_municipio_by_cd(cls, cd_mun: int) -> Optional[Dict]:
        """Busca um municipio por código IBGE"""
        df = cls.get_municipios_dataframe()
        
        if df.empty:
            return None
        
        result = df[df['cd_mun'] == cd_mun]
        
        if result.empty:
            return None
        
        return result.iloc[0].to_dict()
    
    @classmethod
    def get_utp_by_id(cls, utp_id: str) -> Optional[Dict]:
        """Busca uma UTP por ID"""
        df = cls.get_utps_dataframe()
        
        if df.empty:
            return None
        
        result = df[df.get('utp_id', df.get('id')) == utp_id]
        
        if result.empty:
            return None
        
        return result.iloc[0].to_dict()
    
    @classmethod
    def get_municipios_by_utp(cls, utp_id: str) -> pd.DataFrame:
        """Retorna todos os municipios de uma UTP"""
        df = cls.get_municipios_dataframe()
        
        if df.empty:
            return pd.DataFrame()
        
        return df[df.get('utp_id', '') == utp_id]
    
    @classmethod
    def get_modais_data(cls, cd_mun: int) -> Dict:
        """Retorna dados de modais para um municipio"""
        municipio = cls.get_municipio_by_cd(cd_mun)
        
        if municipio is None:
            return {}
        
        return municipio.get('modais', {})
    
    @classmethod
    def get_impedancia_2h(cls, cd_mun: int) -> Optional[float]:
        """Retorna impedancia 2h para um municipio"""
        municipio = cls.get_municipio_by_cd(cd_mun)
        
        if municipio is None:
            return None
        
        return municipio.get('impedancia_2h_filtrada')
    
    @classmethod
    def get_modal_matriz(cls, cd_mun: int, modal: str) -> Dict:
        """Retorna matriz de origem-destino para um municipio e modal"""
        municipio = cls.get_municipio_by_cd(cd_mun)
        
        if municipio is None:
            return {}
        
        modal_matriz = municipio.get('modal_matriz', {})
        return modal_matriz.get(modal, {})
    
    @classmethod
    def search_municipios(cls, term: str) -> pd.DataFrame:
        """Busca municipios por nome"""
        df = cls.get_municipios_dataframe()
        
        if df.empty:
            return pd.DataFrame()
        
        return df[df.get('nm_mun', '').str.lower().str.contains(term.lower(), na=False)]
    
    
    @classmethod
    def get_airport_data(cls, cd_mun: int) -> Optional[Dict]:
        """
        Retorna dados de aeroporto para um município.
        
        Args:
            cd_mun: Código IBGE do município
            
        Returns:
            Dict com dados do aeroporto (icao, cidade, passageiros_anual) ou None
        """
        municipio = cls.get_municipio_by_cd(cd_mun)
        
        if municipio is None:
            return None
        
        return municipio.get('aeroporto')
    
    @classmethod
    def get_municipios_by_uf(cls, uf: str) -> pd.DataFrame:
        """Retorna todos os municipios de um estado"""
        df = cls.get_municipios_dataframe()
        
        if df.empty:
            return pd.DataFrame()
        
        return df[df.get('uf', '') == uf]
    
    @classmethod
    def clear_cache(cls):
        """Limpa o cache de dados"""
        cls._data_cache = None
