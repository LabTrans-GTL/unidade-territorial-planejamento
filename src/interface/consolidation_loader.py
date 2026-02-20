# src/interface/consolidation_loader.py
"""
Loader de Consolidações - Sistema de Cache
Permite executar a consolidação uma vez e reutilizar o resultado
"""
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import pandas as pd


class ConsolidationLoader:
    """Gerencia o carregamento e cache de consolidações."""
    
    def __init__(self):
        self.result_path = Path(__file__).parent.parent.parent / "data" / "consolidation_result.json"
        self.post_unitary_path = Path(__file__).parent.parent.parent / "data" / "post_unitary_consolidation.json"
        self.sede_result_path = Path(__file__).parent.parent.parent / "data" / "sede_consolidation_result.json"
        self.log_path = Path(__file__).parent.parent.parent / "data" / "consolidation_log.json"
        self.result = self._load_result()
        self.post_unitary_result = self._load_post_unitary_result()
        self.sede_result = self._load_sede_result()

    def _load_sede_result(self) -> Dict:
        """Carrega o arquivo de resultado da consolidação de sedes."""
        if not self.sede_result_path.exists():
            return self._default_result()
        
        try:
            with open(self.sede_result_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return self._default_result()
        except Exception as e:
            print(f"Erro ao carregar resultado de sedes: {e}")
            return self._default_result()

    def _load_post_unitary_result(self) -> Dict:
        """Carrega o snapshot após Steps 5+7, antes da Step 6 (sedes)."""
        if not self.post_unitary_path.exists():
            # Fallback: se o snapshot não existe, usa o resultado completo
            return self._load_result()
        
        try:
            with open(self.post_unitary_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            # Arquivo vazio ou inexistente - comportamento normal na primeira execução
            return self._load_result()
        except Exception as e:
            print(f"Erro ao carregar snapshot pós-unitárias: {e}")
            # Fallback para resultado completo
            return self._load_result()

    def _load_result(self) -> Dict:
        """Carrega o arquivo de resultado de consolidação."""
        if not self.result_path.exists():
            return {
                "version": "1.0",
                "status": "not_executed",
                "timestamp_created": None,
                "timestamp_last_updated": None,
                "total_consolidations": 0,
                "utps_mapping": {},  # {source_utp: target_utp}
                "consolidations": []  # Lista de detalhes
            }
        
        try:
            with open(self.result_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return self._default_result()
        except Exception as e:
            print(f"Erro ao carregar resultado: {e}")
            return self._default_result()
    
    def _default_result(self) -> Dict:
        """Retorna estrutura padrão."""
        return {
            "version": "1.0",
            "status": "not_executed",
            "timestamp_created": None,
            "timestamp_last_updated": None,
            "total_consolidations": 0,
            "utps_mapping": {},
            "consolidations": []
        }
    
    def save_result(self):
        """Salva o resultado de consolidação."""
        try:
            self.result_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.result_path, 'w', encoding='utf-8') as f:
                json.dump(self.result, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Erro ao salvar resultado: {e}")
    
    def is_executed(self) -> bool:
        """Verifica se a consolidação já foi executada (independente do número de mudanças)."""
        return self.result["status"] == "executed"
    
    def get_utps_mapping(self) -> Dict:
        """Retorna o mapeamento de UTPs (source -> target)."""
        return self.result["utps_mapping"]
    
    def get_consolidations(self) -> List[Dict]:
        """Retorna a lista de consolidações."""
        return self.result["consolidations"]
    
    def update_from_log(self, log_data: Dict):
        """Atualiza o resultado a partir do log de consolidação."""
        consolidations = log_data.get("consolidations", [])
        
        # Criar mapeamento
        mapping = {}
        for cons in consolidations:
            mapping[cons["source_utp"]] = cons["target_utp"]
        
        self.result = {
            "version": "1.0",
            "status": "executed",
            "timestamp_created": self.result["timestamp_created"] or datetime.now().isoformat(),
            "timestamp_last_updated": datetime.now().isoformat(),
            "total_consolidations": len(consolidations),
            "utps_mapping": mapping,
            "consolidations": consolidations
        }
        
        self.save_result()
    
    def _resolve_mapping_chain(self, utp_id: str, mapping: dict, max_depth: int = 10) -> str:
        """
        Resolve consolidação em cadeia seguindo o mapeamento transitivo.
        
        Exemplo: Se UTP 131 -> 152 e UTP 152 -> 672, então 131 -> 672
        
        Args:
            utp_id: ID da UTP inicial
            mapping: Dicionário de mapeamento {source_utp: target_utp}
            max_depth: Profundidade máxima para evitar loops infinitos
            
        Returns:
            ID final da UTP após seguir toda a cadeia
        """
        visited = set()
        current = utp_id
        depth = 0
        
        while current in mapping and depth < max_depth:
            if current in visited:  # Detecta ciclo
                break
            visited.add(current)
            current = mapping[current]
            depth += 1
        
        return current
    
    def compute_mapping_from_list(self, consolidations: List[Dict]) -> Dict:
        """Computa o mapeamento source->target a partir de uma lista de consolidações."""
        mapping = {}
        for cons in consolidations:
            mapping[cons["source_utp"]] = cons["target_utp"]
        return mapping

    def apply_consolidations_to_dataframe(self, df: pd.DataFrame, custom_mapping: Dict = None) -> pd.DataFrame:
        """
        Aplica as consolidações a um dataframe, cuidando da lógica de sedes.
        
        Args:
            df: DataFrame com colunas 'utp_id', 'sede_utp', 'nm_mun'
            custom_mapping: (Opcional) Dicionário de mapeamento específico. 
                          Se None, usa o do resultado carregado.
        
        Returns:
            DataFrame com UTPs consolidadas e sedes atualizadas
        """
        if custom_mapping is None and not self.is_executed():
            return df.copy()
        
        df_consolidated = df.copy()
        
        # Usa mapeamento customizado ou o carregado do resultado
        mapping = custom_mapping if custom_mapping is not None else self.get_utps_mapping()
        
        if mapping:
            # 1. Identificar quem vai mudar de UTP
            changing_mask = df_consolidated['utp_id'].isin(mapping.keys())
            
            # 2. Resetar flag de sede para quem está sendo movido
            # Se uma UTP unitária é englobada, seu único município deixa de ser sede
            df_consolidated.loc[changing_mask, 'sede_utp'] = False
            
            # 3. Aplicar o mapeamento de UTP IDs RESOLVENDO CADEIAS
            # Importante: seguir toda a cadeia de consolidações (ex: 131->152->672)
            df_consolidated['utp_id'] = df_consolidated['utp_id'].apply(
                lambda x: self._resolve_mapping_chain(x, mapping)
            )
            
            # 4. Atualizar o nome da sede (nm_sede) se a coluna existir
            # Precisamos mapear o UTP_ID final para o nome da sede real daquela UTP
            if 'nm_sede' in df_consolidated.columns:
                # Criar um mapeamento ATUALIZADO de UTP -> Nome da Sede
                # Usamos apenas quem ainda é sede_utp == True após o reset acima
                sedes_atuais = df_consolidated[df_consolidated['sede_utp'] == True]
                
                # Identifica se a coluna de nome é 'nm_mun' (df) ou 'NM_MUN' (gdf)
                name_col = 'nm_mun' if 'nm_mun' in sedes_atuais.columns else 'NM_MUN'
                if name_col in sedes_atuais.columns:
                    sede_mapper = sedes_atuais.set_index('utp_id')[name_col].to_dict()
                    df_consolidated['nm_sede'] = df_consolidated['utp_id'].map(sede_mapper).fillna('')
        
        return df_consolidated
    
    def get_summary(self) -> Dict:
        """Retorna um resumo da consolidação."""
        mapping = self.result["utps_mapping"]
        consolidations = self.result["consolidations"]
        
        reasons = {}
        for cons in consolidations:
            reason = cons.get("reason", "Desconhecido")
            reasons[reason] = reasons.get(reason, 0) + 1
        
        return {
            "status": self.result["status"],
            "executed": self.is_executed(),
            "timestamp": self.result["timestamp_last_updated"],
            "total_consolidations": self.result["total_consolidations"],
            "unique_sources": len(mapping),
            "unique_targets": len(set(mapping.values())),
            "reasons": reasons
        }
    
    def get_statistics(self, df_initial: pd.DataFrame, df_consolidated: pd.DataFrame) -> Dict:
        """Calcula estatísticas de consolidação."""
        return {
            "utps_before": df_initial['utp_id'].nunique(),
            "utps_after": df_consolidated['utp_id'].nunique(),
            "reduction": df_initial['utp_id'].nunique() - df_consolidated['utp_id'].nunique(),
            "reduction_percentage": (
                (df_initial['utp_id'].nunique() - df_consolidated['utp_id'].nunique()) / 
                df_initial['utp_id'].nunique() * 100
            )
        }
    
    def export_as_dataframe(self) -> pd.DataFrame:
        """Exporta as consolidações como DataFrame."""
        consolidations = self.result["consolidations"]
        
        if not consolidations:
            return pd.DataFrame()
        
        return pd.DataFrame([
            {
                "ID": i + 1,
                "UTP Origem": c["source_utp"],
                "UTP Destino": c["target_utp"],
                "Motivo": c.get("reason", "N/A"),
                "Data": c["timestamp"][:10],
                "Hora": c["timestamp"][11:19]
            }
            for i, c in enumerate(consolidations)
        ])
    
    def clear(self):
        """Limpa o resultado de consolidação."""
        self.result = self._default_result()
        self.sede_result = self._default_result()
        self.save_result()
        self.save_sede_result()

    def save_sede_result(self, data: Dict = None):
        """Salva o resultado da consolidação de sedes."""
        if data:
            self.sede_result = data
            
        try:
            self.sede_result_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.sede_result_path, 'w', encoding='utf-8') as f:
                json.dump(self.sede_result, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Erro ao salvar resultado de sedes: {e}")

    def get_sede_result(self) -> Dict:
        """Retorna o resultado específico da consolidação de sedes."""
        return self.sede_result

    def is_sede_executed(self) -> bool:
        """Verifica se a consolidação de sedes foi executada."""
        return self.sede_result["status"] == "executed"

    def get_post_unitary_mapping(self) -> Dict:
        """Retorna o mapeamento de UTPs do snapshot pós-unitárias (Steps 5+7)."""
        return self.post_unitary_result.get("utps_mapping", {})
    
    def get_post_unitary_consolidations(self) -> List[Dict]:
        """Retorna a lista de consolidações do snapshot pós-unitárias (Steps 5+7)."""
        return self.post_unitary_result.get("consolidations", [])
    
    def is_post_unitary_executed(self) -> bool:
        """Verifica se há snapshot pós-unitárias disponível."""
        return self.post_unitary_result.get("status") == "executed"
    
    def apply_post_unitary_to_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica apenas as consolidações pós-unitárias (Steps 5+7) a um dataframe,
        SEM incluir consolidações de sedes.
        
        Args:
            df: DataFrame com colunas 'utp_id', 'sede_utp', 'nm_mun'
        
        Returns:
            DataFrame com consolidações de Steps 5+7 aplicadas (antes da Step 6)
        """
        if not self.is_post_unitary_executed():
            return df.copy()
        
        # Usar o mapeamento do snapshot pós-unitárias
        mapping = self.get_post_unitary_mapping()
        
        if not mapping:
            return df.copy()
        
        df_consolidated = df.copy()
        
        # Aplicar mesma lógica do apply_consolidations_to_dataframe
        # mas usando o mapeamento pós-unitárias
        changing_mask = df_consolidated['utp_id'].isin(mapping.keys())
        df_consolidated.loc[changing_mask, 'sede_utp'] = False
        
        df_consolidated['utp_id'] = df_consolidated['utp_id'].apply(
            lambda x: self._resolve_mapping_chain(x, mapping)
        )
        
        if 'nm_sede' in df_consolidated.columns:
            sedes_atuais = df_consolidated[df_consolidated['sede_utp'] == True]
            name_col = 'nm_mun' if 'nm_mun' in sedes_atuais.columns else 'NM_MUN'
            if name_col in sedes_atuais.columns:
                sede_mapper = sedes_atuais.set_index('utp_id')[name_col].to_dict()
                df_consolidated['nm_sede'] = df_consolidated['utp_id'].map(sede_mapper).fillna('')
        
        return df_consolidated
