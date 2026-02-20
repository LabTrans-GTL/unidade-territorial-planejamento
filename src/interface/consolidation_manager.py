# src/interface/consolidation_manager.py
"""
Gerenciador de Consolidações de UTPs com rastreamento em JSON
"""
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict


class ConsolidationManager:
    """Gerencia o rastreamento de consolidações de UTPs."""
    
    def __init__(self, log_path: Path = None):
        if log_path is None:
            self.log_path = Path(__file__).parent.parent.parent / "data" / "consolidation_log.json"
        else:
            self.log_path = log_path
        
        self.log_data = self._load_log()
    
    def _load_log(self) -> Dict:
        """Carrega o arquivo de log de consolidações."""
        if not self.log_path.exists():
            return {
                "version": "1.0",
                "timestamp_created": datetime.now().isoformat(),
                "consolidations": []
            }
        
        try:
            with open(self.log_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Erro ao carregar log: {e}")
            return {
                "version": "1.0",
                "timestamp_created": datetime.now().isoformat(),
                "consolidations": []
            }
    
    def save_log(self):
        """Salva o arquivo de log de consolidações."""
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_path, 'w', encoding='utf-8') as f:
                json.dump(self.log_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Erro ao salvar log: {e}")
    
    def add_consolidation(self, source_utp: str, target_utp: str, reason: str, details: Dict = None):
        """
        Adiciona uma consolidação ao log.
        
        Args:
            source_utp: ID da UTP de origem (que será consolidada)
            target_utp: ID da UTP de destino (que receberá os municípios)
            reason: Motivo da consolidação (ex: "Dependência funcional", "Adjacência", "REGIC")
            details: Dicionário com detalhes adicionais
        """
        consolidation = {
            "id": len(self.log_data["consolidations"]) + 1,
            "timestamp": datetime.now().isoformat(),
            "source_utp": source_utp,
            "target_utp": target_utp,
            "reason": reason,
            "details": details or {}
        }
        
        self.log_data["consolidations"].append(consolidation)
        self.save_log()
        
        return consolidation
    
    def add_consolidations_batch(self, consolidations: List[Dict]):
        """
        Adiciona múltiplas consolidações em lote.
        
        Args:
            consolidations: Lista de dicionários com chaves:
                - source_utp
                - target_utp
                - reason
                - details (opcional)
        """
        for cons in consolidations:
            self.add_consolidation(
                source_utp=cons['source_utp'],
                target_utp=cons['target_utp'],
                reason=cons['reason'],
                details=cons.get('details')
            )
    
    def get_consolidations(self) -> List[Dict]:
        """Retorna a lista de consolidações realizadas."""
        return self.log_data["consolidations"]
    
    def get_consolidations_by_reason(self, reason: str) -> List[Dict]:
        """Retorna consolidações filtradas por motivo."""
        return [c for c in self.log_data["consolidations"] if c["reason"] == reason]
    
    def get_summary(self) -> Dict:
        """Retorna um resumo das consolidações."""
        consolidations = self.log_data["consolidations"]
        
        return {
            "total_consolidations": len(consolidations),
            "unique_sources": len(set(c["source_utp"] for c in consolidations)),
            "unique_targets": len(set(c["target_utp"] for c in consolidations)),
            "reasons": self._count_by_reason(consolidations),
            "timestamp_created": self.log_data["timestamp_created"],
            "timestamp_last_update": consolidations[-1]["timestamp"] if consolidations else None
        }
    
    def _count_by_reason(self, consolidations: List[Dict]) -> Dict:
        """Conta consolidações por motivo."""
        counts = {}
        for c in consolidations:
            reason = c["reason"]
            counts[reason] = counts.get(reason, 0) + 1
        return counts
    
    def clear_log(self):
        """Limpa o arquivo de log (criar novo vazio)."""
        self.log_data = {
            "version": "1.0",
            "timestamp_created": datetime.now().isoformat(),
            "consolidations": []
        }
        self.save_log()
    
    def export_as_dataframe(self):
        """Exporta as consolidações como DataFrame pandas."""
        import pandas as pd
        
        consolidations = self.log_data["consolidations"]
        if not consolidations:
            return pd.DataFrame()
        
        return pd.DataFrame([
            {
                "ID": c["id"],
                "UTP Origem": c["source_utp"],
                "UTP Destino": c["target_utp"],
                "Motivo": c["reason"],
                "Data": c["timestamp"][:10],
                "Hora": c["timestamp"][11:19]
            }
            for c in consolidations
        ])

    def save_sede_batch(self, consolidations: List[Dict]):
        """
        Salva um lote de consolidações especificamente no arquivo de resultado de sedes via ConsolidationLoader.
        """
        from src.interface.consolidation_loader import ConsolidationLoader
        
        loader = ConsolidationLoader()
        
        # Criar estrutura de resultado
        mapping = {}
        for cons in consolidations:
            mapping[cons["source_utp"]] = cons["target_utp"]
            
        result_data = {
            "version": "1.0",
            "status": "executed",
            "timestamp_created": datetime.now().isoformat(),
            "timestamp_last_updated": datetime.now().isoformat(),
            "total_consolidations": len(consolidations),
            "utps_mapping": mapping,
            "consolidations": consolidations
        }
        
        loader.save_sede_result(result_data)
