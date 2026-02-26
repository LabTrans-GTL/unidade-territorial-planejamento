# src/pipeline/influence_analyzer.py
import logging
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from src.pipeline.analyzer import ODAnalyzer

class InfluenceAnalyzer:
    """
    Analisa a rede de influência entre municípios brasileiros.
    Identifica hierarquias de fluxo (primário, secundário e terciário) para delimitar UTPs.
    """
    
    def __init__(self, data_path: Optional[Path] = None):
        self.logger = logging.getLogger("GeoValida.InfluenceAnalyzer")
        self.data_path = data_path or Path("data")
        self.analyzer = ODAnalyzer()
        
        self.df_flows = None
        self.df_impedance = None
        self.nodes_data = {}
        
        # Resultados
        self.main_destinations = {} # cd_mun -> cd_dest
        self.hierarchies = {} # cd_mun -> 'primaria' | 'secundaria' | 'terciaria'
        self.nuclei = {} # nucleus_id -> set(cd_mun)
        self.mun_to_chain = {} # cd_mun -> chain_id
        
    def load_data(self):
        """Carrega dados necessários para a análise."""
        self.logger.info("Carregando dados para análise de influência...")
        
        # 1. Carregar Fluxos
        self.df_flows = self.analyzer.run_full_analysis()
        
        # 2. Carregar Impedância
        impedance_path = self.data_path / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"
        if impedance_path.exists():
            self.df_impedance = pd.read_csv(impedance_path, sep=';', encoding='latin-1')
            self.df_impedance = self.df_impedance.rename(columns={
                'COD_IBGE_ORIGEM': 'origem',
                'COD_IBGE_DESTINO': 'destino',
                'Tempo': 'tempo_horas',
                'COD_IBGE_ORIGEM_1': 'origem_6',
                'COD_IBGE_DESTINO_1': 'destino_6'
            })
            self.df_impedance['tempo_horas'] = self.df_impedance['tempo_horas'].astype(str).str.replace(',', '.').astype(float)
        else:
            self.logger.warning(f"Matriz de impedância não encontrada em {impedance_path}")
            
        # 3. Carregar Snapshot (para nomes e estados atuais)
        snapshot_path = self.data_path / "03_processed" / "snapshot_step8_final.json"
        if snapshot_path.exists():
            with open(snapshot_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.nodes_data = data.get('nodes', {})
        
    def get_travel_time(self, cd_origem: int, cd_destino: int) -> Optional[float]:
        """Obtém tempo de viagem entre dois municípios (≤ 2h)."""
        if self.df_impedance is None:
            return None
            
        origem_6 = int(cd_origem) // 10
        destino_6 = int(cd_destino) // 10
        
        match = self.df_impedance[
            (self.df_impedance['origem_6'] == origem_6) & 
            (self.df_impedance['destino_6'] == destino_6)
        ]
        
        if not match.empty:
            return float(match.iloc[0]['tempo_horas'])
        return None

    def run_analysis(self):
        """
        Executa a identificação das cadeias de influência.
        Regra: Considera o destino principal DENTRO do limite de 2 horas.
        """
        self.logger.info("Iniciando identificação de hierarquias (fluxo principal ≤ 2h)...")
        
        # 1. Cruzar fluxos com tempos de viagem e filtrar
        # Precisamos converter mun_origem e mun_destino para int e 6 dígitos para o join
        df_flows_temp = self.df_flows.copy()
        df_flows_temp['mun_origem'] = df_flows_temp['mun_origem'].astype(int)
        df_flows_temp['mun_destino'] = df_flows_temp['mun_destino'].astype(int)
        
        # Criar chaves de 6 dígitos para cruzamento com impedância
        df_flows_temp['origem_6'] = df_flows_temp['mun_origem'] // 10
        df_flows_temp['destino_6'] = df_flows_temp['mun_destino'] // 10
        
        # Join com impedância
        if self.df_impedance is not None:
            # Manter apenas viagens com tempo conhecido e <= 2h
            df_merged = df_flows_temp.merge(
                self.df_impedance[['origem_6', 'destino_6', 'tempo_horas']],
                on=['origem_6', 'destino_6'],
                how='inner'
            )
            df_merged = df_merged[df_merged['tempo_horas'] <= 2.0]
        else:
            self.logger.error("Matriz de impedância não carregada. Não é possível aplicar restrição de 2h.")
            return

        # 2. Identificar o destino principal ABSOLUTO (dentro dos 2h) para cada origem
        # Ordenar por origem e viagens descendente ANTES de filtrar por RM
        df_top = df_merged.sort_values(['mun_origem', 'viagens'], ascending=[True, False]).drop_duplicates('mun_origem').copy()
        
        # 3. Aplicar Regras de Região Metropolitana (RM) sobre o destino principal
        def get_rm(cd_mun):
            node = self.nodes_data.get(str(cd_mun), {})
            rm = node.get('regiao_metropolitana', '')
            if not rm or pd.isna(rm) or str(rm).lower() == 'nan' or rm == 'SEM_RM':
                return ''
            return str(rm).strip()

        df_top['rm_origem'] = df_top['mun_origem'].apply(get_rm)
        df_top['rm_destino'] = df_top['mun_destino'].apply(get_rm)
        
        # Filtro de Consistência RM
        df_top['rm_valida'] = df_top.apply(
            lambda x: x['rm_origem'] == x['rm_destino'], 
            axis=1
        )
        
        # Guardar fluxos bloqueados para diagnóstico
        self.blocked_by_rm = df_top[~df_top['rm_valida']].copy()
        
        # Manter apenas fluxos que respeitam RM no destino principal
        df_final = df_top[df_top['rm_valida']]

        for _, row in df_final.iterrows():
            self.main_destinations[int(row['mun_origem'])] = int(row['mun_destino'])
                
        # 4. Identificação de Sedes (Cadeia Primária - Núcleos)
        # Regra: A -> B e B -> A (onde A e B são os principais dentro de 2h)
        visited_pairs = set()
        nucleus_count = 0
        
        for a, b in self.main_destinations.items():
            if b in self.main_destinations and self.main_destinations[b] == a:
                pair = tuple(sorted((a, b)))
                if pair not in visited_pairs:
                    visited_pairs.add(pair)
                    nucleus_id = f"NUCLEO_{nucleus_count}"
                    self.nuclei[nucleus_id] = {a, b}
                    self.hierarchies[a] = 'primaria'
                    self.hierarchies[b] = 'primaria'
                    self.mun_to_chain[a] = nucleus_id
                    self.mun_to_chain[b] = nucleus_id
                    nucleus_count += 1

        # 5. Identificação de Dependentes (Cadeia Secundária)
        nucleus_members = {mun for muns in self.nuclei.values() for mun in muns}
        for mun, dest in self.main_destinations.items():
            if mun in self.hierarchies: continue
            
            if dest in nucleus_members:
                self.hierarchies[mun] = 'secundaria'
                self.mun_to_chain[mun] = self.mun_to_chain.get(dest)
                    
        # 6. Identificação de Satélites (Cadeia Terciária)
        secondary_members = {mun for mun, h in self.hierarchies.items() if h == 'secundaria'}
        for mun, dest in self.main_destinations.items():
            if mun in self.hierarchies: continue
            
            if dest in secondary_members:
                self.hierarchies[mun] = 'terciaria'
                self.mun_to_chain[mun] = self.mun_to_chain.get(dest)
                    
        self.logger.info(f"Análise concluída. Primários: {len(nucleus_members)}, Secundários: {len(secondary_members)}, Terciários: {sum(1 for h in self.hierarchies.values() if h == 'terciaria')}")

    def get_results_df(self) -> pd.DataFrame:
        """Retorna os resultados em formato DataFrame com métricas de UTP e sugestões claras."""
        rows = []
        
        # Pré-processar status das cadeias para sugestões
        chain_utps = {} # chain_id -> set(utp_id)
        for mun_id, chain_id in self.mun_to_chain.items():
            if chain_id not in chain_utps: chain_utps[chain_id] = set()
            utp = self.nodes_data.get(str(mun_id), {}).get('utp_id', 'N/A')
            chain_utps[chain_id].add(utp)

        for mun_id, level in self.hierarchies.items():
            node = self.nodes_data.get(str(mun_id), {})
            dest_id = self.main_destinations.get(mun_id)
            dest_node = self.nodes_data.get(str(dest_id), {})
            chain_id = self.mun_to_chain.get(mun_id, "N/A")
            
            utp_origem = node.get('utp_id', 'N/A')
            utp_destino = dest_node.get('utp_id', 'N/A')
            
            # RM Info
            def get_rm(cd_mun):
                n = self.nodes_data.get(str(cd_mun), {})
                rm = n.get('regiao_metropolitana', '')
                if not rm or pd.isna(rm) or str(rm).lower() == 'nan' or rm == 'SEM_RM':
                    return 'N/A'
                return str(rm).strip()
            
            rm_origem = get_rm(mun_id)
            rm_destino = get_rm(dest_id) if dest_id else 'N/A'
            
            # Determinar sugestão de análise refinada
            sugestao = "Integridade Territorial: Cadeia totalmente contida na mesma UTP."
            
            # Verificar se houve desvio de fluxo por RM
            if not self.blocked_by_rm.empty:
                original_top = self.blocked_by_rm[self.blocked_by_rm['mun_origem'] == mun_id]
                if not original_top.empty:
                    # Se o destino principal absoluto é diferente do escolhido (por causa da RM)
                    abs_dest = original_top.sort_values('viagens', ascending=False).iloc[0]
                    if abs_dest['mun_destino'] != dest_id:
                        sugestao = f"Limite de RM: Fluxo principal redirecionado (Original: {abs_dest['mun_destino']})"

            if level == 'primaria' and not sugestao.startswith("Limite de RM"):
                # Verificar se o núcleo é bipolar cross-UTP
                nucleus_muns = self.nuclei.get(chain_id, set())
                nucleus_utps = {self.nodes_data.get(str(m), {}).get('utp_id') for m in nucleus_muns}
                if len(nucleus_utps) > 1:
                    sugestao = "Conflito Estrutural: O núcleo da cadeia está dividido entre UTPs distintas."
                elif len(chain_utps.get(chain_id, set())) > 1:
                    sugestao = "Expansão de Influência: Núcleo coeso, mas a influência transborda para outras UTPs."
            elif not sugestao.startswith("Limite de RM"):
                if utp_origem != utp_destino and utp_destino != 'N/A':
                    sugestao = "Desvio de Fluxo: Município pertence a uma UTP, mas seu vínculo principal é com um polo externo."
                elif utp_origem == utp_destino and len(chain_utps.get(chain_id, set())) > 1:
                    sugestao = "Vínculo Híbrido: Município e destino estão na mesma UTP, mas respondem a um núcleo externo."

            rows.append({
                'cd_mun': mun_id,
                'nm_mun': node.get('name', 'N/A'),
                'sede_utp': node.get('sede_utp', False),
                'rm_origem': rm_origem,
                'utp_origem': utp_origem,
                'hierarquia': level,
                'id_cadeia': chain_id,
                'status_cadeia': "Interna" if len(chain_utps.get(chain_id, set())) == 1 else "Mista",
                'cd_destino_principal': dest_id,
                'nm_destino_principal': dest_node.get('name', 'N/A'),
                'rm_destino_principal': rm_destino,
                'utp_destino_principal': utp_destino,
                'tempo_viagem_h': self.get_travel_time(mun_id, dest_id) if dest_id else None,
                'sugestao_analise': sugestao
            })
            
        return pd.DataFrame(rows)

    def get_chain_summary_df(self) -> pd.DataFrame:
        """Gera um resumo consolidado por cadeia de influência."""
        df_full = self.get_results_df()
        
        summary_rows = []
        for chain_id, group in df_full.groupby('id_cadeia'):
            nucleus_group = group[group['hierarquia'] == 'primaria']
            nucleus_names = ", ".join(nucleus_group['nm_mun'].tolist())
            utps_involved = sorted(list(group['utp_origem'].unique()))
            
            # UTP Dominante
            utp_counts = group['utp_origem'].value_counts()
            utp_dominante = utp_counts.idxmax()
            coesao = (utp_counts.max() / len(group)) * 100
            
            # Diagnóstico Geral da Cadeia
            if len(utps_involved) == 1:
                diagnostico = "Cadeia Coesa (100% interna)"
            elif len(nucleus_group['utp_origem'].unique()) > 1:
                diagnostico = f"Cadeia Crítica (Núcleo Cross-UTP: {', '.join(nucleus_group['utp_origem'].unique())})"
            else:
                diagnostico = f"Cadeia Mista (Dominante: UTP {utp_dominante})"
                
            summary_rows.append({
                'id_cadeia': chain_id,
                'nucleo': nucleus_names,
                'qtd_municipios': len(group),
                'qtd_sedes': int(group['sede_utp'].sum()),
                'tem_sede': "Sim" if group['sede_utp'].any() else "Não",
                'utp_dominante': utp_dominante,
                'coesao_utp_perc': round(coesao, 1),
                'utps_envolvidas': ", ".join(utps_involved),
                'diagnostico_geral': diagnostico
            })
            
        return pd.DataFrame(summary_rows)

    def export_results(self, output_path: Path):
        """Exporta os resultados completos e o resumo das cadeias."""
        # Exportar completo
        df = self.get_results_df()
        df.to_csv(output_path, index=False, encoding='utf-8-sig', sep=';')
        
        # Exportar resumo
        summary_path = output_path.parent / output_path.name.replace('analise_hierarquia', 'resumo_cadeias')
        df_summary = self.get_chain_summary_df()
        df_summary.to_csv(summary_path, index=False, encoding='utf-8-sig', sep=';')
        
        self.logger.info(f"Resultados exportados para {output_path} e {summary_path}")
