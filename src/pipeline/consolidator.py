# src/pipeline/consolidator.py
import logging
import pandas as pd
import geopandas as gpd
from src.core.validator import TerritorialValidator
from src.interface.consolidation_manager import ConsolidationManager
from typing import Any


class UTPConsolidator:
    def __init__(self, graph, validator: TerritorialValidator):
        self.graph = graph
        self.validator = validator
        self.logger = logging.getLogger("GeoValida.Consolidator")
        self.consolidation_manager = ConsolidationManager()

    def run_functional_merging(self, flow_df: pd.DataFrame, gdf: gpd.GeoDataFrame, map_gen: Any, clear_log: bool = True) -> int:
        """
        Passo 5: Consolidação recursiva de UTPs unitárias com fluxo e adjacência geográfica.
        Prioriza UTPs "Sem RM" com lógica de busca em largura por fluxo total de UTP.
        """
        self.logger.info("Passo 5: Iniciando consolidação funcional recursiva...")
        
        # Reload manager (Step 5 is the first consolidation step, so we clear LOG here)
        self.consolidation_manager = ConsolidationManager()
        if clear_log:
            self.consolidation_manager.clear_log()
            self.logger.info("Log de consolidação limpo para nova execução.")
        else:
            self.logger.info("Reciclando log de consolidação existente.")
        
        if flow_df is None or flow_df.empty:
            self.logger.info("Sem dados de fluxo para consolidação funcional.")
            return 0
        
        if gdf is None or gdf.empty:
            self.logger.info("Sem dados geográficos para consolidação funcional.")
            return 0
        
        # Contagem inicial de UTPs unitárias para estatísticas
        utps_unitarias_inicial = len(self._get_unitary_utps())
        utp_nodes = [n for n, d in self.graph.hierarchy.nodes(data=True) if d.get('type') == 'utp']
        utps_unitarias_com_rm = len([n for n in utp_nodes 
                                      if len(list(self.graph.hierarchy.successors(n))) == 1 
                                      and not self.validator.is_non_rm_utp(n.replace("UTP_", ""))])
        
        self.logger.info(f"Estado Inicial: {utps_unitarias_com_rm} UTPs unitárias Com RM, {utps_unitarias_inicial} Sem RM")
        
        # Etapa 1: Consolidação de UTPs Com RM (Com Restrição)
        self.logger.info("--- Etapa 5.1: Consolidando UTPs unitárias COM RM ---")
        changes_com_rm = self._consolidate_with_rm(flow_df, gdf, map_gen)
        
        # Etapa 2: Consolidação recursiva de UTPs Sem RM (Sem Restrição)
        self.logger.info("--- Etapa 5.2: Consolidando UTPs unitárias SEM RM (Recursivo) ---")
        changes_sem_rm = self._consolidate_without_rm_recursive(flow_df, gdf, map_gen)
        
        total_changes = changes_com_rm + changes_sem_rm
        self.logger.info(f"Passo 5 concluído: {total_changes} consolidações realizadas.")
        return total_changes


    def _consolidate_with_rm(self, flow_df: pd.DataFrame, gdf: gpd.GeoDataFrame, map_gen: Any) -> int:
        """Consolida UTPs unitárias que pertencem a alguma RM.
        
        Implementa desempate por maior fluxo total quando há múltiplas UTPs candidatas.
        """
        changes = 0
        
        # Identifica UTPs unitárias Com RM
        utp_nodes = [n for n, d in self.graph.hierarchy.nodes(data=True) if d.get('type') == 'utp']
        
        for utp_node in utp_nodes:
            filhos = list(self.graph.hierarchy.successors(utp_node))
            if len(filhos) != 1:
                continue
            
            mun_id = filhos[0]
            utp_origem = utp_node.replace("UTP_", "")
            
            # Só processa UTPs Com RM nesta etapa
            if self.validator.is_non_rm_utp(utp_origem):
                continue
            
            # Busca fluxos deste município
            fluxos_mun = flow_df[flow_df['mun_origem'].astype(int) == int(mun_id)]
            if fluxos_mun.empty:
                self.logger.debug(f"Mun {mun_id} (UTP {utp_origem}): Sem dados de fluxo.")
                continue
            
            # Pega RM de origem para validação
            rm_origem = self.validator.get_rm_of_utp(utp_origem)
            
            # Buscar vizinhos geográficos (UTPs adjacentes)
            vizinhos = self.validator.get_neighboring_utps(mun_id, gdf)
            
            # Filtra candidatos: Com RM, mesma RM, e diferente da origem
            candidates = []
            for v_id in vizinhos:
                if v_id == utp_origem:
                    continue
                if self.validator.is_non_rm_utp(v_id):
                    continue
                
                rm_destino = self.validator.get_rm_of_utp(v_id)
                if rm_origem != rm_destino:
                    continue
                
                candidates.append(v_id)
            
            if not candidates:
                self.logger.debug(f"Mun {mun_id} (UTP {utp_origem}): Sem candidatos válidos.")
                continue
            
            # DESEMPATE: Avaliar fluxo total para cada UTP candidata
            best_target, max_flow, best_mun_destino = None, -1, None
            
            for v_id in candidates:
                # Somar fluxo para TODOS os municípios da UTP alvo
                muns_target = list(self.graph.hierarchy.successors(f"UTP_{v_id}"))
                
                fluxos_para_utp = fluxos_mun[
                    fluxos_mun['mun_destino'].astype(int).isin([int(m) for m in muns_target])
                ]
                
                flow = fluxos_para_utp['viagens'].sum()
                
                if flow > max_flow:
                    max_flow = flow
                    best_target = v_id
                    # Pega o município principal (maior fluxo individual) para logging
                    if not fluxos_para_utp.empty:
                        best_mun_destino = int(fluxos_para_utp.nlargest(1, 'viagens').iloc[0]['mun_destino'])
            
            # Consolidar para o melhor alvo
            if best_target and max_flow > 0:
                nm_mun = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', str(mun_id))
                self.logger.info(f"✅ MOVENDO (Com RM): {nm_mun} ({mun_id}) -> UTP {best_target} (Fluxo Total: {max_flow:.0f})")
                self.graph.move_municipality(mun_id, best_target)
                
                # Registrar consolidação com detalhes completos
                self.consolidation_manager.add_consolidation(
                    source_utp=utp_origem,
                    target_utp=best_target,
                    reason="Com RM - Fluxo Principal",
                    details={
                        "mun_id": mun_id, 
                        "nm_mun": nm_mun,
                        "mun_destino": best_mun_destino,
                        "viagens": max_flow,
                        "rm": rm_origem
                    },
                    auto_save=False
                )
                changes += 1
            else:
                self.logger.debug(f"Mun {mun_id}: Fluxo zero para todos os candidatos.")
        
        self.consolidation_manager.save_log()
        return changes

    def _consolidate_without_rm_recursive(self, flow_df: pd.DataFrame, gdf: gpd.GeoDataFrame, map_gen: Any) -> int:
        """
        Consolida recursivamente UTPs unitárias Sem RM usando BFS com fluxo total de UTP.
        Até que não haja mais UTPs unitárias Sem RM.
        """
        total_changes = 0
        iteration = 1
        
        while True:
            # Identifica UTPs unitárias no estado ATUAL do grafo
            unitarias_sem_rm = self._get_unitary_utps()
            
            self.logger.info(f"--- Iteração {iteration} | Unitárias Sem RM: {len(unitarias_sem_rm)} ---")
            
            if not unitarias_sem_rm:
                self.logger.info("Sucesso: Nenhuma UTP unitária Sem RM restante.")
                break
            
            # Sincroniza mapa para ver as fronteiras atualizadas
            if hasattr(map_gen, 'sync_with_graph'):
                map_gen.sync_with_graph(self.graph)
            
            changes_in_iteration = 0
            
            for utp_id in unitarias_sem_rm:
                # Re-verificar se ainda é unitária (pode ter recebido municípios nesta iteração)
                filhos = list(self.graph.hierarchy.successors(f"UTP_{utp_id}"))
                if len(filhos) != 1:
                    continue
                
                mun_id = filhos[0]
                nm_mun = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', str(mun_id))
                
                # Busca vizinhos geográficos
                todos_vizinhos = self.validator.get_neighboring_utps(mun_id, gdf)
                
                # Filtra para candidatos Sem RM
                candidates = [v for v in todos_vizinhos if v != utp_id and self.validator.is_non_rm_utp(v)]
                
                self.logger.info(f"Analisando: {nm_mun} ({mun_id}) na UTP {utp_id}")
                
                if not todos_vizinhos:
                    self.logger.warning(f"  [REJEITADO]: Isolado geograficamente.")
                    continue
                
                if not candidates:
                    rm_vizinhos = [v for v in todos_vizinhos if not self.validator.is_non_rm_utp(v)]
                    self.logger.warning(f"  [REJEITADO]: Vizinhos {rm_vizinhos} são Com RM (filtrado).")
                    continue
                
                # Busca o alvo com maior fluxo total para UTP
                best_target, max_flow = None, -1
                
                self.logger.info(f"  Candidatos Sem RM: {candidates}")
                
                for v_id in candidates:
                    # Soma fluxo para TODOS os municípios da UTP alvo
                    muns_target = list(self.graph.hierarchy.successors(f"UTP_{v_id}"))
                    
                    flow = flow_df[
                        (flow_df['mun_origem'].astype(int) == int(mun_id)) & 
                        (flow_df['mun_destino'].astype(int).isin([int(m) for m in muns_target]))
                    ]['viagens'].sum()
                    
                    self.logger.info(f"    -> Fluxo para UTP {v_id}: {flow:.2f} viagens")
                    
                    if flow > max_flow:
                        max_flow, best_target = flow, v_id
                
                
                # DECISION POINT: Flow-based vs REGIC-based consolidation
                if best_target and max_flow > 0:
                    # PRIMARY PATH: Consolidate based on flow
                    self.logger.info(f"✅ MOVENDO (Sem RM): {nm_mun} -> UTP {best_target} (Fluxo: {max_flow:.2f})")
                    self.graph.move_municipality(mun_id, best_target)
                    
                    self.consolidation_manager.add_consolidation(
                        source_utp=utp_id,
                        target_utp=best_target,
                        reason="Sem RM - Fluxo Total BFS",
                        details={"mun_id": mun_id, "nm_mun": nm_mun, "flow": max_flow},
                        auto_save=False
                    )
                    changes_in_iteration += 1
                    total_changes += 1
                elif candidates:
                    # FALLBACK PATH: Zero flow -> Use REGIC hierarchy
                    self.logger.info(f"  [ZERO FLOW] Usando critério REGIC para {nm_mun}...")
                    
                    # Score all candidates by REGIC
                    scored_candidates = []
                    for v_id in candidates:
                        regic_score = self.validator.get_utp_regic_score(v_id)
                        boundary_len = self.validator.get_shared_boundary_length(mun_id, v_id, gdf)
                        
                        scored_candidates.append({
                            'utp_id': v_id,
                            'regic': regic_score,
                            'boundary': boundary_len
                        })
                        self.logger.info(f"    -> UTP {v_id}: REGIC={regic_score}, Fronteira={boundary_len:.0f}m")
                    
                    # Sort by: Best REGIC (lowest) > Largest Boundary
                    scored_candidates.sort(key=lambda x: (x['regic'], -x['boundary']))
                    best = scored_candidates[0]
                    
                    self.logger.info(f"✅ MOVENDO (Sem RM): {nm_mun} -> UTP {best['utp_id']} (REGIC Fallback: Rank={best['regic']})")
                    self.graph.move_municipality(mun_id, best['utp_id'])
                    
                    self.consolidation_manager.add_consolidation(
                        source_utp=utp_id,
                        target_utp=best['utp_id'],
                        reason="Sem RM - REGIC Fallback",
                        details={"mun_id": mun_id, "nm_mun": nm_mun, "regic_rank": best['regic']},
                        auto_save=False
                    )
                    changes_in_iteration += 1
                    total_changes += 1
                else:
                    self.logger.warning(f"  [REJEITADO]: Sem candidatos válidos.")

            
            
            if changes_in_iteration == 0:
                self.logger.info("Fim da iteração recursiva: sem mudanças aplicáveis.")
                break
            
            self.consolidation_manager.save_log()
            iteration += 1
        
        return total_changes

    def run_territorial_regic(self, gdf: gpd.GeoDataFrame, map_gen: Any) -> int:
        """
        Passo 7: Consolidação de último recurso usando REGIC (Hierarquia Urbana) + 
        Critérios geográficos (Distância + Fronteira Partilhada em EPSG:5880).
        """
        self.logger.info("Passo 7: Iniciando consolidação territorial (REGIC + Geografia)...")
        
        # Reload manager to sync with disk (Step 5 + Step 6 changes)
        self.consolidation_manager = ConsolidationManager()
        
        if gdf is None or gdf.empty:
            self.logger.info("Sem dados geográficos para limpeza territorial.")
            return 0
        
        total_changes = 0
        iteration = 1
        
        while True:
            # Identifica UTPs unitárias restantes
            unitarias_sem_rm = self._get_unitary_utps()
            
            self.logger.info(f"--- Iteração {iteration} | Unitárias restantes: {len(unitarias_sem_rm)} ---")
            
            if not unitarias_sem_rm:
                self.logger.info("Sucesso: Todas as UTPs unitárias foram resolvidas.")
                break
            
            # Sincroniza geometrias para capturar novas fronteiras
            if hasattr(map_gen, 'sync_with_graph'):
                map_gen.sync_with_graph(self.graph)
            
            # Converte para CRS projetado para medições em metros
            gdf_projected = gdf.to_crs(epsg=5880)
            
            changes_in_iteration = 0
            
            for utp_id in unitarias_sem_rm:
                # Re-verificar se ainda é unitária
                muns_origem = list(self.graph.hierarchy.successors(f"UTP_{utp_id}"))
                if len(muns_origem) != 1:
                    continue
                
                mun_id = muns_origem[0]
                nm_mun = self.graph.hierarchy.nodes.get(mun_id, {}).get('name', str(mun_id))
                
                # Busca vizinhos geográficos
                candidates = self.validator.get_neighboring_utps(mun_id, gdf)
                
                # Regra de RM: Deve permanecer na mesma RM (ou ambos sem RM)
                rm_mun = self.validator.get_rm_of_utp(utp_id)
                candidates = [v for v in candidates if v != utp_id and self.validator.get_rm_of_utp(v) == rm_mun]
                
                if not candidates:
                    continue

                scored_candidates = []
                
                # Centroide do município (em metros, EPSG:5880)
                mun_row = gdf_projected[gdf_projected['CD_MUN'] == mun_id]
                if mun_row.empty:
                    continue
                mun_centroid = mun_row.geometry.centroid.values[0]
                
                for v_id in candidates:
                    # Critério 1: Ranking REGIC (hierarquia urbana)
                    sede_v = self.graph.utp_seeds.get(v_id) if hasattr(self.graph, 'utp_seeds') else None
                    regic_rank = self.validator.get_regic_score(sede_v) if sede_v else 999
                    
                    # Critério 2: Distância Euclidiana (em metros)
                    sede_row = gdf_projected[gdf_projected['CD_MUN'] == sede_v] if sede_v else None
                    if sede_row is None or sede_row.empty:
                        dist = float('inf')
                    else:
                        sede_geom = sede_row.geometry.centroid.values[0]
                        dist = mun_centroid.distance(sede_geom)
                    
                    # Critério 3: Comprimento de fronteira partilhada
                    shared_len = self.validator.get_shared_boundary_length(mun_id, v_id, gdf_projected)
                    
                    scored_candidates.append({
                        'utp_id': v_id,
                        'regic': regic_rank,
                        'dist': dist,
                        'boundary': shared_len
                    })
                
                # Ordenação multicritério: Melhor REGIC > Menor Distância > Maior Fronteira
                if scored_candidates:
                    scored_candidates.sort(key=lambda x: (x['regic'], x['dist'], -x['boundary']))
                    best = scored_candidates[0]
                    
                    self.logger.info(f"✅ MOVENDO (REGIC): {nm_mun} -> UTP {best['utp_id']}")
                    self.graph.move_municipality(mun_id, best['utp_id'])
                    
                    # Registrar consolidação
                    self.consolidation_manager.add_consolidation(
                        source_utp=utp_id,
                        target_utp=best['utp_id'],
                        reason="Sem RM - REGIC + Geografia",
                        details={"mun_id": mun_id, "nm_mun": nm_mun, "regic_rank": best['regic']},
                        auto_save=False
                    )
                    
                    # ATUALIZAÇÃO CRÍTICA DO GDF:
                    if gdf is not None and 'UTP_ID' in gdf.columns:
                        mask = gdf['CD_MUN'].astype(str) == str(mun_id)
                        if mask.any():
                            gdf.loc[mask, 'UTP_ID'] = str(best['utp_id'])
                    
                    changes_in_iteration += 1
                    total_changes += 1
            
            if changes_in_iteration == 0:
                break
            
            self.consolidation_manager.save_log()
            iteration += 1
        
        self.logger.info(f"Passo 7 concluído: {total_changes} consolidações realizadas.")
        
        # NOVO: Salvar coloração após Step 7 (unitárias)
        self.logger.info("\n🎨 Gerando coloração pós-consolidação de unitárias...")
        try:
            coloring = self.graph.compute_graph_coloring(gdf)
            
            from pathlib import Path
            import json
            
            data_dir = Path(__file__).parent.parent.parent / "data" / "03_processed"
            data_dir.mkdir(parents=True, exist_ok=True)
            
            coloring_file = data_dir / "post_unitary_coloring.json"
            coloring_str_keys = {str(k): v for k, v in coloring.items()}
            
            with open(coloring_file, 'w') as f:
                json.dump(coloring_str_keys, f, indent=2)
            
            self.logger.info(f"💾 Coloração pós-unitárias salva em: {coloring_file}")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Erro ao salvar coloração pós-unitárias: {e}")
        
        return total_changes

    def _get_unitary_utps(self) -> list:
        """Retorna lista de UTPs unitárias (1 município)."""
        unitarias = []
        
        utp_nodes = [n for n, d in self.graph.hierarchy.nodes(data=True) if d.get('type') == 'utp']
        
        for utp_node in utp_nodes:
            filhos = list(self.graph.hierarchy.successors(utp_node))
            
            # UTP unitária: tem exatamente 1 filho
            if len(filhos) == 1:
                utp_id = utp_node.replace("UTP_", "")
                
                unitarias.append(utp_id)
        
        return unitarias
