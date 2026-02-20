import logging
import json
import pandas as pd
from pathlib import Path

# Importações modulares
from src.config import FILES, setup_logging
from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator
from src.pipeline.analyzer import ODAnalyzer
from src.pipeline.consolidator import UTPConsolidator
from src.pipeline.mapper import UTPMapGenerator
from src.pipeline.sede_consolidator import SedeConsolidator
from src.pipeline.sede_analyzer import SedeAnalyzer

class GeoValidaManager:
    def sync_graph_utp_seeds_and_df(self):
        """
        Sincroniza o grafo, utp_seeds e o DataFrame de municípios.
        Garante que cada UTP ativa tem um município sede com sede_utp=True,
        e que o DataFrame reflete o grafo.
        """
        # Limpamos flags antigas de sede_utp em todos os municípios
        for node, data in self.graph.hierarchy.nodes(data=True):
            if data.get('type') == 'municipality':
                # Garantir que a flag exista e seja False por default
                self.graph.hierarchy.nodes[node]['sede_utp'] = False

        # Marcar todos os municípios em utp_seeds como sede_utp=True
        for utp_id, mun_id in self.graph.utp_seeds.items():
            if self.graph.hierarchy.has_node(mun_id):
                self.graph.hierarchy.nodes[mun_id]['sede_utp'] = True
        # Atualizar DataFrame
        if hasattr(self, 'municipios_data') and self.municipios_data is not None:
            # Ensure the DataFrame has the sede_utp column
            if 'sede_utp' not in self.municipios_data.columns:
                self.municipios_data['sede_utp'] = False

            for idx, row in self.municipios_data.iterrows():
                cd_mun = row['cd_mun']
                # Obter valor atual do nó no grafo (False se não existir)
                is_sede = self.graph.hierarchy.nodes.get(cd_mun, {}).get('sede_utp', False)
                self.municipios_data.at[idx, 'sede_utp'] = is_sede

                utp_id = self.graph.get_municipality_utp(cd_mun)
                if utp_id != "NAO_ENCONTRADO" and utp_id != "SEM_UTP":
                    self.municipios_data.at[idx, 'utp_id'] = utp_id
        # Remover UTPs "fantasmas" (sem municípios ou sem sede)
        self.graph.cleanup_empty_utps()
        # Log de consistência
        utp_count = len([n for n, d in self.graph.hierarchy.nodes(data=True) if d.get('type') == 'utp'])
        sede_count = sum([self.graph.hierarchy.nodes[n].get('sede_utp', False) for n in self.graph.hierarchy.nodes if self.graph.hierarchy.nodes[n].get('type') == 'municipality'])
        self.logger.info(f"🔄 Sync: {utp_count} UTPs, {sede_count} sedes marcadas no grafo.")
    """
    Orquestrador Principal do GeoValida.
    Gere o estado do Grafo, Análise de Fluxos e Geração de Mapas.
    """
    def __init__(self):
        # Inicializa o Logger centralizado
        setup_logging()
        self.logger = logging.getLogger("GeoValida")
        
        # Componentes Core
        self.graph = TerritorialGraph()
        self.validator = TerritorialValidator(self.graph)
        
        # Componentes de Pipeline
        self.analyzer = ODAnalyzer()
        self.map_generator = UTPMapGenerator(self.graph)
        self.consolidator = UTPConsolidator(self.graph, self.validator)
        
        # New Components for Step 6
        # Pass map_generator to enable distance fallback calculation
        self.sede_analyzer = SedeAnalyzer(map_generator=self.map_generator) 
        
        # Note: We need to inject data into analyzer if we want it to use memory data instead of reloading files
        # But SedeAnalyzer currently loads files. We might want to bridge this.
        # Ideally SedeAnalyzer should accept 'manager' or 'dataframes' but it's designed to load from files.
        # For now, we will let it load, or we can manually stick data into it.
        # The Consolidator needs it.
        self.sede_consolidator = SedeConsolidator(self.graph, self.validator, self.sede_analyzer)
    
    @property
    def gdf(self):
        """Propriedade para acessar o GeoDataFrame do map_generator."""
        return self.map_generator.gdf_complete if self.map_generator else None

    def load_from_initialization_json(self):
        """Carrega dados pre-consolidados do initialization.json e popula o grafo territorial."""
        # Using a reliable way to find 'data/initialization.json' relative to this file
        # src/core/manager.py -> src/core -> src -> root -> data/initialization.json
        json_path = Path(__file__).parent.parent.parent / "data" / "initialization.json"
        
        if not json_path.exists():
            self.logger.warning(f"Arquivo {json_path} não encontrado. Usando carregamento tradicional.")
            return False
        
        try:
            self.logger.info(f"Carregando dados pré-consolidados de {json_path}...")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extrair municipios e UTPs
            municipios = data.get('municipios', [])
            utps = data.get('utps', [])
            metadata = data.get('metadata', {})
            
            self.logger.info(f"  ✓ {len(municipios)} municipios carregados")
            self.logger.info(f"  ✓ {len(utps)} UTPs carregadas")
            
            # Converter para dataframes para compatibilidade
            df_municipios = pd.DataFrame(municipios)
            df_utps = pd.DataFrame(utps)
            
            # --- POPULAR O GRAFO TERRITORIAL ---
            self.logger.info("Populando grafo territorial...")
            
            # --- FILTRAR ÁREAS OPERACIONAIS DO IBGE ---
            # IBGE codes 4300001 (Lagoa Mirim) and 4300002 (Lagoa dos Patos)
            # are operational areas (lakes), not real municipalities.
            # They should not be part of the territorial hierarchy.
            OPERATIONAL_AREAS = {4300001, 4300002}
            
            filtered_municipios = [m for m in municipios if m['cd_mun'] not in OPERATIONAL_AREAS]
            filtered_count = len(municipios) - len(filtered_municipios)
            
            if filtered_count > 0:
                self.logger.info(f"  🌊 Filtered out {filtered_count} operational areas (lakes): {OPERATIONAL_AREAS}")
            
            # 1. Criar dicionário de municípios para lookup rápido
            mun_dict = {m['cd_mun']: m for m in filtered_municipios}
            
            # 2. Iterar sobre cada município e criar a hierarquia
            for mun in filtered_municipios:
                cd_mun = int(mun['cd_mun'])
                nm_mun = mun.get('nm_mun', str(cd_mun))
                utp_id = str(mun.get('utp_id', 'SEM_UTP'))
                rm_name = mun.get('regiao_metropolitana', '')
                
                # Define RM (usa SEM_RM se vazio)
                if not rm_name or rm_name.strip() == '':
                    rm_name = "SEM_RM"
                
                # Cria nó da RM se não existir
                rm_node = f"RM_{rm_name}"
                if not self.graph.hierarchy.has_node(rm_node):
                    self.graph.hierarchy.add_node(rm_node, type='rm', name=rm_name)
                    self.graph.hierarchy.add_edge(self.graph.root, rm_node)
                
                # Cria nó da UTP se não existir
                utp_node = f"UTP_{utp_id}"
                if not self.graph.hierarchy.has_node(utp_node):
                    self.graph.hierarchy.add_node(utp_node, type='utp', utp_id=utp_id)
                    self.graph.hierarchy.add_edge(rm_node, utp_node)
                
                # Cria nó do município
                self.graph.hierarchy.add_node(cd_mun, type='municipality', name=nm_mun)
                self.graph.hierarchy.add_edge(utp_node, cd_mun)
                
                # Registra sede e REGIC se for sede
                if mun.get('sede_utp'):
                    self.graph.utp_seeds[utp_id] = cd_mun
                    regic = mun.get('regic', '')
                    if regic:
                        self.graph.mun_regic[cd_mun] = regic
            
            self.logger.info(f"  ✓ Grafo populado: {len(self.graph.hierarchy.nodes)} nós")
            
            # Carregar nos componentes (usando dados filtrados)
            df_municipios_filtered = pd.DataFrame(filtered_municipios)
            self.analyzer.full_flow_df = df_municipios_filtered
            
            # Armazenar dados na memoria (sem áreas operacionais)
            self.municipios_data = df_municipios_filtered
            self.utps_data = df_utps
            self.metadata = metadata
            
            self.logger.info("✓ Dados de initialization.json carregados com sucesso!")
            return True
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Erro ao fazer parsing do JSON: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Erro ao carregar initialization.json: {type(e).__name__}: {e}")
            return False

    def step_0_initialize_data(self):
        """Carrega as bases de dados e sincroniza o Grafo."""
        self.logger.info("Etapa 0: Carregando Bases de Dados...")
        
        # Carrega dados do JSON (que já população o grafo)
        if not self.load_from_initialization_json():
            self.logger.warning("Falha ao carregar de initialization.json, tentando fallback...")
            # Fallback: carregamento tradicional
            try:
                # Define operational areas to filter (lakes, not real municipalities)
                OPERATIONAL_AREAS = {4300001, 4300002}
                
                self.logger.info(f"Carregando UTP base de {FILES['utp_base']}...")
                if str(FILES['utp_base']).endswith('.xlsx'):
                    df_utp = pd.read_excel(FILES['utp_base'], dtype=str)
                else:
                    df_utp = pd.read_csv(FILES['utp_base'], sep=',', encoding='latin1', on_bad_lines='skip', engine='python', dtype=str)
                
                # Filter operational areas from UTP base
                if 'CD_MUN' in df_utp.columns:
                    original_count = len(df_utp)
                    df_utp['CD_MUN_INT'] = df_utp['CD_MUN'].astype(str).str.split('.').str[0].astype(int)
                    df_utp = df_utp[~df_utp['CD_MUN_INT'].isin(OPERATIONAL_AREAS)]
                    df_utp = df_utp.drop(columns=['CD_MUN_INT'])
                    filtered_count = original_count - len(df_utp)
                    if filtered_count > 0:
                        self.logger.info(f"  Filtered out {filtered_count} operational areas (lakes)")
                
                self.logger.info(f"  ✓ UTP: {len(df_utp)} linhas carregadas")
                
                self.logger.info(f"Carregando SEDE+REGIC de {FILES['sede_regic']}...")
                if str(FILES['sede_regic']).endswith('.xlsx'):
                    df_regic = pd.read_excel(FILES['sede_regic'], dtype=str)
                else:
                    df_regic = pd.read_csv(FILES['sede_regic'], sep=',', encoding='latin1', on_bad_lines='skip', engine='python', dtype=str)
                self.logger.info(f"  ✓ REGIC: {len(df_regic)} linhas carregadas")
                
                # Popula o Grafo
                self.graph.load_from_dataframe(df_utp, df_regic)

            except Exception as e:
                self.logger.error(f"Arquivo não encontrado: {e}")
                self.logger.error(f"Verifique se os arquivos estão em data/01_raw/")
                return False
        
        # --- CARREGAR COMPOSIÇÃO DE RMs (NOVO - MOVIDO PARA FORA DO BLOCO CONDICIONAL) ---
        self.logger.info(f"Carregando Composição de RMs de {FILES['rm_composition']}...")
        try:
            df_rm = pd.read_excel(FILES['rm_composition'], dtype={'COD_MUN': str})
            # Filtrar colunas relevantes
            if 'COD_MUN' in df_rm.columns and 'NOME_RECMETROPOL' in df_rm.columns:
                rm_mapping = df_rm.set_index('COD_MUN')['NOME_RECMETROPOL'].to_dict()
                
                count_updates = 0
                # Atualizar info de RM no grafo (se o nó do município existir)
                for mun_node in self.graph.hierarchy.nodes():
                    if self.graph.hierarchy.nodes[mun_node].get('type') == 'municipality':
                        # tenta obter RM do mapping
                        rm_name = rm_mapping.get(str(mun_node))
                        if rm_name:
                            # Atualiza atributo no nó
                            self.graph.hierarchy.nodes[mun_node]['regiao_metropolitana'] = rm_name
                            count_updates += 1
                
                self.logger.info(f"  ✓ RM Composição: {len(df_rm)} linhas. {count_updates} municípios atualizados no grafo.")
                
            else:
                self.logger.warning("  ⚠️ Arquivo de RM não possui colunas 'COD_MUN' e 'NOME_RECMETROPOL'.")

        except Exception as e:
            self.logger.error(f"  ❌ Erro ao carregar Composição de RM: {e}")
            # Não pára o processo, apenas loga o erro

        # Sempre carregar shapefiles (independente da fonte de dados)
        self.logger.info(f"Carregando shapefiles...")
        try:
            self.map_generator.load_shapefiles()
            self.logger.info(f"  ✓ Shapefiles carregados: {len(self.map_generator.gdf_complete)} geometrias")
        except Exception as e:
            self.logger.error(f"  ❌ Erro ao carregar shapefiles: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise
        
        self.logger.info(f"Dados carregados. Grafo: {len(self.graph.hierarchy.nodes)} nós.")
        return True

    def step_1_generate_initial_map(self):
        """Gera o mapa da situação atual das UTPs."""
        self.logger.info("Etapa 1: Gerando Mapa Inicial...")
        (self.map_generator
            .sync_with_graph(self.graph)
            .save_map(FILES['mapa_01'], title="Situação Inicial das UTPs"))
        
        # Gera também o mapa de RMs
        self.map_generator.save_rm_map(FILES['mapa_rm'])
        
        # Export Snapshot Step 1 (Initial State)
        snapshot_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "snapshot_step1_initial.json"
        
        try:
            # FIX: Ensure coloring is computed and attached to the GDF before export
            gdf = self.map_generator.gdf_complete
            if gdf is not None and not gdf.empty:
                 coloring = self.graph.compute_graph_coloring(gdf)
                 # Map coloring back to GDF column so export_snapshot picks it up
                 gdf['COLOR_ID'] = gdf['CD_MUN'].astype(int).map(coloring).fillna(0).astype(int)
            
            self.graph.export_snapshot(snapshot_path, "Initial State", self.map_generator.gdf_complete)
        except Exception as e:
             self.logger.warning(f"⚠️ Failed to export Step 1 snapshot: {e}")

    def step_2_analyze_flows(self):
        """Analisa a Matriz OD para encontrar dependências funcionais."""
        self.logger.info("Etapa 2: Analisando Fluxos de Viagens...")
        self.analyzer.run_full_analysis()
        return self.analyzer.full_flow_df

    def step_5_consolidate_functional(self):
        """Une UTPs baseadas estritamente em Fluxos (Passo 5)."""
        self.logger.info("Etapa 5: Consolidação Funcional (Fluxos)...")
        changes = self.consolidator.run_functional_merging(
            self.analyzer.full_flow_df, 
            self.map_generator.gdf_complete,
            self.map_generator
        )
        
        # Gera o mapa intermédio
        (self.map_generator
            .sync_with_graph(self.graph)
            .save_map(FILES['mapa_05'], title="Pós-Consolidação Funcional (Fluxos)"))
        
        return changes

    def step_7_territorial_cleanup(self):
        """Resolve UTPs unitárias usando REGIC + Adjacência (Passo 7)."""
        self.logger.info("Etapa 7: Limpeza Territorial Final (REGIC + Geografia)...")
        
        # Sincroniza antes de executar
        self.map_generator.sync_with_graph(self.graph)
        
        # Executa a lógica corrigida (EPSG:5880 + Boundary Length)
        changes = self.consolidator.run_territorial_regic(
            self.map_generator.gdf_complete, 
            self.map_generator
        )
        
        # Gera o mapa final absoluto
        (self.map_generator
            .sync_with_graph(self.graph)
            .save_map(FILES['mapa_final'], title="Resultado Final: Mapa Estruturado"))
            
        # Export Snapshot Step 5+7 (Post-Unitary)
        snapshot_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "snapshot_step5_post_unitary.json"
        
        try:
            # FIX: Compute coloring for snapshot
            gdf = self.map_generator.gdf_complete
            if gdf is not None and not gdf.empty:
                # Ensure UTP_ID column exists or copy from utp_id
                if 'utp_id' in gdf.columns and 'UTP_ID' not in gdf.columns:
                    gdf['UTP_ID'] = gdf['utp_id']
                
                coloring = self.graph.compute_graph_coloring(gdf)
                gdf['COLOR_ID'] = gdf['CD_MUN'].astype(int).map(coloring).fillna(0).astype(int)
                
            self.graph.export_snapshot(snapshot_path, "Post-Unitary Consolidation", self.map_generator.gdf_complete)
        except Exception as e:
             self.logger.warning(f"⚠️ Failed to export Step 5+7 snapshot: {e}")
        
        return changes

    def step_6_consolidate_sedes(self):
        """Consolida Sedes com base em dependência e pontuação (Passo 6)."""
        self.logger.info("Etapa 6: Consolidação de Sedes...")
        
        # 1. Ensure SedeAnalyzer has data
        if self.sede_analyzer.df_municipios is None:
            if hasattr(self, 'municipios_data') and self.municipios_data is not None:
                self.logger.info("Injecting data into SedeAnalyzer from Manager...")
                self.sede_analyzer.df_municipios = self.municipios_data.copy()
            else:
                self.logger.info("Loading data for SedeAnalyzer specificially...")
                self.sede_analyzer.load_initialization_data()
        
        # 2. Sync UTP IDs from Graph (Vital because Step 5 or 7 might have changed UTPs)
        # Ensure we are using the LATEST graph state
        # CRITICAL FIX: Also sync sede_utp flag!
        if self.sede_analyzer.df_municipios is not None:
             count_synced = 0
             for idx, row in self.sede_analyzer.df_municipios.iterrows():
                 cd_mun = row['cd_mun']
                 current_utp = self.graph.get_municipality_utp(cd_mun)
                 if current_utp != "NAO_ENCONTRADO" and current_utp != "SEM_UTP":
                     self.sede_analyzer.df_municipios.at[idx, 'utp_id'] = current_utp
                     
                     # CRITICAL FIX: Sync sede_utp flag from graph
                     # Without this, calculate_socioeconomic_metrics() finds ZERO sedes!
                     is_sede = self.graph.hierarchy.nodes.get(cd_mun, {}).get('sede_utp', False)
                     self.sede_analyzer.df_municipios.at[idx, 'sede_utp'] = is_sede
                     
                     count_synced += 1
             self.logger.info(f"Synced {count_synced} UTPs from Graph to Analyzer DataFrame.")
        
        # Load impedance if not loaded
        if self.sede_analyzer.df_impedance is None:
            self.sede_analyzer.load_impedance_data()
            
        changes = self.sede_consolidator.run_sede_consolidation(
            self.analyzer.full_flow_df, 
            self.map_generator.gdf_complete,
            self.map_generator
        )
        # Sincronizar grafo, utp_seeds e DataFrame após consolidação
        self.sync_graph_utp_seeds_and_df()
        # Save map after Step 6
        (self.map_generator
            .sync_with_graph(self.graph)
            .save_map(FILES['mapa_05'].parent / "mapa_06_sedes.png", title="Pós-Consolidação de Sedes"))
        # DEBUG: Log UTP count after Step 6
        if self.map_generator.gdf_complete is not None:
            utp_col = 'UTP_ID' if 'UTP_ID' in self.map_generator.gdf_complete.columns else 'utp_id'
            gdf_utps = self.map_generator.gdf_complete[utp_col].nunique() if utp_col in self.map_generator.gdf_complete.columns else 0
        else:
            gdf_utps = 0
        graph_utps = len([n for n, d in self.graph.hierarchy.nodes(data=True) if d.get('type') == 'utp'])
        self.logger.info(f"   🔍 DEBUG Post-Step6 State:")
        self.logger.info(f"      GDF has {gdf_utps} unique UTPs")
        self.logger.info(f"      Graph has {graph_utps} UTP nodes")
        return changes

    def step_8_border_validation(self):
        """
        Step 8: Border Validation and Refinement
        
        Validates UTP borders by analyzing border municipalities' main flow.
        Reallocates municipalities when flow points to a different UTP
        and RM rules are respected. Runs iteratively until convergence.
        
        Then resolves isolated municipalities (Step 8.5) before generating final outputs.
        """
        self.logger.info("Etapa 8: Validação de Fronteiras + Resolução de Isolados...")
        
        from src.pipeline.border_validator_v2 import BorderValidatorV2
        from src.pipeline.isolated_municipality_resolver import IsolatedMunicipalityResolver
        
        # ===== STEP 8.1: Border Validation V2 =====
        self.logger.info("\n--- Step 8.1: Border Validation V2 (Sede-Centric) ---")
        
        # Ensure impedance is loaded (reusing from SedeAnalyzer or loading fresh)
        if self.sede_analyzer.df_impedance is None:
            self.sede_analyzer.load_impedance_data()
            
        # Create validator V2 instance
        validator_instance = BorderValidatorV2(
            graph=self.graph,
            validator=self.validator,
            impedance_df=self.sede_analyzer.df_impedance
        )
        
        # Sincronizar grafo, utp_seeds e DataFrame antes da validação de fronteiras
        self.sync_graph_utp_seeds_and_df()
        # Sync map with current graph state BEFORE border validation
        self.map_generator.sync_with_graph(self.graph)
        
        # DEBUG: Log UTP count to verify sync
        if self.map_generator.gdf_complete is not None:
            # Check which column name exists
            utp_col = 'UTP_ID' if 'UTP_ID' in self.map_generator.gdf_complete.columns else 'utp_id'
            gdf_utps = self.map_generator.gdf_complete[utp_col].nunique() if utp_col in self.map_generator.gdf_complete.columns else 0
        else:
            gdf_utps = 0
        graph_utps = len([n for n, d in self.graph.hierarchy.nodes(data=True) if d.get('type') == 'utp'])
        self.logger.info(f"   🔍 DEBUG Sync Check:")
        self.logger.info(f"      GDF has {gdf_utps} unique UTPs")
        self.logger.info(f"      Graph has {graph_utps} UTP nodes")
        if gdf_utps != graph_utps:
            self.logger.warning(f"      ⚠️ MISMATCH: GDF and Graph UTP counts don't match!")
        
        # Run border validation
        changes_border = validator_instance.run_border_validation(
            flow_df=self.analyzer.full_flow_df,
            gdf=self.map_generator.gdf_complete,
            max_iterations=100
        )
        
        self.logger.info(f"✅ Border Validation complete: {changes_border} changes")
        
        # ===== STEP 8.5: Isolated Municipality Resolution =====
        self.logger.info("\n--- Step 8.5: Isolated Municipality Resolution ---")
        
        # Create resolver instance
        resolver = IsolatedMunicipalityResolver(
            graph=self.graph,
            validator=self.validator,
            consolidation_manager=self.consolidator.consolidation_manager if hasattr(self.consolidator, 'consolidation_manager') else None
        )
        
        # Sync map with current graph state
        self.map_generator.sync_with_graph(self.graph)
        
        # Run isolated resolution
        changes_isolated = resolver.run_isolated_resolution(
            flow_df=self.analyzer.full_flow_df,
            gdf=self.map_generator.gdf_complete,
            map_gen=self.map_generator
        )
        
        self.logger.info(f"✅ Isolated Resolution complete: {changes_isolated} changes")
        
        # ===== FINAL OUTPUTS (after both sub-steps) =====
        total_changes = changes_border + changes_isolated
        self.logger.info(f"\n✅ Step 8 COMPLETE: {total_changes} total changes ({changes_border} border + {changes_isolated} isolated)")
        
        # Save final map after BOTH border validation AND isolated resolution
        (self.map_generator
            .sync_with_graph(self.graph)
            .save_map(FILES['mapa_05'].parent / "mapa_08_final.png", title="Pós-Validação de Fronteiras + Resolução de Isolados"))
            
        # Export Final Snapshot Step 8 (includes both border validation and isolated resolution)
        snapshot_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "snapshot_step8_final.json"

        try:
            # Prefer exporting the snapshot based on the CURRENT graph state so Step 8 changes
            # (border validation + isolated resolution) are preserved in the final snapshot.
            # Sync map with graph to ensure geometries/attributes are up-to-date.
            self.map_generator.sync_with_graph(self.graph)
            # Ensure coloring is computed and attached to the GDF before export
            gdf = self.map_generator.gdf_complete
            if gdf is not None and not gdf.empty:
                # Ensure UTP_ID column exists
                if 'utp_id' in gdf.columns and 'UTP_ID' not in gdf.columns:
                    gdf['UTP_ID'] = gdf['utp_id']
                try:
                    coloring = self.graph.compute_graph_coloring(gdf)
                    gdf['COLOR_ID'] = gdf['CD_MUN'].astype(int).map(coloring).fillna(0).astype(int)
                except Exception as e_col:
                    self.logger.warning(f"⚠️ Erro ao calcular coloração para snapshot final: {e_col}")

            self.graph.export_snapshot(snapshot_path, "Border Validation + Isolated Resolution", self.map_generator.gdf_complete)
            self.logger.info(f"✅ Final snapshot exported from current graph: {snapshot_path}")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to export final snapshot from current graph: {e}")
            # Fallback: try to generate final snapshot by copying Step 6 snapshot (legacy behaviour)
            try:
                step6_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "snapshot_step6_sede_consolidation.json"
                if step6_path.exists():
                    import json
                    with open(step6_path, 'r', encoding='utf-8') as fh:
                        step6_snapshot = json.load(fh)

                    # Update metadata to reflect final snapshot provenance
                    meta = step6_snapshot.get('metadata', {})
                    meta['timestamp'] = __import__('datetime').datetime.now().isoformat()
                    meta['step'] = 'Final (based on Step 6: Sede Consolidation)'
                    step6_snapshot['metadata'] = meta

                    # Write out final snapshot
                    with open(snapshot_path, 'w', encoding='utf-8') as fh:
                        json.dump(step6_snapshot, fh, indent=2, ensure_ascii=False)

                    self.logger.info(f"✅ Final snapshot generated from Step 6 (fallback): {step6_path} -> {snapshot_path}")
                else:
                    # As a last resort, try computing coloring and exporting from the map gdf
                    gdf = self.map_generator.gdf_complete
                    if gdf is not None and not gdf.empty:
                        if 'utp_id' in gdf.columns and 'UTP_ID' not in gdf.columns:
                            gdf['UTP_ID'] = gdf['utp_id']

                        coloring = self.graph.compute_graph_coloring(gdf)
                        gdf['COLOR_ID'] = gdf['CD_MUN'].astype(int).map(coloring).fillna(0).astype(int)

                    self.graph.export_snapshot(snapshot_path, "Border Validation + Isolated Resolution", self.map_generator.gdf_complete)
            except Exception as e2:
                self.logger.warning(f"⚠️ Failed to export Step 8 snapshot (fallback): {e2}")
        
        return total_changes

